use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, trace};

use crate::backend::{Backend, BackendError, Event};
use crate::proto::etcdserverpb::watch_server::Watch;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb;

use super::KvBridge;

/// Global watch ID counter — matches kine's globally unique watch IDs.
pub(crate) static WATCH_ID_COUNTER: AtomicI64 = AtomicI64::new(1);

/// How often to check if a broadcast progress response should be sent.
/// Matches kine's `progressResponsePeriod` (watch.go:19).
const PROGRESS_ALL_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) type WatchResponseStream =
    Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send>>;

fn backend_err_to_status(e: BackendError) -> Status {
    match e {
        BackendError::Compacted => Status::out_of_range(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

/// Map a backend event to the proto event sent to clients.
///
/// `prev_kv` is only attached when the watch was created with the `prev_kv`
/// option — matching etcd (and etcdctl, which prints the previous value only
/// for `--prev-kv` watches). Backends always carry the previous value, so the
/// stripping happens here at the protocol boundary.
pub(crate) fn event_to_proto(e: &Event, want_prev_kv: bool) -> mvccpb::Event {
    let event_type = if e.delete {
        mvccpb::event::EventType::Delete
    } else {
        mvccpb::event::EventType::Put
    };
    mvccpb::Event {
        r#type: event_type.into(),
        kv: Some(mvccpb::KeyValue {
            key: e.kv.key.as_bytes().to_vec(),
            value: e.kv.value.clone(),
            create_revision: e.kv.create_revision,
            mod_revision: e.kv.mod_revision,
            version: e.kv.version,
            lease: e.kv.lease,
        }),
        prev_kv: if want_prev_kv {
            e.prev_kv.as_ref().map(|pk| mvccpb::KeyValue {
                key: pk.key.as_bytes().to_vec(),
                value: pk.value.clone(),
                create_revision: pk.create_revision,
                mod_revision: pk.mod_revision,
                version: pk.version,
                lease: pk.lease,
            })
        } else {
            None
        },
    }
}

/// Shared mutable state for a Watch stream, holding per-watch cancel and progress handles.
pub(crate) struct WatcherState {
    pub(crate) cancels: HashMap<i64, tokio::sync::oneshot::Sender<()>>,
    /// Progress channels for watches created with `progress_notify=true`.
    /// Capacity-1 mpsc channels: `try_send` failing with Full means the watcher is not synced.
    pub(crate) progress: HashMap<i64, mpsc::Sender<i64>>,
}

/// Background task: broadcast progress if all watchers are synced (every 100ms).
/// Matches kine's `ProgressAll` (watch.go:344-378).
pub(crate) async fn progress_all<B: Backend>(
    backend: Arc<B>,
    resp_tx: mpsc::Sender<Result<WatchResponse, Status>>,
    notify: Arc<AtomicBool>,
    state: Arc<tokio::sync::Mutex<WatcherState>>,
) {
    let mut interval = tokio::time::interval(PROGRESS_ALL_INTERVAL);

    loop {
        interval.tick().await;

        if resp_tx.is_closed() {
            break;
        }

        if !notify.load(Ordering::Acquire) {
            continue;
        }

        let rev = match backend.current_revision().await {
            Ok(r) => r,
            Err(e) => {
                error!("progress_all: failed to get revision: {e}");
                continue;
            }
        };

        // Wait for poll loop to catch up
        backend.wait_for_sync_to(rev).await;

        let state = state.lock().await;

        // Try to send rev 0 (sync check) to ALL progress channels.
        // If any channel is full, that watcher isn't synced — abort.
        let mut all_synced = true;
        for (id, tx) in &state.progress {
            match tx.try_send(0) {
                Ok(()) => {}
                Err(_) => {
                    trace!("progress_all: watcher {id} not synced");
                    all_synced = false;
                    break;
                }
            }
        }

        if !all_synced {
            continue;
        }

        // All synced — clear flag and send broadcast progress response.
        // watch_id = -1 matches kine's clientv3.InvalidWatchID.
        notify.store(false, Ordering::Release);

        let _ = resp_tx
            .send(Ok(WatchResponse {
                header: Some(ResponseHeader {
                    revision: rev,
                    ..Default::default()
                }),
                watch_id: -1,
                ..Default::default()
            }))
            .await;
    }
}

/// Background task: per-watcher progress if synced (every ~5s + jitter).
/// Matches kine's `ProgressIfSynced` (watch.go:380-407).
pub(crate) async fn progress_if_synced<B: Backend>(
    backend: Arc<B>,
    resp_tx: mpsc::Sender<Result<WatchResponse, Status>>,
    notify: Arc<AtomicBool>,
    state: Arc<tokio::sync::Mutex<WatcherState>>,
    notify_interval: Duration,
) {
    // Add jitter: rand(1/10 * notify_interval), matching kine's getProgressReportInterval.
    let jitter_range_ms = notify_interval.as_millis() as u64 / 10;
    let jitter_ms = if jitter_range_ms > 0 {
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64)
            % jitter_range_ms
    } else {
        0
    };
    let interval_with_jitter = notify_interval + Duration::from_millis(jitter_ms);
    let mut interval = tokio::time::interval(interval_with_jitter);

    loop {
        interval.tick().await;

        if resp_tx.is_closed() {
            break;
        }

        // Don't send individual progress if broadcast progress has been requested.
        // Avoids double-progress (one broadcast from ProgressAll, another from this timer).
        if notify.load(Ordering::Acquire) {
            continue;
        }

        let rev = match backend.current_revision().await {
            Ok(r) => r,
            Err(e) => {
                error!("progress_if_synced: failed to get revision: {e}");
                continue;
            }
        };

        backend.wait_for_sync_to(rev).await;

        let state = state.lock().await;

        // Send actual revision to all synced channels (non-blocking).
        // If full, watcher has pending events — skip it.
        for (_id, tx) in &state.progress {
            let _ = tx.try_send(rev);
        }
    }
}

#[tonic::async_trait]
impl<B: Backend> Watch for KvBridge<B> {
    type WatchStream = WatchResponseStream;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut in_stream = request.into_inner();
        let backend = self.backend.clone();
        let notify_interval = self.notify_interval;

        // Channel for sending responses back to the client.
        let (resp_tx, resp_rx) = mpsc::channel::<Result<WatchResponse, Status>>(256);

        // Shared state for this Watch stream
        let notify = Arc::new(AtomicBool::new(false));
        let state = Arc::new(tokio::sync::Mutex::new(WatcherState {
            cancels: HashMap::new(),
            progress: HashMap::new(),
        }));

        // Spawn progress_all background task (100ms interval)
        tokio::spawn(progress_all(
            backend.clone(),
            resp_tx.clone(),
            notify.clone(),
            state.clone(),
        ));

        // Spawn progress_if_synced background task (notify_interval + jitter)
        tokio::spawn(progress_if_synced(
            backend.clone(),
            resp_tx.clone(),
            notify.clone(),
            state.clone(),
            notify_interval,
        ));

        // Spawn inbound request handler
        tokio::spawn(async move {
            while let Some(req) = in_stream.next().await {
                let req = match req {
                    Ok(r) => r,
                    Err(_) => break,
                };

                match req.request_union {
                    Some(watch_request::RequestUnion::CreateRequest(create)) => {
                        tracing::info!(
                            "WATCH create_request: key={:?}, range_end={:?}, start_revision={}, progress_notify={}, prev_kv={}, watch_id={}",
                            String::from_utf8_lossy(&create.key),
                            String::from_utf8_lossy(&create.range_end),
                            create.start_revision,
                            create.progress_notify,
                            create.prev_kv,
                            create.watch_id,
                        );

                        // Reject client-provided watch IDs (kine only accepts AutoWatchID = 0)
                        if create.watch_id != 0 {
                            tracing::warn!("WATCH rejected: client-provided watch_id={}", create.watch_id);
                            let _ = resp_tx
                                .send(Ok(WatchResponse {
                                    header: Some(ResponseHeader::default()),
                                    watch_id: -1, // InvalidWatchID
                                    created: true,
                                    canceled: true,
                                    cancel_reason:
                                        "etcdserver: unsupported options in watch request"
                                            .to_string(),
                                    ..Default::default()
                                }))
                                .await;
                            continue;
                        }

                        let watch_id = WATCH_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
                        tracing::info!("WATCH created: watch_id={}", watch_id);
                        let raw_key = String::from_utf8_lossy(&create.key).to_string();
                        // Redirect compact_rev_key to internal key
                        let key = if raw_key == "compact_rev_key" {
                            "compact_rev_key_apiserver".to_string()
                        } else {
                            raw_key
                        };
                        // Watch the byte range [key, range_end); see
                        // `key_in_watch_range` for the exact semantics.
                        let range_end = String::from_utf8_lossy(&create.range_end).to_string();
                        let start_revision = create.start_revision;
                        let want_prev_kv = create.prev_kv;

                        // Reject negative start revision (kine sends ErrCompacted)
                        if start_revision < 0 {
                            let _ = resp_tx
                                .send(Ok(WatchResponse {
                                    header: Some(ResponseHeader::default()),
                                    watch_id,
                                    canceled: true,
                                    compact_revision: start_revision,
                                    cancel_reason: "compacted".to_string(),
                                    ..Default::default()
                                }))
                                .await;
                            continue;
                        }

                        // Create cancellation channel
                        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

                        // Create progress channel if requested
                        let progress_rx = if create.progress_notify {
                            let (ptx, prx) = mpsc::channel::<i64>(1);
                            state.lock().await.progress.insert(watch_id, ptx);
                            Some(prx)
                        } else {
                            None
                        };

                        state.lock().await.cancels.insert(watch_id, cancel_tx);

                        // Send created confirmation
                        let _ = resp_tx
                            .send(Ok(WatchResponse {
                                header: Some(ResponseHeader::default()),
                                watch_id,
                                created: true,
                                ..Default::default()
                            }))
                            .await;

                        // Spawn an independent task for this watch
                        let backend = backend.clone();
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(async move {
                            tracing::info!("WATCH task started: watch_id={}, key={}, start_revision={}", watch_id, key, start_revision);
                            let watch_result = match backend.watch(&key, &range_end, start_revision).await {
                                Ok(wr) => {
                                    // Only cancel if the requested start_revision falls
                                    // below the compaction watermark (the backend already
                                    // returns Err(Compacted) for the obvious case, but a
                                    // race between the check and compaction running can
                                    // leave compact_revision > start_revision here).
                                    if wr.compact_revision != 0
                                        && start_revision > 0
                                        && start_revision <= wr.compact_revision
                                    {
                                        let _ = resp_tx
                                            .send(Ok(WatchResponse {
                                                header: Some(ResponseHeader {
                                                    revision: wr.current_revision,
                                                    ..Default::default()
                                                }),
                                                watch_id,
                                                canceled: true,
                                                compact_revision: wr.compact_revision,
                                                cancel_reason: "compacted".to_string(),
                                                ..Default::default()
                                            }))
                                            .await;
                                        return;
                                    }
                                    wr
                                }
                                Err(BackendError::Compacted) => {
                                    // Backend returned compacted error — get actual revisions
                                    let current_rev = backend.current_revision().await.unwrap_or(0);
                                    let _ = resp_tx
                                        .send(Ok(WatchResponse {
                                            header: Some(ResponseHeader {
                                                revision: current_rev,
                                                ..Default::default()
                                            }),
                                            watch_id,
                                            canceled: true,
                                            compact_revision: start_revision,
                                            cancel_reason: "compacted".to_string(),
                                            ..Default::default()
                                        }))
                                        .await;
                                    return;
                                }
                                Err(e) => {
                                    let _ = resp_tx.send(Err(backend_err_to_status(e))).await;
                                    return;
                                }
                            };

                            let mut events_rx = watch_result.events;
                            let mut cancel_rx = cancel_rx;
                            let has_progress = progress_rx.is_some();
                            let mut progress_rx = progress_rx;

                            loop {
                                tokio::select! {
                                    event_batch = events_rx.recv() => {
                                        let Some(mut events) = event_batch else {
                                            tracing::info!("WATCH event channel closed: watch_id={}", watch_id);
                                            break; // Channel closed
                                        };
                                        tracing::debug!("WATCH received {} events: watch_id={}", events.len(), watch_id);

                                        // Coalesce queued batches (matching kine's event batching)
                                        while let Ok(more) = events_rx.try_recv() {
                                            events.extend(more);
                                        }

                                        let proto_events: Vec<mvccpb::Event> = events
                                            .iter()
                                            .map(|e| event_to_proto(e, want_prev_kv))
                                            .collect();

                                        let last_rev = events
                                            .last()
                                                                    .map(|e| e.kv.mod_revision)
                                            .unwrap_or(0);

                                        if last_rev >= start_revision || start_revision == 0 {
                                            let event_types: Vec<&str> = proto_events
                                                .iter()
                                                .map(|e| match e.r#type {
                                                    0 => "PUT",
                                                    _ => "DELETE",
                                                })
                                                .collect();
                                            tracing::info!(
                                                "WATCH event sent: watch_id={}, revision={}, events={:?}",
                                                watch_id,
                                                last_rev,
                                                event_types,
                                            );
                                            if resp_tx.send(Ok(WatchResponse {
                                                header: Some(ResponseHeader {
                                                    revision: last_rev,
                                                    ..Default::default()
                                                }),
                                                watch_id,
                                                events: proto_events,
                                                ..Default::default()
                                            })).await.is_err() {
                                                tracing::warn!("WATCH client disconnected: watch_id={}", watch_id);
                                                break; // Client disconnected
                                            }
                                        }
                                    }
                                    Some(revision) = async {
                                        if has_progress {
                                            progress_rx.as_mut().unwrap().recv().await
                                        } else {
                                            // Never resolves — effectively disabled
                                            std::future::pending::<Option<i64>>().await
                                        }
                                    } => {
                                        // Progress notification request.
                                        // revision == 0: "sync check" — the fact that we received
                                        // means we're synced (not blocked on events). No response.
                                        // revision > 0: send a progress response with this revision.
                                        if revision > 0 {
                                            if resp_tx.send(Ok(WatchResponse {
                                                header: Some(ResponseHeader {
                                                    revision,
                                                    ..Default::default()
                                                }),
                                                watch_id,
                                                ..Default::default()
                                            })).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    _ = &mut cancel_rx => {
                                        break; // Watch cancelled
                                    }
                                }
                            }
                        });
                    }
                    Some(watch_request::RequestUnion::CancelRequest(cancel)) => {
                        tracing::info!("WATCH cancel_request: watch_id={}", cancel.watch_id);
                        let mut s = state.lock().await;
                        // Signal the watch task to stop
                        if let Some(cancel_tx) = s.cancels.remove(&cancel.watch_id) {
                            let _ = cancel_tx.send(());
                            tracing::info!("WATCH cancelled: watch_id={}", cancel.watch_id);
                        } else {
                            tracing::warn!("WATCH cancel failed: watch_id={} not found", cancel.watch_id);
                        }
                        // Clean up progress channel
                        s.progress.remove(&cancel.watch_id);
                        drop(s);

                        let _ = resp_tx
                            .send(Ok(WatchResponse {
                                header: Some(ResponseHeader::default()),
                                watch_id: cancel.watch_id,
                                canceled: true,
                                cancel_reason: "watch cancelled by client".to_string(),
                                ..Default::default()
                            }))
                            .await;
                    }
                    Some(watch_request::RequestUnion::ProgressRequest(_)) => {
                        // Set flag for ProgressAll background task to handle.
                        // Matches kine's Progress() (watch.go:335-340).
                        trace!("watch progress request received");
                        notify.store(true, Ordering::Release);
                    }
                    None => {}
                }
            }
        });

        // Convert the mpsc receiver into a stream for tonic
        let output = tokio_stream::wrappers::ReceiverStream::new(resp_rx);
        Ok(Response::new(Box::pin(output) as Self::WatchStream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::KeyValue;

    fn event_with_prev(prev: Option<KeyValue>) -> Event {
        Event {
            create: false,
            delete: false,
            kv: KeyValue {
                key: "/apisix/routes/x".to_string(),
                value: b"new".to_vec(),
                version: 0,
                create_revision: 10,
                mod_revision: 20,
                lease: 0,
            },
            prev_kv: prev,
        }
    }

    fn old_kv() -> KeyValue {
        KeyValue {
            key: "/apisix/routes/x".to_string(),
            value: b"old".to_vec(),
            version: 0,
            create_revision: 10,
            mod_revision: 12,
            lease: 0,
        }
    }

    /// Guard: the previous value is only attached when the watch was created
    /// with `prev_kv=true` (etcd semantics). Otherwise etcdctl prints the old
    /// value alongside the new one for plain watches, which users read as a
    /// duplicated/redundant event.
    #[test]
    fn event_to_proto_omits_prev_kv_unless_requested() {
        let e = event_with_prev(Some(old_kv()));

        let p = event_to_proto(&e, false);
        assert!(p.prev_kv.is_none(), "prev_kv must be stripped when not requested");
        assert_eq!(p.kv.as_ref().unwrap().value, b"new");

        let p = event_to_proto(&e, true);
        let pk = p.prev_kv.as_ref().expect("prev_kv must be attached when requested");
        assert_eq!(pk.value, b"old");
        assert_eq!(pk.mod_revision, 12);
    }

    #[test]
    fn event_to_proto_without_prev_value_stays_none() {
        let e = event_with_prev(None);
        assert!(event_to_proto(&e, true).prev_kv.is_none());
        assert!(event_to_proto(&e, false).prev_kv.is_none());
    }
}
