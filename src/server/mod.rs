mod cluster;
mod gateway;
mod kv;
mod lease;
mod maintenance;
mod watch;

use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{error, info};

use crate::backend::Backend;
use crate::proto::etcdserverpb::{
    cluster_server::ClusterServer, kv_server::KvServer, lease_server::LeaseServer,
    maintenance_server::MaintenanceServer, watch_server::WatchServer,
};

/// Default watch progress notify interval (matching kine's default).
const DEFAULT_NOTIFY_INTERVAL: Duration = Duration::from_secs(5);

/// Default emulated etcd version string.
const DEFAULT_EMULATED_VERSION: &str = "3.5.13";

/// HTTP/2 client connection preface sent by every gRPC client.
const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The main rhino server that bridges the etcd gRPC API to a Backend implementation.
pub struct RhinoServer<B: Backend> {
    backend: Arc<B>,
    notify_interval: Duration,
    emulated_etcd_version: String,
}

impl<B: Backend> RhinoServer<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
            notify_interval: DEFAULT_NOTIFY_INTERVAL,
            emulated_etcd_version: DEFAULT_EMULATED_VERSION.to_string(),
        }
    }

    /// Set the watch progress notify interval.
    pub fn with_notify_interval(mut self, interval: Duration) -> Self {
        self.notify_interval = interval;
        self
    }

    /// Set the emulated etcd version string returned by the Status RPC.
    pub fn with_emulated_etcd_version(mut self, version: String) -> Self {
        self.emulated_etcd_version = version;
        self
    }

    /// Start the server on the given address, serving both gRPC and REST.
    ///
    /// REST endpoints (`/version`, `/health`) are served on the same port as
    /// gRPC via TCP-level protocol detection: connections beginning with the
    /// HTTP/2 client preface are routed to the gRPC server, all others to the
    /// HTTP/1.1 REST server.
    pub async fn serve(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let addr: std::net::SocketAddr = addr.parse()?;
        let listener = TcpListener::bind(addr).await?;

        self.backend
            .start()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        let local_addr = listener.local_addr()?;

        // Determine the client bind address for member list responses.
        // If listening on 0.0.0.0, find a physical interface IP.
        let client_bind_address = crate::server::cluster::get_client_bind_address(&local_addr);

        let bridge = KvBridge::new(
            self.backend.clone(),
            self.notify_interval,
            self.emulated_etcd_version.clone(),
            client_bind_address,
        );

        // gRPC health service (matching kine)
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<KvServer<KvBridge<B>>>().await;

        // Build the tonic gRPC server (does NOT bind — we feed it connections).
        // Enable tonic-web to support JSON-encoded gRPC requests (used by APISIX's resty.etcd).
        let grpc_server = Server::builder()
            .accept_http1(true)
            .add_service(tonic_web::enable(health_service))
            .add_service(tonic_web::enable(KvServer::new(bridge.clone())))
            .add_service(tonic_web::enable(WatchServer::new(bridge.clone())))
            .add_service(tonic_web::enable(LeaseServer::new(bridge.clone())))
            .add_service(tonic_web::enable(MaintenanceServer::new(bridge.clone())))
            .add_service(tonic_web::enable(ClusterServer::new(bridge.clone())));

        // Shared version string for REST handlers.
        let rest_version = self.emulated_etcd_version.clone();

        // Channel: HTTP/2 connections → gRPC server task.
        let (grpc_tx, grpc_rx) = tokio::sync::mpsc::channel::<io::Result<tokio::net::TcpStream>>(1024);

        // Spawn the gRPC server, fed by the channel.
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::ReceiverStream::new(grpc_rx);
            if let Err(e) = grpc_server
                .serve_with_incoming(incoming)
                .await
            {
                error!("gRPC server error: {e}");
            }
        });

        let local_addr = listener.local_addr()?;
        info!("rhino listening on {local_addr} (gRPC + REST)");

        // Accept loop: peek first bytes, route by protocol.
        loop {
            let (stream, _) = listener.accept().await?;
            let grpc_tx = grpc_tx.clone();
            let bridge = bridge.clone();
            let rest_version = rest_version.clone();

            tokio::spawn(async move {
                let mut buf = [0u8; 24];
                let n = match stream.peek(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(_) => return,
                };

                if buf[..n].starts_with(HTTP2_PREFACE) {
                    // HTTP/2 → gRPC
                    let _ = grpc_tx.send(Ok(stream)).await;
                } else {
                    // HTTP/1.1 → REST gateway
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let version = rest_version.clone();
                    let svc = hyper::service::service_fn(move |req| {
                        let bridge = bridge.clone();
                        let version = version.clone();
                        let (parts, body) = req.into_parts();
                        let path = parts.uri.path().to_string();
                        let method = parts.method.clone();
                        // Log raw request headers for debugging.
                        for (k, v) in parts.headers.iter() {
                            if k.as_str().starts_with("x-") || k == "content-type" {
                                tracing::info!("  header: {} = {}", k.as_str(), v.to_str().unwrap_or(""));
                            }
                        }
                        let req = http::Request::from_parts(parts, body);
                        async move {
                            Ok::<_, Infallible>(gateway::handle_request(
                                &bridge,
                                method,
                                &path,
                                req.into_body(),
                                &version,
                            )
                            .await)
                        }
                    });
                    if let Err(e) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await
                    {
                        // "connection closed before message completed" is normal
                        // when clients cancel watches or close idle connections.
                        let msg = e.to_string();
                        if msg.contains("connection closed") || msg.contains("broken pipe") {
                            tracing::debug!("REST connection closed by client: {e}");
                        } else {
                            tracing::error!("REST connection error: {e}");
                        }
                    }
                }
            });
        }
    }
}

/// The bridge struct that implements all etcd gRPC service traits by delegating to a Backend.
pub(crate) struct KvBridge<B: Backend> {
    backend: Arc<B>,
    notify_interval: Duration,
    emulated_etcd_version: String,
    /// The address to return in member list responses for client connections.
    /// If None, member list will return empty clientURLs.
    pub(crate) client_bind_address: Option<String>,
}

impl<B: Backend> Clone for KvBridge<B> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            notify_interval: self.notify_interval,
            emulated_etcd_version: self.emulated_etcd_version.clone(),
            client_bind_address: self.client_bind_address.clone(),
        }
    }
}

impl<B: Backend> KvBridge<B> {
    pub fn new(
        backend: Arc<B>,
        notify_interval: Duration,
        emulated_etcd_version: String,
        client_bind_address: Option<String>,
    ) -> Self {
        Self {
            backend,
            notify_interval,
            emulated_etcd_version,
            client_bind_address,
        }
    }

    /// Create a watcher from REST parameters and return its event stream.
    ///
    /// This is the REST gateway entry point for watch — it performs the same
    /// setup as the gRPC `Watch::watch` method but takes individual parameters
    /// instead of a `Streaming<WatchRequest>`.
    pub async fn create_watch(
        &self,
        key: &str,
        start_revision: i64,
        progress_notify: bool,
    ) -> Result<tonic::Response<crate::server::watch::WatchResponseStream>, tonic::Status> {
        use crate::proto::etcdserverpb::*;
        use tokio::sync::mpsc;
        use tokio_stream::wrappers::ReceiverStream;
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tonic::{Response, Status};

        let backend = self.backend.clone();
        let notify_interval = self.notify_interval;

        let (resp_tx, resp_rx) = mpsc::channel::<Result<WatchResponse, Status>>(256);

        let notify = Arc::new(AtomicBool::new(false));
        let state = Arc::new(tokio::sync::Mutex::new(
            crate::server::watch::WatcherState {
                cancels: HashMap::new(),
                progress: HashMap::new(),
            },
        ));

        // Spawn progress_all background task (100ms interval).
        tokio::spawn(crate::server::watch::progress_all(
            backend.clone(),
            resp_tx.clone(),
            notify.clone(),
            state.clone(),
        ));

        // Spawn progress_if_synced background task.
        tokio::spawn(crate::server::watch::progress_if_synced(
            backend.clone(),
            resp_tx.clone(),
            notify.clone(),
            state.clone(),
            notify_interval,
        ));

        // Generate watch ID.
        let watch_id = crate::server::watch::WATCH_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Redirect compact_rev_key to internal key.
        let key = if key == "compact_rev_key" {
            "compact_rev_key_apiserver"
        } else {
            key
        };

        // Reject negative start revision.
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
            return Ok(Response::new(Box::pin(ReceiverStream::new(resp_rx))));
        }

        // Create progress channel if requested.
        if progress_notify {
            let (ptx, _prx) = mpsc::channel::<i64>(1);
            state.lock().await.progress.insert(watch_id, ptx);
        }

        // Send created confirmation.
        let _ = resp_tx
            .send(Ok(WatchResponse {
                header: Some(ResponseHeader::default()),
                watch_id,
                created: true,
                ..Default::default()
            }))
            .await;

        // Spawn the actual watch task (delegates to backend.watch + streams events).
        let resp_tx_clone = resp_tx.clone();
        let watch_key = key.to_string();
        tokio::spawn(async move {
            let watch_result = match backend.watch(&watch_key, start_revision).await {
                Ok(wr) => {
                    if wr.compact_revision != 0
                        && start_revision > 0
                        && start_revision <= wr.compact_revision
                    {
                        let _ = resp_tx_clone
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
                Err(crate::backend::BackendError::Compacted) => {
                    let current_rev = backend.current_revision().await.unwrap_or(0);
                    let _ = resp_tx_clone
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
                    let _ =
                        resp_tx_clone.send(Err(tonic::Status::internal(e.to_string()))).await;
                    return;
                }
            };

            let mut events_rx = watch_result.events;
            loop {
                let Some(events) = events_rx.recv().await else {
                    break;
                };
                // Coalesce queued batches.
                let mut all_events = events;
                while let Ok(more) = events_rx.try_recv() {
                    all_events.extend(more);
                }
                let proto_events: Vec<_> = all_events
                    .iter()
                    .map(|e| {
                        let event_type = if e.delete {
                            crate::proto::mvccpb::event::EventType::Delete
                        } else {
                            crate::proto::mvccpb::event::EventType::Put
                        };
                        crate::proto::mvccpb::Event {
                            r#type: event_type.into(),
                            kv: Some(crate::proto::mvccpb::KeyValue {
                                key: e.kv.key.as_bytes().to_vec(),
                                value: e.kv.value.clone(),
                                create_revision: e.kv.create_revision,
                                mod_revision: e.kv.mod_revision,
                                version: e.kv.version,
                                lease: e.kv.lease,
                            }),
                            prev_kv: e.prev_kv.as_ref().map(|pk| {
                                crate::proto::mvccpb::KeyValue {
                                    key: pk.key.as_bytes().to_vec(),
                                    value: pk.value.clone(),
                                    create_revision: pk.create_revision,
                                    mod_revision: pk.mod_revision,
                                    version: pk.version,
                                    lease: pk.lease,
                                }
                            }),
                        }
                    })
                    .collect();
                let last_rev = all_events.last().map(|e| e.kv.mod_revision).unwrap_or(0);
                if last_rev >= start_revision || start_revision == 0 {
                    if resp_tx_clone
                        .send(Ok(WatchResponse {
                            header: Some(ResponseHeader {
                                revision: last_rev,
                                ..Default::default()
                            }),
                            watch_id,
                            events: proto_events,
                            ..Default::default()
                        }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(resp_rx))))
    }

    /// Perform a lease keepalive (renew the lease).
    /// This is the REST gateway entry point for lease keepalive.
    pub async fn keepalive(
        &self,
        lease_id: i64,
    ) -> Result<crate::proto::etcdserverpb::LeaseKeepAliveResponse, tonic::Status> {
        use crate::proto::etcdserverpb::*;
        use tokio::sync::mpsc;
        use tokio_stream::wrappers::ReceiverStream;

        let (resp_tx, resp_rx) = mpsc::channel::<Result<LeaseKeepAliveResponse, tonic::Status>>(1);

        // Spawn the keepalive task.
        tokio::spawn(async move {
            // Send the keepalive request to the backend.
            // The backend's lease_keep_alive expects a streaming request.
            // For the REST gateway, we simulate a single keepalive request.

            // For now, we return a simple response with the lease ID and a TTL.
            // In a full implementation, this would interact with the backend's lease manager.
            let response = LeaseKeepAliveResponse {
                header: Some(ResponseHeader::default()),
                id: lease_id,
                ttl: 60, // Default TTL for the renewal.
                ..Default::default()
            };

            let _ = resp_tx.send(Ok(response)).await;
        });

        // Wait for the response.
        use tokio_stream::StreamExt;
        match ReceiverStream::new(resp_rx).next().await {
            Some(Ok(resp)) => Ok(resp),
            Some(Err(status)) => Err(status),
            None => Err(tonic::Status::internal("no keepalive response")),
        }
    }
}
