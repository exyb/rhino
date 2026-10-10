//! JSON-to-gRPC gateway: translates REST/JSON requests into gRPC calls
//! to the same `KvBridge` server.
//!
//! This mirrors etcd's `grpc-gateway` behavior. Each REST endpoint accepts
//! a JSON body matching the protobuf JSON format (camelCase fields, base64
//! bytes, int64 as strings) and returns the same format.

use base64::{Engine as _, engine::general_purpose};
use bytes::{Buf, Bytes};
use futures::stream::Stream;
use http_body::Body;
use serde_json::Value;
use std::pin::Pin;
use tonic::{Request, Status};

use crate::backend::Backend;
use crate::proto::etcdserverpb::{
    cluster_server::Cluster, kv_server::Kv, lease_server::Lease, maintenance_server::Maintenance,
};
use crate::server::KvBridge;

// Re-export proto types for convenience.
use crate::proto::etcdserverpb as proto;
use crate::proto::mvccpb;

/// A body that yields exactly one data frame then ends.
pub(crate) struct FullBody {
    data: Option<Bytes>,
}

impl FullBody {
    fn new(data: Bytes) -> Self {
        Self { data: Some(data) }
    }
}

impl http_body::Body for FullBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Ready(self.data.take().map(|d| Ok(http_body::Frame::data(d))))
    }
}

/// Response body type that can be either a fixed JSON response or a streaming SSE body.
pub(crate) enum ResponseBody {
    Fixed(FullBody),
    Stream(NdjsonBody),
}

impl http_body::Body for ResponseBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match &mut *self {
            ResponseBody::Fixed(body) => {
                // FullBody::Error = Infallible, which can never occur.
                // Use unreachable!() in the error path since Infallible has no values.
                Pin::new(body).poll_frame(cx).map(|opt| {
                    opt.map(|res| {
                        res.map_err(|_: std::convert::Infallible| -> Self::Error {
                            unreachable!("Infallible")
                        })
                    })
                })
            }
            ResponseBody::Stream(body) => Pin::new(body).poll_frame(cx),
        }
    }
}

// ---------------------------------------------------------------------------
// Base64 helpers
// ---------------------------------------------------------------------------

fn b64_encode(bytes: &[u8]) -> String {
    general_purpose::STANDARD.encode(bytes)
}

fn b64_decode(s: &str) -> Vec<u8> {
    general_purpose::STANDARD.decode(s).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Gateway entry point
// ---------------------------------------------------------------------------

/// Create a fixed JSON response body.
fn box_json(body: String) -> FullBody {
    FullBody::new(Bytes::from(body))
}

/// Collect a `hyper::body::Incoming` into a `Vec<u8>`.
async fn collect_body(mut body: hyper::body::Incoming) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    loop {
        // Poll the body inside a scope so the Context is dropped before any await.
        let frame = {
            let waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            std::pin::Pin::new(&mut body).poll_frame(&mut cx)
        };
        match frame {
            std::task::Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    bytes.extend_from_slice(data.chunk());
                }
            }
            std::task::Poll::Ready(Some(Err(e))) => return Err(e.to_string()),
            std::task::Poll::Ready(None) => break,
            std::task::Poll::Pending => {
                // Yield and try again (should not happen with Incoming in practice).
                tokio::task::yield_now().await;
            }
        }
    }
    Ok(bytes)
}

/// Handle a single REST request, returning an HTTP response.
///
/// This is the entry point called from the accept loop. It collects the
/// body, dispatches by method + path, and returns a JSON response.
pub async fn handle_request<B: Backend>(
    bridge: &KvBridge<B>,
    method: http::Method,
    path: &str,
    body: hyper::body::Incoming,
    version: &str,
) -> http::Response<ResponseBody> {
    // Collect body into bytes.
    let body_bytes = match collect_body(body).await {
        Ok(bytes) => bytes,
        Err(e) => {
            return json_error(400, &format!("failed to read body: {e}"));
        }
    };

    dispatch(bridge, &method, &path, &body_bytes, version).await
}

/// Route a request to the appropriate handler.
async fn dispatch<B: Backend>(
    bridge: &KvBridge<B>,
    method: &http::Method,
    path: &str,
    body: &[u8],
    version: &str,
) -> http::Response<ResponseBody> {
    // Special management endpoints.
    if method == http::Method::GET {
        return match path {
            "/version" => {
                let json = serde_json::json!({
                    "etcdserver": version,
                    "etcdcluster": version,
                });
                json_response(200, json.to_string())
            }
            "/health" => {
                let json = serde_json::json!({"health": "true"});
                json_response(200, json.to_string())
            }
            _ => json_error(404, "not found"),
        };
    }

    if method != http::Method::POST {
        return json_error(405, "method not allowed");
    }

    // Parse the JSON body.
    let value: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    // Watch endpoint returns a streaming SSE response.
    if path == "/v3/watch" {
        return handle_watch(bridge, &value).await;
    }

    // Route by path. All handlers return Result<String, Status>; convert to response.
    tracing::info!("REST dispatch: path={}, body={}", path, value);
    let result: Result<String, Status> = match path {
        "/v3/kv/range" => handle_range(bridge, &value).await,
        "/v3/kv/put" => handle_put(bridge, &value).await,
        "/v3/kv/deleterange" => handle_delete_range(bridge, &value).await,
        "/v3/kv/txn" => handle_txn(bridge, &value).await,
        "/v3/kv/compact" => handle_compact(bridge, &value).await,
        "/v3/lease/grant" => handle_lease_grant(bridge, &value).await,
        "/v3/lease/revoke" => handle_lease_revoke(bridge, &value).await,
        "/v3/lease/keepalive" => handle_lease_keepalive(bridge, &value).await,
        "/v3/kv/lease/timetolive" => handle_lease_time_to_live(bridge, &value).await,
        "/v3/lease/leases" => handle_lease_leases(bridge, &value).await,
        "/v3/cluster/member/list" => handle_member_list(bridge, &value).await,
        "/v3/maintenance/status" => handle_status(bridge, &value).await,
        "/v3/maintenance/defragment" => handle_defragment(bridge, &value).await,
        "/v3/maintenance/hash" => handle_hash(bridge, &value).await,
        "/v3/maintenance/snapshot" => handle_snapshot(bridge, &value).await,
        _ => Err(Status::unimplemented(format!(
            "unknown endpoint: {path}"
        ))),
    };

    match result {
        Ok(json) => {
            tracing::info!("REST {} {} -> {}", method, path, json);
            json_response(200, json)
        }
        Err(status) => {
            let (code, msg) = map_grpc_error(&status);
            tracing::error!("REST {} {} -> {} {}", method, path, code, msg);
            json_error(code, &msg)
        }
    }
}

// ---------------------------------------------------------------------------
// KV handlers
// ---------------------------------------------------------------------------

async fn handle_range<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let req: proto::RangeRequest = parse_range_request(value);
    tracing::info!("handle_range proto: key={:?}, range_end={:?}, key_len={}, range_end_len={}",
        String::from_utf8_lossy(&req.key),
        String::from_utf8_lossy(&req.range_end),
        req.key.len(),
        req.range_end.len()
    );
    let resp = bridge.range(Request::new(req)).await?;
    Ok(range_response_to_json(&resp.into_inner()))
}

async fn handle_put<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    // Parse the lease TTL from the request.
    // APISIX sends a TTL in the "lease" field when it wants to create a lease.
    let ttl = i64_from_json(value, "lease");
    let key_raw = value.get("key").and_then(|v| v.as_str()).unwrap_or("");
    tracing::info!("PUT request: key_raw={}, lease_ttl={}, has_lease_field={}, full_body={}",
        key_raw,
        ttl,
        value.get("lease").is_some(),
        value
    );

    // If a TTL is specified, create a lease first and use its ID.
    let lease_id = if ttl > 0 {
        let lease_resp = bridge
            .lease_grant(Request::new(proto::LeaseGrantRequest {
                ttl,
                id: 0,
            }))
            .await?;
        lease_resp.into_inner().id
    } else {
        0
    };

    let mut req: proto::PutRequest = parse_put_request(value);
    // Use the created lease ID (or the original lease field if no TTL was given).
    if lease_id > 0 {
        req.lease = lease_id;
    }

    let resp = bridge.put(Request::new(req)).await?;
    let put_resp = resp.into_inner();

    // Build the response, including lease_id if a lease was created.
    // APISIX expects lease_id as a string in the response body.
    let mut json = put_response_to_json(&put_resp);
    if lease_id > 0 {
        if let Ok(mut map) = serde_json::from_str::<serde_json::Map<String, Value>>(&json) {
            map.insert(
                "lease_id".to_string(),
                Value::String(lease_id.to_string()),
            );
            json = serde_json::to_string(&Value::Object(map)).unwrap_or_default();
        }
    }
    Ok(json)
}

async fn handle_delete_range<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let req: proto::DeleteRangeRequest = parse_delete_range_request(value);
    let resp = bridge.delete_range(Request::new(req)).await?;
    Ok(delete_range_response_to_json(&resp.into_inner()))
}

async fn handle_txn<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let req: proto::TxnRequest = parse_txn_request(value);
    let resp = bridge.txn(Request::new(req)).await?;
    Ok(txn_response_to_json(&resp.into_inner()))
}

async fn handle_compact<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let req: proto::CompactionRequest = parse_compaction_request(value);
    let resp = bridge.compact(Request::new(req)).await?;
    Ok(compaction_response_to_json(&resp.into_inner()))
}

// ---------------------------------------------------------------------------
// Lease handlers
// ---------------------------------------------------------------------------

async fn handle_lease_grant<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let req: proto::LeaseGrantRequest = parse_lease_grant_request(value);
    let resp = bridge.lease_grant(Request::new(req)).await?;
    Ok(lease_grant_response_to_json(&resp.into_inner()))
}

async fn handle_lease_revoke<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let req: proto::LeaseRevokeRequest = parse_lease_revoke_request(value);
    let resp = bridge.lease_revoke(Request::new(req)).await?;
    Ok(lease_revoke_response_to_json(&resp.into_inner()))
}

async fn handle_lease_time_to_live<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let req: proto::LeaseTimeToLiveRequest = parse_lease_time_to_live_request(value);
    let resp = bridge.lease_time_to_live(Request::new(req)).await?;
    Ok(lease_time_to_live_response_to_json(&resp.into_inner()))
}

async fn handle_lease_leases<B: Backend>(
    bridge: &KvBridge<B>,
    _value: &Value,
) -> Result<String, Status> {
    let resp = bridge
        .lease_leases(Request::new(proto::LeaseLeasesRequest {}))
        .await?;
    Ok(lease_leases_response_to_json(&resp.into_inner()))
}

async fn handle_lease_keepalive<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let lease_id = i64_from_json(value, "id");
    let resp = bridge.keepalive(lease_id).await?;
    Ok(lease_keepalive_response_to_json(&resp))
}

// ---------------------------------------------------------------------------
// Cluster handlers
// ---------------------------------------------------------------------------

async fn handle_member_list<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> Result<String, Status> {
    let req = proto::MemberListRequest {
        linearizable: bool_from_json(value, "linearizable"),
    };
    let resp = bridge.member_list(Request::new(req)).await?;
    Ok(member_list_response_to_json(&resp.into_inner()))
}

// ---------------------------------------------------------------------------
// Maintenance handlers
// ---------------------------------------------------------------------------

async fn handle_status<B: Backend>(
    bridge: &KvBridge<B>,
    _value: &Value,
) -> Result<String, Status> {
    let resp = bridge.status(Request::new(proto::StatusRequest {})).await?;
    Ok(status_response_to_json(&resp.into_inner()))
}

async fn handle_defragment<B: Backend>(
    bridge: &KvBridge<B>,
    _value: &Value,
) -> Result<String, Status> {
    let resp = bridge
        .defragment(Request::new(proto::DefragmentRequest {}))
        .await?;
    Ok(defragment_response_to_json(&resp.into_inner()))
}

async fn handle_hash<B: Backend>(
    bridge: &KvBridge<B>,
    _value: &Value,
) -> Result<String, Status> {
    let resp = bridge.hash(Request::new(proto::HashRequest {})).await?;
    Ok(hash_response_to_json(&resp.into_inner()))
}

async fn handle_snapshot<B: Backend>(
    bridge: &KvBridge<B>,
    _value: &Value,
) -> Result<String, Status> {
    let resp = bridge
        .snapshot(Request::new(proto::SnapshotRequest {}))
        .await?;
    // Snapshot is a stream — collect all chunks into NDJSON.
    let stream = resp.into_inner();
    let results: Vec<Result<proto::SnapshotResponse, Status>> =
        tokio_stream::StreamExt::collect(stream).await;
    let mut lines = Vec::new();
    for result in results {
        let chunk = result?;
        lines.push(snapshot_response_to_json(&chunk));
    }
    Ok(lines.join("\n"))
}

// ---------------------------------------------------------------------------
// Watch (SSE streaming)
// ---------------------------------------------------------------------------

async fn handle_watch<B: Backend>(
    bridge: &KvBridge<B>,
    value: &Value,
) -> http::Response<ResponseBody> {
    // Parse the create request parameters from JSON body.
    let create_request = parse_watch_create_request(value);

    // Watch the byte range [key, range_end); an empty range_end means a single
    // key (or prefix when the key ends with '/') — see `key_in_watch_range`.
    let watch_key = String::from_utf8_lossy(&create_request.key).to_string();
    let watch_range_end = String::from_utf8_lossy(&create_request.range_end).to_string();

    // Call bridge.create_watch() which sets up the watcher and returns an event stream.
    let watch_resp = match bridge
        .create_watch(
            &watch_key,
            &watch_range_end,
            create_request.start_revision,
            create_request.progress_notify,
            create_request.prev_kv,
        )
        .await
    {
        Ok(resp) => resp,
        Err(status) => {
            let (code, msg) = map_grpc_error(&status);
            return json_error(code, &msg);
        }
    };

    // Convert the response stream into NDJSON format (newline-delimited JSON).
    // This matches etcd's grpc-gateway behavior: each WatchResponse is a JSON object
    // followed by a newline, streamed with chunked transfer encoding.
    let resp_rx = watch_resp.into_inner();
    let body = NdjsonBody::new(resp_rx);

    // Note: Do NOT set content-length or transfer-encoding manually.
    // hyper automatically uses chunked transfer encoding when the body size is unknown.
    http::Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .header("x-accel-buffering", "no")
        .body(ResponseBody::Stream(body))
    .unwrap_or_else(|_| json_error(500, "failed to build watch response"))
}

/// Error type for streaming.
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A streaming body that converts WatchResponse events to NDJSON format.
/// Each JSON object is followed by a newline, matching etcd's grpc-gateway behavior.
pub(crate) struct NdjsonBody {
    inner: Pin<Box<dyn Stream<Item = Result<proto::WatchResponse, Status>> + Send>>,
}

impl NdjsonBody {
    fn new(
        stream: Pin<Box<dyn Stream<Item = Result<proto::WatchResponse, Status>> + Send>>,
    ) -> Self {
        Self { inner: stream }
    }
}

impl http_body::Body for NdjsonBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.inner.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(result)) => {
                let json = match result {
                    Ok(watch_resp) => watch_response_to_json(&watch_resp),
                    Err(status) => {
                        serde_json::json!({"error": status.message()}).to_string()
                    }
                };
                // NDJSON: each JSON object followed by a newline.
                let mut line = json;
                line.push('\n');
                std::task::Poll::Ready(Some(Ok(http_body::Frame::data(Bytes::from(line)))))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Convert a WatchResponse to JSON, wrapped in a "result" field to match
/// etcd's grpc-gateway format expected by APISIX.
fn watch_response_to_json(resp: &proto::WatchResponse) -> String {
    let mut inner = serde_json::Map::new();

    if let Some(ref header) = resp.header {
        inner.insert("header".to_string(), header_to_json(header));
    }

    // Use snake_case for watch_id to match etcd's JSON format.
    if resp.watch_id != 0 {
        inner.insert(
            "watch_id".to_string(),
            Value::String(resp.watch_id.to_string()),
        );
    }

    if resp.created {
        inner.insert("created".to_string(), Value::Bool(true));
    }

    if resp.canceled {
        inner.insert("canceled".to_string(), Value::Bool(true));
        if !resp.cancel_reason.is_empty() {
            inner.insert(
                "cancel_reason".to_string(),
                Value::String(resp.cancel_reason.clone()),
            );
        }
        if resp.compact_revision != 0 {
            inner.insert(
                "compact_revision".to_string(),
                Value::String(resp.compact_revision.to_string()),
            );
        }
    }

    if resp.compact_revision != 0 && !resp.canceled {
        inner.insert(
            "compact_revision".to_string(),
            Value::String(resp.compact_revision.to_string()),
        );
    }

    if resp.fragment {
        inner.insert("fragment".to_string(), Value::Bool(true));
    }

    if !resp.events.is_empty() {
        let events: Vec<Value> = resp.events.iter().map(event_to_json).collect();
        inner.insert("events".to_string(), Value::Array(events));
    }

    // Wrap in "result" field to match etcd grpc-gateway format.
    let mut outer = serde_json::Map::new();
    outer.insert("result".to_string(), Value::Object(inner));
    serde_json::to_string(&Value::Object(outer)).unwrap_or_default()
}

/// Convert an mvccpb::Event to JSON Value.
fn event_to_json(event: &mvccpb::Event) -> Value {
    let type_str = match event.r#type {
        0 => "PUT",    // event::EventType::Put
        _ => "DELETE", // event::EventType::Delete
    };
    let mut obj = serde_json::Map::new();
    obj.insert("type".to_string(), Value::String(type_str.to_string()));
    obj.insert(
        "kv".to_string(),
        event.kv.as_ref().map(kv_to_json).unwrap_or(Value::Null),
    );
    // snake_case matches etcd's grpc-gateway (protojson with UseProtoNames,
    // which omits unset message fields) and what lua-resty-etcd reads
    // (`event.prev_kv`). The server layer only populates this field when the
    // watch was created with prev_kv=true.
    if let Some(pk) = &event.prev_kv {
        obj.insert("prev_kv".to_string(), kv_to_json(pk));
    }
    Value::Object(obj)
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

fn json_response(status: u16, body: String) -> http::Response<ResponseBody> {
    let content_length = body.len();
    let body = ResponseBody::Fixed(box_json(body));
    http::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("content-length", content_length)
        .body(body)
        .unwrap()
}

fn json_error(status: u16, message: &str) -> http::Response<ResponseBody> {
    let body = format!(r#"{{"error":"{message}"}}"#);
    json_response(status, body)
}

/// Map a gRPC `Status` to an (HTTP status code, error message).
fn map_grpc_error(status: &Status) -> (u16, String) {
    let code = match status.code() {
        tonic::Code::Ok => 200,
        tonic::Code::InvalidArgument => 400,
        tonic::Code::NotFound => 404,
        tonic::Code::AlreadyExists => 409,
        tonic::Code::PermissionDenied => 403,
        tonic::Code::Unauthenticated => 401,
        tonic::Code::Internal => 500,
        tonic::Code::Unimplemented => 501,
        tonic::Code::Unavailable => 503,
        tonic::Code::FailedPrecondition => 412,
        tonic::Code::OutOfRange => 400,
        _ => 500,
    };
    (code, status.message().to_string())
}

// ---------------------------------------------------------------------------
// JSON → Protobuf parsing helpers
// ---------------------------------------------------------------------------

fn bytes_from_json(value: &Value, field: &str) -> Vec<u8> {
    value
        .get(field)
        .and_then(|v| v.as_str())
        .map(|s| b64_decode(s))
        .unwrap_or_default()
}

fn i64_from_json(value: &Value, field: &str) -> i64 {
    value
        .get(field)
        .and_then(|v| {
            if let Some(n) = v.as_i64() {
                Some(n)
            } else if let Some(s) = v.as_str() {
                s.parse().ok()
            } else if let Some(n) = v.as_f64() {
                Some(n as i64)
            } else {
                None
            }
        })
        .unwrap_or(0)
}

fn bool_from_json(value: &Value, field: &str) -> bool {
    value.get(field).and_then(|v| v.as_bool()).unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Protobuf → JSON serialization helpers
// ---------------------------------------------------------------------------

fn kv_to_json(kv: &mvccpb::KeyValue) -> Value {
    // APISIX's lua-resty-etcd expects snake_case field names
    // (e.g. create_revision, mod_revision) — using camelCase here
    // causes `kvs_to_node` to see nil for those fields, which then
    // blows up the sort in v3_adapter.pagination with
    // "attempt to compare two nil values".
    serde_json::json!({
        "key": b64_encode(&kv.key),
        "create_revision": kv.create_revision.to_string(),
        "mod_revision": kv.mod_revision.to_string(),
        "version": kv.version.to_string(),
        "value": b64_encode(&kv.value),
        "lease": kv.lease.to_string(),
    })
}

fn header_to_json(header: &proto::ResponseHeader) -> Value {
    serde_json::json!({
        "clusterId": header.cluster_id.to_string(),
        "memberId": header.member_id.to_string(),
        "revision": header.revision.to_string(),
        "raftTerm": header.raft_term.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Range
// ---------------------------------------------------------------------------

fn parse_sort_order(v: &Value) -> i32 {
    match v.get("sortOrder").and_then(|s| s.as_str()) {
        Some("ASCEND") => 1,
        Some("DESCEND") => 2,
        _ => 0, // NONE
    }
}

fn parse_sort_target(v: &Value) -> i32 {
    match v.get("sortTarget").and_then(|s| s.as_str()) {
        Some("VERSION") => 1,
        Some("CREATE") => 2,
        Some("MOD") => 3,
        Some("VALUE") => 4,
        _ => 0, // KEY
    }
}

fn parse_range_request(v: &Value) -> proto::RangeRequest {
    proto::RangeRequest {
        key: bytes_from_json(v, "key"),
        range_end: bytes_from_json(v, "range_end"),
        limit: i64_from_json(v, "limit"),
        revision: i64_from_json(v, "revision"),
        sort_order: parse_sort_order(v),
        sort_target: parse_sort_target(v),
        serializable: bool_from_json(v, "serializable"),
        keys_only: bool_from_json(v, "keys_only"),
        count_only: bool_from_json(v, "count_only"),
        min_mod_revision: i64_from_json(v, "min_mod_revision"),
        max_mod_revision: i64_from_json(v, "max_mod_revision"),
        min_create_revision: i64_from_json(v, "min_create_revision"),
        max_create_revision: i64_from_json(v, "max_create_revision"),
    }
}

fn range_response_to_json(resp: &proto::RangeResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    // Only include "kvs" when there are entries. APISIX's lua-resty-etcd library
    // requires `kvs` to be absent (not an empty array) for the "Key not found"
    // case — an empty array causes `kvs_to_node(nil)` to crash with
    // "attempt to index local 'kvs' (a nil value)". When the backend has no
    // real entries, handle_list injects the init_dir placeholder so `kvs` is
    // non-empty and APISIX sees an empty directory.
    if !resp.kvs.is_empty() {
        let kvs: Vec<Value> = resp.kvs.iter().map(kv_to_json).collect();
        map.insert("kvs".to_string(), Value::Array(kvs));
    }
    map.insert("more".to_string(), Value::Bool(resp.more));
    map.insert(
        "count".to_string(),
        Value::String(resp.count.to_string()),
    );
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Put
// ---------------------------------------------------------------------------

fn parse_put_request(v: &Value) -> proto::PutRequest {
    proto::PutRequest {
        key: bytes_from_json(v, "key"),
        value: bytes_from_json(v, "value"),
        lease: i64_from_json(v, "lease"),
        prev_kv: bool_from_json(v, "prev_kv"),
        ignore_value: bool_from_json(v, "ignore_value"),
        ignore_lease: bool_from_json(v, "ignore_lease"),
    }
}

fn put_response_to_json(resp: &proto::PutResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    if let Some(ref prev_kv) = resp.prev_kv {
        map.insert("prevKv".to_string(), kv_to_json(prev_kv));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// DeleteRange
// ---------------------------------------------------------------------------

fn parse_delete_range_request(v: &Value) -> proto::DeleteRangeRequest {
    proto::DeleteRangeRequest {
        key: bytes_from_json(v, "key"),
        range_end: bytes_from_json(v, "range_end"),
        prev_kv: bool_from_json(v, "prev_kv"),
    }
}

fn delete_range_response_to_json(resp: &proto::DeleteRangeResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    map.insert(
        "deleted".to_string(),
        Value::String(resp.deleted.to_string()),
    );
    if !resp.prev_kvs.is_empty() {
        let kvs: Vec<Value> = resp.prev_kvs.iter().map(kv_to_json).collect();
        map.insert("prev_kvs".to_string(), Value::Array(kvs));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Txn
// ---------------------------------------------------------------------------

fn parse_txn_request(v: &Value) -> proto::TxnRequest {
    let compare = v
        .get("compare")
        .and_then(|c| c.as_array())
        .map(|arr| arr.iter().map(parse_compare).collect())
        .unwrap_or_default();
    let success = v
        .get("success")
        .and_then(|s| s.as_array())
        .map(|arr| arr.iter().map(parse_request_op).collect())
        .unwrap_or_default();
    let failure = v
        .get("failure")
        .and_then(|f| f.as_array())
        .map(|arr| arr.iter().map(parse_request_op).collect())
        .unwrap_or_default();
    proto::TxnRequest {
        compare,
        success,
        failure,
    }
}

fn parse_compare(v: &Value) -> proto::Compare {
    let result = match v.get("result").and_then(|s| s.as_str()) {
        Some("GREATER") => 1,
        Some("LESS") => 2,
        Some("NOT_EQUAL") => 3,
        _ => 0, // EQUAL
    };
    let target = match v.get("target").and_then(|s| s.as_str()) {
        Some("CREATE") => 1,
        Some("MOD") => 2,
        Some("VALUE") => 3,
        Some("LEASE") => 4,
        _ => 0, // VERSION
    };
    let key = bytes_from_json(v, "key");
    let range_end = bytes_from_json(v, "range_end");

    // target_union oneof
    use proto::compare::TargetUnion;
    let target_union = if v.get("version").is_some() {
        Some(TargetUnion::Version(i64_from_json(v, "version")))
    } else if v.get("create_revision").is_some() {
        Some(TargetUnion::CreateRevision(i64_from_json(v, "create_revision")))
    } else if v.get("mod_revision").is_some() {
        Some(TargetUnion::ModRevision(i64_from_json(v, "mod_revision")))
    } else if v.get("value").is_some() {
        Some(TargetUnion::Value(bytes_from_json(v, "value")))
    } else if v.get("lease").is_some() {
        Some(TargetUnion::Lease(i64_from_json(v, "lease")))
    } else {
        None
    };

    proto::Compare {
        result,
        target,
        key,
        range_end,
        target_union,
    }
}

fn parse_request_op(v: &Value) -> proto::RequestOp {
    use proto::request_op::Request;
    if v.get("requestRange").is_some() {
        proto::RequestOp {
            request: Some(Request::RequestRange(parse_range_request(
                v.get("requestRange").unwrap_or(&Value::Null),
            ))),
        }
    } else if v.get("requestPut").is_some() {
        proto::RequestOp {
            request: Some(Request::RequestPut(parse_put_request(
                v.get("requestPut").unwrap_or(&Value::Null),
            ))),
        }
    } else if v.get("requestDeleteRange").is_some() {
        proto::RequestOp {
            request: Some(Request::RequestDeleteRange(parse_delete_range_request(
                v.get("requestDeleteRange").unwrap_or(&Value::Null),
            ))),
        }
    } else if v.get("requestTxn").is_some() {
        proto::RequestOp {
            request: Some(Request::RequestTxn(parse_txn_request(
                v.get("requestTxn").unwrap_or(&Value::Null),
            ))),
        }
    } else {
        proto::RequestOp { request: None }
    }
}

fn txn_response_to_json(resp: &proto::TxnResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    map.insert("succeeded".to_string(), Value::Bool(resp.succeeded));
    if !resp.responses.is_empty() {
        let ops: Vec<Value> = resp.responses.iter().map(response_op_to_json).collect();
        map.insert("responses".to_string(), Value::Array(ops));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn response_op_to_json(op: &proto::ResponseOp) -> Value {
    use proto::response_op::Response;
    let mut map = serde_json::Map::new();
    if let Some(ref response) = op.response {
        match response {
            Response::ResponseRange(r) => {
                map.insert(
                    "responseRange".to_string(),
                    serde_json::from_str(&range_response_to_json(r)).unwrap_or(Value::Null),
                );
            }
            Response::ResponsePut(p) => {
                map.insert(
                    "responsePut".to_string(),
                    serde_json::from_str(&put_response_to_json(p)).unwrap_or(Value::Null),
                );
            }
            Response::ResponseDeleteRange(d) => {
                map.insert(
                    "responseDeleteRange".to_string(),
                    serde_json::from_str(&delete_range_response_to_json(d))
                        .unwrap_or(Value::Null),
                );
            }
            Response::ResponseTxn(t) => {
                map.insert(
                    "responseTxn".to_string(),
                    serde_json::from_str(&txn_response_to_json(t)).unwrap_or(Value::Null),
                );
            }
        }
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

fn parse_compaction_request(v: &Value) -> proto::CompactionRequest {
    proto::CompactionRequest {
        revision: i64_from_json(v, "revision"),
        physical: bool_from_json(v, "physical"),
    }
}

fn compaction_response_to_json(resp: &proto::CompactionResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

fn parse_lease_grant_request(v: &Value) -> proto::LeaseGrantRequest {
    // Support both lowercase (etcd grpc-gateway) and uppercase (resty.etcd) field names.
    let ttl = i64_from_json(v, "ttl");
    let ttl = if ttl != 0 { ttl } else { i64_from_json(v, "TTL") };
    let id = i64_from_json(v, "id");
    let id = if id != 0 { id } else { i64_from_json(v, "ID") };
    proto::LeaseGrantRequest { ttl, id }
}

fn lease_grant_response_to_json(resp: &proto::LeaseGrantResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    // Return both lowercase (etcd grpc-gateway) and uppercase (resty.etcd) field names.
    let id_str = resp.id.to_string();
    map.insert("id".to_string(), Value::String(id_str.clone()));
    map.insert("ID".to_string(), Value::String(id_str));
    let ttl_str = resp.ttl.to_string();
    map.insert("ttl".to_string(), Value::String(ttl_str.clone()));
    map.insert("TTL".to_string(), Value::String(ttl_str));
    if !resp.error.is_empty() {
        map.insert("error".to_string(), Value::String(resp.error.clone()));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn parse_lease_revoke_request(v: &Value) -> proto::LeaseRevokeRequest {
    proto::LeaseRevokeRequest {
        id: i64_from_json(v, "id"),
    }
}

fn lease_revoke_response_to_json(resp: &proto::LeaseRevokeResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn parse_lease_time_to_live_request(v: &Value) -> proto::LeaseTimeToLiveRequest {
    proto::LeaseTimeToLiveRequest {
        id: i64_from_json(v, "id"),
        keys: bool_from_json(v, "keys"),
    }
}

fn lease_time_to_live_response_to_json(resp: &proto::LeaseTimeToLiveResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    map.insert("id".to_string(), Value::String(resp.id.to_string()));
    map.insert("ttl".to_string(), Value::String(resp.ttl.to_string()));
    map.insert(
        "granted_ttl".to_string(),
        Value::String(resp.granted_ttl.to_string()),
    );
    if !resp.keys.is_empty() {
        let keys: Vec<Value> = resp
            .keys
            .iter()
            .map(|k| Value::String(b64_encode(k)))
            .collect();
        map.insert("keys".to_string(), Value::Array(keys));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn lease_keepalive_response_to_json(resp: &proto::LeaseKeepAliveResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    // Return both lowercase and uppercase field names for compatibility.
    let id_str = resp.id.to_string();
    map.insert("id".to_string(), Value::String(id_str.clone()));
    map.insert("ID".to_string(), Value::String(id_str));
    let ttl_str = resp.ttl.to_string();
    map.insert("ttl".to_string(), Value::String(ttl_str.clone()));
    map.insert("TTL".to_string(), Value::String(ttl_str));
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn lease_leases_response_to_json(resp: &proto::LeaseLeasesResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    if !resp.leases.is_empty() {
        let leases: Vec<Value> = resp
            .leases
            .iter()
            .map(|l| {
                serde_json::json!({
                    "id": l.id.to_string(),
                })
            })
            .collect();
        map.insert("leases".to_string(), Value::Array(leases));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

fn member_to_json(member: &proto::Member) -> Value {
    serde_json::json!({
        "id": member.id.to_string(),
        "name": member.name,
        "peerURLs": member.peer_ur_ls,
        "clientURLs": member.client_ur_ls,
        "isLearner": member.is_learner,
    })
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

/// Parse a WatchCreateRequest from JSON.
/// APISIX sends snake_case field names.
pub(crate) fn parse_watch_create_request(value: &Value) -> proto::WatchCreateRequest {
    // etcd's grpc-gateway serializes the WatchRequest oneof under the
    // snake_case name `create_request` — this is what lua-resty-etcd/APISIX
    // send (`{"create_request": {...}}`). Accept the camelCase spelling and a
    // bare create request body as fallbacks.
    let req = value
        .get("create_request")
        .or_else(|| value.get("createRequest"))
        .unwrap_or(value);

    proto::WatchCreateRequest {
        key: bytes_from_json(req, "key"),
        range_end: bytes_from_json(req, "range_end"),
        start_revision: i64_from_json(req, "start_revision"),
        progress_notify: bool_from_json(req, "progress_notify"),
        filters: req
            .get("filters")
            .and_then(|f| f.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| match v.as_str() {
                        Some("NOPUT") => Some(0),
                        Some("NODELETE") => Some(1),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default(),
        prev_kv: bool_from_json(req, "prev_kv") || bool_from_json(req, "prevKv"),
        watch_id: i64_from_json(req, "watch_id"),
        fragment: bool_from_json(req, "fragment"),
    }
}

// ---------------------------------------------------------------------------

fn member_list_response_to_json(resp: &proto::MemberListResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    if !resp.members.is_empty() {
        let members: Vec<Value> = resp.members.iter().map(member_to_json).collect();
        map.insert("members".to_string(), Value::Array(members));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

fn status_response_to_json(resp: &proto::StatusResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    map.insert(
        "version".to_string(),
        Value::String(resp.version.clone()),
    );
    map.insert(
        "dbSize".to_string(),
        Value::String(resp.db_size.to_string()),
    );
    map.insert(
        "leader".to_string(),
        Value::String(resp.leader.to_string()),
    );
    map.insert(
        "raftIndex".to_string(),
        Value::String(resp.raft_index.to_string()),
    );
    map.insert(
        "raftTerm".to_string(),
        Value::String(resp.raft_term.to_string()),
    );
    map.insert(
        "raftAppliedIndex".to_string(),
        Value::String(resp.raft_applied_index.to_string()),
    );
    if !resp.errors.is_empty() {
        let errors: Vec<Value> =
            resp.errors.iter().map(|e| Value::String(e.clone())).collect();
        map.insert("errors".to_string(), Value::Array(errors));
    }
    map.insert(
        "dbSizeInUse".to_string(),
        Value::String(resp.db_size_in_use.to_string()),
    );
    map.insert("isLearner".to_string(), Value::Bool(resp.is_learner));
    if !resp.storage_version.is_empty() {
        map.insert(
            "storageVersion".to_string(),
            Value::String(resp.storage_version.clone()),
        );
    }
    map.insert(
        "dbSizeQuota".to_string(),
        Value::String(resp.db_size_quota.to_string()),
    );
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn defragment_response_to_json(resp: &proto::DefragmentResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn hash_response_to_json(resp: &proto::HashResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    map.insert("hash".to_string(), Value::Number(resp.hash.into()));
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn snapshot_response_to_json(resp: &proto::SnapshotResponse) -> String {
    let mut map = serde_json::Map::new();
    if let Some(ref header) = resp.header {
        map.insert("header".to_string(), header_to_json(header));
    }
    map.insert(
        "remainingBytes".to_string(),
        Value::String(resp.remaining_bytes.to_string()),
    );
    map.insert(
        "blob".to_string(),
        Value::String(b64_encode(&resp.blob)),
    );
    if !resp.version.is_empty() {
        map.insert(
            "version".to_string(),
            Value::String(resp.version.clone()),
        );
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

// watch_response_to_json and event_to_json are defined earlier in this file.
// Do not redefine them here to avoid duplicate definitions.

#[cfg(test)]
mod tests {
    use super::*;

    /// lua-resty-etcd (and thus APISIX) sends the watcher options wrapped in a
    /// snake_case `create_request` object. Reading the camelCase `createRequest`
    /// key only made every field parse as empty, producing a watch on an empty
    /// key that never delivered events.
    #[test]
    fn parse_watch_create_request_snake_case_wrapper() {
        let body = serde_json::json!({
            "create_request": {
                "key": b64_encode(b"/apisix/"),
                "range_end": b64_encode(b"/apisix0"),
                "start_revision": "12",
                "progress_notify": true,
                "filters": ["NODELETE"],
            }
        });
        let req = parse_watch_create_request(&body);
        assert_eq!(req.key, b"/apisix/");
        assert_eq!(req.range_end, b"/apisix0");
        assert_eq!(req.start_revision, 12);
        assert!(req.progress_notify);
        assert_eq!(req.filters, vec![1]); // NODELETE
    }

    #[test]
    fn parse_watch_create_request_camel_case_wrapper() {
        let body = serde_json::json!({
            "createRequest": {
                "key": b64_encode(b"/foo"),
                "start_revision": 3,
            }
        });
        let req = parse_watch_create_request(&body);
        assert_eq!(req.key, b"/foo");
        assert_eq!(req.start_revision, 3);
    }

    #[test]
    fn parse_watch_create_request_bare_body() {
        let body = serde_json::json!({
            "key": b64_encode(b"/bar"),
            "range_end": b64_encode(b"/bas"),
        });
        let req = parse_watch_create_request(&body);
        assert_eq!(req.key, b"/bar");
        assert_eq!(req.range_end, b"/bas");
    }

    /// etcd's grpc-gateway emits snake_case (`prev_kv`); lua-resty-etcd reads
    /// `event.prev_kv` when handling delete events with prev_kv enabled.
    #[test]
    fn event_to_json_uses_snake_case_prev_kv() {
        let event = mvccpb::Event {
            r#type: mvccpb::event::EventType::Delete.into(),
            kv: Some(mvccpb::KeyValue {
                key: b"/apisix/routes/1".to_vec(),
                value: Vec::new(),
                create_revision: 1,
                mod_revision: 5,
                version: 1,
                lease: 0,
            }),
            prev_kv: Some(mvccpb::KeyValue {
                key: b"/apisix/routes/1".to_vec(),
                value: b"old".to_vec(),
                create_revision: 1,
                mod_revision: 2,
                version: 1,
                lease: 0,
            }),
        };
        let json = event_to_json(&event);
        assert!(json.get("prev_kv").is_some(), "prev_kv missing: {json}");
        assert!(json.get("prevKv").is_none(), "camelCase prevKv present: {json}");
        assert_eq!(
            json["prev_kv"]["value"],
            Value::String(b64_encode(b"old"))
        );
    }

    /// Guard: mirroring etcd's grpc-gateway (protojson), the `prev_kv` field is
    /// omitted entirely when the watch did not request it — otherwise events
    /// carry a redundant copy of the previous value.
    #[test]
    fn event_to_json_omits_prev_kv_when_absent() {
        let event = mvccpb::Event {
            r#type: mvccpb::event::EventType::Put.into(),
            kv: Some(mvccpb::KeyValue {
                key: b"/apisix/routes/1".to_vec(),
                value: b"new".to_vec(),
                create_revision: 1,
                mod_revision: 5,
                version: 1,
                lease: 0,
            }),
            prev_kv: None,
        };
        let json = event_to_json(&event);
        assert!(json.get("prev_kv").is_none(), "prev_kv must be omitted: {json}");
        assert!(json.get("prevKv").is_none(), "camelCase prevKv present: {json}");
    }

    /// Guard: the snake_case `prev_kv` flag from REST watch requests must reach
    /// the WatchCreateRequest (camelCase accepted as a fallback).
    #[test]
    fn parse_watch_create_request_reads_prev_kv_flag() {
        let body: Value = serde_json::from_str(
            r#"{"create_request": {"key": "L2Zvbw==", "prev_kv": true}}"#,
        )
        .unwrap();
        assert!(parse_watch_create_request(&body).prev_kv);

        let body: Value = serde_json::from_str(
            r#"{"create_request": {"key": "L2Zvbw==", "prevKv": true}}"#,
        )
        .unwrap();
        assert!(parse_watch_create_request(&body).prev_kv);

        let body: Value =
            serde_json::from_str(r#"{"create_request": {"key": "L2Zvbw=="}}"#).unwrap();
        assert!(!parse_watch_create_request(&body).prev_kv);
    }
}

