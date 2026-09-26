use tonic::{Request, Response, Status};

use crate::backend::{Backend, BackendError, KeyValue as BackendKv};
use crate::proto::etcdserverpb::kv_server::Kv;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb;

use super::KvBridge;

/// The key the Kubernetes API server uses for its compaction state.
const COMPACT_REV_KEY: &str = "compact_rev_key";
/// Internal storage key to avoid collision with rhino's own compact_rev_key.
const COMPACT_REV_KEY_API: &str = "compact_rev_key_apiserver";

fn response_header(rev: i64) -> Option<ResponseHeader> {
    Some(ResponseHeader {
        revision: rev,
        ..Default::default()
    })
}

fn to_proto_kv(kv: &BackendKv) -> mvccpb::KeyValue {
    mvccpb::KeyValue {
        key: kv.key.as_bytes().to_vec(),
        value: kv.value.clone(),
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        lease: kv.lease,
    }
}

fn to_proto_kvs(kv: Option<&BackendKv>) -> Vec<mvccpb::KeyValue> {
    kv.map(|kv| vec![to_proto_kv(kv)]).unwrap_or_default()
}

fn backend_err_to_status(e: BackendError) -> Status {
    match e {
        BackendError::KeyExists => Status::failed_precondition(e.to_string()),
        BackendError::Compacted => Status::out_of_range(e.to_string()),
        BackendError::FutureRev => Status::out_of_range(e.to_string()),
        BackendError::Internal(msg) => Status::internal(msg),
    }
}

/// Detect the Create transaction pattern:
/// Compare: single MOD == 0 or VERSION == 0
/// Success: Put, optionally followed by Range ops
/// Failure: empty
fn is_create(txn: &TxnRequest) -> Option<&PutRequest> {
    if txn.compare.len() == 1
        && txn.compare[0].result() == compare::CompareResult::Equal
        && ((txn.compare[0].target() == compare::CompareTarget::Mod
            && txn.compare[0].mod_revision() == 0)
            || (txn.compare[0].target() == compare::CompareTarget::Version
                && txn.compare[0].version() == 0))
        && txn.failure.is_empty()
        && txn.success.len() >= 1
    {
        // First op must be a Put
        let put = txn.success[0].request.as_ref().and_then(|r| match r {
            request_op::Request::RequestPut(put) => Some(put),
            _ => None,
        })?;
        // All remaining ops must be Range requests
        for op in &txn.success[1..] {
            match op.request.as_ref() {
                Some(request_op::Request::RequestRange(_)) => {}
                _ => return None,
            }
        }
        Some(put)
    } else {
        None
    }
}

/// Detect the Update transaction pattern:
/// Compare: single MOD == expected_rev
/// Success: Put, optionally followed by Range ops
/// Failure: single Range
fn is_update(txn: &TxnRequest) -> Option<(i64, &[u8], &[u8], i64)> {
    if txn.compare.len() == 1
        && txn.compare[0].target() == compare::CompareTarget::Mod
        && txn.compare[0].result() == compare::CompareResult::Equal
        && txn.success.len() >= 1
        && txn.failure.len() == 1
    {
        let put = txn.success[0].request.as_ref().and_then(|r| match r {
            request_op::Request::RequestPut(put) => Some(put),
            _ => None,
        })?;
        // All remaining success ops must be Range requests
        for op in &txn.success[1..] {
            match op.request.as_ref() {
                Some(request_op::Request::RequestRange(_)) => {}
                _ => return None,
            }
        }
        // Verify failure is a Range
        txn.failure[0].request.as_ref().and_then(|r| match r {
            request_op::Request::RequestRange(_) => Some(()),
            _ => None,
        })?;
        Some((
            txn.compare[0].mod_revision(),
            &txn.compare[0].key,
            &put.value,
            put.lease,
        ))
    } else {
        None
    }
}

/// Detect the Delete transaction pattern. Two forms:
/// 1) No compare, success = [Range, DeleteRange], failure = empty
/// 2) Compare: MOD == rev, success = [DeleteRange], failure = [Range]
fn is_delete(txn: &TxnRequest) -> Option<(i64, &[u8])> {
    // Form 1: unconditional delete
    if txn.compare.is_empty() && txn.failure.is_empty() && txn.success.len() == 2 {
        let is_range = txn.success[0]
            .request
            .as_ref()
            .is_some_and(|r| matches!(r, request_op::Request::RequestRange(_)));
        let del = txn.success[1].request.as_ref().and_then(|r| match r {
            request_op::Request::RequestDeleteRange(del) => Some(del),
            _ => None,
        });
        if is_range && let Some(del) = del {
            return Some((0, &del.key));
        }
    }

    // Form 2: conditional delete
    if txn.compare.len() == 1
        && txn.compare[0].target() == compare::CompareTarget::Mod
        && txn.compare[0].result() == compare::CompareResult::Equal
        && txn.success.len() == 1
        && txn.failure.len() == 1
    {
        let del = txn.success[0].request.as_ref().and_then(|r| match r {
            request_op::Request::RequestDeleteRange(del) => Some(del),
            _ => None,
        })?;
        txn.failure[0].request.as_ref().and_then(|r| match r {
            request_op::Request::RequestRange(_) => Some(()),
            _ => None,
        })?;
        return Some((txn.compare[0].mod_revision(), &del.key));
    }

    None
}

/// Detect the Compact transaction pattern used by the Kubernetes API server:
/// Compare: VERSION == N on key "compact_rev_key"
/// Success: single Put
/// Failure: single Range
fn is_compact(txn: &TxnRequest) -> Option<(i64, &[u8])> {
    if txn.compare.len() == 1
        && txn.compare[0].target() == compare::CompareTarget::Version
        && txn.compare[0].result() == compare::CompareResult::Equal
        && txn.compare[0].key == COMPACT_REV_KEY.as_bytes()
        && txn.success.len() == 1
        && txn.failure.len() == 1
    {
        let put = txn.success[0].request.as_ref().and_then(|r| match r {
            request_op::Request::RequestPut(put) => Some(put),
            _ => None,
        })?;
        txn.failure[0].request.as_ref().and_then(|r| match r {
            request_op::Request::RequestRange(_) => Some(()),
            _ => None,
        })?;
        Some((txn.compare[0].version(), &put.value))
    } else {
        None
    }
}

/// Redirect compact_rev_key to internal key to avoid collision.
fn redirect_key(key: &str) -> &str {
    if key == COMPACT_REV_KEY {
        COMPACT_REV_KEY_API
    } else {
        key
    }
}

/// Encode a version + value for compact_rev_key storage.
fn encode_version(version: i64, value: &[u8]) -> Vec<u8> {
    format!("{}|", version)
        .into_bytes()
        .into_iter()
        .chain(value.iter().copied())
        .collect()
}

/// Decode a version + value from compact_rev_key storage.
fn decode_version(data: &[u8]) -> (i64, Vec<u8>) {
    if let Some(pos) = data.iter().position(|&b| b == b'|') {
        let version_str = String::from_utf8_lossy(&data[..pos]);
        let version = version_str.parse::<i64>().unwrap_or(0);
        let value = data[pos + 1..].to_vec();
        (version, value)
    } else {
        // Old format: just the version number
        let version = String::from_utf8_lossy(data).parse::<i64>().unwrap_or(0);
        (version, b"0".to_vec())
    }
}

/// Helper to access fields from a Compare's target_union.
trait CompareExt {
    fn mod_revision(&self) -> i64;
    fn version(&self) -> i64;
}

impl CompareExt for Compare {
    fn mod_revision(&self) -> i64 {
        match &self.target_union {
            Some(compare::TargetUnion::ModRevision(r)) => *r,
            _ => 0,
        }
    }

    fn version(&self) -> i64 {
        match &self.target_union {
            Some(compare::TargetUnion::Version(v)) => *v,
            _ => 0,
        }
    }
}

/// Extract Range requests from success ops after the first (Put) op.
fn extract_trailing_ranges(success: &[RequestOp]) -> Vec<RangeRequest> {
    success[1..]
        .iter()
        .filter_map(|op| match op.request.as_ref() {
            Some(request_op::Request::RequestRange(r)) => Some(r.clone()),
            _ => None,
        })
        .collect()
}

impl<B: Backend> KvBridge<B> {
    /// Detect the single-key lookup pattern used by lua-resty-etcd's readdir:
    /// range_end is key with its last byte incremented by 1.
    ///
    /// To distinguish from a prefix query (which uses the same range_end pattern),
    /// we also require that the byte before the last byte of key is '/'.
    /// This works because APISIX builds single-key paths as "/<resource>/<id>",
    /// while prefix paths are just "/<resource>".
    ///
    /// Examples:
    ///   Single key: key="/apisix/routes/1" -> range_end="/apisix/routes/2"  (matched)
    ///   Prefix:     key="/apisix/routes"  -> range_end="/apisix/routet"  (not matched)
    fn is_single_key_lookup(key: &[u8], range_end: &[u8]) -> bool {
        if key.len() != range_end.len() || key.len() < 2 {
            return false;
        }
        // The byte before the last must be '/' to distinguish from prefix queries
        if key[key.len() - 2] != b'/' {
            return false;
        }
        // All bytes except the last must be equal
        if key[..key.len() - 1] != range_end[..range_end.len() - 1] {
            return false;
        }
        // The last byte of range_end must be last byte of key + 1
        let last_key = key[key.len() - 1];
        let last_range = range_end[range_end.len() - 1];
        last_range == last_key.wrapping_add(1)
    }

    async fn handle_range(&self, r: &RangeRequest) -> Result<RangeResponse, Status> {
        let key_str = String::from_utf8_lossy(&r.key);
        let range_end_empty = r.range_end.is_empty();
        let is_single = range_end_empty && !key_str.ends_with('/')
            || !range_end_empty && Self::is_single_key_lookup(&r.key, &r.range_end);
        tracing::info!("handle_range: key={:?}, range_end={:?}, is_single={}",
            key_str, String::from_utf8_lossy(&r.range_end), is_single);

        // Distinguish single key lookup vs prefix query:
        // - If range_end is empty AND key does NOT end with '/', it's a single key lookup
        //   e.g. key="/apisix/routes/1" -> handle_get
        // - If range_end is key with last byte + 1 (lua-resty-etcd readdir pattern),
        //   it's also a single key lookup
        //   e.g. key="/apisix/routes/1", range_end="/apisix/routes/2" -> handle_get
        // - If range_end is empty AND key ends with '/', it's a prefix query
        //   e.g. key="/apisix/routes/" -> handle_list
        // - Otherwise, it's a range/prefix query
        if is_single {
            tracing::info!("handle_range: routing to handle_get (single key)");
            self.handle_get(r).await
        } else {
            tracing::info!("handle_range: routing to handle_list (prefix/range query)");
            self.handle_list(r).await
        }
    }

    /// Get a single key (not used by APISIX REST gateway, but kept for completeness).
    #[allow(dead_code)]
    async fn handle_get(&self, r: &RangeRequest) -> Result<RangeResponse, Status> {
        let raw_key = String::from_utf8_lossy(&r.key);
        let key = redirect_key(&raw_key);
        let range_end = String::from_utf8_lossy(&r.range_end);

        let (rev, kv) = self
            .backend
            .get(key, &range_end, r.limit, r.revision, r.keys_only)
            .await
            .map_err(backend_err_to_status)?;

        let kvs = if key != raw_key.as_ref() {
            // Redirected key — rewrite key and decode version in response
            kv.as_ref()
                .map(|kv| {
                    let (version, value) = decode_version(&kv.value);
                    let mut proto = to_proto_kv(kv);
                    proto.key = raw_key.as_bytes().to_vec();
                    proto.value = value;
                    proto.version = version;
                    vec![proto]
                })
                .unwrap_or_default()
        } else {
            to_proto_kvs(kv.as_ref())
        };
        let count = kvs.len() as i64;
        Ok(RangeResponse {
            header: response_header(rev),
            kvs,
            count,
            more: false,
        })
    }

    async fn handle_list(&self, r: &RangeRequest) -> Result<RangeResponse, Status> {
        let range_end = &r.range_end;
        let key_str = String::from_utf8_lossy(&r.key);

        // Calculate the prefix for the range query.
        // When range_end is empty (APISIX style), use the key as the prefix.
        // When range_end is provided (standard etcd v3), calculate prefix from range_end.
        let prefix = if range_end.is_empty() {
            // APISIX sends empty range_end for prefix queries.
            // Use the key as the prefix (append '/' if not already present).
            let mut s = key_str.to_string();
            if !s.ends_with('/') {
                s.push('/');
            }
            s
        } else {
            let mut p = range_end.clone();
            if let Some(last) = p.last_mut() {
                *last = last.wrapping_sub(1);
            }
            let mut s = String::from_utf8_lossy(&p).to_string();
            if !s.ends_with('/') {
                s.push('/');
            }
            s
        };
        let start = key_str.to_string();
        tracing::info!("handle_list: key={:?}, range_end={:?}, prefix={:?}, start={}", r.key, range_end, prefix, start);
        let revision = if r.revision > 0 { r.revision } else { 0 };

        if r.count_only {
            let (rev, count) = self
                .backend
                .count(&prefix, &start, revision)
                .await
                .map_err(backend_err_to_status)?;

            return Ok(RangeResponse {
                header: response_header(rev),
                kvs: vec![],
                count,
                more: false,
            });
        }

        let limit = if r.limit > 0 { r.limit + 1 } else { 0 };
        let (rev, mut kvs) = self
            .backend
            .list(&prefix, &start, limit, revision, r.keys_only)
            .await
            .map_err(backend_err_to_status)?;

        // APISIX's lua-resty-etcd expects `kvs[1]` to exist for directory
        // listings — an empty `kvs` array crashes with "attempt to index
        // local 'kvs' (a nil value)". For known APISIX resource directories,
        // inject the init_dir placeholder when the filtered list is empty so
        // APISIX sees an empty directory node (count still excludes it).
        const INIT_DIR_PREFIXES: &[&str] = &[
            "/apisix/services/",
            "/apisix/routes/",
            "/apisix/upstreams/",
            "/apisix/consumer_groups/",
            "/apisix/plugin_configs/",
            "/apisix/secrets/",
            "/apisix/protos/",
            "/apisix/global_rules/",
            "/apisix/ssls/",
            "/apisix/consumers/",
        ];
        let needs_init_dir = INIT_DIR_PREFIXES.iter().any(|p| prefix.starts_with(p));
        if kvs.is_empty() && needs_init_dir {
            // init_dir marker 的 key 就是 prefix 本身（带尾部 /），如 /apisix/services/
            if let Ok((_, Some(init_kv))) = self.backend.get(&prefix, "", 1, 0, false).await {
                if init_kv.value == b"init_dir" {
                    tracing::info!("handle_list: injecting init_dir for empty dir {}", prefix);
                    kvs.push(init_kv);
                }
            }
        }

        let proto_kvs: Vec<mvccpb::KeyValue> = kvs.iter().map(to_proto_kv).collect();
        let count = proto_kvs.len() as i64;

        // count excludes the init_dir placeholder (real entries only)
        let real_count = if kvs.len() == 1 && kvs[0].value == b"init_dir" {
            0
        } else {
            count
        };

        let (kvs, more, count) = if r.limit > 0 && count > r.limit {
            let trimmed = proto_kvs[..r.limit as usize].to_vec();
            let rev_for_count = if revision == 0 { rev } else { revision };
            let (_, total_count) = self
                .backend
                .count(&prefix, &start, rev_for_count)
                .await
                .map_err(backend_err_to_status)?;
            (trimmed, true, total_count)
        } else {
            (proto_kvs, false, real_count)
        };

        Ok(RangeResponse {
            header: response_header(rev),
            kvs,
            count,
            more,
        })
    }

    async fn handle_create(
        &self,
        put: &PutRequest,
        trailing_ranges: &[RangeRequest],
    ) -> Result<TxnResponse, Status> {
        if put.ignore_lease {
            return Err(Status::unimplemented("ignoreLease is not implemented"));
        }
        if put.ignore_value {
            return Err(Status::unimplemented("ignoreValue is not implemented"));
        }
        if put.prev_kv {
            return Err(Status::unimplemented("prevKv is not implemented"));
        }

        let key = String::from_utf8_lossy(&put.key);

        match self.backend.create(&key, &put.value, put.lease).await {
            Ok(rev) => {
                let mut responses = vec![ResponseOp {
                    response: Some(response_op::Response::ResponsePut(PutResponse {
                        header: response_header(rev),
                        prev_kv: None,
                    })),
                }];
                for r in trailing_ranges {
                    let range_resp = self.handle_range(r).await?;
                    responses.push(ResponseOp {
                        response: Some(response_op::Response::ResponseRange(range_resp)),
                    });
                }
                Ok(TxnResponse {
                    header: response_header(rev),
                    succeeded: true,
                    responses,
                })
            }
            Err(BackendError::KeyExists) => {
                let rev = self
                    .backend
                    .current_revision()
                    .await
                    .map_err(backend_err_to_status)?;
                Ok(TxnResponse {
                    header: response_header(rev),
                    succeeded: false,
                    responses: vec![],
                })
            }
            Err(e) => Err(backend_err_to_status(e)),
        }
    }

    async fn handle_update(
        &self,
        rev: i64,
        key: &[u8],
        value: &[u8],
        lease: i64,
        trailing_ranges: &[RangeRequest],
    ) -> Result<TxnResponse, Status> {
        let key_str = String::from_utf8_lossy(key);

        if rev == 0 {
            // rev==0 means "create if not exists, get current if exists"
            match self.backend.create(&key_str, value, lease).await {
                Ok(new_rev) => {
                    let mut responses = vec![ResponseOp {
                        response: Some(response_op::Response::ResponsePut(PutResponse {
                            header: response_header(new_rev),
                            prev_kv: None,
                        })),
                    }];
                    for r in trailing_ranges {
                        let range_resp = self.handle_range(r).await?;
                        responses.push(ResponseOp {
                            response: Some(response_op::Response::ResponseRange(range_resp)),
                        });
                    }
                    return Ok(TxnResponse {
                        header: response_header(new_rev),
                        succeeded: true,
                        responses,
                    });
                }
                Err(BackendError::KeyExists) => {
                    // Key already exists; fall through to get current value
                    let (current_rev, kv) = self
                        .backend
                        .get(&key_str, "", 1, 0, false)
                        .await
                        .map_err(backend_err_to_status)?;
                    let kvs = to_proto_kvs(kv.as_ref());
                    return Ok(TxnResponse {
                        header: response_header(current_rev),
                        succeeded: false,
                        responses: vec![ResponseOp {
                            response: Some(response_op::Response::ResponseRange(RangeResponse {
                                header: response_header(current_rev),
                                kvs,
                                count: 1,
                                more: false,
                            })),
                        }],
                    });
                }
                Err(e) => return Err(backend_err_to_status(e)),
            }
        }

        let (new_rev, kv, ok) = self
            .backend
            .update(&key_str, value, rev, lease)
            .await
            .map_err(backend_err_to_status)?;

        if ok {
            let mut responses = vec![ResponseOp {
                response: Some(response_op::Response::ResponsePut(PutResponse {
                    header: response_header(new_rev),
                    prev_kv: None,
                })),
            }];
            for r in trailing_ranges {
                let range_resp = self.handle_range(r).await?;
                responses.push(ResponseOp {
                    response: Some(response_op::Response::ResponseRange(range_resp)),
                });
            }
            Ok(TxnResponse {
                header: response_header(new_rev),
                succeeded: true,
                responses,
            })
        } else {
            let kvs = to_proto_kvs(kv.as_ref());
            let count = kvs.len() as i64;
            Ok(TxnResponse {
                header: response_header(new_rev),
                succeeded: false,
                responses: vec![ResponseOp {
                    response: Some(response_op::Response::ResponseRange(RangeResponse {
                        header: response_header(new_rev),
                        kvs,
                        count,
                        more: false,
                    })),
                }],
            })
        }
    }

    /// Handle the compact_rev_key Txn pattern from the Kubernetes API server.
    /// Version-checks the stored compact key, updates it on match.
    async fn handle_compact_txn(
        &self,
        expected_version: i64,
        new_value: &[u8],
    ) -> Result<TxnResponse, Status> {
        let key = COMPACT_REV_KEY_API;

        // Get current state
        let (current_rev, existing) = self
            .backend
            .get(key, "", 1, 0, false)
            .await
            .map_err(backend_err_to_status)?;

        let (current_version, current_value) = existing
            .as_ref()
            .map(|kv| decode_version(&kv.value))
            .unwrap_or((0, b"0".to_vec()));

        if current_version == expected_version {
            // Version matches — update with incremented version
            let new_version = current_version + 1;
            let encoded = encode_version(new_version, new_value);

            let rev = if expected_version == 0 {
                // First time: create
                match self.backend.create(key, &encoded, 0).await {
                    Ok(rev) => rev,
                    Err(BackendError::KeyExists) => {
                        // Race: someone else created it. Get and update.
                        let (_, kv) = self
                            .backend
                            .get(key, "", 1, 0, false)
                            .await
                            .map_err(backend_err_to_status)?;
                        let mod_rev = kv.as_ref().map(|k| k.mod_revision).unwrap_or(0);
                        let (rev, _, _) = self
                            .backend
                            .update(key, &encoded, mod_rev, 0)
                            .await
                            .map_err(backend_err_to_status)?;
                        rev
                    }
                    Err(e) => return Err(backend_err_to_status(e)),
                }
            } else {
                let mod_rev = existing.as_ref().map(|kv| kv.mod_revision).unwrap_or(0);
                let (rev, _, _) = self
                    .backend
                    .update(key, &encoded, mod_rev, 0)
                    .await
                    .map_err(backend_err_to_status)?;
                rev
            };

            // Kine returns no ResponseOps on compact success
            Ok(TxnResponse {
                header: response_header(rev),
                succeeded: true,
                responses: vec![],
            })
        } else {
            // Version mismatch — return current state
            let kv = existing.as_ref().map(|kv| {
                let mut proto = to_proto_kv(kv);
                // Rewrite key back to what the client expects
                proto.key = COMPACT_REV_KEY.as_bytes().to_vec();
                proto.value = current_value.clone();
                proto.version = current_version;
                proto
            });
            let kvs = kv.into_iter().collect::<Vec<_>>();

            // Kine returns empty inner header and Count: 1
            Ok(TxnResponse {
                header: response_header(current_rev),
                succeeded: false,
                responses: vec![ResponseOp {
                    response: Some(response_op::Response::ResponseRange(RangeResponse {
                        header: Some(ResponseHeader::default()),
                        kvs,
                        count: 1,
                        more: false,
                    })),
                }],
            })
        }
    }

    async fn handle_delete(&self, key: &[u8], revision: i64) -> Result<TxnResponse, Status> {
        let key_str = String::from_utf8_lossy(key);

        let (rev, kv, ok) = self
            .backend
            .delete(&key_str, revision)
            .await
            .map_err(backend_err_to_status)?;

        let kvs = to_proto_kvs(kv.as_ref());

        if ok {
            Ok(TxnResponse {
                header: response_header(rev),
                succeeded: true,
                responses: vec![ResponseOp {
                    response: Some(response_op::Response::ResponseDeleteRange(
                        DeleteRangeResponse {
                            header: response_header(rev),
                            prev_kvs: kvs.clone(),
                            deleted: kvs.len() as i64,
                        },
                    )),
                }],
            })
        } else {
            let count = kvs.len() as i64;
            Ok(TxnResponse {
                header: response_header(rev),
                succeeded: false,
                responses: vec![ResponseOp {
                    response: Some(response_op::Response::ResponseRange(RangeResponse {
                        header: response_header(rev),
                        kvs,
                        count,
                        more: false,
                    })),
                }],
            })
        }
    }
}

#[tonic::async_trait]
impl<B: Backend> Kv for KvBridge<B> {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let r = request.into_inner();

        // Reject unsupported Range options (matching kine)
        if r.max_create_revision != 0 {
            return Err(Status::unimplemented(
                "maxCreateRevision is not implemented",
            ));
        }
        if r.sort_order != 0 {
            return Err(Status::unimplemented("sortOrder is not implemented"));
        }
        if r.sort_target != 0 {
            return Err(Status::unimplemented("sortTarget is not implemented"));
        }
        if r.serializable {
            return Err(Status::unimplemented("serializable is not implemented"));
        }
        if r.min_mod_revision != 0 {
            return Err(Status::unimplemented("minModRevision is not implemented"));
        }
        if r.min_create_revision != 0 {
            return Err(Status::unimplemented(
                "minCreateRevision is not implemented",
            ));
        }
        if r.max_mod_revision != 0 {
            return Err(Status::unimplemented("maxModRevision is not implemented"));
        }

        let resp = self.handle_range(&r).await?;
        Ok(Response::new(resp))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let put = request.into_inner();

        // Reject unsupported Put options (matching kine)
        if put.ignore_value {
            return Err(Status::unimplemented("ignoreValue is not implemented"));
        }
        if put.ignore_lease {
            return Err(Status::unimplemented("ignoreLease is not implemented"));
        }

        let raw_key = String::from_utf8_lossy(&put.key);
        let key = redirect_key(&raw_key);

        // Try create first; if key exists, update unconditionally.
        match self.backend.create(&key, &put.value, put.lease).await {
            Ok(rev) => Ok(Response::new(PutResponse {
                header: response_header(rev),
                prev_kv: None,
            })),
            Err(BackendError::KeyExists) => {
                // Get current revision so we can do an unconditional update.
                let (_, existing) = self
                    .backend
                    .get(&key, "", 1, 0, false)
                    .await
                    .map_err(backend_err_to_status)?;
                let mod_rev = existing.as_ref().map(|kv| kv.mod_revision).unwrap_or(0);
                let prev_kv = if put.prev_kv {
                    existing.as_ref().map(to_proto_kv)
                } else {
                    None
                };
                let (new_rev, _, _) = self
                    .backend
                    .update(&key, &put.value, mod_rev, put.lease)
                    .await
                    .map_err(backend_err_to_status)?;
                Ok(Response::new(PutResponse {
                    header: response_header(new_rev),
                    prev_kv,
                }))
            }
            Err(e) => Err(backend_err_to_status(e)),
        }
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let r = request.into_inner();
        let key = String::from_utf8_lossy(&r.key);

        if r.range_end.is_empty() {
            // Single key delete
            let (rev, prev_kv, ok) = self
                .backend
                .delete(&key, 0)
                .await
                .map_err(backend_err_to_status)?;

            let (deleted, prev_kvs) = if ok && prev_kv.is_some() {
                let pvs = if r.prev_kv {
                    prev_kv.iter().map(to_proto_kv).collect()
                } else {
                    vec![]
                };
                (1, pvs)
            } else {
                (0, vec![])
            };

            Ok(Response::new(DeleteRangeResponse {
                header: response_header(rev),
                deleted,
                prev_kvs,
            }))
        } else {
            // Prefix delete: derive prefix from range_end (same logic as handle_list)
            let prefix = {
                let mut p = r.range_end.clone();
                if let Some(last) = p.last_mut() {
                    *last = last.wrapping_sub(1);
                }
                let mut s = String::from_utf8_lossy(&p).to_string();
                if !s.ends_with('/') {
                    s.push('/');
                }
                s
            };

            let (last_rev, deleted, backend_prev_kvs) = self
                .backend
                .delete_prefix(&prefix)
                .await
                .map_err(backend_err_to_status)?;

            let prev_kvs = if r.prev_kv {
                backend_prev_kvs.iter().map(to_proto_kv).collect()
            } else {
                vec![]
            };

            Ok(Response::new(DeleteRangeResponse {
                header: response_header(last_rev),
                deleted,
                prev_kvs,
            }))
        }
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let txn = request.into_inner();

        if let Some(put) = is_create(&txn) {
            let put = put.clone();
            let trailing = extract_trailing_ranges(&txn.success);
            return Ok(Response::new(self.handle_create(&put, &trailing).await?));
        }
        if let Some((rev, key, value, lease)) = is_update(&txn) {
            let key = key.to_vec();
            let value = value.to_vec();
            let trailing = extract_trailing_ranges(&txn.success);
            return Ok(Response::new(
                self.handle_update(rev, &key, &value, lease, &trailing)
                    .await?,
            ));
        }
        if let Some((rev, key)) = is_delete(&txn) {
            let key = key.to_vec();
            return Ok(Response::new(self.handle_delete(&key, rev).await?));
        }
        if let Some((version, value)) = is_compact(&txn) {
            let value = value.to_vec();
            return Ok(Response::new(
                self.handle_compact_txn(version, &value).await?,
            ));
        }

        Err(Status::invalid_argument(
            "etcdserver: unsupported operations in txn request",
        ))
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let r = request.into_inner();
        let rev = self
            .backend
            .compact(r.revision)
            .await
            .map_err(backend_err_to_status)?;
        Ok(Response::new(CompactionResponse {
            header: response_header(rev),
        }))
    }
}
