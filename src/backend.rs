use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("key already exists")]
    KeyExists,
    #[error("revision has been compacted")]
    Compacted,
    #[error("revision is in the future")]
    FutureRev,
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: Vec<u8>,
    pub version: i64,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub lease: i64,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub delete: bool,
    pub create: bool,
    pub kv: KeyValue,
    pub prev_kv: Option<KeyValue>,
}

pub struct WatchResult {
    pub current_revision: i64,
    pub compact_revision: i64,
    pub events: mpsc::Receiver<Vec<Event>>,
}

pub type Result<T> = std::result::Result<T, BackendError>;

/// The core backend trait that database drivers must implement.
///
/// This mirrors kine's `Backend` interface. Each method corresponds to a
/// high-level etcd operation that the gRPC server layer dispatches to.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Start the backend, performing any initialization (schema creation, etc).
    async fn start(&self) -> Result<()>;

    /// Get a single key. Returns (current_revision, Option<kv>).
    async fn get(
        &self,
        key: &str,
        range_end: &str,
        limit: i64,
        revision: i64,
        keys_only: bool,
    ) -> Result<(i64, Option<KeyValue>)>;

    /// Create a new key. Returns the revision of the create.
    async fn create(&self, key: &str, value: &[u8], lease: i64) -> Result<i64>;

    /// Delete a key at the given revision. Returns (revision, prev_kv, succeeded).
    async fn delete(&self, key: &str, revision: i64) -> Result<(i64, Option<KeyValue>, bool)>;

    /// Delete all live keys matching a prefix in a single transaction.
    /// Returns (latest_revision, deleted_count, prev_kvs).
    async fn delete_prefix(&self, prefix: &str) -> Result<(i64, i64, Vec<KeyValue>)>;

    /// List keys matching prefix. Returns (current_revision, kvs).
    async fn list(
        &self,
        prefix: &str,
        start_key: &str,
        limit: i64,
        revision: i64,
        keys_only: bool,
    ) -> Result<(i64, Vec<KeyValue>)>;

    /// Count keys matching prefix. Returns (current_revision, count).
    async fn count(&self, prefix: &str, start_key: &str, revision: i64) -> Result<(i64, i64)>;

    /// Update a key at the given revision. Returns (revision, prev_kv, succeeded).
    async fn update(
        &self,
        key: &str,
        value: &[u8],
        revision: i64,
        lease: i64,
    ) -> Result<(i64, Option<KeyValue>, bool)>;

    /// Watch for changes on the byte range `[key, range_end)` starting from the
    /// given revision. An empty `range_end` watches a single key (with the
    /// legacy rhino convention that a key ending in `/` is a prefix); a
    /// range_end of `"\0"` watches all keys `>= key`. See `key_in_watch_range`.
    async fn watch(&self, key: &str, range_end: &str, revision: i64) -> Result<WatchResult>;

    /// Return the size of the database in bytes.
    async fn db_size(&self) -> Result<i64>;

    /// Return the current revision.
    async fn current_revision(&self) -> Result<i64>;

    /// Block until the poll loop has processed all rows up to (and including) the given revision.
    /// This ensures that any events at or below `revision` have been broadcast to watchers.
    async fn wait_for_sync_to(&self, revision: i64);

    /// Compact revisions up to the given revision. Returns the compacted revision.
    async fn compact(&self, revision: i64) -> Result<i64>;
}

/// Returns true if `key` falls within the watch range `[start, range_end)`.
///
/// Follows etcd range semantics (byte-wise comparison):
/// - `range_end == "\0"`: all keys `>= start` (open-ended range).
/// - non-empty `range_end`: `start <= key < range_end`. Clients express prefix
///   queries by incrementing the last byte of the key (e.g. APISIX's
///   watchdir("/apisix/") sends `range_end = "/apisix0"`).
/// - empty `range_end`: exactly `start`. As a legacy rhino extension (needed by
///   APISIX-style clients that rely on bare trailing-slash keys), a `start`
///   ending in `/` matches every key with that prefix.
pub fn key_in_watch_range(start: &str, range_end: &str, key: &str) -> bool {
    if range_end.is_empty() {
        if start.ends_with('/') {
            key.starts_with(start)
        } else {
            key == start
        }
    } else if range_end == "\0" {
        key >= start
    } else {
        key >= start && key < range_end
    }
}

/// Longest common prefix of two strings (a slice of `a`).
///
/// Used by the SQL drivers as a cheap primary-key prefilter for watch ranges:
/// every key in `[start, range_end)` shares the common prefix of the bounds,
/// so `name LIKE '<common>%'` is a superset that the Rust side then narrows
/// with [`key_in_watch_range`].
pub fn common_prefix<'a>(a: &'a str, b: &str) -> &'a str {
    let mut end = 0;
    let mut b_chars = b.chars();
    for (i, c) in a.char_indices() {
        if b_chars.next() != Some(c) {
            break;
        }
        end = i + c.len_utf8();
    }
    &a[..end]
}

#[cfg(test)]
mod tests {
    use super::{common_prefix, key_in_watch_range};

    #[test]
    fn exact_match_when_range_end_empty() {
        assert!(key_in_watch_range("/foo/bar", "", "/foo/bar"));
        assert!(!key_in_watch_range("/foo/bar", "", "/foo/bar/baz"));
        assert!(!key_in_watch_range("/foo/bar", "", "/foo/ba"));
    }

    #[test]
    fn trailing_slash_prefix_when_range_end_empty() {
        assert!(key_in_watch_range("/apisix/", "", "/apisix/"));
        assert!(key_in_watch_range("/apisix/", "", "/apisix/routes/1"));
        assert!(!key_in_watch_range("/apisix/", "", "/other"));
    }

    #[test]
    fn apisix_watchdir_range() {
        // config_etcd.lua watches prefix "/apisix/" with range_end "/apisix0"
        // (last byte incremented).
        assert!(key_in_watch_range("/apisix/", "/apisix0", "/apisix/"));
        assert!(key_in_watch_range(
            "/apisix/",
            "/apisix0",
            "/apisix/routes/1"
        ));
        // The byte interval ("/apisix/", "/apisix0") is empty — only keys
        // carrying the "/apisix/" prefix fall inside.
        assert!(!key_in_watch_range(
            "/apisix/",
            "/apisix0",
            "/apisix-extra"
        ));
        assert!(!key_in_watch_range("/apisix/", "/apisix0", "/apisix0"));
        assert!(!key_in_watch_range("/apisix/", "/apisix0", "/apisixz"));
        assert!(!key_in_watch_range("/apisix/", "/apisix0", "/apisi"));
    }

    #[test]
    fn etcdctl_prefix_range() {
        // etcdctl --prefix /foo sends range_end "/fop".
        assert!(key_in_watch_range("/foo", "/fop", "/foo"));
        assert!(key_in_watch_range("/foo", "/fop", "/foobar"));
        assert!(!key_in_watch_range("/foo", "/fop", "/fop"));
        assert!(!key_in_watch_range("/foo", "/fop", "/fo"));
    }

    #[test]
    fn open_ended_range_with_null_range_end() {
        assert!(key_in_watch_range("/a", "\0", "/a"));
        assert!(key_in_watch_range("/a", "\0", "/b"));
        assert!(!key_in_watch_range("/b", "\0", "/a"));
    }

    #[test]
    fn common_prefix_of_range_bounds() {
        assert_eq!(common_prefix("/apisix/", "/apisix0"), "/apisix");
        assert_eq!(common_prefix("/foo", "/fop"), "/fo");
        assert_eq!(common_prefix("/foo/bar", "/foo/bar"), "/foo/bar");
        assert_eq!(common_prefix("abc", "ab"), "ab");
        assert_eq!(common_prefix("abc", "xyz"), "");
        assert_eq!(common_prefix("", "abc"), "");
        // Multi-byte characters must not be split mid-codepoint.
        assert_eq!(common_prefix("/路径/a", "/路径/b"), "/路径/");
    }

    /// The SQL prefilter (LIKE '<common>%') must never drop a key that the
    /// exact Rust-side range check would accept.
    #[test]
    fn common_prefix_is_a_superset_prefilter() {
        for (start, end) in [("/apisix/", "/apisix0"), ("/foo", "/fop"), ("/", "/0")] {
            for key in ["/apisix/", "/apisix/routes/1", "/foo", "/foobar", "/", "/0"] {
                if key_in_watch_range(start, end, key) {
                    let cp = common_prefix(start, end);
                    assert!(
                        key.starts_with(cp),
                        "key {key:?} in range [{start:?}, {end:?}) but lacks common prefix {cp:?}"
                    );
                }
            }
        }
    }
}
