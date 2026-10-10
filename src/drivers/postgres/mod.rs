//! PostgreSQL backend driver for rhino.
//!
//! Implements the [`Backend`] trait using PostgreSQL with the same schema and
//! log-structured approach as kine. Uses `BIGSERIAL` for revision IDs,
//! `RETURNING id` for inserts, `DISTINCT ON` for efficient list queries,
//! and the `USING` clause for compaction deletes.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tokio::sync::{Notify, broadcast, mpsc};
use tracing::{debug, error, trace, warn};

use crate::backend::{
    Backend, BackendError, Event, KeyValue, Result, WatchResult, common_prefix, key_in_watch_range,
};

const COMPACT_REV_KEY: &str = "compact_rev_key";
const COMPACT_MIN_RETAIN: i64 = 1000;
const COMPACT_BATCH_SIZE: i64 = 1000;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const COMPACT_INTERVAL: Duration = Duration::from_secs(300);

/// etcd v2 init_dir placeholder value. APISIX uses directory markers
/// (key="/apisix/routes/", value="init_dir") to distinguish directories.
/// These are not real data and must be excluded from query results.
const INIT_DIR_MARKER: &[u8] = b"init_dir";

/// Configuration for the PostgreSQL backend.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// PostgreSQL connection string. Defaults to `"postgres://postgres:postgres@localhost/kubernetes"`.
    pub dsn: String,
    /// Interval between compaction runs. Set to `Duration::ZERO` to disable.
    pub compact_interval: Duration,
    /// Minimum number of recent revisions to retain during compaction.
    pub compact_min_retain: i64,
    /// Max revisions to compact per transaction batch.
    pub compact_batch_size: i64,
    /// Maximum number of connections in the pool.
    pub max_connections: u32,
    /// Name of the kine table. Defaults to `"kine"`.
    pub kine_table_name: String,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            dsn: "postgres://postgres:postgres@localhost/kubernetes".to_string(),
            compact_interval: COMPACT_INTERVAL,
            compact_min_retain: COMPACT_MIN_RETAIN,
            compact_batch_size: COMPACT_BATCH_SIZE,
            max_connections: 5,
            kine_table_name: "kine".to_string(),
        }
    }
}

/// PostgreSQL-backed implementation of the rhino [`Backend`] trait.
pub struct PostgresBackend {
    pool: PgPool,
    current_rev: AtomicI64,
    /// Shared with the poll loop task so `insert`/`update`/`delete` calls via
    /// `notify_waiters()` wake the poll loop immediately instead of waiting for
    /// the next poll tick. Must be an `Arc` — a fresh `Notify` per task would
    /// silently lose every wakeup.
    notify: Arc<Notify>,
    broadcast_tx: broadcast::Sender<Vec<Event>>,
    /// Broadcasts the revision that the poll loop has processed up to.
    polled_rev: Arc<tokio::sync::watch::Sender<i64>>,
    config: PostgresConfig,
}

/// Validate a table name to prevent SQL injection via identifier interpolation.
fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(BackendError::Internal(format!(
            "invalid table name '{}': only alphanumeric and underscore allowed",
            name
        )));
    }
    Ok(())
}

impl PostgresBackend {
    /// Create a new PostgreSQL backend with the given configuration.
    pub async fn new(config: PostgresConfig) -> std::result::Result<Self, BackendError> {
        validate_table_name(&config.kine_table_name)?;

        let opts: PgConnectOptions = config
            .dsn
            .parse()
            .map_err(|e| BackendError::Internal(format!("invalid connection string: {e}")))?;

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(opts)
            .await
            .map_err(|e| BackendError::Internal(format!("failed to connect to database: {e}")))?;

        let (broadcast_tx, _) = broadcast::channel(1024);
        let (polled_rev_tx, _) = tokio::sync::watch::channel(0i64);

        Ok(Self {
            pool,
            current_rev: AtomicI64::new(0),
            notify: Arc::new(Notify::new()),
            broadcast_tx,
            polled_rev: Arc::new(polled_rev_tx),
            config,
        })
    }

    async fn setup_schema(&self) -> Result<()> {
        let k = &self.config.kine_table_name;
        let statements = [
            format!(
                "CREATE TABLE IF NOT EXISTS {k} (
                id BIGSERIAL PRIMARY KEY,
                name TEXT COLLATE \"C\" NOT NULL,
                created INTEGER NOT NULL,
                deleted INTEGER NOT NULL,
                create_revision BIGINT NOT NULL,
                prev_revision BIGINT NOT NULL,
                lease INTEGER NOT NULL,
                value BYTEA,
                old_value BYTEA
            )"
            ),
            format!("CREATE INDEX IF NOT EXISTS {k}_name_index ON {k} (name)"),
            format!("CREATE INDEX IF NOT EXISTS {k}_name_id_index ON {k} (name, id)"),
            format!("CREATE INDEX IF NOT EXISTS {k}_id_deleted_index ON {k} (id, deleted)"),
            format!("CREATE INDEX IF NOT EXISTS {k}_prev_revision_index ON {k} (prev_revision)"),
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS {k}_name_prev_revision_uindex ON {k} (name, prev_revision)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {k}_list_query_index ON {k} (name, id DESC, deleted)"
            ),
        ];

        for stmt in &statements {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(|e| BackendError::Internal(format!("schema setup failed: {e}")))?;
        }

        debug!("database schema and indexes are up to date");
        Ok(())
    }

    async fn ensure_compact_rev_key(&self) -> Result<()> {
        let count: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {} WHERE name = $1",
            self.config.kine_table_name
        ))
        .bind(COMPACT_REV_KEY)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;

        if count.0 == 0 {
            self.insert(COMPACT_REV_KEY, true, false, 0, 0, 0, b"", b"")
                .await?;
        }
        Ok(())
    }

    /// Insert a row, returning the new revision via RETURNING id.
    #[allow(clippy::too_many_arguments)]
    async fn insert(
        &self,
        key: &str,
        create: bool,
        delete: bool,
        create_revision: i64,
        prev_revision: i64,
        lease: i64,
        value: &[u8],
        old_value: &[u8],
    ) -> Result<i64> {
        let c = if create { 1i32 } else { 0 };
        let d = if delete { 1i32 } else { 0 };

        let row: (i64,) = sqlx::query_as(&format!(
            "INSERT INTO {}(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
             VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id",
            self.config.kine_table_name
        ))
        .bind(key)
        .bind(c)
        .bind(d)
        .bind(create_revision)
        .bind(prev_revision)
        .bind(lease)
        .bind(value)
        .bind(old_value)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                BackendError::KeyExists
            } else {
                BackendError::Internal(e.to_string())
            }
        })?;

        let id = row.0;
        self.current_rev.store(id, Ordering::Release);
        self.notify.notify_waiters();
        Ok(id)
    }

    /// Fill a gap at the given revision with a placeholder record.
    async fn fill(&self, revision: i64) -> Result<()> {
        let name = format!("gap-{revision}");
        // Use ON CONFLICT DO NOTHING (Postgres equivalent of INSERT OR IGNORE)
        let kine_table = &self.config.kine_table_name;
        sqlx::query(&format!(
            "INSERT INTO {}(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
             VALUES($1, $2, 0, 1, 0, 0, 0, NULL, NULL)
             ON CONFLICT DO NOTHING",
            kine_table
        ))
        .bind(revision)
        .bind(&name)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;

        // Reset the sequence to avoid conflicts with future inserts
        let seq_name = format!("{}_id_seq", kine_table);
        sqlx::query(&format!(
            "SELECT setval('{seq_name}', GREATEST(nextval('{seq_name}'), $1))"
        ))
        .bind(revision + 1)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn db_current_revision(&self) -> Result<i64> {
        let row: (Option<i64>,) = sqlx::query_as(&format!(
            "SELECT MAX(id) FROM {}",
            self.config.kine_table_name
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;
        Ok(row.0.unwrap_or(0))
    }

    async fn get_compact_revision(&self) -> Result<i64> {
        let row: (Option<i64>,) = sqlx::query_as(&format!(
            "SELECT MAX(prev_revision) FROM {} WHERE name = $1",
            self.config.kine_table_name
        ))
        .bind(COMPACT_REV_KEY)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;
        Ok(row.0.unwrap_or(0))
    }

    async fn get_internal(
        &self,
        key: &str,
        revision: i64,
        include_deleted: bool,
        keys_only: bool,
    ) -> Result<(i64, Option<Event>)> {
        let escaped = key.replace('_', "^_");
        let (rev, events) = self
            .list_internal(&escaped, "", 1, revision, include_deleted, keys_only, false)
            .await?;
        Ok((rev, events.into_iter().next()))
    }

    /// Core list query using PostgreSQL's DISTINCT ON for efficient latest-version lookups.
    ///
    /// `filter_init_dir`: when true, excludes etcd v2 init_dir placeholder records.
    /// List/count queries pass true; get_internal (used by create/delete/update) passes false.
    async fn list_internal(
        &self,
        prefix: &str,
        start_key: &str,
        limit: i64,
        revision: i64,
        include_deleted: bool,
        keys_only: bool,
        filter_init_dir: bool,
    ) -> Result<(i64, Vec<Event>)> {
        let value_cols = if keys_only {
            ""
        } else {
            ", kv.value, kv.old_value"
        };

        let (extra_where, bind_start_key) = if revision == 0 {
            if start_key.is_empty() {
                (String::new(), false)
            } else {
                ("AND kv.name >= $2".to_string(), true)
            }
        } else if start_key.is_empty() {
            (format!("AND kv.id <= {revision}"), false)
        } else {
            (format!("AND kv.name >= $2 AND kv.id <= {revision}"), true)
        };

        let limit_clause = if limit > 0 {
            format!("LIMIT {limit}")
        } else {
            String::new()
        };

        // Use DISTINCT ON (name) to get the latest version of each key efficiently.
        // This is the Postgres-specific optimization that kine uses.
        //
        // Filter out etcd v2 init_dir placeholder records at the SQL level.
        // The init_dir parameter number depends on whether start_key is bound:
        //   - bind_start_key=true  → $1=prefix, $2=start_key, $3=include_deleted, $4=init_dir
        //   - bind_start_key=false → $1=prefix, $2=include_deleted, $3=init_dir
        let init_dir_filter = if filter_init_dir {
            let param = if bind_start_key { 4 } else { 3 };
            format!("AND (kv.value IS NULL OR kv.value != ${param})")
        } else {
            String::new()
        };
        let k = &self.config.kine_table_name;
        let sql = format!(
            "SELECT
                (SELECT MAX(rkv.id) FROM {k} AS rkv),
                (SELECT MAX(crkv.prev_revision) FROM {k} AS crkv WHERE crkv.name = '{COMPACT_REV_KEY}'),
                maxkv.*
            FROM (
                SELECT DISTINCT ON (name)
                    kv.id AS theid,
                    kv.name,
                    kv.created,
                    kv.deleted,
                    kv.create_revision,
                    kv.prev_revision,
                    kv.lease
                    {value_cols}
                FROM {k} AS kv
                WHERE kv.name LIKE $1 ESCAPE '^'
                {extra_where}
                {init_dir_filter}
                ORDER BY kv.name, theid DESC
            ) AS maxkv
            WHERE maxkv.deleted = 0 OR ${}
            ORDER BY maxkv.name, maxkv.theid DESC
            {limit_clause}",
            if bind_start_key { "3" } else { "2" }
        );

        let include_deleted_val: bool = include_deleted;

        let rows = if bind_start_key {
            let mut q = sqlx::query(&sql)
                .bind(prefix)
                .bind(start_key)
                .bind(include_deleted_val);
            if filter_init_dir {
                q = q.bind(INIT_DIR_MARKER);
            }
            q.fetch_all(&self.pool).await
        } else {
            let mut q = sqlx::query(&sql).bind(prefix).bind(include_deleted_val);
            if filter_init_dir {
                q = q.bind(INIT_DIR_MARKER);
            }
            q.fetch_all(&self.pool).await
        }
        .map_err(|e| BackendError::Internal(e.to_string()))?;

        let mut current_rev: i64 = 0;
        let mut compact_rev: i64 = 0;
        let mut events = Vec::with_capacity(rows.len());

        for row in &rows {
            let rev: Option<i64> = row.get(0);
            let compact: Option<i64> = row.get(1);
            current_rev = rev.unwrap_or(current_rev);
            compact_rev = compact.unwrap_or(compact_rev);

            let event = self.row_to_event(row, keys_only, false)?;
            events.push(event);
        }

        if current_rev == 0 {
            current_rev = self.cached_revision().await?;
        }

        if revision > current_rev {
            return Err(BackendError::FutureRev);
        }
        if revision > 0 && revision < compact_rev {
            return Err(BackendError::Compacted);
        }

        Ok((current_rev, events))
    }

    /// Returns true when every revision in `[from, to)` exists in the kine
    /// table. The poll loop uses this to tell a real gap (rows lost) from a
    /// span of rows that exist but are hidden from `after()` (e.g. APISIX
    /// `init_dir` markers): the latter must advance the cursor silently.
    async fn revisions_present(&self, from: i64, to: i64) -> Result<bool> {
        let row: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {} WHERE id >= $1 AND id < $2",
            self.config.kine_table_name
        ))
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;
        Ok(row.0 == to - from)
    }

    async fn after(
        &self,
        prefix: &str,
        revision: i64,
        limit: i64,
    ) -> Result<(i64, i64, Vec<Event>)> {
        let limit_clause = if limit > 0 {
            format!("LIMIT {limit}")
        } else {
            String::new()
        };

        let k = &self.config.kine_table_name;
        let sql = format!(
            "SELECT
                (SELECT MAX(rkv.id) FROM {k} AS rkv),
                (SELECT MAX(crkv.prev_revision) FROM {k} AS crkv WHERE crkv.name = '{COMPACT_REV_KEY}'),
                kv.id AS theid,
                kv.name,
                kv.created,
                kv.deleted,
                kv.create_revision,
                kv.prev_revision,
                kv.lease,
                kv.value,
                kv.old_value
            FROM {k} AS kv
            WHERE kv.name LIKE $1 ESCAPE '^' AND kv.id > $2
            AND (kv.value IS NULL OR kv.value != $3)
            ORDER BY kv.id ASC
            {limit_clause}"
        );

        let rows = sqlx::query(&sql)
            .bind(prefix)
            .bind(revision)
            .bind(INIT_DIR_MARKER)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| BackendError::Internal(e.to_string()))?;

        let mut current_rev: i64 = 0;
        let mut compact_rev: i64 = 0;
        let mut events = Vec::with_capacity(rows.len());

        for row in &rows {
            let rev: Option<i64> = row.get(0);
            let compact: Option<i64> = row.get(1);
            current_rev = rev.unwrap_or(current_rev);
            compact_rev = compact.unwrap_or(compact_rev);

            let event = self.row_to_event(row, false, true)?;
            events.push(event);
        }

        Ok((current_rev, compact_rev, events))
    }

    fn row_to_event(
        &self,
        row: &sqlx::postgres::PgRow,
        keys_only: bool,
        with_old_value: bool,
    ) -> Result<Event> {
        let mod_revision: i64 = row.get(2);
        let name: String = row.get(3);
        let created: i32 = row.get(4);
        let deleted: i32 = row.get(5);
        let create_revision: i64 = row.get(6);
        let prev_revision: i64 = row.get(7);
        let lease: i32 = row.get(8);

        let value: Vec<u8> = if keys_only {
            vec![]
        } else {
            row.try_get::<Vec<u8>, _>(9).unwrap_or_default()
        };

        let old_value: Vec<u8> = if !keys_only && with_old_value {
            row.try_get::<Vec<u8>, _>(10).unwrap_or_default()
        } else {
            vec![]
        };

        let is_create = created != 0;
        let is_delete = deleted != 0;
        let actual_create_rev = if is_create {
            mod_revision
        } else {
            create_revision
        };

        let kv = KeyValue {
            key: name.clone(),
            value,
            version: 0,
            create_revision: actual_create_rev,
            mod_revision,
            lease: lease as i64,
        };

        let prev_kv = if is_create {
            None
        } else {
            Some(KeyValue {
                key: name,
                value: old_value,
                version: 0,
                create_revision: actual_create_rev,
                mod_revision: prev_revision,
                lease: lease as i64,
            })
        };

        Ok(Event {
            create: is_create,
            delete: is_delete,
            kv,
            prev_kv,
        })
    }

    async fn cached_revision(&self) -> Result<i64> {
        let cached = self.current_rev.load(Ordering::Acquire);
        if cached != 0 {
            return Ok(cached);
        }
        let rev = self.db_current_revision().await?;
        self.current_rev
            .compare_exchange(0, rev, Ordering::AcqRel, Ordering::Acquire)
            .ok();
        Ok(self.current_rev.load(Ordering::Acquire))
    }

    async fn poll_loop(self: Arc<Self>) {
        let mut poll_revision = match self.db_current_revision().await {
            Ok(rev) => rev,
            Err(e) => {
                error!("poll loop failed to get initial revision: {e}");
                return;
            }
        };

        let mut interval = tokio::time::interval(POLL_INTERVAL);
        let mut skip: i64 = 0;
        let mut skip_time = tokio::time::Instant::now();

        loop {
            // Create the notified() future BEFORE querying so that any
            // notify_waiters() from insert/update/delete that lands while the
            // query is in flight is not lost — otherwise the wakeup waits for
            // the next 1s interval tick (Notify::notify_waiters stores no
            // permit for waiters that were not yet registered).
            let notified = self.notify.notified();
            tokio::pin!(notified);

            // Update polled revision before querying (matching kine sql.go:504-508).
            let _ = self.polled_rev.send(poll_revision);

            let (_, _, events) = match self.after("%", poll_revision, 500).await {
                Ok(r) => r,
                Err(e) => {
                    error!("poll error: {e}");
                    tokio::select! {
                        _ = interval.tick() => {},
                        _ = &mut notified => {},
                    }
                    continue;
                }
            };

            if events.is_empty() {
                tokio::select! {
                    _ = interval.tick() => {},
                    _ = &mut notified => {},
                }
                continue;
            }

            trace!("poll: {} events after rev {}", events.len(), poll_revision);

            let mut rev = poll_revision;
            let mut sequential = Vec::new();

            for event in &events {
                let next = rev + 1;
                if event.kv.mod_revision != next {
                    trace!(
                        "revision gap: expected {next}, got {}",
                        event.kv.mod_revision
                    );
                    if self
                        .revisions_present(next, event.kv.mod_revision)
                        .await
                        .unwrap_or(false)
                    {
                        // All revisions in the span exist but are hidden from
                        // after() (e.g. APISIX init_dir markers): nothing was
                        // lost, so advance the cursor past them silently.
                        trace!(
                            "advancing past {} hidden revisions before rev {}",
                            event.kv.mod_revision - next,
                            event.kv.mod_revision
                        );
                    } else if skip == next && skip_time.elapsed() > Duration::from_secs(1) {
                        error!(
                            "skipping revision gap at {next}, current event rev={}",
                            event.kv.mod_revision
                        );
                    } else if skip != next {
                        skip = next;
                        skip_time = tokio::time::Instant::now();
                        self.notify.notify_waiters();
                        // Postgres needs a slightly longer delay for transaction visibility
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        break;
                    } else {
                        if let Err(e) = self.fill(next).await {
                            warn!("fill revision {next} failed: {e}");
                        } else {
                            trace!("filled revision gap at {next}");
                            self.notify.notify_waiters();
                        }
                        break;
                    }
                }

                rev = event.kv.mod_revision;
                if !event.kv.key.starts_with("gap-") {
                    sequential.push(event.clone());
                }
            }

            if rev > poll_revision {
                self.current_rev
                    .compare_exchange(poll_revision, rev, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
                poll_revision = rev;

                if !sequential.is_empty() {
                    let _ = self.broadcast_tx.send(sequential);
                }
            }
        }
    }

    async fn compact_loop(self: Arc<Self>) {
        if self.config.compact_interval.is_zero() {
            debug!("automatic compaction disabled");
            return;
        }

        let mut interval = tokio::time::interval(self.config.compact_interval);
        interval.tick().await;

        loop {
            interval.tick().await;

            if let Err(e) = self.compact_once().await
                && !matches!(e, BackendError::Compacted)
            {
                error!("compaction error: {e}");
            }
        }
    }

    async fn compact_once(&self) -> Result<()> {
        let compact_rev = self.get_compact_revision().await?;
        let current_rev = self.cached_revision().await?;

        let mut target = current_rev - self.config.compact_min_retain;
        if target <= compact_rev {
            return Err(BackendError::Compacted);
        }
        if target < 0 {
            target = 0;
        }

        let mut iter_rev = compact_rev;
        while iter_rev < target {
            iter_rev += self.config.compact_batch_size;
            if iter_rev > target {
                iter_rev = target;
            }

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| BackendError::Internal(e.to_string()))?;

            // PostgreSQL compact uses USING clause instead of IN subquery
            let deleted = sqlx::query(&format!(
                "DELETE FROM {} AS kv
                 USING (
                    SELECT kp.prev_revision AS id
                    FROM {} AS kp
                    WHERE kp.name != $1 AND kp.prev_revision != 0 AND kp.id <= $2
                    UNION
                    SELECT kd.id AS id
                    FROM {} AS kd
                    WHERE kd.deleted != 0 AND kd.id <= $3
                 ) AS ks
                 WHERE kv.id = ks.id",
                self.config.kine_table_name,
                self.config.kine_table_name,
                self.config.kine_table_name
            ))
            .bind(COMPACT_REV_KEY)
            .bind(iter_rev)
            .bind(iter_rev)
            .execute(&mut *tx)
            .await
            .map_err(|e| BackendError::Internal(e.to_string()))?;

            sqlx::query(&format!(
                "UPDATE {} SET prev_revision = $1 WHERE name = $2",
                self.config.kine_table_name
            ))
            .bind(iter_rev)
            .bind(COMPACT_REV_KEY)
            .execute(&mut *tx)
            .await
            .map_err(|e| BackendError::Internal(e.to_string()))?;

            tx.commit()
                .await
                .map_err(|e| BackendError::Internal(e.to_string()))?;

            debug!(
                "compacted {} rows up to revision {}/{}",
                deleted.rows_affected(),
                iter_rev,
                current_rev
            );
        }

        Ok(())
    }
}

/// Check if a sqlx error is a PostgreSQL unique violation (SQLSTATE 23505).
fn is_unique_violation(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        db_err.code().is_some_and(|code| code == "23505")
    } else {
        false
    }
}

/// Translate start key: replace trailing null byte with 0x1a (substitute)
/// as PostgreSQL does not allow null bytes in UTF-8 strings.
fn translate_start_key(start_key: &str) -> String {
    if let Some(s) = start_key.strip_suffix('\x00') {
        format!("{s}\x1a")
    } else {
        start_key.to_string()
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    async fn start(&self) -> Result<()> {
        self.setup_schema().await?;
        self.ensure_compact_rev_key().await?;

        match self
            .create("/registry/health", b"{\"health\":\"true\"}", 0)
            .await
        {
            Ok(_) | Err(BackendError::KeyExists) => {}
            Err(e) => warn!("failed to create health check key: {e}"),
        }

        let poll_self = Arc::new(PostgresBackend {
            pool: self.pool.clone(),
            current_rev: AtomicI64::new(self.current_rev.load(Ordering::Acquire)),
            // Share the Notify with the request-handling instance so that
            // insert/update/delete calls wake the poll loop immediately.
            notify: self.notify.clone(),
            broadcast_tx: self.broadcast_tx.clone(),
            polled_rev: self.polled_rev.clone(),
            config: self.config.clone(),
        });

        let compact_self = Arc::new(PostgresBackend {
            pool: self.pool.clone(),
            current_rev: AtomicI64::new(0),
            notify: self.notify.clone(),
            broadcast_tx: self.broadcast_tx.clone(),
            polled_rev: self.polled_rev.clone(),
            config: self.config.clone(),
        });

        tokio::spawn(async move { poll_self.poll_loop().await });
        tokio::spawn(async move { compact_self.compact_loop().await });

        debug!("postgres backend started");
        Ok(())
    }

    async fn get(
        &self,
        key: &str,
        _range_end: &str,
        _limit: i64,
        revision: i64,
        keys_only: bool,
    ) -> Result<(i64, Option<KeyValue>)> {
        let (rev, event) = self.get_internal(key, revision, false, keys_only).await?;
        Ok((rev, event.map(|e| e.kv)))
    }

    async fn create(&self, key: &str, value: &[u8], lease: i64) -> Result<i64> {
        // get_internal uses filter_init_dir=false so init_dir markers are visible.
        let (_rev, existing) = self.get_internal(key, 0, true, false).await?;

        // Treat init_dir placeholders as non-existent so APISIX can re-create them
        // without triggering a unique constraint violation on (name, prev_revision).
        if let Some(ref event) = existing
            && !event.delete
            && event.kv.value != INIT_DIR_MARKER
        {
            return Err(BackendError::KeyExists);
        }

        let prev_revision = existing.as_ref().map(|e| e.kv.mod_revision).unwrap_or(0);

        let old_value = existing
            .as_ref()
            .map(|e| e.kv.value.as_slice())
            .unwrap_or(b"");

        self.insert(key, true, false, 0, prev_revision, lease, value, old_value)
            .await
    }

    async fn delete(&self, key: &str, revision: i64) -> Result<(i64, Option<KeyValue>, bool)> {
        let (rev, event) = self.get_internal(key, 0, true, false).await?;

        let Some(event) = event else {
            return Ok((rev, None, true));
        };

        if event.delete {
            return Ok((rev, None, false));
        }

        if revision != 0 && event.kv.mod_revision != revision {
            return Ok((rev, Some(event.kv), false));
        }

        let prev_kv = event.kv.clone();
        let old_value = event.kv.value.clone();

        match self
            .insert(
                key,
                false,
                true,
                event.kv.create_revision,
                event.kv.mod_revision,
                event.kv.lease,
                &prev_kv.value,
                &old_value,
            )
            .await
        {
            Ok(new_rev) => Ok((new_rev, Some(prev_kv), true)),
            Err(_) => {
                let (rev, latest) = self.get_internal(key, 0, true, false).await?;
                Ok((rev, latest.map(|e| e.kv), false))
            }
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(i64, i64, Vec<KeyValue>)> {
        let like_prefix = format!("{}%", prefix.replace('_', "^_"));
        let (_, kvs) = self.list(&like_prefix, "", 0, 0, false).await?;

        let mut prev_kvs = Vec::new();
        let mut last_rev = 0i64;

        for kv in &kvs {
            match self.delete(&kv.key, 0).await {
                Ok((rev, prev, true)) => {
                    last_rev = rev;
                    if let Some(p) = prev {
                        prev_kvs.push(p);
                    }
                }
                Ok((rev, _, false)) => {
                    last_rev = rev;
                }
                Err(e) => return Err(e),
            }
        }

        if last_rev == 0 {
            last_rev = self.cached_revision().await?;
        }

        let deleted = prev_kvs.len() as i64;
        Ok((last_rev, deleted, prev_kvs))
    }

    async fn list(
        &self,
        prefix: &str,
        start_key: &str,
        limit: i64,
        revision: i64,
        keys_only: bool,
    ) -> Result<(i64, Vec<KeyValue>)> {
        // Prefix match: a prefix that does not end with '/' covers the key
        // itself plus every key with that prefix (etcd readdir range
        // [key, increment(key)) semantics).
        let like_prefix = format!("{}%", prefix.replace('_', "^_"));

        let translated_start = translate_start_key(start_key);

        let (rev, events) = self
            .list_internal(
                &like_prefix,
                &translated_start,
                limit,
                revision,
                false,
                keys_only,
                true,
            )
            .await?;

        let kvs = events.into_iter().map(|e| e.kv).collect();
        Ok((rev, kvs))
    }

    async fn count(&self, prefix: &str, start_key: &str, revision: i64) -> Result<(i64, i64)> {
        // Prefix match (see list).
        let like_prefix = format!("{}%", prefix.replace('_', "^_"));

        let translated_start = translate_start_key(start_key);

        let (rev, events) = self
            .list_internal(&like_prefix, &translated_start, 0, revision, false, true, true)
            .await?;

        Ok((rev, events.len() as i64))
    }

    async fn update(
        &self,
        key: &str,
        value: &[u8],
        revision: i64,
        lease: i64,
    ) -> Result<(i64, Option<KeyValue>, bool)> {
        let (rev, event) = self.get_internal(key, 0, false, false).await?;

        let Some(event) = event else {
            return Ok((rev, None, false));
        };

        if event.kv.mod_revision != revision {
            return Ok((rev, Some(event.kv), false));
        }

        let old_value = event.kv.value.clone();

        match self
            .insert(
                key,
                false,
                false,
                event.kv.create_revision,
                event.kv.mod_revision,
                lease,
                value,
                &old_value,
            )
            .await
        {
            Ok(new_rev) => {
                let updated_kv = KeyValue {
                    key: key.to_string(),
                    value: value.to_vec(),
                    version: 0,
                    create_revision: event.kv.create_revision,
                    mod_revision: new_rev,
                    lease,
                };
                Ok((new_rev, Some(updated_kv), true))
            }
            Err(_) => {
                let (rev, latest) = self.get_internal(key, 0, false, false).await?;
                Ok((rev, latest.map(|e| e.kv), false))
            }
        }
    }

    async fn watch(&self, key: &str, range_end: &str, revision: i64) -> Result<WatchResult> {
        let (tx, rx) = mpsc::channel(100);

        let current_rev = self.cached_revision().await?;
        let compact_rev = self.get_compact_revision().await?;

        if revision > 0 && revision < compact_rev {
            return Err(BackendError::Compacted);
        }

        let mut broadcast_rx = self.broadcast_tx.subscribe();
        let start_key = key.to_string();
        let end_key = range_end.to_string();
        let start_rev = if revision > 0 { revision - 1 } else { 0 };
        let pool = self.pool.clone();
        let kine_table_name = self.config.kine_table_name.clone();

        tokio::spawn(async move {
            // Fetch historical events
            if start_rev > 0 {
                // SQL-level prefilter (a superset of the requested range). The
                // exact `[start, range_end)` semantics are applied in Rust below
                // via `key_in_watch_range`, since SQL string comparison follows
                // the database collation rather than etcd's byte order.
                let (name_cond, name_bind) = if end_key.is_empty() {
                    let pattern = if start_key.ends_with('/') {
                        format!("{start_key}%")
                    } else {
                        start_key.clone()
                    };
                    ("kv.name LIKE $1 ESCAPE '^'", pattern)
                } else if end_key == "\0" {
                    ("kv.name >= $1", start_key.clone())
                } else {
                    (
                        "kv.name LIKE $1 ESCAPE '^'",
                        format!("{}%", common_prefix(&start_key, &end_key)),
                    )
                };

                let sql = format!(
                    "SELECT
                        (SELECT MAX(rkv.id) FROM {} AS rkv),
                        (SELECT MAX(crkv.prev_revision) FROM {} AS crkv WHERE crkv.name = 'compact_rev_key'),
                        kv.id AS theid,
                        kv.name,
                        kv.created,
                        kv.deleted,
                        kv.create_revision,
                        kv.prev_revision,
                        kv.lease,
                        kv.value,
                        kv.old_value
                    FROM {} AS kv
                    WHERE {name_cond} AND kv.id > $2
                    AND (kv.value IS NULL OR kv.value != $3)
                    ORDER BY kv.id ASC",
                    kine_table_name,
                    kine_table_name,
                    kine_table_name
                );

                if let Ok(rows) = sqlx::query(&sql)
                    .bind(&name_bind)
                    .bind(start_rev)
                    .bind(INIT_DIR_MARKER)
                    .fetch_all(&pool)
                    .await
                {
                    let mut events = Vec::new();
                    for row in &rows {
                        let mod_revision: i64 = row.get(2);
                        let name: String = row.get(3);
                        let created: i32 = row.get(4);
                        let deleted: i32 = row.get(5);
                        let create_revision: i64 = row.get(6);
                        let prev_revision: i64 = row.get(7);
                        let lease: i32 = row.get(8);
                        let value: Vec<u8> = row.try_get(9).unwrap_or_default();
                        let old_value: Vec<u8> = row.try_get(10).unwrap_or_default();

                        if name.starts_with("gap-") {
                            continue;
                        }
                        // Narrow the SQL prefilter to the exact key range.
                        if !key_in_watch_range(&start_key, &end_key, &name) {
                            continue;
                        }

                        let is_create = created != 0;
                        let is_delete = deleted != 0;
                        let actual_create_rev = if is_create {
                            mod_revision
                        } else {
                            create_revision
                        };

                        events.push(Event {
                            create: is_create,
                            delete: is_delete,
                            kv: KeyValue {
                                key: name.clone(),
                                value,
                                version: 0,
                                create_revision: actual_create_rev,
                                mod_revision,
                                lease: lease as i64,
                            },
                            prev_kv: if is_create {
                                None
                            } else {
                                Some(KeyValue {
                                    key: name,
                                    value: old_value,
                                    version: 0,
                                    create_revision: actual_create_rev,
                                    mod_revision: prev_revision,
                                    lease: lease as i64,
                                })
                            },
                        });
                    }

                    if !events.is_empty() && tx.send(events).await.is_err() {
                        return;
                    }
                }
            }

            // Stream live events, filtering by the requested key range.
            loop {
                match broadcast_rx.recv().await {
                    Ok(events) => {
                        let filtered: Vec<Event> = events
                            .into_iter()
                            .filter(|e| key_in_watch_range(&start_key, &end_key, &e.kv.key))
                            .collect();
                        if !filtered.is_empty() && tx.send(filtered).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("watch subscriber lagged by {n} events");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return;
                    }
                }
            }
        });

        Ok(WatchResult {
            current_revision: current_rev,
            compact_revision: compact_rev,
            events: rx,
        })
    }

    async fn db_size(&self) -> Result<i64> {
        let row: (i64,) = sqlx::query_as(&format!(
            "SELECT pg_total_relation_size('{}')",
            self.config.kine_table_name
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;
        Ok(row.0)
    }

    async fn current_revision(&self) -> Result<i64> {
        self.cached_revision().await
    }

    async fn wait_for_sync_to(&self, revision: i64) {
        let mut rx = self.polled_rev.subscribe();
        while *rx.borrow() < revision {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }

    async fn compact(&self, revision: i64) -> Result<i64> {
        let compact_rev = self.get_compact_revision().await?;
        let current_rev = self.cached_revision().await?;

        let mut target = revision;
        let safe_rev = current_rev - self.config.compact_min_retain;
        if target > safe_rev {
            target = safe_rev;
        }
        if target <= compact_rev || target < 0 {
            return Ok(current_rev);
        }

        // PostgreSQL compact uses USING clause
        let deleted = sqlx::query(&format!(
            "DELETE FROM {} AS kv
             USING (
                SELECT kp.prev_revision AS id
                FROM {} AS kp
                WHERE kp.name != $1 AND kp.prev_revision != 0 AND kp.id <= $2
                UNION
                SELECT kd.id AS id
                FROM {} AS kd
                WHERE kd.deleted != 0 AND kd.id <= $3
             ) AS ks
             WHERE kv.id = ks.id",
            self.config.kine_table_name,
            self.config.kine_table_name,
            self.config.kine_table_name
        ))
        .bind(COMPACT_REV_KEY)
        .bind(target)
        .bind(target)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;

        sqlx::query(&format!(
            "UPDATE {} SET prev_revision = $1 WHERE name = $2",
            self.config.kine_table_name
        ))
        .bind(target)
        .bind(COMPACT_REV_KEY)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Internal(e.to_string()))?;

        debug!(
            "manual compact: deleted {} rows up to revision {}",
            deleted.rows_affected(),
            target
        );

        self.cached_revision().await
    }
}
