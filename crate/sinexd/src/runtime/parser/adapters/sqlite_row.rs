//! Adapter for reading rows from a `SQLite` database.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

#[cfg(feature = "messaging")]
use crate::runtime::parser::InputShapeAdapterExt;
#[cfg(feature = "messaging")]
use crate::runtime::parser::adapters::{SnapshotLaneSpec, SqliteSnapshotConfig};
use crate::runtime::parser::{
    InputShapeAdapter, ParserError, ParserResult, SourceRecordFingerprint,
};

// =============================================================================
// SqliteRowAdapter
// =============================================================================

/// Adapter for reading rows from a `SQLite` database.
///
/// Yields one [`SourceRecord`] per row. Uses rowid-based cursor for
/// resumption. The database is opened read-only.
///
/// # Path resolution
///
/// The database path can be supplied in two ways:
///
/// 1. **Constructor:** `SqliteRowAdapter::new(path)` — pass the path at
///    construction time. Useful in tests and imperative callers.
/// 2. **Config field:** set `path` in [`SqliteRowConfig`] — the config
///    value takes priority over the constructor value. Required when the
///    adapter is wired via `register_source!`, where the adapter
///    is constructed via `Default` and the path arrives from the source's JSON
///    config at `initialize` time.
#[derive(Debug, Clone, Default)]
pub struct SqliteRowAdapter {
    path: String,
}

impl SqliteRowAdapter {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    /// Returns the path this adapter reads from.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    fn effective_path(&self, config: &SqliteRowConfig) -> String {
        if config.path.is_empty() {
            self.path.clone()
        } else {
            config.path.clone()
        }
    }

    fn open_connection(
        path: &str,
        read_only: bool,
        immutable: bool,
    ) -> rusqlite::Result<rusqlite::Connection> {
        let mut flags = rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = if read_only {
            flags |=
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI;
            let uri = if immutable {
                format!("file:{path}?immutable=1&mode=ro")
            } else {
                format!("file:{path}?mode=ro")
            };
            rusqlite::Connection::open_with_flags(&uri, flags)
        } else {
            flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
            rusqlite::Connection::open_with_flags(path, flags)
        }?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(conn)
    }

    fn is_lock_error(error: &rusqlite::Error) -> bool {
        matches!(
            error,
            rusqlite::Error::SqliteFailure(inner, _)
                if matches!(
                    inner.code,
                    rusqlite::ffi::ErrorCode::DatabaseBusy
                        | rusqlite::ffi::ErrorCode::DatabaseLocked
                )
        )
    }

    fn copy_sqlite_snapshot(path: &str) -> std::io::Result<(tempfile::TempDir, String)> {
        let source = Path::new(path);
        let dir = tempfile::tempdir()?;
        let dest = dir.path().join("snapshot.sqlite");
        fs::copy(source, &dest)?;

        for suffix in ["-wal", "-shm"] {
            let sidecar = Path::new(&format!("{path}{suffix}")).to_path_buf();
            if sidecar.exists() {
                fs::copy(&sidecar, dir.path().join(format!("snapshot.sqlite{suffix}")))?;
            }
        }

        let dest = dest
            .to_str()
            .ok_or_else(|| std::io::Error::other("temporary SQLite snapshot path is not UTF-8"))?
            .to_owned();
        Ok((dir, dest))
    }

    fn collect_rows(
        connection: &rusqlite::Connection,
        sql: &str,
        bind_rowid: Option<i64>,
    ) -> rusqlite::Result<Vec<(i64, serde_json::Value)>> {
        let mut stmt = connection.prepare(sql)?;
        let column_names: Vec<String> = (0..stmt.column_count())
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let mut rows = Vec::new();
        if let Some(rowid_val) = bind_rowid {
            let mapped = stmt.query_map([rowid_val], |row| {
                let rowid: i64 = row.get(0)?;
                let json = row_to_json(row, &column_names);
                Ok((rowid, json))
            })?;
            for row in mapped {
                rows.push(row?);
            }
        } else {
            let mapped = stmt.query_map([], |row| {
                let rowid: i64 = row.get(0)?;
                let json = row_to_json(row, &column_names);
                Ok((rowid, json))
            })?;
            for row in mapped {
                rows.push(row?);
            }
        }

        Ok(rows)
    }

    fn collect_rows_from_path(
        path: &str,
        read_only: bool,
        immutable: bool,
        sql: &str,
        bind_rowid: Option<i64>,
    ) -> rusqlite::Result<Vec<(i64, serde_json::Value)>> {
        let connection = Self::open_connection(path, read_only, immutable)?;
        Self::collect_rows(&connection, sql, bind_rowid)
    }

    fn collect_rows_with_snapshot_fallback(
        path: &str,
        config: &SqliteRowConfig,
        sql: &str,
        bind_rowid: Option<i64>,
    ) -> ParserResult<Vec<(i64, serde_json::Value)>> {
        match Self::collect_rows_from_path(
            path,
            config.read_only,
            config.immutable,
            sql,
            bind_rowid,
        ) {
            Ok(rows) => Ok(rows),
            Err(error) if Self::is_lock_error(&error) => {
                let (_dir, snapshot_path) = Self::copy_sqlite_snapshot(path).map_err(|io_error| {
                    ParserError::Adapter(format!(
                        "failed to snapshot locked SQLite database {path}: {io_error}"
                    ))
                })?;
                Self::collect_rows_from_path(&snapshot_path, true, false, sql, bind_rowid)
                    .map_err(|snapshot_error| {
                        ParserError::Adapter(format!(
                            "failed to query SQLite snapshot for {path}: {snapshot_error}"
                        ))
                    })
            }
            Err(error) => Err(ParserError::Adapter(format!("query error: {error}"))),
        }
    }

    fn fingerprint_from_path(
        path: &str,
        read_only: bool,
        immutable: bool,
    ) -> rusqlite::Result<SourceRecordFingerprint> {
        let connection = Self::open_connection(path, read_only, immutable)?;
        SourceRecordFingerprint::from_sqlite_connection(&connection)
    }

    fn input_fingerprint_with_snapshot_fallback(
        path: &str,
        config: &SqliteRowConfig,
    ) -> ParserResult<SourceRecordFingerprint> {
        match Self::fingerprint_from_path(path, config.read_only, config.immutable) {
            Ok(fingerprint) => Ok(fingerprint),
            Err(error) if Self::is_lock_error(&error) => {
                let (_dir, snapshot_path) = Self::copy_sqlite_snapshot(path).map_err(|io_error| {
                    ParserError::Adapter(format!(
                        "failed to snapshot locked SQLite database {path}: {io_error}"
                    ))
                })?;
                Self::fingerprint_from_path(&snapshot_path, true, false).map_err(|error| {
                    ParserError::Adapter(format!(
                        "failed to fingerprint SQLite snapshot for {path}: {error}"
                    ))
                })
            }
            Err(error) => Err(ParserError::Adapter(format!(
                "failed to fingerprint SQLite schema: {error}"
            ))),
        }
    }
}

/// Configuration for [`SqliteRowAdapter`].
///
/// `Default` is hand-rolled to match the serde-default values: `read_only`
/// and `immutable` are `true`, `batch_size` is `10_000`, etc. Deriving
/// `Default` instead silently gives `batch_size = 0` / `read_only = false`
/// — a regression that masked an adapter that returns zero rows.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SqliteRowConfig {
    /// Path to the `SQLite` database file.
    ///
    /// When non-empty, overrides the path supplied to [`SqliteRowAdapter::new`].
    /// Required when using the adapter via `register_source!` (where
    /// the adapter is constructed via `Default` and the path must come from
    /// the runtime config JSON).
    #[serde(default)]
    pub path: String,

    /// SQL query or table name to read from.
    ///
    /// If the query does not contain `SELECT`, it is treated as a table
    /// name and wrapped as `SELECT rowid, * FROM <table>`.
    pub query: String,

    /// Table name for anchor purposes.
    pub table: String,

    /// Rowid column to use for ordering and cursor advancement.
    #[serde(default = "default_rowid_column")]
    pub rowid_column: String,

    /// Open the `SQLite` database read-only.
    ///
    /// Browser-history and similar sources that share their DB with a
    /// long-running writer (qutebrowser, chromium, atuin) must open
    /// read-only to avoid `attempt to write a readonly database` errors
    /// when the writer holds an exclusive lock. Cursor state lives in the
    /// runtime's checkpoint store, not in the DB, so read-only is safe by
    /// default.
    #[serde(default = "default_read_only")]
    pub read_only: bool,

    /// When `read_only` is true, also pass `immutable=1` in the open URI.
    ///
    /// `immutable=1` tells `SQLite` the file will not change while open,
    /// which lets it skip WAL/-shm setup entirely. That's ideal for
    /// rarely-mutated source DBs (qutebrowser History between sessions,
    /// Atuin history sync intervals). But it FAILS to open the DB when a
    /// concurrent writer holds the WAL active — observed on
    /// `aw-server-rust` where the `ActivityWatch` server keeps a live WAL
    /// open and `immutable=1` opens return `SQLITE_CANTOPEN`.
    ///
    /// For sources that share their DB with a continuously-writing
    /// service, set `immutable = false`. `SQLite` then opens normally
    /// read-only and reads through WAL.
    ///
    /// Default `true` preserves prior behavior for atuin / qutebrowser.
    #[serde(default = "default_immutable")]
    pub immutable: bool,

    /// Per-open row batch limit. The adapter appends `LIMIT <batch_size>`
    /// to the inner cursor query so a single `open()` call returns at
    /// most this many rows even when the underlying table is huge. The
    /// runtime's continuous-poll loop re-opens the adapter each cycle, so
    /// the next batch resumes from the last persisted rowid.
    ///
    /// Bounds peak memory: a desktop.activitywatch DB with millions of
    /// rows previously loaded entire result sets (~5.8 GB heap); with
    /// the default batch the per-cycle working set stays in tens of MB.
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,

    /// Optional parallel **file-snapshot lane**: periodically capture the
    /// `SQLite` DB file itself as a single source material, separately from
    /// the per-row stream.  Disabled by default (`interval_seconds: 0`).
    ///
    /// See [`SqliteSnapshotConfig`] for tunables. When enabled, the hosting
    /// [`AdapterBackedSource`] spawns a tokio task that captures the file
    /// at the configured cadence; per-row events continue to flow through
    /// the normal drain loop unaffected.
    ///
    /// Only present when the `messaging` feature is enabled — the snapshot
    /// lane publishes source materials via the acquisition manager, which is
    /// itself behind `messaging`.
    ///
    /// [`AdapterBackedSource`]: crate::runtime::parser::adapter_source::AdapterBackedSource
    #[cfg(feature = "messaging")]
    #[serde(default)]
    pub snapshot: SqliteSnapshotConfig,
}

impl Default for SqliteRowConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            query: String::new(),
            table: String::new(),
            rowid_column: default_rowid_column(),
            read_only: default_read_only(),
            immutable: default_immutable(),
            batch_size: default_batch_size(),
            #[cfg(feature = "messaging")]
            snapshot: SqliteSnapshotConfig::default(),
        }
    }
}

fn default_rowid_column() -> String {
    "rowid".into()
}

fn default_immutable() -> bool {
    true
}

fn default_read_only() -> bool {
    true
}

fn default_batch_size() -> u32 {
    10_000
}

/// Cursor for [`SqliteRowAdapter`] — the last-seen rowid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteRowCursor {
    pub last_rowid: i64,
}

impl SqliteRowCursor {
    #[must_use]
    pub const fn start() -> Self {
        Self { last_rowid: 0 }
    }
}

#[async_trait]
impl InputShapeAdapter for SqliteRowAdapter {
    type Config = SqliteRowConfig;
    type Cursor = SqliteRowCursor;
    const KIND: InputShapeKind = InputShapeKind::SqliteQuery;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        // Config path takes priority; fall back to constructor-supplied path.
        let path = self.effective_path(config);
        let table = config.table.clone();
        let rowid_col = config.rowid_column.clone();
        let last_rowid = cursor.map_or(0, |c| c.last_rowid);

        // Build the full query from the config.
        let base_query = if config.query.to_uppercase().contains("SELECT") {
            config.query.clone()
        } else {
            format!("SELECT rowid, * FROM {}", config.query)
        };

        let batch_size = config.batch_size;
        let (sql, bind_rowid) = if last_rowid > 0 {
            (
                format!(
                    "SELECT {rowid_col}, * FROM ({base_query}) WHERE {rowid_col} > ?1 ORDER BY {rowid_col} ASC LIMIT {batch_size}"
                ),
                Some(last_rowid),
            )
        } else {
            (
                format!(
                    "SELECT {rowid_col}, * FROM ({base_query}) ORDER BY {rowid_col} ASC LIMIT {batch_size}"
                ),
                None,
            )
        };

        let rows = Self::collect_rows_with_snapshot_fallback(&path, config, &sql, bind_rowid)?;
        let records: Vec<ParserResult<SourceRecord>> = rows
            .into_iter()
            .map(|(rowid, json)| {
                let bytes = serde_json::to_vec(&json)
                    .map_err(|e| ParserError::Parse(format!("failed to serialize row: {e}")))?;

                Ok(SourceRecord {
                    material_id,
                    anchor: MaterialAnchor::SqliteRow {
                        table: table.clone(),
                        rowid,
                    },
                    bytes,
                    logical_path: Some(Utf8Path::new(&path).to_owned()),
                    source_ts_hint: None,
                    metadata: json,
                })
            })
            .collect();

        Ok(stream::iter(records).boxed())
    }

    fn input_fingerprint(
        &self,
        config: &Self::Config,
    ) -> ParserResult<Option<SourceRecordFingerprint>> {
        let path = self.effective_path(config);
        let fingerprint = Self::input_fingerprint_with_snapshot_fallback(&path, config)?;
        Ok(Some(fingerprint))
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        match &record.anchor {
            MaterialAnchor::SqliteRow { rowid, .. } => Ok(SqliteRowCursor { last_rowid: *rowid }),
            other => Err(ParserError::Cursor(format!(
                "expected SqliteRow anchor, got {other:?}"
            ))),
        }
    }
}

#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for SqliteRowAdapter {
    fn snapshot_lane(&self, source_id: &str, config: &Self::Config) -> Option<SnapshotLaneSpec> {
        // Resolve the effective path the same way `open()` does — config wins
        // over constructor — and feed it to the snapshot-lane builder.  The
        // lane is omitted whenever snapshot config is disabled or the path is
        // unresolved.
        let path = if !config.path.is_empty() {
            config.path.as_str()
        } else if !self.path.is_empty() {
            self.path.as_str()
        } else {
            return None;
        };
        SnapshotLaneSpec::from_sqlite_config(path, source_id, &config.snapshot)
    }
}

// =============================================================================
// SQLite helpers
// =============================================================================

/// Convert a rusqlite Row to a JSON object using column names.
pub(crate) fn row_to_json(row: &rusqlite::Row<'_>, column_names: &[String]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (i, name) in column_names.iter().enumerate() {
        let val: rusqlite::Result<rusqlite::types::Value> = row.get(i);
        let json_val = match val {
            Ok(rusqlite::types::Value::Null) | Err(_) => serde_json::Value::Null,
            Ok(rusqlite::types::Value::Integer(n)) => serde_json::json!(n),
            Ok(rusqlite::types::Value::Real(f)) => serde_json::json!(f),
            Ok(rusqlite::types::Value::Text(s)) => serde_json::json!(s),
            Ok(rusqlite::types::Value::Blob(b)) => {
                use base64::Engine;
                serde_json::json!(base64::engine::general_purpose::STANDARD.encode(&b))
            }
        };
        map.insert(name.clone(), json_val);
    }
    serde_json::Value::Object(map)
}

#[cfg(test)]
#[path = "sqlite_row_test.rs"]
mod tests;
