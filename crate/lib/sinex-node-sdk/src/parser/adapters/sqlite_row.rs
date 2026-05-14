//! Adapter for reading rows from a SQLite database.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// SqliteRowAdapter
// =============================================================================

/// Adapter for reading rows from a SQLite database.
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
///    adapter is wired via `register_adapter_ingestor!`, where the adapter
///    is constructed via `Default` and the path arrives from the node's JSON
///    config at `initialize` time.
#[derive(Debug, Clone)]
pub struct SqliteRowAdapter {
    path: String,
}

impl Default for SqliteRowAdapter {
    fn default() -> Self {
        Self { path: String::new() }
    }
}

impl SqliteRowAdapter {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
        }
    }

    /// Returns the path this adapter reads from.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Configuration for [`SqliteRowAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
pub struct SqliteRowConfig {
    /// Path to the SQLite database file.
    ///
    /// When non-empty, overrides the path supplied to [`SqliteRowAdapter::new`].
    /// Required when using the adapter via `register_adapter_ingestor!` (where
    /// the adapter is constructed via `Default` and the path must come from
    /// the node config JSON).
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

    /// Open the SQLite database read-only.
    ///
    /// Browser-history and similar sources that share their DB with a
    /// long-running writer (qutebrowser, chromium, atuin) must open
    /// read-only to avoid `attempt to write a readonly database` errors
    /// when the writer holds an exclusive lock. Cursor state lives in the
    /// SDK's checkpoint store, not in the DB, so read-only is safe by
    /// default.
    #[serde(default = "default_read_only")]
    pub read_only: bool,

    /// Per-open row batch limit. The adapter appends `LIMIT <batch_size>`
    /// to the inner cursor query so a single `open()` call returns at
    /// most this many rows even when the underlying table is huge. The
    /// SDK's continuous-poll loop re-opens the adapter each cycle, so
    /// the next batch resumes from the last persisted rowid.
    ///
    /// Bounds peak memory: a desktop.activitywatch DB with millions of
    /// rows previously loaded entire result sets (~5.8 GB heap); with
    /// the default batch the per-cycle working set stays in tens of MB.
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

fn default_rowid_column() -> String {
    "rowid".into()
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
        let path = if !config.path.is_empty() {
            config.path.clone()
        } else {
            self.path.clone()
        };
        let table = config.table.clone();
        let rowid_col = config.rowid_column.clone();
        let last_rowid = cursor.map_or(0, |c| c.last_rowid);

        // Build the full query from the config.
        let base_query = if config.query.to_uppercase().contains("SELECT") {
            config.query.clone()
        } else {
            format!("SELECT rowid, * FROM {}", config.query)
        };

        // Open the database.
        //
        // For shared DBs (qutebrowser History, atuin history, etc.) we need
        // `immutable=1` in the URI so SQLite skips WAL/-shm operations
        // entirely. Without it, opening a WAL-mode DB from a user that can't
        // write the -shm file fails with "attempt to write a readonly
        // database" even with SQLITE_OPEN_READ_ONLY — the read-only flag
        // permits no DB writes, but SQLite still tries to create the WAL
        // companion files in the same directory. `immutable=1` tells SQLite
        // "this file will not change while open" which skips WAL setup.
        // Trade-off: data added by the writer after we open is not visible
        // until the next adapter open() — acceptable because we re-open per
        // scan/poll cycle.
        let mut flags = rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let connection = if config.read_only {
            flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_URI;
            let uri = format!("file:{path}?immutable=1&mode=ro");
            rusqlite::Connection::open_with_flags(&uri, flags)
        } else {
            flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
            rusqlite::Connection::open_with_flags(&path, flags)
        }
        .map_err(|e| {
            ParserError::Adapter(format!(
                "failed to open SQLite database {path}: {e}"
            ))
        })?;

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

        let mut stmt = connection.prepare(&sql).map_err(|e| {
            ParserError::Adapter(format!("failed to prepare query: {e}"))
        })?;

        let column_names: Vec<String> = (0..stmt.column_count())
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let rows_result: Result<Vec<(i64, serde_json::Value)>, ParserError> = if let Some(rowid_val) = bind_rowid {
            let mapped = stmt
                .query_map([rowid_val], |row| {
                    let rowid: i64 = row.get(0)?;
                    let json = row_to_json(row, &column_names);
                    Ok((rowid, json))
                })
                .map_err(|e| ParserError::Adapter(format!("query error: {e}")))?;

            let mut rows = Vec::new();
            for r in mapped {
                match r {
                    Ok((rowid, json)) => rows.push((rowid, json)),
                    Err(e) => return Err(ParserError::Adapter(format!("row error: {e}"))),
                }
            }
            Ok(rows)
        } else {
            let mapped = stmt
                .query_map([], |row| {
                    let rowid: i64 = row.get(0)?;
                    let json = row_to_json(row, &column_names);
                    Ok((rowid, json))
                })
                .map_err(|e| ParserError::Adapter(format!("query error: {e}")))?;

            let mut rows = Vec::new();
            for r in mapped {
                match r {
                    Ok((rowid, json)) => rows.push((rowid, json)),
                    Err(e) => return Err(ParserError::Adapter(format!("row error: {e}"))),
                }
            }
            Ok(rows)
        };

        // Drop statement and connection explicitly before we build records.
        drop(stmt);
        drop(connection);

        let rows = rows_result?;
        let records: Vec<ParserResult<SourceRecord>> = rows
            .into_iter()
            .map(|(rowid, json)| {
                let bytes = serde_json::to_vec(&json).map_err(|e| {
                    ParserError::Parse(format!("failed to serialize row: {e}"))
                })?;

                Ok(SourceRecord {
                    material_id,
                    anchor: MaterialAnchor::SqliteRow {
                        table: table.clone(),
                        rowid,
                    },
                    bytes,
                    logical_path: Some(Utf8Path::new(&path).to_owned().into()),
                    source_ts_hint: None,
                    metadata: json,
                })
            })
            .collect();

        Ok(stream::iter(records).boxed())
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        match &record.anchor {
            MaterialAnchor::SqliteRow { rowid, .. } => Ok(SqliteRowCursor {
                last_rowid: *rowid,
            }),
            other => Err(ParserError::Cursor(format!(
                "expected SqliteRow anchor, got {other:?}"
            ))),
        }
    }
}

// =============================================================================
// SQLite helpers
// =============================================================================

/// Convert a rusqlite Row to a JSON object using column names.
pub(crate) fn row_to_json(
    row: &rusqlite::Row<'_>,
    column_names: &[String],
) -> serde_json::Value {
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
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;
    use tempfile::NamedTempFile;

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    fn make_test_db() -> NamedTempFile {
        let f = NamedTempFile::with_suffix(".db").unwrap();
        let conn = rusqlite::Connection::open(f.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value REAL);
             INSERT INTO items (id, name, value) VALUES (1, 'alpha', 1.5);
             INSERT INTO items (id, name, value) VALUES (2, 'beta', 2.5);
             INSERT INTO items (id, name, value) VALUES (3, 'gamma', 3.5);",
        )
        .unwrap();
        f
    }

    #[sinex_test]
    async fn test_sqlite_yields_one_record_per_row() -> xtask::sandbox::TestResult<()> {
        let db = make_test_db();
        let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
        let config = SqliteRowConfig {
            query: "SELECT rowid, * FROM items".into(),
            table: "items".into(),
            rowid_column: "rowid".into(),
            ..Default::default()
        };

        let stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_sqlite_cursor_resumes_after_rowid() -> xtask::sandbox::TestResult<()> {
        let db = make_test_db();
        let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
        let config = SqliteRowConfig {
            query: "SELECT rowid, * FROM items".into(),
            table: "items".into(),
            rowid_column: "rowid".into(),
            ..Default::default()
        };

        let stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
        let records: Vec<_> = stream.collect().await;
        let cursor_after_row1 = adapter.cursor_after(records[0].as_ref().unwrap()).unwrap();

        let stream2 = adapter
            .open(dummy_material_id(), &config, Some(cursor_after_row1))
            .await
            .unwrap();
        let records2: Vec<_> = stream2.collect().await;

        assert_eq!(records2.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_sqlite_anchor_contains_table_name() -> xtask::sandbox::TestResult<()> {
        let db = make_test_db();
        let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
        let config = SqliteRowConfig {
            query: "SELECT rowid, * FROM items".into(),
            table: "items".into(),
            rowid_column: "rowid".into(),
            ..Default::default()
        };

        let mut stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
        let record = stream.next().await.unwrap().unwrap();

        assert!(matches!(&record.anchor, MaterialAnchor::SqliteRow { table, .. } if table == "items"));
        Ok(())
    }

    #[sinex_test]
    async fn test_sqlite_cursor_after_wrong_anchor_errors() -> xtask::sandbox::TestResult<()> {
        let db = make_test_db();
        let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
        let record = SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 5 },
            bytes: b"x".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        assert!(adapter.cursor_after(&record).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_sqlite_missing_db_returns_error() -> xtask::sandbox::TestResult<()> {
        let adapter = SqliteRowAdapter::new("/nonexistent/path.db");
        let config = SqliteRowConfig {
            query: "SELECT rowid, * FROM items".into(),
            table: "items".into(),
            rowid_column: "rowid".into(),
            ..Default::default()
        };
        assert!(adapter.open(dummy_material_id(), &config, None).await.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_sqlite_row_json_has_column_keys() -> xtask::sandbox::TestResult<()> {
        let db = make_test_db();
        let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
        let config = SqliteRowConfig {
            query: "SELECT rowid, * FROM items".into(),
            table: "items".into(),
            rowid_column: "rowid".into(),
            ..Default::default()
        };

        let mut stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
        let record = stream.next().await.unwrap().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&record.bytes).unwrap();

        assert!(json.get("name").is_some());
        assert!(json.get("value").is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_sqlite_monotonic_cursor() -> xtask::sandbox::TestResult<()> {
        let db = make_test_db();
        let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
        let config = SqliteRowConfig {
            query: "SELECT rowid, * FROM items".into(),
            table: "items".into(),
            rowid_column: "rowid".into(),
            ..Default::default()
        };

        let stream = adapter.open(dummy_material_id(), &config, None).await.unwrap();
        let records: Vec<_> = stream.collect().await;

        let cursors: Vec<SqliteRowCursor> = records
            .iter()
            .map(|r| adapter.cursor_after(r.as_ref().unwrap()).unwrap())
            .collect();

        // Cursors must be strictly increasing (monotonic).
        for w in cursors.windows(2) {
            assert!(w[0].last_rowid < w[1].last_rowid);
        }
        Ok(())
    }
}
