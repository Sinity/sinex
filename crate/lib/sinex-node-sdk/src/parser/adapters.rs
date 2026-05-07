//! Input-shape adapter implementations.
//!
//! These adapters implement [`InputShapeAdapter`] for common source shapes:
//! static files, append-only files, and SQLite databases.

use async_trait::async_trait;
use camino::Utf8Path;
use futures::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use super::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// StaticFileAdapter
// =============================================================================

/// Adapter for a single static file read once.
///
/// Yields one [`SourceRecord`] containing the entire file contents.
/// Suitable for JSON/CSV/XML exports and other one-shot file formats.
#[derive(Debug, Clone, Default)]
pub struct StaticFileAdapter;

/// Configuration for [`StaticFileAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticFileConfig {
    /// Path to the file on disk.
    pub path: String,
}

/// Cursor for [`StaticFileAdapter`] — a single boolean indicating
/// whether the file has been processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticFileCursor {
    pub processed: bool,
}

#[async_trait]
impl InputShapeAdapter for StaticFileAdapter {
    type Config = StaticFileConfig;
    type Cursor = StaticFileCursor;
    const KIND: InputShapeKind = InputShapeKind::StaticFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        if cursor.map_or(false, |c| c.processed) {
            return Ok(stream::empty().boxed());
        }

        let path = config.path.clone();

        let bytes = std::fs::read(&path)?;

        let len = bytes.len() as u64;
        let record = SourceRecord {
            material_id,
            anchor: MaterialAnchor::ByteRange { start: 0, len },
            bytes,
            logical_path: Some(Utf8Path::new(&path).to_owned().into()),
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };

        Ok(stream::once(async move { Ok(record) }).boxed())
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(StaticFileCursor { processed: true })
    }
}

// =============================================================================
// AppendOnlyFileAdapter
// =============================================================================

/// Adapter for a file that grows by appending lines.
///
/// Yields one [`SourceRecord`] per line.
/// Supports resumption via line-number cursor.
#[derive(Debug, Clone, Default)]
pub struct AppendOnlyFileAdapter;

/// Configuration for [`AppendOnlyFileAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendOnlyFileConfig {
    /// Path to the file on disk.
    pub path: String,

    /// If true, skip empty lines.
    #[serde(default)]
    pub skip_empty: bool,
}

/// Cursor for [`AppendOnlyFileAdapter`] — tracks the last-read line number
/// and byte offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendOnlyCursor {
    pub last_line: u64,
    pub last_byte_offset: u64,
}

impl AppendOnlyCursor {
    #[must_use]
    pub const fn start() -> Self {
        Self {
            last_line: 0,
            last_byte_offset: 0,
        }
    }
}

#[async_trait]
impl InputShapeAdapter for AppendOnlyFileAdapter {
    type Config = AppendOnlyFileConfig;
    type Cursor = AppendOnlyCursor;
    const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let path = config.path.clone();
        let skip_empty = config.skip_empty;
        let start_offset = cursor.as_ref().map_or(0, |c| c.last_byte_offset);
        let start_line = cursor.as_ref().map_or(1, |c| c.last_line + 1);

        let content = std::fs::read_to_string(&path)?;

        let mut records = Vec::new();
        let mut line_num: u64 = 0;
        let mut byte_offset: u64 = 0;

        for line in content.lines() {
            line_num += 1;
            let line_bytes = line.as_bytes().to_vec();
            let line_len = line_bytes.len() as u64;

            if line_num < start_line {
                byte_offset += line_len + 1; // +1 for newline
                continue;
            }

            if byte_offset < start_offset {
                byte_offset += line_len + 1;
                continue;
            }

            if skip_empty && line.is_empty() {
                byte_offset += line_len + 1;
                continue;
            }

            records.push(SourceRecord {
                material_id,
                anchor: MaterialAnchor::Line {
                    byte_start: byte_offset,
                    line: line_num,
                },
                bytes: line_bytes,
                logical_path: Some(Utf8Path::new(&path).to_owned().into()),
                source_ts_hint: None,
                metadata: serde_json::Value::Null,
            });

            byte_offset += line_len + 1;
        }

        Ok(stream::iter(records.into_iter().map(Ok)).boxed())
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        match &record.anchor {
            MaterialAnchor::Line { byte_start, line } => Ok(AppendOnlyCursor {
                last_line: *line,
                last_byte_offset: *byte_start,
            }),
            other => Err(ParserError::Cursor(format!(
                "expected Line anchor, got {other:?}"
            ))),
        }
    }
}

// =============================================================================
// SqliteRowAdapter
// =============================================================================

/// Adapter for reading rows from a SQLite database.
///
/// Yields one [`SourceRecord`] per row. Uses rowid-based cursor for
/// resumption. The database is opened read-only.
#[derive(Debug, Clone)]
pub struct SqliteRowAdapter {
    path: String,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteRowConfig {
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
}

fn default_rowid_column() -> String {
    "rowid".into()
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
        let path = self.path.clone();
        let table = config.table.clone();
        let rowid_col = config.rowid_column.clone();
        let last_rowid = cursor.map_or(0, |c| c.last_rowid);

        // Build the full query from the config.
        let base_query = if config.query.to_uppercase().contains("SELECT") {
            config.query.clone()
        } else {
            format!("SELECT rowid, * FROM {}", config.query)
        };

        // Open read-only, no mutex (single-threaded access).
        let connection = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| {
            ParserError::Adapter(format!(
                "failed to open SQLite database {path}: {e}"
            ))
        })?;

        let (sql, bind_rowid) = if last_rowid > 0 {
            (
                format!(
                    "SELECT {rowid_col}, * FROM ({base_query}) WHERE {rowid_col} > ?1 ORDER BY {rowid_col} ASC"
                ),
                Some(last_rowid),
            )
        } else {
            (
                format!(
                    "SELECT {rowid_col}, * FROM ({base_query}) ORDER BY {rowid_col} ASC"
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

        // Drop statement and connection explicitly before we build records
        // (the path is already cloned into `path` above).
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
fn row_to_json(
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
