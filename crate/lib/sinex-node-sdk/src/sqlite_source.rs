#[cfg(feature = "messaging")]
use crate::{NodeResult, acquisition_manager::AcquisitionManager};
use camino::{Utf8Path, Utf8PathBuf};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Params, Row};
#[cfg(feature = "messaging")]
use serde_json::Value as JsonValue;
use sinex_primitives::Uuid;
use std::path::Path;
use std::{error::Error, fmt};
use tracing::warn;

fn open_read_only(path: &Utf8Path) -> Result<Connection, rusqlite::Error> {
    Connection::open_with_flags(Path::new(path.as_str()), OpenFlags::SQLITE_OPEN_READ_ONLY)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqliteTableCheckError {
    MissingPath {
        path: Utf8PathBuf,
    },
    OpenFailed {
        path: Utf8PathBuf,
        error: String,
    },
    MetadataQueryFailed {
        path: Utf8PathBuf,
        table: String,
        error: String,
    },
    MissingTables {
        path: Utf8PathBuf,
        tables: Vec<String>,
    },
}

impl fmt::Display for SqliteTableCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPath { path } => {
                write!(f, "SQLite path does not exist: {path}")
            }
            Self::OpenFailed { path, error } => {
                write!(f, "failed to open SQLite database {path}: {error}")
            }
            Self::MetadataQueryFailed { path, table, error } => {
                write!(
                    f,
                    "failed to inspect SQLite schema for table {table} in {path}: {error}"
                )
            }
            Self::MissingTables { path, tables } => {
                write!(
                    f,
                    "SQLite database {path} is missing required tables: {}",
                    tables.join(", ")
                )
            }
        }
    }
}

impl Error for SqliteTableCheckError {}

pub fn ensure_sqlite_with_tables(
    path: &Utf8Path,
    tables: &[&str],
) -> Result<(), SqliteTableCheckError> {
    if !Path::new(path.as_str()).exists() {
        return Err(SqliteTableCheckError::MissingPath {
            path: path.to_path_buf(),
        });
    }

    let conn = open_read_only(path).map_err(|error| SqliteTableCheckError::OpenFailed {
        path: path.to_path_buf(),
        error: error.to_string(),
    })?;

    let mut missing_tables = Vec::new();
    for table in tables {
        let exists = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?1)",
                [table],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|error| SqliteTableCheckError::MetadataQueryFailed {
                path: path.to_path_buf(),
                table: (*table).to_string(),
                error: error.to_string(),
            })?;
        if !exists {
            missing_tables.push((*table).to_string());
        }
    }

    if missing_tables.is_empty() {
        Ok(())
    } else {
        Err(SqliteTableCheckError::MissingTables {
            path: path.to_path_buf(),
            tables: missing_tables,
        })
    }
}

pub fn is_sqlite_with_tables(path: &Utf8Path, tables: &[&str]) -> bool {
    ensure_sqlite_with_tables(path, tables).is_ok()
}

pub fn read_rows_after<T, F>(
    path: &Utf8Path,
    query: &str,
    from_row_id: i64,
    map: F,
) -> Result<(Vec<T>, i64), rusqlite::Error>
where
    F: FnMut(&Row<'_>) -> Result<T, rusqlite::Error>,
{
    read_rows_with_params(path, query, [from_row_id], from_row_id, map)
}

pub fn read_rows_with_params<T, P, F>(
    path: &Utf8Path,
    query: &str,
    params: P,
    initial_row_id: i64,
    mut map: F,
) -> Result<(Vec<T>, i64), rusqlite::Error>
where
    P: Params,
    F: FnMut(&Row<'_>) -> Result<T, rusqlite::Error>,
{
    let conn = open_read_only(path)?;
    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query(params)?;
    let mut items = Vec::new();
    let mut last_row_id = initial_row_id;

    while let Some(row) = rows.next()? {
        let row_id = row.get::<_, i64>(0)?;
        last_row_id = last_row_id.max(row_id);
        match map(row) {
            Ok(item) => items.push(item),
            Err(error) => {
                warn!(
                    sqlite_path = %path,
                    row_id,
                    error = %error,
                    "Skipping malformed SQLite history row"
                );
            }
        }
    }

    Ok((items, last_row_id))
}

pub fn max_row_id_for_query(path: &Utf8Path, query: &str) -> Result<i64, rusqlite::Error> {
    let conn = open_read_only(path)?;
    let max_id: Option<i64> = conn.query_row(query, [], |row| row.get(0)).optional()?;
    Ok(max_id.unwrap_or(0))
}

#[must_use]
fn stable_material_id(source_identifier: &str, stable_key: &str) -> Uuid {
    let stable_key = format!("{source_identifier}#{stable_key}");
    Uuid::new_v5(&Uuid::NAMESPACE_URL, stable_key.as_bytes())
}

#[cfg(feature = "messaging")]
pub async fn stage_stable_material(
    acquisition: &AcquisitionManager,
    source_identifier: &str,
    stable_key: &str,
    bytes: &[u8],
    reason: &str,
    metadata: Option<JsonValue>,
) -> NodeResult<Uuid> {
    let material_id = stable_material_id(source_identifier, stable_key);
    let mut builder = acquisition
        .build_material(source_identifier)
        .with_material_id(material_id);
    if let Some(metadata_value) = metadata.clone() {
        builder = builder.with_metadata(metadata_value);
    }

    let mut handle = builder.begin().await?;
    acquisition.append_slice(&mut handle, bytes).await?;

    if let Some(metadata_value) = metadata {
        acquisition
            .finalize_with_metadata(handle, reason, metadata_value)
            .await?;
    } else {
        acquisition.finalize(handle, reason).await?;
    }

    Ok(material_id)
}
