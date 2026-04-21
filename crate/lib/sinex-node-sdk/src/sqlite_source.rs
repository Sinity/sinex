use camino::{Utf8Path, Utf8PathBuf};
use rusqlite::{Connection, OpenFlags, Params, Row};
use std::path::Path;
use std::{error::Error, fmt};

fn open_read_only(path: &Utf8Path) -> Result<Connection, rusqlite::Error> {
    let conn =
        Connection::open_with_flags(Path::new(path.as_str()), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    // Set a busy timeout so we retry on SQLITE_BUSY (source app holding a write lock)
    // instead of failing immediately. 5 seconds is generous enough for WAL checkpoints.
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(conn)
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
        match map(row) {
            Ok(item) => {
                last_row_id = last_row_id.max(row_id);
                items.push(item);
            }
            Err(error) => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::new(std::io::Error::other(format!(
                        "failed to map SQLite row {row_id} from {path}: {error}"
                    ))),
                ));
            }
        }
    }

    Ok((items, last_row_id))
}

pub fn max_row_id_for_query(path: &Utf8Path, query: &str) -> Result<i64, rusqlite::Error> {
    let conn = open_read_only(path)?;
    let max_id: Option<i64> = conn.query_row(query, [], |row| row.get::<_, Option<i64>>(0))?;
    Ok(max_id.unwrap_or(0))
}
