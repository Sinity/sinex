//! SQLite row fixture.
//!
//! Creates a temporary SQLite database, applies the schema provided by the
//! caller, inserts the given rows, and returns the database path.
//!
//! The `data` bytes for this fixture are expected to be newline-delimited
//! SQL INSERT statements. For structured row insertion the adapter exposes
//! a typed builder.

use std::path::PathBuf;
use tempfile::{NamedTempFile, TempPath};

use super::{FixtureBinding, FixtureHandle};

/// A row to insert: ordered column values as `(column_name, value)` pairs.
pub type SqliteRow<'a> = &'a [(&'a str, &'a str)];

/// Build a SQLite fixture from a table definition and rows.
///
/// - `table_ddl` — a CREATE TABLE statement executed before the rows.
/// - `rows` — column-value pairs for each INSERT.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::FilePath`
/// pointing at the temp DB file.
///
/// # Errors
///
/// Returns an error if the DB cannot be created or populated.
pub fn build(table_ddl: &str, rows: &[&[(&str, &str)]]) -> Result<FixtureHandle, String> {
    // Create the temp file first so it has a path; rusqlite opens by path.
    let tmp =
        NamedTempFile::new().map_err(|e| format!("failed to create sqlite temp file: {e}"))?;
    let path: PathBuf = tmp.path().to_owned();

    {
        let conn = rusqlite::Connection::open(&path)
            .map_err(|e| format!("failed to open sqlite fixture db: {e}"))?;

        conn.execute_batch(table_ddl)
            .map_err(|e| format!("failed to apply sqlite fixture schema: {e}"))?;

        for row in rows {
            if row.is_empty() {
                continue;
            }
            let cols: Vec<&str> = row.iter().map(|(col, _)| *col).collect();
            let placeholders: Vec<String> = (0..row.len()).map(|i| format!("?{}", i + 1)).collect();
            // Extract table name from DDL heuristically (first word after CREATE TABLE).
            let table_name = extract_table_name(table_ddl)?;
            let sql = format!(
                "INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
                cols = cols.join(", "),
                placeholders = placeholders.join(", "),
            );
            let values: Vec<&dyn rusqlite::ToSql> =
                row.iter().map(|(_, v)| v as &dyn rusqlite::ToSql).collect();
            conn.execute(&sql, rusqlite::params_from_iter(values.iter()))
                .map_err(|e| format!("failed to insert sqlite fixture row: {e}"))?;
        }
    }

    // Keep the NamedTempFile alive via TempPath so it isn't deleted before the test completes.
    let temp_path: TempPath = tmp.into_temp_path();
    Ok(FixtureHandle::with_resource(
        FixtureBinding::FilePath(path),
        temp_path,
    ))
}

/// Build a SQLite fixture from raw INSERT statements embedded in `data` bytes.
///
/// `data` should be UTF-8 newline-delimited SQL. The first non-empty line is
/// expected to be a CREATE TABLE statement; subsequent lines are INSERT
/// statements.
///
/// # Errors
///
/// Returns an error if the data is not valid UTF-8 or the SQL fails.
pub fn build_from_bytes(data: &[u8]) -> Result<FixtureHandle, String> {
    let sql = std::str::from_utf8(data)
        .map_err(|e| format!("sqlite fixture data is not valid UTF-8: {e}"))?;
    let statements: Vec<&str> = sql.lines().filter(|l| !l.trim().is_empty()).collect();
    if statements.is_empty() {
        return Err("sqlite fixture data is empty".to_string());
    }

    let tmp =
        NamedTempFile::new().map_err(|e| format!("failed to create sqlite temp file: {e}"))?;
    let path: PathBuf = tmp.path().to_owned();
    {
        let conn = rusqlite::Connection::open(&path)
            .map_err(|e| format!("failed to open sqlite fixture db: {e}"))?;
        for stmt in statements {
            conn.execute_batch(stmt)
                .map_err(|e| format!("failed to execute sqlite fixture stmt '{stmt}': {e}"))?;
        }
    }
    let temp_path: TempPath = tmp.into_temp_path();
    Ok(FixtureHandle::with_resource(
        FixtureBinding::FilePath(path),
        temp_path,
    ))
}

fn extract_table_name(ddl: &str) -> Result<String, String> {
    let upper = ddl.to_uppercase();
    let after_table = upper
        .find("CREATE TABLE")
        .and_then(|i| ddl.get(i + "CREATE TABLE".len()..))
        .or_else(|| {
            upper
                .find("CREATE VIRTUAL TABLE")
                .and_then(|i| ddl.get(i + "CREATE VIRTUAL TABLE".len()..))
        })
        .ok_or_else(|| format!("cannot extract table name from DDL: {ddl}"))?;
    let name = after_table
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(|c| c == '(' || c == '"' || c == '`' || c == '[' || c == ']');
    if name.is_empty() {
        return Err(format!("empty table name extracted from DDL: {ddl}"));
    }
    Ok(name.to_string())
}
