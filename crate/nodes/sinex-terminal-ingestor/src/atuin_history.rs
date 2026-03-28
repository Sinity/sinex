//! Atuin shell history parser.
//!
//! Atuin stores history in an `SQLite` database at
//! `~/.local/share/atuin/history.db`. This module provides incremental reads for
//! the terminal ingestor so historical backfill can flow through the normal
//! node pipeline instead of the separate CLI import path.

use camino::Utf8PathBuf;
use sinex_node_sdk::{
    SqliteTableCheckError, ensure_sqlite_with_tables, max_row_id_for_query, read_rows_after,
    read_rows_with_params,
};
use sinex_primitives::Timestamp;
use std::io::{Error as IoError, ErrorKind};

/// Represents a single command from Atuin history.
#[derive(Debug, Clone)]
pub struct AtuinHistoryEntry {
    pub row_id: i64,
    pub history_id: String,
    pub timestamp_ns: i64,
    pub duration_ns: i64,
    pub exit_code: i64,
    pub command: String,
    pub cwd: String,
    pub session_id: String,
    pub hostname: String,
}

pub fn ensure_atuin_sqlite_history(path: &Utf8PathBuf) -> Result<(), SqliteTableCheckError> {
    ensure_sqlite_with_tables(path, &["history"])
}

/// Read Atuin history entries starting from a given row offset.
///
/// Returns a tuple of `(entries, last_row_id)` where `last_row_id` is the
/// highest row ID encountered, which can be used as the starting point for the
/// next read.
pub fn read_atuin_history(
    path: &Utf8PathBuf,
    from_row_id: i64,
    end_time: Option<Timestamp>,
) -> Result<(Vec<AtuinHistoryEntry>, i64), rusqlite::Error> {
    fn map_atuin_row(row: &rusqlite::Row<'_>) -> Result<AtuinHistoryEntry, rusqlite::Error> {
        Ok(AtuinHistoryEntry {
            row_id: row.get(0)?,
            history_id: row.get(1)?,
            timestamp_ns: row.get(2)?,
            duration_ns: row.get(3)?,
            exit_code: row.get(4)?,
            command: row.get(5)?,
            cwd: row.get(6)?,
            session_id: row.get(7)?,
            hostname: row.get(8)?,
        })
    }

    if let Some(end_time) = end_time {
        let end_time_ns = encode_query_timestamp_ns(end_time)?;
        read_rows_with_params(
            path,
            "SELECT
                ROWID,
                id,
                timestamp,
                duration,
                exit,
                command,
                cwd,
                session,
                hostname
             FROM history
             WHERE deleted_at IS NULL AND ROWID > ?1 AND timestamp <= ?2
             ORDER BY ROWID ASC",
            (from_row_id, end_time_ns),
            from_row_id,
            map_atuin_row,
        )
    } else {
        read_rows_after(
            path,
            "SELECT
                ROWID,
                id,
                timestamp,
                duration,
                exit,
                command,
                cwd,
                session,
                hostname
             FROM history
             WHERE deleted_at IS NULL AND ROWID > ?
             ORDER BY ROWID ASC",
            from_row_id,
            map_atuin_row,
        )
    }
}

fn encode_query_timestamp_ns(end_time: Timestamp) -> Result<i64, rusqlite::Error> {
    i64::try_from(end_time.inner().unix_timestamp_nanos()).map_err(|error| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "Atuin query end_time is outside SQLite i64 nanosecond range: {} ({error})",
                end_time.format_rfc3339()
            ),
        )))
    })
}

/// Get the current maximum row ID from the Atuin history database.
pub fn get_max_row_id(path: &Utf8PathBuf) -> Result<i64, rusqlite::Error> {
    max_row_id_for_query(
        path,
        "SELECT MAX(ROWID) FROM history WHERE deleted_at IS NULL",
    )
}
