//! Fish SQLite history reader.
//!
//! Native Fish history is a YAML-like text file at `~/.local/share/fish/fish_history`.
//! This module only handles explicitly SQLite-backed Fish history sources.

use camino::Utf8PathBuf;
use sinex_node_sdk::{
    SqliteTableCheckError, ensure_sqlite_with_tables, max_row_id_for_query, read_rows_after,
};
use sinex_primitives::Timestamp;
use sinex_node_sdk::read_rows_with_params;

/// Represents a single command from Fish history
#[derive(Debug, Clone)]
pub struct FishHistoryEntry {
    /// Stable SQLite row identifier for idempotent historical replay.
    pub row_id: i64,
    /// The command text
    pub command: String,
    /// Unix timestamp when the command was executed (if available)
    pub when: Option<i64>,
}

pub fn ensure_fish_sqlite_history(path: &Utf8PathBuf) -> Result<(), SqliteTableCheckError> {
    ensure_sqlite_with_tables(path, &["history"])
}

/// Read Fish history entries starting from a given row offset
///
/// Returns a tuple of (entries, `last_row_id`) where `last_row_id` is the highest
/// row ID encountered, which can be used as the starting point for the next read.
pub fn read_fish_history(
    path: &Utf8PathBuf,
    from_row_id: i64,
    end_time: Option<Timestamp>,
) -> Result<(Vec<FishHistoryEntry>, i64), rusqlite::Error> {
    fn map_fish_row(row: &rusqlite::Row<'_>) -> Result<FishHistoryEntry, rusqlite::Error> {
        Ok(FishHistoryEntry {
            row_id: row.get(0)?,
            command: row.get(1)?,
            when: row.get(2)?,
        })
    }

    if let Some(end_time) = end_time {
        read_rows_with_params(
            path,
            "SELECT ROWID, command, \"when\"
             FROM history
             WHERE ROWID > ?1 AND (\"when\" IS NULL OR \"when\" <= ?2)
             ORDER BY ROWID ASC",
            (from_row_id, end_time.inner().unix_timestamp()),
            from_row_id,
            map_fish_row,
        )
    } else {
        read_rows_after(
            path,
            "SELECT ROWID, command, \"when\" FROM history WHERE ROWID > ? ORDER BY ROWID ASC",
            from_row_id,
            map_fish_row,
        )
    }
}

/// Get the current maximum row ID from the Fish history database
///
/// This can be used to initialize tracking or to check if new entries are available.
pub fn get_max_row_id(path: &Utf8PathBuf) -> Result<i64, rusqlite::Error> {
    max_row_id_for_query(path, "SELECT MAX(ROWID) FROM history")
}
