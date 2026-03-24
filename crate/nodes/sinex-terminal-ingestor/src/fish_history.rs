//! Fish shell history parser
//!
//! Fish stores its history in an `SQLite` database at `~/.local/share/fish/fish_history`.
//! This module provides functionality to read command history from that database.

use camino::Utf8PathBuf;
use sinex_node_sdk::{is_sqlite_with_tables, max_row_id_for_query, read_rows_after};

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

/// Check if a path points to a Fish `SQLite` history file
#[must_use]
pub fn is_fish_sqlite_history(path: &Utf8PathBuf) -> bool {
    is_sqlite_with_tables(path, &["history"])
}

/// Read Fish history entries starting from a given row offset
///
/// Returns a tuple of (entries, `last_row_id`) where `last_row_id` is the highest
/// row ID encountered, which can be used as the starting point for the next read.
pub fn read_fish_history(
    path: &Utf8PathBuf,
    from_row_id: i64,
) -> Result<(Vec<FishHistoryEntry>, i64), rusqlite::Error> {
    read_rows_after(
        path,
        "SELECT ROWID, command, \"when\" FROM history WHERE ROWID > ? ORDER BY ROWID ASC",
        from_row_id,
        |row| {
            Ok(FishHistoryEntry {
                row_id: row.get(0)?,
                command: row.get(1)?,
                when: row.get::<_, Option<i64>>(2).ok().flatten(),
            })
        },
    )
}

/// Get the current maximum row ID from the Fish history database
///
/// This can be used to initialize tracking or to check if new entries are available.
pub fn get_max_row_id(path: &Utf8PathBuf) -> Result<i64, rusqlite::Error> {
    max_row_id_for_query(path, "SELECT MAX(ROWID) FROM history")
}
