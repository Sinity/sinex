//! Fish shell history parser
//!
//! Fish stores its history in an `SQLite` database at `~/.local/share/fish/fish_history`.
//! This module provides functionality to read command history from that database.

use camino::Utf8PathBuf;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::Path;

/// Represents a single command from Fish history
#[derive(Debug, Clone)]
pub struct FishHistoryEntry {
    /// The command text
    pub command: String,
    /// Unix timestamp when the command was executed (if available)
    pub when: Option<i64>,
}

/// Check if a path points to a Fish `SQLite` history file
#[must_use]
pub fn is_fish_sqlite_history(path: &Utf8PathBuf) -> bool {
    // Fish history is stored in SQLite format
    // We can detect this by checking if it's a valid SQLite database
    let path_std = Path::new(path.as_str());

    if !path_std.exists() {
        return false;
    }

    // Try to open as SQLite database (read-only)
    match Connection::open_with_flags(path_std, OpenFlags::SQLITE_OPEN_READ_ONLY) {
        Ok(conn) => {
            // Check if it has the expected Fish history schema
            let has_history_table: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='history'",
                    [],
                    |row| {
                        let count: i64 = row.get(0)?;
                        Ok(count > 0)
                    },
                )
                .unwrap_or(false);

            has_history_table
        }
        Err(_) => false,
    }
}

/// Read Fish history entries starting from a given row offset
///
/// Returns a tuple of (entries, `last_row_id`) where `last_row_id` is the highest
/// row ID encountered, which can be used as the starting point for the next read.
pub fn read_fish_history(
    path: &Utf8PathBuf,
    from_row_id: i64,
) -> Result<(Vec<FishHistoryEntry>, i64), rusqlite::Error> {
    let path_std = Path::new(path.as_str());
    let conn = Connection::open_with_flags(path_std, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    // Fish history schema typically has columns: id, command, when (timestamp)
    // We query for all entries with id > from_row_id
    let mut stmt = conn.prepare(
        "SELECT ROWID, command, \"when\" FROM history WHERE ROWID > ? ORDER BY ROWID ASC",
    )?;

    let entries = stmt
        .query_map([from_row_id], |row| {
            Ok((
                row.get::<_, i64>(0)?, // ROWID
                FishHistoryEntry {
                    command: row.get(1)?,
                    when: row.get::<_, Option<i64>>(2).ok().flatten(),
                },
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Find the highest row ID encountered
    let last_row_id = entries
        .iter()
        .map(|(row_id, _)| *row_id)
        .max()
        .unwrap_or(from_row_id);

    let history_entries = entries.into_iter().map(|(_, entry)| entry).collect();

    Ok((history_entries, last_row_id))
}

/// Get the current maximum row ID from the Fish history database
///
/// This can be used to initialize tracking or to check if new entries are available.
pub fn get_max_row_id(path: &Utf8PathBuf) -> Result<i64, rusqlite::Error> {
    let path_std = Path::new(path.as_str());
    let conn = Connection::open_with_flags(path_std, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    let max_id: Option<i64> = conn
        .query_row("SELECT MAX(ROWID) FROM history", [], |row| row.get(0))
        .optional()?;

    Ok(max_id.unwrap_or(0))
}
