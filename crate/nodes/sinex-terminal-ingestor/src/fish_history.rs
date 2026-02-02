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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_fish_history(dir: &TempDir) -> Utf8PathBuf {
        let db_path = dir.path().join("fish_history");
        let conn = Connection::open(&db_path).unwrap();

        // Create Fish history schema
        conn.execute(
            "CREATE TABLE history (
                command TEXT NOT NULL,
                \"when\" INTEGER
            )",
            [],
        )
        .unwrap();

        // Insert test data
        conn.execute(
            "INSERT INTO history (command, \"when\") VALUES (?, ?)",
            ["echo hello", "1234567890"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO history (command, \"when\") VALUES (?, ?)",
            ["ls -la", "1234567891"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO history (command, \"when\") VALUES (?, ?)",
            ["cd /tmp", "1234567892"],
        )
        .unwrap();

        Utf8PathBuf::from_path_buf(db_path).expect("temp path should be valid utf8")
    }

    #[test]
    fn test_is_fish_sqlite_history_detects_valid_database() {
        let temp_dir = tempfile::tempdir().unwrap();
        let history_path = create_test_fish_history(&temp_dir);

        assert!(is_fish_sqlite_history(&history_path));
    }

    #[test]
    fn test_is_fish_sqlite_history_rejects_invalid_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let invalid_path = temp_dir.path().join("not_a_db.txt");
        fs::write(&invalid_path, "just some text").unwrap();

        let invalid_utf8 =
            Utf8PathBuf::from_path_buf(invalid_path).expect("temp path should be valid utf8");

        assert!(!is_fish_sqlite_history(&invalid_utf8));
    }

    #[test]
    fn test_read_fish_history_returns_all_entries() {
        let temp_dir = tempfile::tempdir().unwrap();
        let history_path = create_test_fish_history(&temp_dir);

        let (entries, last_row_id) = read_fish_history(&history_path, 0).unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].command, "echo hello");
        assert_eq!(entries[1].command, "ls -la");
        assert_eq!(entries[2].command, "cd /tmp");
        assert_eq!(last_row_id, 3);
    }

    #[test]
    fn test_read_fish_history_incremental() {
        let temp_dir = tempfile::tempdir().unwrap();
        let history_path = create_test_fish_history(&temp_dir);

        // Read first 2 entries
        let (entries, last_row_id) = read_fish_history(&history_path, 0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(last_row_id, 3);

        // Add a new entry
        let db_path = history_path.as_std_path();
        let conn = Connection::open(db_path).unwrap();
        conn.execute(
            "INSERT INTO history (command, \"when\") VALUES (?, ?)",
            ["echo new", "1234567893"],
        )
        .unwrap();

        // Read only new entries
        let (new_entries, new_last_row_id) = read_fish_history(&history_path, last_row_id).unwrap();
        assert_eq!(new_entries.len(), 1);
        assert_eq!(new_entries[0].command, "echo new");
        assert_eq!(new_last_row_id, 4);
    }

    #[test]
    fn test_get_max_row_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let history_path = create_test_fish_history(&temp_dir);

        let max_id = get_max_row_id(&history_path).unwrap();
        assert_eq!(max_id, 3);
    }
}
