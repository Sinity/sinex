use camino::Utf8PathBuf;
use rusqlite::Connection;
use sinex_terminal_ingestor::fish_history::{
    get_max_row_id, is_fish_sqlite_history, read_fish_history,
};
use std::fs;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

fn create_test_fish_history(dir: &TempDir) -> Utf8PathBuf {
    let db_path = dir.path().join("fish_history");
    let conn = Connection::open(&db_path).unwrap();

    conn.execute(
        "CREATE TABLE history (
            command TEXT NOT NULL,
            \"when\" INTEGER
        )",
        [],
    )
    .unwrap();

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

#[sinex_test]
async fn test_is_fish_sqlite_history_detects_valid_database() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let history_path = create_test_fish_history(&temp_dir);

    assert!(is_fish_sqlite_history(&history_path));
    Ok(())
}

#[sinex_test]
async fn test_is_fish_sqlite_history_rejects_invalid_file() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let invalid_path = temp_dir.path().join("not_a_db.txt");
    fs::write(&invalid_path, "just some text").unwrap();

    let invalid_utf8 = Utf8PathBuf::from_path_buf(invalid_path).expect("temp path should be valid utf8");

    assert!(!is_fish_sqlite_history(&invalid_utf8));
    Ok(())
}

#[sinex_test]
async fn test_read_fish_history_returns_all_entries() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let history_path = create_test_fish_history(&temp_dir);

    let (entries, last_row_id) = read_fish_history(&history_path, 0).unwrap();

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].command, "echo hello");
    assert_eq!(entries[1].command, "ls -la");
    assert_eq!(entries[2].command, "cd /tmp");
    assert_eq!(last_row_id, 3);
    Ok(())
}

#[sinex_test]
async fn test_read_fish_history_incremental() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let history_path = create_test_fish_history(&temp_dir);

    let (entries, last_row_id) = read_fish_history(&history_path, 0).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(last_row_id, 3);

    let db_path = history_path.as_std_path();
    let conn = Connection::open(db_path).unwrap();
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?, ?)",
        ["echo new", "1234567893"],
    )
    .unwrap();

    let (new_entries, new_last_row_id) = read_fish_history(&history_path, last_row_id).unwrap();
    assert_eq!(new_entries.len(), 1);
    assert_eq!(new_entries[0].command, "echo new");
    assert_eq!(new_last_row_id, 4);
    Ok(())
}

#[sinex_test]
async fn test_get_max_row_id() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let history_path = create_test_fish_history(&temp_dir);

    let max_id = get_max_row_id(&history_path).unwrap();
    assert_eq!(max_id, 3);
    Ok(())
}
