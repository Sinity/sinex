use camino::Utf8PathBuf;
use color_eyre::eyre::Context;
use rusqlite::Connection;
use sinex_primitives::Timestamp;
use sinex_terminal_ingestor::fish_history::{
    get_max_row_id, is_fish_sqlite_history, read_fish_history,
};
use std::fs;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

fn create_test_fish_history(dir: &TempDir) -> TestResult<Utf8PathBuf> {
    let db_path = dir.path().join("fish_history");
    let conn = Connection::open(&db_path).wrap_err("open fish history test database")?;

    conn.execute(
        "CREATE TABLE history (
            command TEXT NOT NULL,
            \"when\" INTEGER
        )",
        [],
    )
    .wrap_err("create fish history table")?;

    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?, ?)",
        ["echo hello", "1234567890"],
    )
    .wrap_err("insert fish history row 1")?;
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?, ?)",
        ["ls -la", "1234567891"],
    )
    .wrap_err("insert fish history row 2")?;
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?, ?)",
        ["cd /tmp", "1234567892"],
    )
    .wrap_err("insert fish history row 3")?;

    Utf8PathBuf::from_path_buf(db_path)
        .map_err(|_| color_eyre::eyre::eyre!("temporary fish history path should be valid UTF-8"))
}

#[sinex_test]
async fn test_is_fish_sqlite_history_detects_valid_database() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_fish_history(&temp_dir)?;

    assert!(is_fish_sqlite_history(&history_path));
    Ok(())
}

#[sinex_test]
async fn test_is_fish_sqlite_history_rejects_invalid_file() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let invalid_path = temp_dir.path().join("not_a_db.txt");
    fs::write(&invalid_path, "just some text").wrap_err("write invalid history file")?;

    let invalid_utf8 = Utf8PathBuf::from_path_buf(invalid_path).map_err(|_| {
        color_eyre::eyre::eyre!("temporary invalid history path should be valid UTF-8")
    })?;

    assert!(!is_fish_sqlite_history(&invalid_utf8));
    Ok(())
}

#[sinex_test]
async fn test_read_fish_history_returns_all_entries() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_fish_history(&temp_dir)?;

    let (entries, last_row_id) =
        read_fish_history(&history_path, 0, None).wrap_err("read full fish history")?;

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].command, "echo hello");
    assert_eq!(entries[1].command, "ls -la");
    assert_eq!(entries[2].command, "cd /tmp");
    assert_eq!(last_row_id, 3);
    Ok(())
}

#[sinex_test]
async fn test_read_fish_history_incremental() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_fish_history(&temp_dir)?;

    let (entries, last_row_id) =
        read_fish_history(&history_path, 0, None).wrap_err("read initial fish history")?;
    assert_eq!(entries.len(), 3);
    assert_eq!(last_row_id, 3);

    let db_path = history_path.as_std_path();
    let conn = Connection::open(db_path).wrap_err("re-open fish history database")?;
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?, ?)",
        ["echo new", "1234567893"],
    )
    .wrap_err("insert incremental fish history row")?;

    let (new_entries, new_last_row_id) =
        read_fish_history(&history_path, last_row_id, None)
            .wrap_err("read incremental fish history")?;
    assert_eq!(new_entries.len(), 1);
    assert_eq!(new_entries[0].command, "echo new");
    assert_eq!(new_last_row_id, 4);
    Ok(())
}

#[sinex_test]
async fn test_read_fish_history_respects_end_time_boundary() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_fish_history(&temp_dir)?;
    let end_time = Timestamp::from_unix_timestamp(1_234_567_891)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid Fish end time"))?;

    let (entries, last_row_id) =
        read_fish_history(&history_path, 0, Some(end_time)).wrap_err("read bounded fish history")?;

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].command, "echo hello");
    assert_eq!(entries[1].command, "ls -la");
    assert_eq!(last_row_id, 2);
    Ok(())
}

#[sinex_test]
async fn test_get_max_row_id() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_fish_history(&temp_dir)?;

    let max_id = get_max_row_id(&history_path).wrap_err("query max row id")?;
    assert_eq!(max_id, 3);
    Ok(())
}
