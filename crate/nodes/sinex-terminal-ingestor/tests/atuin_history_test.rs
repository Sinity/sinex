use camino::Utf8PathBuf;
use color_eyre::eyre::Context;
use rusqlite::Connection;
use sinex_primitives::Timestamp;
use sinex_terminal_ingestor::atuin_history::{
    ensure_atuin_sqlite_history, get_max_row_id, read_atuin_history,
};
use std::fs;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

fn create_test_atuin_history(dir: &TempDir) -> TestResult<Utf8PathBuf> {
    let db_path = dir.path().join("history.db");
    let conn = Connection::open(&db_path).wrap_err("open Atuin history test database")?;

    conn.execute(
        "CREATE TABLE history (
            id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            duration INTEGER NOT NULL,
            exit INTEGER NOT NULL,
            command TEXT NOT NULL,
            cwd TEXT NOT NULL,
            session TEXT NOT NULL,
            hostname TEXT NOT NULL,
            deleted_at INTEGER
        )",
        [],
    )
    .wrap_err("create Atuin history table")?;

    conn.execute(
        "INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        ("h1", 1_700_000_000_000_000_000i64, 50_000_000i64, 0i64, "echo hello", "/tmp", "s1", "host-a"),
    )
    .wrap_err("insert Atuin history row 1")?;
    conn.execute(
        "INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        ("h2", 1_700_000_100_000_000_000i64, 75_000_000i64, 1i64, "ls -la", "/realm", "s2", "host-b"),
    )
    .wrap_err("insert Atuin history row 2")?;

    Utf8PathBuf::from_path_buf(db_path)
        .map_err(|_| color_eyre::eyre::eyre!("temporary Atuin history path should be valid UTF-8"))
}

#[sinex_test]
async fn test_ensure_atuin_sqlite_history_detects_valid_database() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_atuin_history(&temp_dir)?;

    ensure_atuin_sqlite_history(&history_path)?;
    Ok(())
}

#[sinex_test]
async fn test_ensure_atuin_sqlite_history_rejects_invalid_file() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let invalid_path = temp_dir.path().join("not_a_db.txt");
    fs::write(&invalid_path, "just some text").wrap_err("write invalid history file")?;

    let invalid_utf8 = Utf8PathBuf::from_path_buf(invalid_path).map_err(|_| {
        color_eyre::eyre::eyre!("temporary invalid history path should be valid UTF-8")
    })?;

    let error = ensure_atuin_sqlite_history(&invalid_utf8)
        .expect_err("invalid Atuin history file must surface the SQLite validation error");
    assert!(
        !error.to_string().is_empty(),
        "invalid Atuin history file should preserve error context"
    );
    Ok(())
}

#[sinex_test]
async fn test_read_atuin_history_returns_all_entries() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_atuin_history(&temp_dir)?;

    let (entries, last_row_id) =
        read_atuin_history(&history_path, 0, None).wrap_err("read full Atuin history")?;

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].history_id, "h1");
    assert_eq!(entries[0].command, "echo hello");
    assert_eq!(entries[1].history_id, "h2");
    assert_eq!(entries[1].exit_code, 1);
    assert_eq!(last_row_id, 2);
    Ok(())
}

#[sinex_test]
async fn test_read_atuin_history_incremental() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_atuin_history(&temp_dir)?;

    let (entries, last_row_id) =
        read_atuin_history(&history_path, 0, None).wrap_err("read initial Atuin history")?;
    assert_eq!(entries.len(), 2);
    assert_eq!(last_row_id, 2);

    let conn = Connection::open(history_path.as_std_path()).wrap_err("re-open Atuin database")?;
    conn.execute(
        "INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        ("h3", 1_700_000_200_000_000_000i64, 10_000_000i64, 0i64, "pwd", "/tmp", "s3", "host-c"),
    )
    .wrap_err("insert incremental Atuin history row")?;

    let (new_entries, new_last_row_id) =
        read_atuin_history(&history_path, last_row_id, None)
            .wrap_err("read incremental Atuin history")?;
    assert_eq!(new_entries.len(), 1);
    assert_eq!(new_entries[0].history_id, "h3");
    assert_eq!(new_last_row_id, 3);
    Ok(())
}

#[sinex_test]
async fn test_read_atuin_history_respects_end_time_boundary() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_atuin_history(&temp_dir)?;
    let end_time = Timestamp::from_unix_timestamp_nanos(1_700_000_050_000_000_000i128)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid Atuin end time"))?;

    let (entries, last_row_id) = read_atuin_history(&history_path, 0, Some(end_time))
        .wrap_err("read bounded Atuin history")?;

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].history_id, "h1");
    assert_eq!(last_row_id, 1);
    Ok(())
}

#[sinex_test]
async fn test_get_max_row_id() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_atuin_history(&temp_dir)?;

    let max_id = get_max_row_id(&history_path).wrap_err("query max row id")?;
    assert_eq!(max_id, 2);
    Ok(())
}
