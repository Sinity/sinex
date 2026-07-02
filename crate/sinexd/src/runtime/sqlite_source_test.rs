use super::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn sqlite_snapshot_uses_online_backup_and_reports_shape() -> TestResult<()> {
    let temp = tempfile::NamedTempFile::new()?;
    let conn = rusqlite::Connection::open(temp.path())?;
    conn.execute(
        "CREATE TABLE history (id INTEGER PRIMARY KEY, value TEXT)",
        [],
    )?;
    conn.execute("INSERT INTO history (value) VALUES ('one'), ('two')", [])?;
    drop(conn);

    let path = Utf8PathBuf::from_path_buf(temp.path().to_path_buf()).map_err(|path| {
        SinexError::validation("test path is not valid UTF-8")
            .with_context("path", format!("{path:?}"))
    })?;
    let capture = capture_sqlite_snapshot(&path, "test://sqlite")?;
    assert!(capture.metadata().total_bytes > 0);
    assert!(capture.metadata().page_size > 0);
    assert!(capture.metadata().page_count > 0);
    assert_eq!(
        capture.metadata().capture_method,
        SQLITE_ONLINE_BACKUP_METHOD
    );
    assert_eq!(capture.metadata().source_identifier, "test://sqlite");

    let snapshot_conn = rusqlite::Connection::open(capture.path().as_std_path())?;
    let count: i64 =
        snapshot_conn.query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))?;
    assert_eq!(count, 2);
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn sqlite_reads_fall_back_to_immutable_when_wal_sidecars_are_readonly() -> TestResult<()>
{
    use std::os::unix::fs::PermissionsExt;

    struct PermissionRestore {
        dir: std::path::PathBuf,
        db: std::path::PathBuf,
    }

    impl Drop for PermissionRestore {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o700));
            let _ = std::fs::set_permissions(&self.db, std::fs::Permissions::from_mode(0o600));
        }
    }

    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("history.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            r"
            PRAGMA journal_mode=WAL;
            CREATE TABLE history (value TEXT);
            INSERT INTO history (value) VALUES ('one'), ('two');
            ",
        )?;
    }
    let _ = std::fs::remove_file(temp.path().join("history.db-wal"));
    let _ = std::fs::remove_file(temp.path().join("history.db-shm"));
    std::fs::set_permissions(&db_path, std::fs::Permissions::from_mode(0o400))?;
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o500))?;
    let _restore = PermissionRestore {
        dir: temp.path().to_path_buf(),
        db: db_path.clone(),
    };

    let path = Utf8PathBuf::from_path_buf(db_path).map_err(|path| {
        SinexError::validation("test path is not valid UTF-8")
            .with_context("path", format!("{path:?}"))
    })?;

    ensure_sqlite_with_tables(&path, &["history"])?;
    let max_id = max_row_id_for_query(&path, "SELECT MAX(ROWID) FROM history")?;
    let (rows, last_row_id) = read_rows_after(
        &path,
        "SELECT ROWID, value FROM history WHERE ROWID > ? ORDER BY ROWID ASC",
        0,
        |row| row.get::<_, String>(1),
    )?;

    assert_eq!(max_id, 2);
    assert_eq!(last_row_id, 2);
    assert_eq!(rows, vec!["one".to_string(), "two".to_string()]);
    Ok(())
}

#[sinex_test]
async fn sqlite_snapshot_policy_uses_explicit_boundaries() -> TestResult<()> {
    let now = Timestamp::now();
    let mut state = SqliteSnapshotState::default();
    let policy = SqliteSnapshotPolicy::disabled()
        .with_first_observation(true)
        .with_historical_boundary(true)
        .with_min_row_delta(Some(10))
        .with_min_elapsed(Some(std::time::Duration::from_mins(1)));

    assert_eq!(
        policy.decide(&state, 0, false, now),
        Some(SqliteSnapshotTrigger::FirstObservation)
    );

    state.record_success(now, 7);
    assert_eq!(
        policy.decide(&state, 8, true, now),
        Some(SqliteSnapshotTrigger::HistoricalBoundary)
    );
    assert_eq!(
        policy.decide(&state, 17, false, now),
        Some(SqliteSnapshotTrigger::RowDelta)
    );
    Ok(())
}

#[sinex_test]
async fn sqlite_snapshot_policy_skips_until_cadence_boundary() -> TestResult<()> {
    let now = Timestamp::now();
    let mut state = SqliteSnapshotState::default();
    state.record_success(now, 100);
    let policy = SqliteSnapshotPolicy::disabled()
        .with_first_observation(true)
        .with_min_row_delta(Some(10))
        .with_min_elapsed(Some(std::time::Duration::from_mins(30)))
        .with_stale_clean_shutdown_after(Some(std::time::Duration::from_hours(12)));

    assert_eq!(policy.decide(&state, 109, false, now), None);
    assert_eq!(
        policy.decide(&state, 110, false, now),
        Some(SqliteSnapshotTrigger::RowDelta)
    );

    let elapsed = now + time::Duration::minutes(30);
    assert_eq!(
        policy.decide(&state, 101, false, elapsed),
        Some(SqliteSnapshotTrigger::ElapsedDuration)
    );

    let clean_shutdown = now - time::Duration::hours(13);
    state.last_snapshot_at = Some(now);
    state.last_clean_shutdown_at = Some(clean_shutdown);
    assert_eq!(
        policy.decide(&state, 101, false, now),
        Some(SqliteSnapshotTrigger::StaleCleanShutdown)
    );
    Ok(())
}
