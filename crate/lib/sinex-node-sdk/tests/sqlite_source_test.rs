use std::{io::Error as IoError, sync::Arc};

use camino::Utf8PathBuf;
use rusqlite::Connection;
use sinex_node_sdk::{
    SqliteHistoryImportError, SqliteHistoryRowOutcome, SqliteHistoryWarningDisposition,
    import_sqlite_history_lenient,
    import_sqlite_history_strict, read_rows_after,
};
use sinex_primitives::Timestamp;
use tokio::sync::Mutex;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn sqlite_history_lenient_import_advances_past_skippable_warning()
-> TestResult<()>
{
    let seen_rows = Arc::new(Mutex::new(Vec::new()));
    let expected_end_time =
        Timestamp::from_unix_timestamp(42).expect("static timestamp should be valid");

    let report = import_sqlite_history_lenient(
        5,
        Some(expected_end_time),
        |from_row_id, end_time| {
            assert_eq!(from_row_id, 5);
            assert_eq!(end_time, Some(expected_end_time));
            Ok::<_, IoError>((vec![1_i64, 2, 3], 9))
        },
        |row_id| *row_id,
        |row_id| {
            let seen_rows = Arc::clone(&seen_rows);
            async move {
                seen_rows.lock().await.push(row_id);
                if row_id == 2 {
                    Err(format!("row {row_id} was malformed"))
                } else if row_id == 3 {
                    Ok(SqliteHistoryRowOutcome::Skipped)
                } else {
                    Ok(SqliteHistoryRowOutcome::Processed)
                }
            }
        },
        |_warning| SqliteHistoryWarningDisposition::SkipRow,
    )
    .await?;

    assert_eq!(report.processed_rows, 1);
    assert_eq!(report.last_row_id, 5);
    assert_eq!(report.warnings, vec!["row 2 was malformed".to_string()]);
    assert_eq!(*seen_rows.lock().await, vec![1, 2, 3]);

    Ok(())
}

#[sinex_test]
async fn sqlite_history_lenient_import_advances_across_processed_and_skipped_rows() -> TestResult<()>
{
    let report = import_sqlite_history_lenient(
        0,
        None,
        |_from_row_id, _end_time| Ok::<_, IoError>((vec![1_i64, 2, 3], 3)),
        |row_id| *row_id,
        |row_id| async move {
            if row_id == 2 {
                Ok::<_, String>(SqliteHistoryRowOutcome::Skipped)
            } else {
                Ok::<_, String>(SqliteHistoryRowOutcome::Processed)
            }
        },
        |_warning| SqliteHistoryWarningDisposition::Retry,
    )
    .await?;

    assert_eq!(report.processed_rows, 2);
    assert_eq!(report.last_row_id, 3);
    assert!(report.warnings.is_empty());

    Ok(())
}

#[sinex_test]
async fn sqlite_history_lenient_import_retries_without_advancing_on_retryable_warning()
-> TestResult<()>
{
    let seen_rows = Arc::new(Mutex::new(Vec::new()));

    let report = import_sqlite_history_lenient(
        5,
        None,
        |_from_row_id, _end_time| Ok::<_, IoError>((vec![1_i64, 2, 3], 9)),
        |row_id| *row_id,
        |row_id| {
            let seen_rows = Arc::clone(&seen_rows);
            async move {
                seen_rows.lock().await.push(row_id);
                if row_id == 2 {
                    Err(format!("row {row_id} should be retried"))
                } else {
                    Ok(SqliteHistoryRowOutcome::Processed)
                }
            }
        },
        |_warning| SqliteHistoryWarningDisposition::Retry,
    )
    .await?;

    assert_eq!(report.processed_rows, 1);
    assert_eq!(report.last_row_id, 5);
    assert_eq!(report.warnings, vec!["row 2 should be retried".to_string()]);
    assert_eq!(*seen_rows.lock().await, vec![1, 2]);

    Ok(())
}

#[sinex_test]
async fn sqlite_history_strict_import_stops_on_processing_failure() -> TestResult<()> {
    let seen_rows = Arc::new(Mutex::new(Vec::new()));

    let error = import_sqlite_history_strict(
        0,
        None,
        |_from_row_id, _end_time| Ok::<_, IoError>((vec![1_i64, 2, 3], 3)),
        |row_id| {
            let seen_rows = Arc::clone(&seen_rows);
            async move {
                seen_rows.lock().await.push(row_id);
                if row_id == 2 {
                    Err(IoError::other("process failed"))
                } else {
                    Ok(SqliteHistoryRowOutcome::Processed)
                }
            }
        },
    )
    .await
    .expect_err("strict importer should stop on the first processing failure");

    match error {
        SqliteHistoryImportError::Process(error) => {
            assert_eq!(error.to_string(), "process failed");
        }
        other => panic!("expected processing failure, got {other:?}"),
    }
    assert_eq!(*seen_rows.lock().await, vec![1, 2]);

    Ok(())
}

#[sinex_test]
async fn sqlite_history_strict_import_surfaces_read_failures() -> TestResult<()> {
    let error = import_sqlite_history_strict::<i64, _, _, _, _, IoError>(
        17,
        None,
        |from_row_id, end_time| {
            assert_eq!(from_row_id, 17);
            assert_eq!(end_time, None);
            Err(IoError::other("read failed"))
        },
        |_row_id| async move { Ok(SqliteHistoryRowOutcome::Processed) },
    )
    .await
    .expect_err("strict importer should return the read failure");

    match error {
        SqliteHistoryImportError::Read(error) => {
            assert_eq!(error.to_string(), "read failed");
        }
        other => panic!("expected read failure, got {other:?}"),
    }

    Ok(())
}

#[sinex_test]
async fn sqlite_row_reader_rejects_malformed_rows_without_advancing() -> TestResult<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("history.sqlite");
    let conn = Connection::open(&db_path)?;
    conn.execute(
        "CREATE TABLE history (
            command TEXT NOT NULL,
            \"when\" INTEGER
        )",
        [],
    )?;
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?1, ?2)",
        rusqlite::params!["echo ok", 1_234_567_890_i64],
    )?;
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?1, ?2)",
        rusqlite::params!["echo broken", "not-a-timestamp"],
    )?;

    let path = Utf8PathBuf::from_path_buf(db_path)
        .map_err(|_| color_eyre::eyre::eyre!("temporary sqlite path should be valid UTF-8"))?;

    let error = read_rows_after(
        &path,
        "SELECT ROWID, command, \"when\" FROM history WHERE ROWID > ? ORDER BY ROWID ASC",
        0,
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        },
    )
    .expect_err("malformed sqlite row should fail the read");

    assert!(
        error.to_string().contains("failed to map SQLite row 2"),
        "unexpected error: {error}"
    );

    Ok(())
}
