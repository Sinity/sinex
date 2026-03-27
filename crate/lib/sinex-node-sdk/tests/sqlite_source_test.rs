use std::{io::Error as IoError, sync::Arc};

use sinex_node_sdk::{
    SqliteHistoryImportError, SqliteHistoryRowOutcome, import_sqlite_history_lenient,
    import_sqlite_history_strict,
};
use sinex_primitives::Timestamp;
use tokio::sync::Mutex;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn sqlite_history_lenient_import_collects_warnings_and_advances_checkpoint() -> TestResult<()>
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
    )
    .await?;

    assert_eq!(report.processed_rows, 1);
    assert_eq!(report.last_row_id, 9);
    assert_eq!(report.warnings, vec!["row 2 was malformed".to_string()]);
    assert_eq!(*seen_rows.lock().await, vec![1, 2, 3]);

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
