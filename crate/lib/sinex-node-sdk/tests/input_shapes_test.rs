use std::{io::Error as IoError, sync::Arc};

use camino::Utf8PathBuf;
use sinex_node_sdk::{
    AppendOnlyFileState, SqliteHistoryImportError, SqliteHistoryRowOutcome,
    SqliteHistoryWarningDisposition, SqliteSourceCheckpointState,
    checkpointed_sqlite_history_lenient, checkpointed_sqlite_history_strict,
    checkpointed_sqlite_source_lenient, discover_importable_files_at_root,
    poll_append_only_utf8_source,
};
use tempfile::tempdir;
use tokio::{fs, io::AsyncWriteExt, sync::Mutex};
use xtask::sandbox::prelude::*;

fn utf8_path(path: &std::path::Path) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(path.to_path_buf())
        .unwrap_or_else(|path| panic!("non-utf8 path: {}", path.display()))
}

#[sinex_test]
async fn checkpointed_sqlite_history_lenient_advances_cursor(_ctx: TestContext) -> TestResult<()> {
    let mut cursor = 5_i64;
    let seen_rows = Arc::new(Mutex::new(Vec::new()));

    let report = checkpointed_sqlite_history_lenient(
        &mut cursor,
        None,
        |_from_row_id, _end_time| Ok::<_, IoError>((vec![6_i64, 7, 8], 8)),
        |row_id| *row_id,
        |row_id| {
            let seen_rows = Arc::clone(&seen_rows);
            async move {
                seen_rows.lock().await.push(row_id);
                if row_id == 7 {
                    Err(format!("row {row_id} was malformed"))
                } else {
                    Ok(SqliteHistoryRowOutcome::Processed)
                }
            }
        },
        |_warning| SqliteHistoryWarningDisposition::SkipRow,
    )
    .await?;

    assert_eq!(report.processed_rows, 2);
    assert_eq!(report.last_row_id, 8);
    assert_eq!(cursor, 8);
    assert_eq!(*seen_rows.lock().await, vec![6, 7, 8]);
    Ok(())
}

#[sinex_test]
async fn checkpointed_sqlite_history_strict_advances_cursor(_ctx: TestContext) -> TestResult<()> {
    let mut cursor = 1_i64;

    let report = checkpointed_sqlite_history_strict(
        &mut cursor,
        None,
        |_from_row_id, _end_time| Ok::<_, IoError>((vec![2_i64, 3], 3)),
        |_row_id| async move { Ok::<_, IoError>(SqliteHistoryRowOutcome::Processed) },
    )
    .await?;

    assert_eq!(report.processed_rows, 2);
    assert_eq!(report.last_row_id, 3);
    assert_eq!(cursor, 3);
    Ok(())
}

#[sinex_test]
async fn checkpointed_sqlite_history_strict_preserves_cursor_on_failure(
    _ctx: TestContext,
) -> TestResult<()> {
    let mut cursor = 9_i64;

    let error = checkpointed_sqlite_history_strict(
        &mut cursor,
        None,
        |_from_row_id, _end_time| Ok::<_, IoError>((vec![10_i64], 10)),
        |_row_id| async move { Err::<SqliteHistoryRowOutcome, _>(IoError::other("boom")) },
    )
    .await
    .expect_err("strict checkpointed import should stop on process failure");

    match error {
        SqliteHistoryImportError::Process(error) => {
            assert_eq!(error.to_string(), "boom");
        }
        other => panic!("expected process failure, got {other:?}"),
    }
    assert_eq!(cursor, 9);
    Ok(())
}

#[sinex_test]
async fn checkpointed_sqlite_source_state_tracks_keyed_cursors(
    _ctx: TestContext,
) -> TestResult<()> {
    let mut state = SqliteSourceCheckpointState::default();

    let report = checkpointed_sqlite_source_lenient(
        &mut state,
        "browser::/tmp/history.sqlite",
        None,
        |_from_row_id, _end_time| Ok::<_, IoError>((vec![1_i64, 2_i64], 2)),
        |row_id| *row_id,
        |_row_id| async move { Ok::<_, String>(SqliteHistoryRowOutcome::Processed) },
        |_warning| SqliteHistoryWarningDisposition::Retry,
    )
    .await?;

    assert_eq!(report.last_row_id, 2);
    assert_eq!(state.cursor("browser::/tmp/history.sqlite"), 2);
    assert!(!state.is_empty());
    Ok(())
}

#[sinex_test]
async fn poll_append_only_source_updates_tracked_state(_ctx: TestContext) -> TestResult<()> {
    let dir = tempdir()?;
    let path = utf8_path(&dir.path().join("history.log"));
    let mut state = AppendOnlyFileState::default();

    fs::write(&path, "echo one\necho two\n").await?;

    let first = poll_append_only_utf8_source(&path, &mut state).await?;
    assert_eq!(first.lines, vec!["echo one", "echo two"]);
    assert_eq!(state.offset_bytes, "echo one\necho two\n".len() as u64);

    let mut file = fs::OpenOptions::new().append(true).open(&path).await?;
    file.write_all(b"echo three\n").await?;
    file.flush().await?;

    let second = poll_append_only_utf8_source(&path, &mut state).await?;
    assert_eq!(second.lines, vec!["echo three"]);
    assert_eq!(
        state.offset_bytes,
        "echo one\necho two\necho three\n".len() as u64
    );
    Ok(())
}

#[sinex_test]
async fn discover_importable_files_remembers_scan_root(_ctx: TestContext) -> TestResult<()> {
    let dir = tempdir()?;
    let path = utf8_path(&dir.path().join("history.jsonl"));
    std::fs::write(path.as_std_path(), "{\"url\":\"https://example.com\"}\n")?;

    let root = utf8_path(dir.path());
    let mut state = sinex_node_sdk::BatchImporterState::default();
    let files = discover_importable_files_at_root(&mut state, &root, &[".jsonl"])?;

    assert_eq!(files.len(), 1);
    assert!(state.scan_roots.contains(&root));
    Ok(())
}
