use camino::Utf8PathBuf;
use color_eyre::eyre::Context;
use rusqlite::Connection;
use serde_json::json;
use sinex_db::repositories::{DbPoolExt, source_material_relation_types};
use sinex_node_sdk::{
    AcquisitionManager, BufferedRecordSourceHarness, RecordProcessingOutcome, RecordReadHorizon,
    RecordSources, RecordWarningDisposition, SqliteRowCheckpoint, SqliteSnapshotLinker,
    SqliteSnapshotPolicy, SqliteSnapshotState,
};
use sinex_primitives::Timestamp;
use sinex_terminal_ingestor::atuin_history::{
    ensure_atuin_sqlite_history, get_max_row_id, read_atuin_history,
};
use std::fs;
use std::sync::Arc;
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
async fn atuin_history_snapshot_scenario_links_row_stream_to_sqlite_evidence(
    ctx: TestContext,
) -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_atuin_history(&temp_dir)?;
    let source = RecordSources::sqlite(
        history_path.clone(),
        "terminal.atuin://history.db",
        read_atuin_history,
        |entry: &sinex_terminal_ingestor::atuin_history::AtuinHistoryEntry| entry.row_id,
    )
    .with_snapshot_policy(SqliteSnapshotPolicy::disabled().with_first_observation(true));
    let ctx = ctx.with_nats().shared().await?;
    let scope = PipelineScope::new(&ctx).await?;
    let acquisition = Arc::new(AcquisitionManager::new_with_namespace(
        ctx.nats_client(),
        sinex_node_sdk::RotationPolicy::default(),
        "atuin-sqlite-evidence-scenario".to_string(),
        Some(ctx.pipeline_namespace().prefix().to_string()),
    ));
    let harness = BufferedRecordSourceHarness::buffered_default(source, acquisition.clone());
    let mut checkpoint = SqliteRowCheckpoint::default();
    let mut snapshot_state = SqliteSnapshotState::default();

    let mut report = harness
        .read_process_lenient_with_snapshot(
            &mut checkpoint,
            RecordReadHorizon::Unbounded,
            &mut snapshot_state,
            &acquisition,
            |entry, material| async move {
                material
                    .append_json_line(&json!({
                        "row_id": entry.row_id,
                        "history_id": entry.history_id,
                        "command": entry.command,
                        "cwd": entry.cwd,
                        "hostname": entry.hostname,
                    }))
                    .await?;
                Ok::<_, sinex_primitives::SinexError>(RecordProcessingOutcome::Processed)
            },
            |_| RecordWarningDisposition::Retry,
        )
        .await?;
    harness
        .finalize_with_snapshot_evidence(
            "atuin-sqlite-evidence-scenario",
            &mut report,
            Some(SqliteSnapshotLinker::new(ctx.pool())),
        )
        .await?;

    assert_eq!(checkpoint, SqliteRowCheckpoint::new(2));
    assert_eq!(report.processed_records, 2);
    assert_eq!(report.material_anchors.len(), 2);
    let snapshot = report
        .sqlite_snapshot
        .ok_or_else(|| color_eyre::eyre::eyre!("missing Atuin snapshot evidence report"))?;
    let snapshot_material_id = snapshot
        .snapshot_material_id
        .ok_or_else(|| color_eyre::eyre::eyre!("missing Atuin snapshot material id"))?;
    assert_eq!(snapshot.failure, None);
    assert_eq!(snapshot.linked_material_count, 1);
    assert!(snapshot.link_errors.is_empty());
    assert_eq!(snapshot_state.last_snapshot_row_id, Some(2));

    let links = ctx
        .pool()
        .source_materials()
        .links_from(report.material_anchors[0].material_id)
        .await?;
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].to_material_id, snapshot_material_id);
    assert_eq!(
        links[0].relation_type,
        source_material_relation_types::BACKED_BY
    );
    assert_eq!(links[0].metadata["evidence_role"], "sqlite_snapshot");
    assert_eq!(
        links[0].metadata["source_identifier"],
        "terminal.atuin://history.db"
    );
    scope.shutdown().await?;
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

    let (new_entries, new_last_row_id) = read_atuin_history(&history_path, last_row_id, None)
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
async fn test_read_atuin_history_rejects_unrepresentable_end_time_filter() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_atuin_history(&temp_dir)?;
    let end_time = Timestamp::from_unix_timestamp_nanos(i128::from(i64::MAX) + 1)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid far-future timestamp"))?;

    let error = read_atuin_history(&history_path, 0, Some(end_time))
        .expect_err("far-future end_time filter should fail honestly");

    assert!(
        error
            .to_string()
            .contains("outside SQLite i64 nanosecond range")
    );
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
