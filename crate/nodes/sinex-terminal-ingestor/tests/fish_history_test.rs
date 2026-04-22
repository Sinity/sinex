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
use sinex_terminal_ingestor::fish_history::{
    ensure_fish_sqlite_history, get_max_row_id, read_fish_history,
};
use std::fs;
use std::sync::Arc;
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
async fn test_ensure_fish_sqlite_history_detects_valid_database() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_fish_history(&temp_dir)?;

    ensure_fish_sqlite_history(&history_path)?;
    Ok(())
}

#[sinex_test]
async fn test_ensure_fish_sqlite_history_rejects_invalid_file() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let invalid_path = temp_dir.path().join("not_a_db.txt");
    fs::write(&invalid_path, "just some text").wrap_err("write invalid history file")?;

    let invalid_utf8 = Utf8PathBuf::from_path_buf(invalid_path).map_err(|_| {
        color_eyre::eyre::eyre!("temporary invalid history path should be valid UTF-8")
    })?;

    let error = ensure_fish_sqlite_history(&invalid_utf8)
        .expect_err("invalid Fish history file must surface the SQLite validation error");
    assert!(
        !error.to_string().is_empty(),
        "invalid Fish history file should preserve error context"
    );
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
async fn fish_history_snapshot_scenario_links_row_stream_to_sqlite_evidence(
    ctx: TestContext,
) -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_fish_history(&temp_dir)?;
    let source = RecordSources::sqlite(
        history_path.clone(),
        "terminal.fish_sqlite://history",
        read_fish_history,
        |entry: &sinex_terminal_ingestor::fish_history::FishHistoryEntry| entry.row_id,
    )
    .with_snapshot_policy(SqliteSnapshotPolicy::disabled().with_first_observation(true));
    let ctx = ctx.with_nats().shared().await?;
    let scope = PipelineScope::new(&ctx).await?;
    let acquisition = Arc::new(AcquisitionManager::new_with_namespace(
        ctx.nats_client(),
        sinex_node_sdk::RotationPolicy::default(),
        "fish-sqlite-evidence-scenario".to_string(),
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
                        "command": entry.command,
                        "when": entry.when,
                    }))
                    .await?;
                Ok::<_, sinex_primitives::SinexError>(RecordProcessingOutcome::Processed)
            },
            |_| RecordWarningDisposition::Retry,
        )
        .await?;
    harness
        .finalize_with_snapshot_evidence(
            "fish-sqlite-evidence-scenario",
            &mut report,
            Some(SqliteSnapshotLinker::new(ctx.pool())),
        )
        .await?;

    assert_eq!(checkpoint, SqliteRowCheckpoint::new(3));
    assert_eq!(report.processed_records, 3);
    assert_eq!(report.material_anchors.len(), 3);
    let snapshot = report
        .sqlite_snapshot
        .ok_or_else(|| color_eyre::eyre::eyre!("missing Fish snapshot evidence report"))?;
    let snapshot_material_id = snapshot
        .snapshot_material_id
        .ok_or_else(|| color_eyre::eyre::eyre!("missing Fish snapshot material id"))?;
    assert_eq!(snapshot.failure, None);
    assert_eq!(snapshot.linked_material_count, 1);
    assert!(snapshot.link_errors.is_empty());

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
        "terminal.fish_sqlite://history"
    );
    scope.shutdown().await?;
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

    let (new_entries, new_last_row_id) = read_fish_history(&history_path, last_row_id, None)
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

    let (entries, last_row_id) = read_fish_history(&history_path, 0, Some(end_time))
        .wrap_err("read bounded fish history")?;

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].command, "echo hello");
    assert_eq!(entries[1].command, "ls -la");
    assert_eq!(last_row_id, 2);
    Ok(())
}

#[sinex_test]
async fn test_read_fish_history_rejects_invalid_when_type() -> TestResult<()> {
    let temp_dir = tempfile::tempdir().wrap_err("create tempdir")?;
    let history_path = create_test_fish_history(&temp_dir)?;

    let conn =
        Connection::open(history_path.as_std_path()).wrap_err("re-open fish history database")?;
    conn.execute(
        "INSERT INTO history (command, \"when\") VALUES (?, ?)",
        rusqlite::params!["echo broken", "not-a-timestamp"],
    )
    .wrap_err("insert invalid fish history row")?;

    let error = read_fish_history(&history_path, 0, None)
        .expect_err("invalid when type should fail fish history read");
    assert!(
        error.to_string().contains("failed to map SQLite row 4"),
        "unexpected error: {error}"
    );
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
