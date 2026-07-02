use super::*;
use sinex_primitives::Uuid;
use std::sync::Arc;
use tempfile::NamedTempFile;
use xtask::sandbox::prelude::*;

fn make_acquisition_manager(
    work_dir: &Path,
    nats_client: async_nats::Client,
    label: &str,
) -> Arc<AcquisitionManager> {
    let namespace = format!("{label}-{}", sinex_primitives::primitives::Uuid::new_v4());
    Arc::new(
        AcquisitionManager::new_with_namespace(
            nats_client,
            crate::runtime::acquisition_manager::RotationPolicy::default(),
            label.to_string(),
            Some(namespace),
        )
        .with_work_dir(work_dir),
    )
}

fn make_sqlite_db_with_payload(payload: &str) -> NamedTempFile {
    let f = NamedTempFile::with_suffix(".db").unwrap();
    let conn = rusqlite::Connection::open(f.path()).unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE k (v TEXT);
         INSERT INTO k (v) VALUES ('{payload}');",
    ))
    .unwrap();
    f
}

#[sinex_test]
async fn snapshot_disabled_by_default() -> TestResult<()> {
    let cfg = SqliteSnapshotConfig::default();
    assert!(!cfg.enabled());
    let spec = SnapshotLaneSpec::from_sqlite_config("/tmp/x.db", "test.unit", &cfg);
    assert!(spec.is_none());
    Ok(())
}

#[sinex_test]
async fn snapshot_spec_built_when_enabled() -> TestResult<()> {
    let cfg = SqliteSnapshotConfig {
        interval_seconds: 60,
        dedup_by_content_hash: true,
    };
    assert!(cfg.enabled());
    let spec = SnapshotLaneSpec::from_sqlite_config("/tmp/x.db", "test.unit", &cfg).unwrap();
    assert_eq!(spec.source_identifier, "test.unit.snapshot");
    assert_eq!(spec.interval, Duration::from_mins(1));
    assert!(spec.dedup_by_content_hash);
    Ok(())
}

#[sinex_test]
async fn capture_once_produces_one_material(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-one");

    let db = make_sqlite_db_with_payload("hello");
    let spec = SnapshotLaneSpec {
        path: db.path().to_path_buf(),
        source_identifier: "test.atuin.snapshot".to_string(),
        interval: Duration::from_hours(1),
        dedup_by_content_hash: true,
    };
    let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

    assert_eq!(lane.snapshots_captured(), 0);
    lane.capture_once().await?;
    assert_eq!(lane.snapshots_captured(), 1);
    Ok(())
}

#[sinex_test]
async fn capture_once_publishes_latest_snapshot_evidence(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-latest");

    let db = make_sqlite_db_with_payload("latest");
    let spec = SnapshotLaneSpec {
        path: db.path().to_path_buf(),
        source_identifier: "test.atuin.snapshot".to_string(),
        interval: Duration::from_hours(1),
        dedup_by_content_hash: true,
    };
    let latest = LatestSqliteSnapshotEvidence::default();
    let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager))
        .with_latest_evidence(latest.clone());

    assert!(latest.latest().is_none());
    lane.capture_once().await?;

    let evidence = latest
        .latest()
        .ok_or_else(|| SinexError::processing("missing latest snapshot evidence"))?;
    assert_ne!(evidence.material_id.to_uuid(), Uuid::nil());
    assert_eq!(evidence.source_identifier, "test.atuin.snapshot");
    assert_eq!(evidence.source_path, db.path().display().to_string());
    assert_eq!(
        evidence.size_bytes,
        std::fs::metadata(db.path())?.len() as usize
    );
    assert!(!evidence.content_hash_blake3.is_empty());
    Ok(())
}

#[sinex_test]
async fn capture_dedups_unchanged_db(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-dedup");

    let db = make_sqlite_db_with_payload("hello");
    let spec = SnapshotLaneSpec {
        path: db.path().to_path_buf(),
        source_identifier: "test.atuin.snapshot".to_string(),
        interval: Duration::from_hours(1),
        dedup_by_content_hash: true,
    };
    let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

    lane.capture_once().await?;
    lane.capture_once().await?;
    lane.capture_once().await?;
    // First capture lands; subsequent identical-hash captures dedup.
    assert_eq!(lane.snapshots_captured(), 1);
    Ok(())
}

#[sinex_test]
async fn capture_emits_new_material_when_content_changes(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-changes");

    // Reuse the same path across two distinct DBs by writing through the
    // same NamedTempFile (path stable). We rebuild the DB to mutate
    // content while keeping the path the same.
    let path = tempfile::NamedTempFile::with_suffix(".db").unwrap();
    {
        let conn = rusqlite::Connection::open(path.path()).unwrap();
        conn.execute_batch("CREATE TABLE k (v TEXT); INSERT INTO k VALUES ('a');")
            .unwrap();
    }

    let spec = SnapshotLaneSpec {
        path: path.path().to_path_buf(),
        source_identifier: "test.atuin.snapshot".to_string(),
        interval: Duration::from_hours(1),
        dedup_by_content_hash: true,
    };
    let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

    lane.capture_once().await?;
    assert_eq!(lane.snapshots_captured(), 1);

    // Mutate the DB so the file hash changes.
    {
        let conn = rusqlite::Connection::open(path.path()).unwrap();
        conn.execute_batch("INSERT INTO k VALUES ('b');").unwrap();
    }

    lane.capture_once().await?;
    assert_eq!(lane.snapshots_captured(), 2);
    Ok(())
}

#[sinex_test]
async fn capture_with_dedup_disabled_always_emits(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-no-dedup");

    let db = make_sqlite_db_with_payload("xyz");
    let spec = SnapshotLaneSpec {
        path: db.path().to_path_buf(),
        source_identifier: "test.atuin.snapshot".to_string(),
        interval: Duration::from_hours(1),
        dedup_by_content_hash: false,
    };
    let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

    lane.capture_once().await?;
    lane.capture_once().await?;
    lane.capture_once().await?;
    assert_eq!(lane.snapshots_captured(), 3);
    Ok(())
}

#[sinex_test]
async fn missing_path_returns_error(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-missing");

    let spec = SnapshotLaneSpec {
        path: PathBuf::from("/definitely/does/not/exist.db"),
        source_identifier: "test.atuin.snapshot".to_string(),
        interval: Duration::from_hours(1),
        dedup_by_content_hash: true,
    };
    let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

    assert!(lane.capture_once().await.is_err());
    assert_eq!(lane.snapshots_captured(), 0);
    Ok(())
}

#[sinex_test]
async fn run_loop_exits_on_shutdown(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-run");

    let db = make_sqlite_db_with_payload("loopy");
    let spec = SnapshotLaneSpec {
        path: db.path().to_path_buf(),
        source_identifier: "test.atuin.snapshot".to_string(),
        // Long interval — only the initial-capture should run before shutdown.
        interval: Duration::from_hours(1),
        dedup_by_content_hash: true,
    };
    let lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(async move { lane.run(rx).await });

    // Give the lane a beat to do its initial capture, then shut it down.
    tokio::time::sleep(Duration::from_millis(200)).await;
    tx.send(true).unwrap();
    task.await.expect("task join")?;
    Ok(())
}
