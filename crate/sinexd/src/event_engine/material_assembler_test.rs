// Inline because this exercises private orphan-state cleanup paths.
use super::test_support::{build_test_assembler, build_test_content_store};
use super::{MaterialAssembler, maintenance::MaterialTaskOutcome, signal_ready};
use crate::event_engine::MaterialReadySet;
use sinex_db::DbPoolExt;
use sinex_primitives::{Id, domain::MaterialStatus};
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::time::Duration;
use tokio::task::JoinSet;
use xtask::sandbox::prelude::*;

async fn test_assembler(
    ctx: &TestContext,
) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
    build_test_assembler(ctx, "orphan-cleanup-test").await
}

#[sinex_test]
async fn check_orphaned_folder_rejects_non_uuid_name(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let path = state_dir.path().join("not-a-uuid");
    tokio::fs::create_dir_all(&path).await?;

    let error = assembler
        .check_orphaned_folder(path)
        .await
        .expect_err("invalid state directory names must fail honestly");
    assert!(error.to_string().contains("invalid material id"));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn check_orphaned_folder_rejects_non_utf8_name(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let invalid_name = std::ffi::OsString::from_vec(vec![0xff, 0xfe, b'x']);
    let path = state_dir.path().join(invalid_name);
    tokio::fs::create_dir_all(&path).await?;

    let error = assembler
        .check_orphaned_folder(path)
        .await
        .expect_err("non-utf8 state directory names must fail honestly");
    assert!(error.to_string().contains("not valid UTF-8"));
    Ok(())
}

#[sinex_test]
async fn ready_signal_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    drop(rx);

    assert!(!signal_ready(Some(tx), "material-assembler"));
    Ok(())
}

#[sinex_test]
async fn stale_cleanup_marks_orphaned_sensing_registry_rows_failed(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::new_v4();
    let started_at = Timestamp::now() - time::Duration::hours(2);

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test.orphaned-sensing",
            Some("test://orphaned-sensing"),
            serde_json::json!({"test": "orphaned-sensing"}),
            started_at,
        )
        .await?;

    assembler.reconcile_orphaned_sensing_materials().await?;

    let record = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("orphaned material row should still exist");
    assert_eq!(record.status, MaterialStatus::Failed);
    assert_eq!(
        record.metadata["failure_reason"],
        serde_json::json!("orphaned_sensing_material")
    );
    Ok(())
}

#[sinex_test]
async fn wait_for_material_tasks_accepts_clean_shutdown() -> TestResult<()> {
    let mut tasks = JoinSet::<MaterialTaskOutcome>::new();
    tasks.spawn(async { ("material frame consumer", Ok(Ok(()))) });

    let error =
        MaterialAssembler::wait_for_material_tasks(&mut tasks, Duration::from_secs(1)).await;

    assert!(error.is_none(), "clean shutdown should not report an error");
    assert!(tasks.is_empty(), "all tracked tasks should be drained");
    Ok(())
}

#[sinex_test]
async fn wait_for_material_tasks_preserves_first_shutdown_error() -> TestResult<()> {
    let mut tasks = JoinSet::<MaterialTaskOutcome>::new();
    tasks.spawn(async {
        (
            "material frame consumer",
            Ok(Err(sinex_primitives::error::SinexError::service(
                "frame consumer failed",
            ))),
        )
    });
    tasks.spawn(async { ("material stale cleanup task", Ok(Ok(()))) });

    let error = MaterialAssembler::wait_for_material_tasks(&mut tasks, Duration::from_secs(1))
        .await
        .expect("shutdown error should be preserved");

    assert!(error.to_string().contains("material frame consumer"));
    assert!(
        error.to_string().contains("shutdown"),
        "cleanup path should annotate the shutdown phase"
    );
    Ok(())
}

#[sinex_test]
async fn wait_for_material_tasks_times_out_hung_tasks() -> TestResult<()> {
    let mut tasks = JoinSet::<MaterialTaskOutcome>::new();
    let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completed_flag = completed.clone();
    tasks.spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        completed_flag.store(true, std::sync::atomic::Ordering::Release);
        ("material stale cleanup task", Ok(Ok(())))
    });

    let error =
        MaterialAssembler::wait_for_material_tasks(&mut tasks, Duration::from_millis(10))
            .await
            .expect("hung task should time out");

    assert!(error.to_string().contains("timed out waiting"));
    assert!(
        !completed.load(std::sync::atomic::Ordering::Acquire),
        "timed out shutdown should abort lingering material tasks"
    );
    assert!(tasks.is_empty(), "timed out tasks should still be drained");
    Ok(())
}

#[sinex_test]
async fn assembler_rejects_unrepresentable_max_material_size(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (content_store, _content_store_dir) =
        build_test_content_store("oversized-config-test").await?;
    let state_dir = tempfile::tempdir()?;

    let error = MaterialAssembler::new(
        ctx.nats_client(),
        ctx.pool.clone(),
        content_store,
        state_dir.path().to_path_buf(),
        Some(ctx.pipeline_namespace().prefix().to_string()),
        1_000,
        Some(MaterialReadySet::default()),
        100,
        u64::MAX,
        300,
        3_600,
        90,
    )
    .err()
    .expect("oversized material limits must fail honestly");

    assert!(
        error
            .to_string()
            .contains("max_material_size_bytes exceeds i64 range")
    );
    Ok(())
}

#[sinex_test]
async fn find_stale_materials_does_not_hold_dashmap_refs_across_await(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::new_v4();

    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.last_slice_received = Timestamp::now() - time::Duration::minutes(10);
    let state_handle = assembler.insert_state_handle(material_id, state);

    let locked_state = state_handle.lock().await;
    let scan_assembler = assembler.clone_for_task();
    let scan_task = tokio::spawn(async move { scan_assembler.find_stale_materials().await });
    tokio::task::yield_now().await;

    let replacement_state = assembler.create_placeholder_state(material_id).await?;
    let assembler_clone = assembler.clone_for_task();
    tokio::time::timeout(
        Duration::from_millis(200),
        tokio::task::spawn_blocking(move || {
            assembler_clone.insert_state_handle(material_id, replacement_state);
        }),
    )
    .await
    .expect("stale scan should not block insert_state_handle on dashmap shard locks")
    .expect("spawn_blocking join should not panic");

    drop(locked_state);
    let stale_materials = scan_task.await?;
    assert_eq!(stale_materials, vec![(material_id, 600)]);
    Ok(())
}
