// Inline because this exercises private orphan-state cleanup paths.
use super::test_support::{build_test_assembler, build_test_content_store};
use super::{
    MaterialAssembler, disk_usage_allows_assembly, maintenance::MaterialTaskOutcome, signal_ready,
};
use crate::event_engine::MaterialReadySet;
use crate::event_engine::durable_failure::DURABLE_FAILURE_ID_HEADER;
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

/// sinex-wb1: `route_material_error` must propagate a DLQ-publish failure
/// instead of silently logging and returning `()`. Triggers a REAL failure
/// (no mocking of the NATS client): a `context` payload large enough that
/// the encoded `MaterialDlqPayload` exceeds `NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES`
/// (900KB) is rejected by `ensure_nats_payload_fits` before any network call,
/// deterministically and without depending on server/network state. Before
/// this fix, `route_material_error` returned `()` unconditionally and this
/// failure was only ever logged — callers had no way to know DLQ publication
/// didn't happen and would proceed to settle the material Failed with zero
/// durable trace. Reverting `route_material_error`'s signature to `()` (or
/// swallowing this error internally again) makes this test fail to compile
/// or fail its `expect_err`.
#[sinex_test]
async fn route_material_error_propagates_oversized_dlq_payload_failure(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = uuid::Uuid::now_v7();

    // > 900KB NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES once serialized alongside
    // the rest of MaterialDlqPayload's fields.
    let oversized_context = serde_json::json!({
        "padding": "x".repeat(1024 * 1024),
    });

    let error = assembler
        .route_material_error(material_id, "test_oversized_dlq_payload", oversized_context)
        .await
        .expect_err(
            "an oversized DLQ payload must be rejected and propagated, not silently \
             swallowed (sinex-wb1)",
        );
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("oversiz") || message.contains("payload"),
        "error should describe the payload-size rejection from ensure_nats_payload_fits, \
         got: {error}"
    );
    Ok(())
}

/// The material route enters the production JetStream path and the live test
/// database. A successful return therefore proves both the Postgres witness
/// was written and the JetStream publish was server-confirmed; replacing the
/// confirmed publish with core NATS, or removing the evidence insert, makes
/// this fail (the stream is bootstrapped only by this test).
#[sinex_test]
async fn material_dlq_requires_and_records_durable_evidence(ctx: TestContext) -> TestResult<()> {
    // The production material assembler uses the process-wide JetStream
    // topology. Keep this route proof on the shared sandbox, as the adjacent
    // material-DLQ tests do, so the test exercises the same responder and
    // namespace setup rather than a second ephemeral server lifecycle.
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    super::pipeline::bootstrap_streams(&assembler).await?;
    let dlq_stream_name = ctx.env().nats_stream_name_with_namespace(
        Some(ctx.pipeline_namespace().prefix()),
        "SINEX_RAW_EVENTS_DLQ",
    );
    async_nats::jetstream::new(ctx.nats_client())
        .create_or_update_stream(async_nats::jetstream::stream::Config {
            name: dlq_stream_name.clone(),
            subjects: vec![assembler.dlq_subject.clone()],
            retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
            storage: async_nats::jetstream::stream::StorageType::File,
            max_age: tokio::time::Duration::from_secs(300),
            allow_direct: true,
            ..Default::default()
        })
        .await?;
    let mut dlq_stream = async_nats::jetstream::new(ctx.nats_client())
        .get_stream(&dlq_stream_name)
        .await?;
    let dlq_info = dlq_stream.info().await?;
    assert_eq!(
        dlq_info.config.subjects,
        vec![assembler.dlq_subject.clone()]
    );
    let material_id = uuid::Uuid::now_v7();
    let durable_failure_id = assembler
        .route_material_error(
            material_id,
            "test_material_failure",
            serde_json::json!({"fixture": true}),
        )
        .await?;

    let evidence = sqlx::query!(
        "SELECT failed_event_id, error_category FROM sinex_schemas.dlq_events WHERE dlq_id = $1",
        durable_failure_id,
    )
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(evidence.failed_event_id, material_id);
    assert_eq!(evidence.error_category, "permanent");

    let mut stream = async_nats::jetstream::new(ctx.nats_client())
        .get_stream(&dlq_stream_name)
        .await?;
    let state = stream.info().await?.state.clone();
    assert_eq!(state.messages, 1);
    let entry = stream.direct_get(state.first_sequence).await?;
    let expected_failure_id = durable_failure_id.to_string();
    assert_eq!(
        entry
            .headers
            .get(DURABLE_FAILURE_ID_HEADER)
            .map(|value| value.as_str()),
        Some(expected_failure_id.as_str()),
        "the confirmed DLQ message must carry the Postgres witness identity"
    );
    Ok(())
}

#[sinex_test]
async fn material_dlq_publish_failure_preserves_retryable_material_state(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (mut assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = uuid::Uuid::now_v7();

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://material-dlq-publish-failure"),
            json!({}),
            sinex_primitives::Timestamp::now(),
        )
        .await?;

    // No JetStream stream owns this unique subject. The production publish
    // path therefore fails at confirmation after the Postgres witness is
    // written, exercising the same error boundary as an unavailable DLQ.
    assembler.dlq_subject = format!(
        "{}.missing.{}",
        ctx.pipeline_namespace().subject("events.dlq.event_engine"),
        material_id
    );
    let error = assembler
        .route_material_error_then_finalize_failed(
            material_id,
            "material_dlq_publish_failure",
            json!({"fixture": true}),
        )
        .await
        .expect_err("a missing DLQ stream must not settle the material terminally");
    assert!(
        error.to_string().contains("DLQ") || error.to_string().contains("publish"),
        "unexpected DLQ publish error: {error}"
    );

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(sinex_primitives::Id::from_uuid(material_id))
        .await?
        .expect("material should remain registered");
    assert_eq!(
        material.status,
        MaterialStatus::Sensing,
        "failed DLQ publication must preserve a retryable nonterminal material"
    );
    let evidence = sqlx::query_scalar!(
        r#"SELECT count(*)::bigint AS "count!: i64"
           FROM sinex_schemas.dlq_events
           WHERE failed_event_id = $1"#,
        material_id,
    )
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(evidence, 1, "the failure still has a durable witness");
    Ok(())
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
async fn disk_backpressure_allows_high_percent_with_large_free_floor() -> TestResult<()> {
    let one_gib_blocks = 1024 * 1024;
    let total_blocks = 1000 * one_gib_blocks;
    let available_blocks = 84 * one_gib_blocks;

    assert!(
        disk_usage_allows_assembly(
            total_blocks,
            available_blocks,
            1024,
            90,
            4 * 1024 * 1024 * 1024
        ),
        "91% used on a large filesystem with 84 GiB free must not reject tiny assemblies"
    );
    Ok(())
}

#[sinex_test]
async fn disk_backpressure_rejects_high_percent_with_low_free_floor() -> TestResult<()> {
    let one_gib_blocks = 1024 * 1024;
    let total_blocks = 1000 * one_gib_blocks;
    let available_blocks = one_gib_blocks;

    assert!(
        !disk_usage_allows_assembly(
            total_blocks,
            available_blocks,
            1024,
            90,
            4 * 1024 * 1024 * 1024,
        ),
        "high-percent usage should still reject once absolute free space is below the floor"
    );
    Ok(())
}

/// A zero-parsed-event orphaned sensing row is recovered as a partial
/// success, not hard-failed: `reconcile_orphaned_sensing_materials` routes
/// materials with `parsed_event_count == 0` through
/// `recover_orphaned_zero_event_source_material`, which marks the row
/// `RecoveredPartial` (not `Failed`) and skips the DLQ. This intentionally
/// changed from an unconditional `Failed` outcome — see the
/// `orphan_reconcile_recovers_globally_stale_zero_event_source_material_without_dlq`
/// sibling test in `maintenance_test.rs`, which covers the same code path
/// with the current expected shape.
#[sinex_test]
async fn stale_cleanup_marks_orphaned_sensing_registry_rows_recovered_partial(
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
    assert_eq!(record.status, MaterialStatus::RecoveredPartial);
    assert_eq!(
        record.metadata["recovery_info"]["recovery_reason"],
        serde_json::json!("orphaned_zero_event_source_material_recovered_partial")
    );
    assert_eq!(
        record.metadata["orphaned_sensing_material"]["source_identifier"],
        serde_json::json!("test://orphaned-sensing")
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

    let error = MaterialAssembler::wait_for_material_tasks(&mut tasks, Duration::from_millis(10))
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
async fn assembler_rejects_unrepresentable_max_material_size(ctx: TestContext) -> TestResult<()> {
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
