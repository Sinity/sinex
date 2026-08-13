use super::*;
use crate::event_engine::material_assembler::FinalizationState;
use crate::event_engine::material_assembler::finalization_transaction::{
    FinalizationErrorKind, FinalizationRequest, FinalizationTransaction,
};
use crate::event_engine::material_assembler::{io, state};
use crate::runtime::content_store::ContentStoreKey;
use crate::runtime::{FaultInjector, FaultPoint};
use serde_json::json;
use sinex_db::{
    models::blob::Blob,
    repositories::{DbPoolExt, TemporalLedgerEntry},
};
use sinex_primitives::{MaterialManifestV1, MaterialStatus, MetadataAvailability};
use tokio::time::timeout;
use tokio_stream::StreamExt;
use xtask::sandbox::prelude::*;

async fn test_assembler(
    ctx: &TestContext,
) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
    super::super::test_support::build_test_assembler(ctx, "finalize-test").await
}

#[sinex_test]
async fn finalize_failed_material_skips_material_already_finalizing(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://finalizing"),
            json!({}),
            Timestamp::now(),
        )
        .await?;

    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.phase = AssemblyPhase::Finalizing;
    assembler.insert_state_handle(material_id, state);

    assembler
        .finalize_failed_material(material_id, "slice_arrival_timeout")
        .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::Sensing);
    assert!(assembler.assembler_state.contains_key(&material_id));
    Ok(())
}

#[sinex_test]
async fn finalize_worker_termination_reverts_phase_and_emits_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.phase = AssemblyPhase::Finalizing;
    let state_handle = assembler.insert_state_handle(material_id, state);

    let panicked_worker = tokio::spawn(async {
        panic!("test-only finalization worker panic");
        #[allow(unreachable_code)]
        Ok::<(), SinexError>(())
    });
    assembler
        .observe_finalize_worker(material_id, state_handle.clone(), panicked_worker)
        .await;

    assert_eq!(
        state_handle.lock().await.phase,
        AssemblyPhase::Accumulating,
        "anti-vacuity: removing worker-termination recovery leaves a panicked finalizer stuck in Finalizing"
    );
    let panic_dlq = timeout(std::time::Duration::from_secs(1), dlq_sub.next())
        .await?
        .expect("panicked finalizer must publish visible recovery evidence");
    assert!(
        std::str::from_utf8(&panic_dlq.payload)?.contains("material_finalize_worker_terminated"),
        "anti-vacuity: bypassing the worker JoinError branch removes the panic recovery record"
    );

    state_handle.lock().await.phase = AssemblyPhase::Finalizing;
    let aborted_worker = tokio::spawn(std::future::pending::<EventEngineResult<()>>());
    aborted_worker.abort();
    assembler
        .observe_finalize_worker(material_id, state_handle.clone(), aborted_worker)
        .await;

    assert_eq!(
        state_handle.lock().await.phase,
        AssemblyPhase::Accumulating,
        "anti-vacuity: removing worker-termination recovery leaves an aborted finalizer stuck in Finalizing"
    );
    let abort_dlq = timeout(std::time::Duration::from_secs(1), dlq_sub.next())
        .await?
        .expect("aborted finalizer must publish visible recovery evidence");
    assert!(
        std::str::from_utf8(&abort_dlq.payload)?.contains("material_finalize_worker_terminated"),
        "anti-vacuity: bypassing the worker JoinError branch removes the abort recovery record"
    );
    Ok(())
}

#[sinex_test]
async fn finalize_failed_material_skips_terminal_material_without_state(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://completed"),
            json!({}),
            Timestamp::now(),
        )
        .await?;
    ctx.pool
        .source_materials()
        .finalize_in_flight(material_id_typed, None, None, None, Some(42))
        .await?;

    assembler
        .finalize_failed_material(material_id, "slice_arrival_timeout")
        .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::Completed);
    Ok(())
}

#[sinex_test]
async fn finalize_failed_material_recovers_timeout_when_events_were_admitted(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "browser.history",
            Some("browser.history#material=test-timeout"),
            json!({}),
            Timestamp::now(),
        )
        .await?;
    sqlx::query!(
        "UPDATE raw.source_material_registry SET parsed_event_count = 42 WHERE id = $1",
        material_id,
    )
    .execute(ctx.pool())
    .await?;

    assembler
        .finalize_failed_material(material_id, "slice_arrival_timeout")
        .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::RecoveredPartial);
    assert_eq!(
        material.metadata["recovery_info"]["recovery_reason"],
        json!("slice_arrival_timeout_with_admitted_events")
    );
    assert_eq!(
        material.metadata["timeout_partial_recovery"]["parsed_event_count"],
        json!(42)
    );
    assert_eq!(
        material.metadata["failure_reason"],
        json!("slice_arrival_timeout")
    );
    Ok(())
}

#[sinex_test]
async fn finalize_failed_material_preserves_retry_state_when_failure_mark_is_not_durable(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();

    let mut state = assembler.create_placeholder_state(material_id).await?;
    let temp_path = state.temp_path.clone();
    tokio::fs::write(&temp_path, b"partial").await?;
    state.phase = AssemblyPhase::Accumulating;
    let state_handle = assembler.insert_state_handle(material_id, state);

    ctx.pool.close().await;

    let error = assembler
        .finalize_failed_material_claimed_checked(
            material_id,
            "material_hash_mismatch",
            AssemblyPhase::Accumulating,
        )
        .await
        .expect_err("cleanup should fail honestly when the durable failure mark cannot land");

    assert!(
        error
            .to_string()
            .contains("Failed to mark material as failed in database"),
        "unexpected error: {error}"
    );
    assert!(
        assembler.assembler_state.contains_key(&material_id),
        "retry state must be preserved until the failure mark lands durably"
    );
    assert!(
        temp_path.exists(),
        "staged material should remain on disk for retry"
    );
    assert_eq!(state_handle.lock().await.phase, AssemblyPhase::Accumulating);
    Ok(())
}

#[sinex_test]
async fn route_material_error_propagates_terminal_settlement_failure(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let dlq_stream_name = ctx.env().nats_stream_name_with_namespace(
        Some(ctx.pipeline_namespace().prefix()),
        "SINEX_RAW_EVENTS_DLQ",
    );
    async_nats::jetstream::new(ctx.nats_client())
        .create_or_update_stream(async_nats::jetstream::stream::Config {
            name: dlq_stream_name,
            subjects: vec![dlq_subject.clone()],
            retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
            storage: async_nats::jetstream::stream::StorageType::Memory,
            max_age: tokio::time::Duration::from_secs(300),
            ..Default::default()
        })
        .await?;
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://terminal-settlement-failure"),
            json!({}),
            Timestamp::now(),
        )
        .await?;

    // Hold the per-material lock so the route cannot enter terminal settlement
    // until this test has observed the successful DLQ publication.
    let state = assembler.create_placeholder_state(material_id).await?;
    let state_handle = assembler.insert_state_handle(material_id, state);
    let state_guard = state_handle.lock().await;
    let mut route = Box::pin(assembler.route_material_error_then_finalize_failed(
        material_id,
        "material_persist_failed",
        json!({"fault_injection": "closed_database_pool"}),
    ));

    let dlq_message = tokio::select! {
        result = &mut route => panic!(
            "route must not settle before its DLQ publication is observed; result={result:?}"
        ),
        message = timeout(Duration::from_secs(1), dlq_sub.next()) => {
            message?
                .ok_or_else(|| SinexError::processing("terminal material failure did not publish a DLQ record"))?
        },
    };
    assert!(
        std::str::from_utf8(&dlq_message.payload)?.contains("material_persist_failed"),
        "the settlement failure must be observed after a successful DLQ publication"
    );

    ctx.pool.close().await;
    drop(state_guard);

    let error = route
        .await
        .expect_err("DLQ success must not hide terminal settlement failure");

    assert!(
        error
            .to_string()
            .contains("Failed to mark material as failed in database"),
        "unexpected settlement error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn try_finalize_pending_end_routes_invalid_end_timestamp_to_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://invalid-ended-at"),
            json!({}),
            Timestamp::now(),
        )
        .await?;

    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.material_kind = "test".to_string();
    state.source_identifier = "test://invalid-ended-at".to_string();
    state.phase = AssemblyPhase::Accumulating;
    state.expected_offset = 4;
    state.slice_count = 1;
    state.pending_end = Some(MaterialEndMessage {
        material_id: material_id.to_string(),
        ended_at: "not-a-timestamp".to_string(),
        content_hash: blake3::hash(b"data").to_hex().to_string(),
        total_slices: 1,
        total_size_bytes: 4,
        metadata: json!({}),
    });
    let state_handle = assembler.insert_state_handle(material_id, state);

    assembler
        .try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Error)
        .await?;

    let msg = timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await?
        .ok_or_else(|| SinexError::invalid_state("missing DLQ message"))?;
    let payload: JsonValue = serde_json::from_slice(&msg.payload)?;
    assert_eq!(payload["error"], "material_end_timestamp_invalid");
    assert_eq!(payload["material_id"], material_id.to_string());
    assert_eq!(payload["context"]["ended_at"], "not-a-timestamp");

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::Failed);
    assert!(
        !assembler.assembler_state.contains_key(&material_id),
        "invalid end timestamp should clean up assembler state instead of retrying forever"
    );

    Ok(())
}

#[sinex_test]
async fn try_finalize_pending_end_routes_missing_material_file_to_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://missing-material-file"),
            json!({}),
            Timestamp::now(),
        )
        .await?;

    let mut state = assembler.create_placeholder_state(material_id).await?;
    tokio::fs::write(&state.temp_path, b"data").await?;
    let missing_path = state.temp_path.clone();
    tokio::fs::remove_file(&missing_path).await?;
    state.material_kind = "test".to_string();
    state.source_identifier = "test://missing-material-file".to_string();
    state.phase = AssemblyPhase::Accumulating;
    state.expected_offset = 4;
    state.slice_count = 1;
    state.pending_end = Some(MaterialEndMessage {
        material_id: material_id.to_string(),
        ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
        content_hash: blake3::hash(b"data").to_hex().to_string(),
        total_slices: 1,
        total_size_bytes: 4,
        metadata: json!({}),
    });
    let state_handle = assembler.insert_state_handle(material_id, state);

    assembler
        .try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Error)
        .await?;

    let msg = timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await?
        .ok_or_else(|| SinexError::invalid_state("missing DLQ message"))?;
    let payload: JsonValue = serde_json::from_slice(&msg.payload)?;
    assert_eq!(payload["error"], "material_stat_failed");
    assert_eq!(payload["material_id"], material_id.to_string());
    assert_eq!(
        payload["context"]["path"],
        missing_path.display().to_string()
    );

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::Failed);
    assert!(
        !assembler.assembler_state.contains_key(&material_id),
        "missing staged material file should clean up assembler state"
    );

    Ok(())
}

#[sinex_test]
async fn handle_end_before_slice_waits_for_missing_slice_instead_of_failing(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let started_at = Timestamp::now();
    let payload = b"data".to_vec();

    assembler
        .handle_end(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
            content_hash: blake3::hash(&payload).to_hex().to_string(),
            total_slices: 1,
            total_size_bytes: payload.len() as i64,
            metadata: json!({}),
        })
        .await?;

    assert!(
        assembler.assembler_state.contains_key(&material_id),
        "out-of-order end should keep placeholder state for later slices"
    );

    state::handle_begin(
        &assembler,
        material_id,
        state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://out-of-order-end".to_string(),
            metadata: json!({}),
            started_at: sinex_primitives::temporal::format_rfc3339(started_at),
        },
    )
    .await?;

    io::handle_slice(&assembler, material_id, 0, payload).await?;

    // Finalization is decoupled from the frame path onto a bounded worker set
    // (#2187), so the slice that completes an out-of-order material schedules
    // the finalize rather than running it inline. Await the worker's commit
    // (in-memory state removal is its last step) before asserting.
    let state_map = assembler.assembler_state.clone();
    WaitHelpers::wait_for_condition(
        || {
            let state_map = state_map.clone();
            async move { Ok::<bool, SinexError>(!state_map.contains_key(&material_id)) }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::Completed);
    assert!(
        !assembler.assembler_state.contains_key(&material_id),
        "completed out-of-order assembly should clean up in-memory state"
    );

    Ok(())
}

#[sinex_test]
async fn slice_completed_pending_end_claims_finalization_before_extra_slice(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let started_at = Timestamp::now();
    let payload = b"data".to_vec();

    let finalize_permit = assembler.finalize_semaphore.clone().acquire_owned().await?;

    assembler
        .handle_end(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
            content_hash: blake3::hash(&payload).to_hex().to_string(),
            total_slices: 1,
            total_size_bytes: payload.len() as i64,
            metadata: json!({}),
        })
        .await?;
    state::handle_begin(
        &assembler,
        material_id,
        state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://slice-completed-claim".to_string(),
            metadata: json!({}),
            started_at: sinex_primitives::temporal::format_rfc3339(started_at),
        },
    )
    .await?;

    io::handle_slice(&assembler, material_id, 0, payload.clone()).await?;

    let state_handle = assembler
        .get_state_handle(&material_id)
        .ok_or_else(|| SinexError::invalid_state("missing claimed assembler state"))?;

    // Finalization is decoupled from the frame path onto a bounded worker set
    // (#2187): phase only flips to Finalizing once the spawned worker
    // actually acquires a semaphore permit and runs try_finalize_pending_end.
    // On this test's single-threaded runtime that worker cannot be polled
    // until this task yields at a real await point, so checking phase
    // immediately here is deterministically wrong, not flaky — wait for the
    // transition first, matching the pattern used elsewhere in this file.
    {
        let state_handle = state_handle.clone();
        WaitHelpers::wait_for_condition(
            || {
                let state_handle = state_handle.clone();
                async move {
                    let state = state_handle.lock().await;
                    Ok::<bool, SinexError>(state.phase == AssemblyPhase::Finalizing)
                }
            },
            Timeouts::STANDARD,
        )
        .await?;
    }
    {
        let state = state_handle.lock().await;
        assert_eq!(state.phase, AssemblyPhase::Finalizing);
        assert_eq!(state.expected_offset, payload.len() as i64);
    }

    io::handle_slice(
        &assembler,
        material_id,
        payload.len() as i64,
        b"late".to_vec(),
    )
    .await?;
    {
        let state = state_handle.lock().await;
        assert_eq!(
            state.expected_offset,
            payload.len() as i64,
            "extra slice must not mutate a material already claimed for finalization"
        );
        assert_eq!(state.slice_count, 1);
    }

    drop(finalize_permit);
    let state_map = assembler.assembler_state.clone();
    WaitHelpers::wait_for_condition(
        || {
            let state_map = state_map.clone();
            async move { Ok::<bool, SinexError>(!state_map.contains_key(&material_id)) }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::Completed);
    assert_eq!(material.total_bytes, Some(payload.len() as i64));

    Ok(())
}

#[sinex_test]
async fn pending_end_ignores_late_slice_beyond_contract_before_completion(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let started_at = Timestamp::now();
    let payload = b"data".to_vec();

    let finalize_permit = assembler.finalize_semaphore.clone().acquire_owned().await?;

    assembler
        .handle_end(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
            content_hash: blake3::hash(&payload).to_hex().to_string(),
            total_slices: 1,
            total_size_bytes: payload.len() as i64,
            metadata: json!({}),
        })
        .await?;
    state::handle_begin(
        &assembler,
        material_id,
        state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://late-slice-before-complete".to_string(),
            metadata: json!({}),
            started_at: sinex_primitives::temporal::format_rfc3339(started_at),
        },
    )
    .await?;

    io::handle_slice(
        &assembler,
        material_id,
        payload.len() as i64,
        b"late".to_vec(),
    )
    .await?;

    let state_handle = assembler
        .get_state_handle(&material_id)
        .ok_or_else(|| SinexError::invalid_state("missing assembler state"))?;
    {
        let state = state_handle.lock().await;
        assert_eq!(
            state.expected_offset, 0,
            "late slice beyond END must not advance assembly"
        );
        assert_eq!(state.slice_count, 0);
        assert!(
            state.buffered_slices.is_empty(),
            "late slice beyond END must not be buffered as future material"
        );
        assert!(state.pending_end.is_some());
    }

    io::handle_slice(&assembler, material_id, 0, payload.clone()).await?;

    // Finalization is decoupled from the frame path onto a bounded worker set
    // (#2187): the slice that completes the material schedules the finalize
    // via dispatch_finalize rather than running it inline, and phase only
    // flips to Finalizing once that worker actually acquires a semaphore
    // permit and starts (see try_finalize_pending_end). On this test's
    // single-threaded runtime the spawned worker cannot be polled until this
    // task yields at a real await point, so checking phase immediately here
    // is deterministically wrong, not flaky — wait for the transition first,
    // matching the pattern used above for the out-of-order-completion case.
    {
        let state_handle = state_handle.clone();
        WaitHelpers::wait_for_condition(
            || {
                let state_handle = state_handle.clone();
                async move {
                    let state = state_handle.lock().await;
                    Ok::<bool, SinexError>(state.phase == AssemblyPhase::Finalizing)
                }
            },
            Timeouts::STANDARD,
        )
        .await?;
    }
    {
        let state = state_handle.lock().await;
        assert_eq!(state.phase, AssemblyPhase::Finalizing);
        assert_eq!(state.expected_offset, payload.len() as i64);
        assert_eq!(state.slice_count, 1);
    }

    drop(finalize_permit);
    let state_map = assembler.assembler_state.clone();
    WaitHelpers::wait_for_condition(
        || {
            let state_map = state_map.clone();
            async move { Ok::<bool, SinexError>(!state_map.contains_key(&material_id)) }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::Completed);
    assert_eq!(material.total_bytes, Some(payload.len() as i64));

    Ok(())
}

#[sinex_test]
async fn finalization_persists_canonical_manifest_and_registry_reference(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let started_at = Timestamp::now();
    let ended_at = Timestamp::now();
    let payload = b"finalizer manifest route".to_vec();

    state::handle_begin(
        &assembler,
        material_id,
        state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test-material".to_string(),
            source_identifier: "test://manifest-route".to_string(),
            metadata: json!({
                "mime_type": "text/plain",
                "charset": null,
                "explicit_unknown": { "availability": "unknown" }
            }),
            started_at: sinex_primitives::temporal::format_rfc3339(started_at),
        },
    )
    .await?;
    io::handle_slice(&assembler, material_id, 0, payload.clone()).await?;

    let state_handle = assembler
        .get_state_handle(&material_id)
        .ok_or_else(|| SinexError::invalid_state("missing assembler state"))?;
    {
        let mut state = state_handle.lock().await;
        state.pending_end = Some(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: sinex_primitives::temporal::format_rfc3339(ended_at),
            content_hash: blake3::hash(&payload).to_hex().to_string(),
            total_slices: 1,
            total_size_bytes: payload.len() as i64,
            metadata: json!({}),
        });
    }

    assembler
        .try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Error)
        .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("finalized material should exist");
    let manifest_reference = material.metadata["material_manifest"]["content_key"]
        .as_str()
        .expect("finalization must attach a manifest CAS key");
    let manifest_key = ContentStoreKey::parse(manifest_reference)?;
    assert!(
        manifest_key.is_local_blake3_cas(),
        "manifest must be stored in the local BLAKE3 CAS"
    );
    let manifest_path = assembler
        .content_store
        .path_if_local(manifest_reference)?
        .expect("manifest CAS key must resolve to a local path");
    let manifest_bytes = tokio::fs::read(manifest_path).await?;
    let manifest: MaterialManifestV1 = serde_json::from_slice(&manifest_bytes)?;
    manifest
        .validate()
        .expect("finalizer must emit a valid manifest");
    assert_eq!(manifest.source_material_id, material_id);
    assert_eq!(manifest.bytes.encoded_size, payload.len() as u64);
    assert_eq!(
        manifest.bytes.encoded.value_hex,
        blake3::hash(&payload).to_hex().to_string()
    );
    assert_eq!(
        manifest.interpretation.charset.availability,
        MetadataAvailability::Unknown
    );
    assert_eq!(
        manifest.extensions["capture_metadata"]["explicit_unknown"]["availability"],
        json!("unknown")
    );

    Ok(())
}

#[sinex_test]
async fn post_commit_response_failure_reconciles_real_cas_finalization(
    ctx: TestContext,
) -> TestResult<()> {
    Box::pin(post_commit_response_failure_reconciles_real_cas_finalization_inner(ctx)).await
}

async fn post_commit_response_failure_reconciles_real_cas_finalization_inner(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let injector = FaultInjector::default();
    injector.fail_once(FaultPoint::MaterialCommitPostCommitResponse);
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("post-commit-response-recovery")
            .fault_injector(injector)
            .build(&ctx)
            .await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let started_at = Timestamp::now();
    let ended_at = Timestamp::now();
    let payload = b"post-commit response recovery bytes".to_vec();
    let content_hash = blake3::hash(&payload).to_hex().to_string();
    let content_key = ContentStoreKey::local_blake3(payload.len() as u64, content_hash.clone())?;

    state::handle_begin(
        &assembler,
        material_id,
        state::MaterialBeginMessage {
            material_id: material_id.to_string(),
            material_kind: "test-material".to_string(),
            source_identifier: "test://post-commit-response-recovery".to_string(),
            metadata: json!({}),
            started_at: sinex_primitives::temporal::format_rfc3339(started_at),
        },
    )
    .await?;
    io::handle_slice(&assembler, material_id, 0, payload.clone()).await?;

    let state_handle = assembler
        .get_state_handle(&material_id)
        .ok_or_else(|| SinexError::invalid_state("missing assembler state"))?;
    {
        let mut state = state_handle.lock().await;
        state.pending_end = Some(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: sinex_primitives::temporal::format_rfc3339(ended_at),
            content_hash: content_hash.clone(),
            total_slices: 1,
            total_size_bytes: payload.len() as i64,
            metadata: json!({}),
        });
    }

    let first_error = assembler
        .try_finalize_pending_end(material_id, state_handle.clone(), PendingEndBehavior::Error)
        .await
        .expect_err("the injected response loss must reach the caller as an error");
    assert_eq!(
        first_error.context_map().get("commit_outcome"),
        Some(&"unknown".to_string()),
        "post-commit response loss must not be mistaken for a pre-commit failure: {first_error}"
    );
    assert_eq!(
        first_error.context_map().get("commit_landed"),
        Some(&"true".to_string())
    );
    assert_eq!(
        first_error.context_map().get("response_failure"),
        Some(&"post_commit".to_string())
    );

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("the PostgreSQL commit must have landed before the response error");
    assert_eq!(material.status, MaterialStatus::Completed);
    assert!(material.optional_blob_id.is_some());
    assert_eq!(
        assembler.content_store.list_write_leases().await?.len(),
        0,
        "the landed commit path must clean its CAS lease even when the caller receives an error"
    );
    let cas_path = assembler
        .content_store
        .path_if_local(&content_key.key)?
        .expect("the published CAS object must remain addressable");
    assert_eq!(tokio::fs::read(cas_path).await?, payload);
    {
        let state = state_handle.lock().await;
        assert_eq!(state.phase, AssemblyPhase::Accumulating);
        assert!(state.pending_end.is_some());
    }

    // Re-drive the ordinary finalization route. It must observe the already
    // landed material and release the retry's newly created lease instead of
    // inserting another blob or material authority.
    assembler
        .try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Error)
        .await?;

    assert!(
        assembler
            .content_store
            .list_write_leases()
            .await?
            .is_empty()
    );
    let material_after_retry = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("reconciled material should remain authoritative");
    assert_eq!(material_after_retry.status, MaterialStatus::Completed);
    assert_eq!(
        material_after_retry.optional_blob_id,
        material.optional_blob_id
    );
    assert_eq!(
        material_after_retry.metadata["material_manifest"], material.metadata["material_manifest"],
        "reconciliation must not replace the already committed material authority"
    );

    let material_count = sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!: i64" FROM raw.source_material_registry WHERE id = $1"#,
        material_id,
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(material_count, 1);
    let blob_count = sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!: i64" FROM core.blobs
           WHERE annex_backend = $1 AND content_hash = $2 AND size_bytes = $3"#,
        content_key.storage_backend(),
        content_key.digest,
        content_key.size as i64,
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(blob_count, 1);
    Ok(())
}

#[sinex_test]
async fn finalization_transaction_is_idempotent_after_commit_lands(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::from_uuid(material_id);
    let content_key = ContentStoreKey::parse("SHA256E-s4--hash")?;

    let blob = ctx
        .pool
        .blobs()
        .insert(
            Blob::builder()
                .storage_backend(content_key.storage_backend().to_string())
                .content_hash(content_key.digest.clone())
                .original_filename("material.bin".to_string())
                .size_bytes(content_key.size as i64)
                .checksum_blake3("hash".to_string())
                .metadata(json!({ "material_id": material_id }))
                .build(),
        )
        .await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://idempotent-finalize"),
            json!({}),
            Timestamp::now(),
        )
        .await?;
    ctx.pool
        .source_materials()
        .finalize_in_flight(
            material_id_typed,
            Some(blob.id),
            None,
            None,
            Some(content_key.size as i64),
        )
        .await?;
    ctx.pool
        .source_materials()
        .append_temporal_ledger(TemporalLedgerEntry::realtime_capture(
            material_id,
            content_key.size as i64,
            Timestamp::now(),
        ))
        .await?;

    let final_state = FinalizationState {
        material_id,
        temp_path: state_dir.path().join("material.bin"),
        expected_offset: content_key.size as i64,
        slice_count: 1,
        buffered_count: 0,
        metadata: json!({}),
        material_kind: "test".to_string(),
        source_identifier: "test://idempotent-finalize".to_string(),
        started_at: Timestamp::now(),
    };

    let end = MaterialEndMessage {
        material_id: material_id.to_string(),
        total_slices: 1,
        total_size_bytes: content_key.size as i64,
        content_hash: "hash".to_string(),
        metadata: json!({}),
        ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
    };

    let ledger_count_before = sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!: i64" FROM raw.temporal_ledger WHERE source_material_id = $1"#,
        material_id
    )
    .fetch_one(&ctx.pool)
    .await?;

    let handle = FinalizationTransaction::new(&assembler)
        .finalize(FinalizationRequest {
            final_state: &final_state,
            content_key: &content_key,
            content_hash: &end.content_hash,
            total_size_bytes: end.total_size_bytes,
            metadata: json!({}),
            final_status: MaterialStatus::Completed,
            write_lease: None,
            manifest_key: None,
            manifest_lease: None,
        })
        .await?;
    assert_eq!(*handle.blob_id.as_uuid(), *blob.id.as_uuid());
    assert!(
        handle.reused_existing_commit,
        "retrying a landed commit should report a reused committed handle"
    );

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should still exist");
    assert_eq!(material.status, MaterialStatus::Completed);
    assert_eq!(material.optional_blob_id, Some(*blob.id.as_uuid()));

    let ledger_count_after = sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!: i64" FROM raw.temporal_ledger WHERE source_material_id = $1"#,
        material_id
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        ledger_count_after, ledger_count_before,
        "retrying finalization after a landed commit should not duplicate ledger entries"
    );

    Ok(())
}

#[sinex_test]
async fn finalization_transaction_rolls_back_blob_material_and_ledger_on_finalize_failure(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::<SourceMaterialRecord>::from_uuid(material_id);
    let content_key = ContentStoreKey::parse("SHA256E-s32--rollback-blob-hash")?;
    let started_at = Timestamp::now();

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://rollback-finalize"),
            json!({ "original": true }),
            started_at,
        )
        .await?;

    let final_state = FinalizationState {
        material_id,
        temp_path: state_dir.path().join("rollback-material.bin"),
        expected_offset: content_key.size as i64,
        slice_count: 1,
        buffered_count: 0,
        metadata: json!({ "original": true }),
        material_kind: "test".to_string(),
        source_identifier: "test://rollback-finalize".to_string(),
        started_at,
    };

    let error = FinalizationTransaction::new(&assembler)
        .finalize(FinalizationRequest {
            final_state: &final_state,
            content_key: &content_key,
            content_hash: "rollback-blake3",
            total_size_bytes: -1,
            metadata: json!({ "finalized": true }),
            final_status: MaterialStatus::Completed,
            write_lease: None,
            manifest_key: None,
            manifest_lease: None,
        })
        .await
        .expect_err("negative total_bytes should fail source-material finalization");

    assert_eq!(error.kind(), FinalizationErrorKind::FinalizeMaterialRecord);
    assert!(
        error.to_string().contains("Failed to finalize material"),
        "unexpected error: {error}"
    );

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should still exist");
    assert_eq!(material.status, MaterialStatus::Sensing);
    assert_eq!(material.optional_blob_id, None);
    assert_eq!(material.metadata["original"], true);
    assert_eq!(material.metadata.get("finalized"), None);

    let blob = ctx
        .pool
        .blobs()
        .get_by_content(
            content_key.storage_backend(),
            &content_key.digest,
            content_key.size as i64,
        )
        .await?;
    assert!(
        blob.is_none(),
        "blob insert must roll back when finalization fails"
    );

    let ledger_entries = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!: i64"
        FROM raw.temporal_ledger
        WHERE source_material_id = $1
        "#,
        material_id
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        ledger_entries, 0,
        "ledger write must not escape a failed transaction"
    );

    Ok(())
}

#[sinex_test]
async fn finalization_transaction_reuses_existing_blob_inside_transaction(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::<SourceMaterialRecord>::from_uuid(material_id);
    let content_key = ContentStoreKey::parse("SHA256E-s32--existing-blob-hash")?;

    let existing_blob = ctx
        .pool
        .blobs()
        .insert(
            Blob::builder()
                .storage_backend(content_key.storage_backend().to_string())
                .content_hash(content_key.digest.clone())
                .original_filename("existing-material.bin".to_string())
                .size_bytes(content_key.size as i64)
                .checksum_blake3("existing-blob-blake3".to_string())
                .metadata(json!({ "seeded": true }))
                .build(),
        )
        .await?;
    let started_at = Timestamp::now();

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://existing-blob-finalize"),
            json!({}),
            started_at,
        )
        .await?;

    let final_state = FinalizationState {
        material_id,
        temp_path: state_dir.path().join("existing-material.bin"),
        expected_offset: content_key.size as i64,
        slice_count: 1,
        buffered_count: 0,
        metadata: json!({}),
        material_kind: "test".to_string(),
        source_identifier: "test://existing-blob-finalize".to_string(),
        started_at,
    };

    let end = MaterialEndMessage {
        material_id: material_id.to_string(),
        total_slices: 1,
        total_size_bytes: content_key.size as i64,
        content_hash: "existing-blob-blake3".to_string(),
        metadata: json!({}),
        ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
    };

    let handle = FinalizationTransaction::new(&assembler)
        .finalize(FinalizationRequest {
            final_state: &final_state,
            content_key: &content_key,
            content_hash: &end.content_hash,
            total_size_bytes: end.total_size_bytes,
            metadata: json!({}),
            final_status: MaterialStatus::Completed,
            write_lease: None,
            manifest_key: None,
            manifest_lease: None,
        })
        .await?;
    assert_eq!(*handle.blob_id.as_uuid(), *existing_blob.id.as_uuid());
    assert!(
        !handle.reused_existing_commit,
        "first successful transaction should not be reported as a pre-existing committed state"
    );

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");

    assert_eq!(material.status, MaterialStatus::Completed);
    assert_eq!(material.optional_blob_id, Some(*existing_blob.id.as_uuid()));

    let ledger_entries = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!: i64"
        FROM raw.temporal_ledger
        WHERE source_material_id = $1
        "#,
        material_id
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        ledger_entries, 0,
        "#1570 Prong B: finalization no longer writes whole-material ledger \
         entries — material-tier timing lives on the source-material registry"
    );

    Ok(())
}

#[sinex_test]
async fn finalization_transaction_reuses_existing_blob_by_blake3_inside_transaction(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
    let material_id = Uuid::now_v7();
    let material_id_typed = Id::<SourceMaterialRecord>::from_uuid(material_id);
    let content_hash = "abababababababababababababababababababababababababababababababab";
    let content_key = ContentStoreKey::parse(&format!("SINEXBLAKE3-s32--{content_hash}"))?;

    let existing_blob = ctx
        .pool
        .blobs()
        .insert(
            Blob::builder()
                .storage_backend("SHA256E".to_string())
                .content_hash("existing-sha256-hash".to_string())
                .original_filename("existing-material.bin".to_string())
                .size_bytes(content_key.size as i64)
                .checksum_blake3(content_hash.to_string())
                .metadata(json!({ "seeded": true }))
                .build(),
        )
        .await?;
    let started_at = Timestamp::now();

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://existing-blob-by-blake3-finalize"),
            json!({}),
            started_at,
        )
        .await?;

    let final_state = FinalizationState {
        material_id,
        temp_path: state_dir.path().join("existing-material-by-blake3.bin"),
        expected_offset: content_key.size as i64,
        slice_count: 1,
        buffered_count: 0,
        metadata: json!({}),
        material_kind: "test".to_string(),
        source_identifier: "test://existing-blob-by-blake3-finalize".to_string(),
        started_at,
    };

    let end = MaterialEndMessage {
        material_id: material_id.to_string(),
        total_slices: 1,
        total_size_bytes: content_key.size as i64,
        content_hash: content_hash.to_string(),
        metadata: json!({}),
        ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
    };

    let handle = FinalizationTransaction::new(&assembler)
        .finalize(FinalizationRequest {
            final_state: &final_state,
            content_key: &content_key,
            content_hash: &end.content_hash,
            total_size_bytes: end.total_size_bytes,
            metadata: json!({}),
            final_status: MaterialStatus::Completed,
            write_lease: None,
            manifest_key: None,
            manifest_lease: None,
        })
        .await?;
    assert_eq!(*handle.blob_id.as_uuid(), *existing_blob.id.as_uuid());

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(material_id_typed)
        .await?
        .expect("material should exist");

    assert_eq!(material.status, MaterialStatus::Completed);
    assert_eq!(material.optional_blob_id, Some(*existing_blob.id.as_uuid()));

    Ok(())
}
