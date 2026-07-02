use super::*;
use crate::event_engine::material_assembler::FinalizationState;
use crate::event_engine::material_assembler::finalization_transaction::{
    FinalizationErrorKind, FinalizationRequest, FinalizationTransaction,
};
use crate::event_engine::material_assembler::{io, state};
use crate::runtime::content_store::ContentStoreKey;
use serde_json::json;
use sinex_db::{
    models::blob::Blob,
    repositories::{DbPoolExt, TemporalLedgerEntry},
};
use sinex_primitives::MaterialStatus;
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
        .await;

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
        .await;

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
        .await;

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
    let content_hash = "existing-blob-blake3";
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
