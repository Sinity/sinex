use crate::runtime::content_store::ContentStoreKey;
use sinex_primitives::MaterialStatus;
use sinex_primitives::Uuid;
use xtask::sandbox::prelude::*;

use super::*;

#[sinex_test]
async fn rollback_finalization_failure_preserves_original_error_context() -> TestResult<()> {
    let error = rollback_finalization_failure(
        SinexError::validation("original finalize failure"),
        "rollback broke too",
        "record_ledger_entry",
    );

    let rendered = error.to_string();
    assert!(rendered.contains("Failed to rollback material finalization transaction"));
    assert!(rendered.contains("rollback broke too"));
    assert!(rendered.contains("original finalize failure"));
    assert!(rendered.contains("record_ledger_entry"));
    Ok(())
}

#[sinex_test]
async fn finalization_unknown_commit_error_preserves_retry_context() -> TestResult<()> {
    let content_key = ContentStoreKey::parse("SHA256E-s4--retry")?;
    let error = finalization_unknown_commit_error(
        SinexError::database("commit failed"),
        &SinexError::database("reconcile failed"),
        Uuid::now_v7(),
        &content_key,
        MaterialStatus::Completed,
    );

    assert!(finalization_commit_outcome_unknown(&error));
    assert_eq!(
        error.context_map().get("retry_state_preserved"),
        Some(&"true".to_string())
    );
    assert_eq!(
        error.context_map().get("terminal_failure_routed"),
        Some(&"false".to_string())
    );
    assert_eq!(
        error.context_map().get("final_status"),
        Some(&MaterialStatus::Completed.to_string())
    );
    assert_eq!(
        error.context_map().get("content_key"),
        Some(&content_key.key),
    );
    assert!(
        error
            .context_map()
            .get("reconcile_error")
            .is_some_and(|value| value.contains("reconcile failed"))
    );
    Ok(())
}

#[sinex_test]
async fn finalization_commit_outcome_unknown_ignores_unflagged_errors() -> TestResult<()> {
    let error = SinexError::database("ordinary failure");
    assert!(
        !finalization_commit_outcome_unknown(&error),
        "only explicitly flagged commit-reconciliation failures should preserve retry state"
    );
    Ok(())
}

#[sinex_test]
async fn finalization_releases_cas_lease_after_metadata_commit(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) =
        super::super::test_support::build_test_assembler(&ctx, "lease-order").await?;
    let material_id = Uuid::now_v7();
    let source_path = assembler
        .content_store
        .root_path()
        .join("lease-order-source.bin");
    tokio::fs::write(&source_path, b"lease ordering bytes").await?;
    let (content_key, lease) = assembler
        .content_store
        .store_file_with_lease(&source_path)
        .await?;
    assert_eq!(
        assembler.content_store.list_write_leases().await?.len(),
        1,
        "CAS publish must still be leased before the metadata transaction"
    );

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://lease-order"),
            serde_json::json!({}),
            sinex_primitives::Timestamp::now(),
        )
        .await?;
    let final_state = super::FinalizationState {
        material_id,
        temp_path: state_dir.path().join("lease-order.bin"),
        expected_offset: content_key.size as i64,
        slice_count: 1,
        buffered_count: 0,
        metadata: serde_json::json!({}),
        material_kind: "test".to_string(),
        source_identifier: "test://lease-order".to_string(),
        started_at: sinex_primitives::Timestamp::now(),
    };

    FinalizationTransaction::new(&assembler)
        .finalize(FinalizationRequest {
            final_state: &final_state,
            content_key: &content_key,
            content_hash: &content_key.digest,
            total_size_bytes: content_key.size as i64,
            metadata: serde_json::json!({}),
            final_status: MaterialStatus::Completed,
            write_lease: Some(&lease),
        })
        .await?;

    assert!(
        assembler
            .content_store
            .list_write_leases()
            .await?
            .is_empty(),
        "the lease must be released only after the metadata commit succeeds"
    );
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("finalized material should exist");
    assert_eq!(material.status, MaterialStatus::Completed);
    assert!(material.optional_blob_id.is_some());
    Ok(())
}

#[sinex_test]
async fn finalized_material_retry_releases_renewed_cas_lease(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) =
        super::super::test_support::build_test_assembler(&ctx, "landed-lease-release").await?;
    let material_id = Uuid::now_v7();
    let source_path = assembler
        .content_store
        .root_path()
        .join("landed-lease-release-source.bin");
    tokio::fs::write(&source_path, b"landed lease release bytes").await?;
    let (content_key, initial_lease) = assembler
        .content_store
        .store_file_with_lease(&source_path)
        .await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some("test://landed-lease-release"),
            serde_json::json!({}),
            sinex_primitives::Timestamp::now(),
        )
        .await?;
    let final_state = super::FinalizationState {
        material_id,
        temp_path: state_dir.path().join("landed-lease-release.bin"),
        expected_offset: content_key.size as i64,
        slice_count: 1,
        buffered_count: 0,
        metadata: serde_json::json!({}),
        material_kind: "test".to_string(),
        source_identifier: "test://landed-lease-release".to_string(),
        started_at: sinex_primitives::Timestamp::now(),
    };
    let request = |write_lease| FinalizationRequest {
        final_state: &final_state,
        content_key: &content_key,
        content_hash: &content_key.digest,
        total_size_bytes: content_key.size as i64,
        metadata: serde_json::json!({}),
        final_status: MaterialStatus::Completed,
        write_lease,
    };

    FinalizationTransaction::new(&assembler)
        .finalize(request(Some(&initial_lease)))
        .await?;
    let (_, retry_lease) = assembler
        .content_store
        .store_file_with_lease(&source_path)
        .await?;
    assert_eq!(assembler.content_store.list_write_leases().await?.len(), 1);

    let handle = FinalizationTransaction::new(&assembler)
        .finalize(request(Some(&retry_lease)))
        .await?;

    assert!(handle.reused_existing_commit);
    assert!(
        assembler
            .content_store
            .list_write_leases()
            .await?
            .is_empty(),
        "anti-vacuity: a retry that reconciles a landed material must release its renewed CAS lease; otherwise fsck protects an orphan indefinitely"
    );
    Ok(())
}
