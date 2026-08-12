use super::super::RESTORED_SELF_OBSERVATION_ORPHAN_TIMEOUT_SECS;

use futures::StreamExt;
use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::{Id, MaterialStatus, Timestamp, Uuid};
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "sinex-x6sk open: a Completed material stuck with total_bytes=NULL (finalizing UPDATE \
            never ran) has no reconciliation path -- reconcile_orphaned_sensing_materials only \
            handles status=Sensing rows"]
async fn orphan_reconcile_does_not_repair_completed_material_stuck_with_null_total_bytes(
    ctx: TestContext,
) -> TestResult<()> {
    // sinex-x6sk: register_material (the only production INSERT path) never
    // accepts total_bytes -- the row is immediately status=Completed with
    // total_bytes=NULL, and callers that know the byte size finalize it via
    // a SEPARATE, non-transactional raw sqlx::query! UPDATE afterward
    // (ops/media.rs, sources.rs, ops.rs::register_email_staged_material). If
    // that UPDATE never runs (handler crash, request abort), the material is
    // permanently mis-scored as anchor-unstable
    // (ContinuityScorecard::from_material_facts treats total_bytes.is_none()
    // as "still sensing"), and nothing reconciles it --
    // reconcile_orphaned_sensing_materials only handles status=Sensing rows.
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("x6sk-orphan-total-bytes")
            .slice_timeout_secs(3_600)
            .build(&ctx)
            .await?;

    let material = sinex_db::repositories::SourceMaterial::file("x6sk-orphan-material.bin");
    let record = ctx
        .pool
        .source_materials()
        .register_material(material)
        .await?;
    assert_eq!(record.status, MaterialStatus::Completed);
    assert!(record.total_bytes.is_none());

    assembler.reconcile_orphaned_sensing_materials().await?;

    let after = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(record.id))
        .await?
        .expect("material should still exist");

    assert!(
        after.total_bytes.is_some(),
        "sinex-x6sk: a Completed material with total_bytes still NULL after finalization has \
         no reconciliation path -- reconcile_orphaned_sensing_materials only handles \
         status=Sensing rows, so this permanently mis-scored anchor-unstable state is never \
         repaired"
    );

    Ok(())
}

#[sinex_test]
async fn end_before_begin_without_slices_short_timeouts(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(3_600)
            .build(&ctx)
            .await?;

    let material_id = Uuid::now_v7();
    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.last_slice_received = Timestamp::now() - time::Duration::seconds(301);
    state.pending_end = Some(super::super::state::MaterialEndMessage {
        material_id: material_id.to_string(),
        ended_at: Timestamp::now().format_rfc3339(),
        content_hash: "0".repeat(64),
        total_slices: 49,
        total_size_bytes: 12_725_348,
        metadata: json!({}),
    });
    assembler.insert_state_handle(material_id, state);

    let stale = assembler.find_stale_materials().await;

    assert!(
        stale.iter().any(|(id, _)| *id == material_id),
        "end-before-begin placeholders with no slices cannot self-heal for an hour"
    );
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-2nsg open: a material wedged in AssemblyPhase::Finalizing is never surfaced by \
            find_stale_materials no matter how old it is (maintenance.rs unconditional `continue` \
            on Finalizing), so a panicked detached finalize worker leaves it permanently invisible \
            to the stale sweep; un-ignore once Finalizing gets an age bound"]
async fn finalizing_phase_material_stuck_for_hours_is_never_flagged_stale(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(60)
            .build(&ctx)
            .await?;

    let material_id = Uuid::now_v7();
    let mut state = assembler.create_placeholder_state(material_id).await?;
    // Simulate a detached finalize worker that panicked mid-flight: the phase
    // was set to Finalizing and never reverted, hours ago.
    state.phase = super::super::state::AssemblyPhase::Finalizing;
    state.last_slice_received = Timestamp::now() - time::Duration::hours(6);
    assembler.insert_state_handle(material_id, state);

    let stale = assembler.find_stale_materials().await;

    assert!(
        stale.iter().any(|(id, _)| *id == material_id),
        "a material wedged in Finalizing for 6 hours must eventually be surfaced as stale, \
         not silently excluded forever regardless of age"
    );
    Ok(())
}

#[sinex_test]
async fn orphan_reconcile_does_not_short_timeout_live_self_observation_registry_rows(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(3_600)
            .build(&ctx)
            .await?;

    let short_stale_start = Timestamp::now()
        - time::Duration::seconds(RESTORED_SELF_OBSERVATION_ORPHAN_TIMEOUT_SECS + 1);
    let self_observation_id = Uuid::now_v7();
    let ordinary_id = Uuid::now_v7();

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            self_observation_id,
            "self_observation",
            Some(&format!(
                "sinex.self-observation.test#material={self_observation_id}"
            )),
            json!({}),
            short_stale_start,
        )
        .await?;
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            ordinary_id,
            "test",
            Some(&format!("test://ordinary-orphan/{ordinary_id}")),
            json!({}),
            short_stale_start,
        )
        .await?;

    assembler.reconcile_orphaned_sensing_materials().await?;

    let self_observation = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(self_observation_id))
        .await?
        .expect("self-observation material should exist");
    let ordinary = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(ordinary_id))
        .await?
        .expect("ordinary material should exist");

    assert_eq!(self_observation.status, MaterialStatus::Sensing);
    assert_eq!(ordinary.status, MaterialStatus::Sensing);
    Ok(())
}

#[sinex_test]
async fn orphan_reconcile_recovers_globally_stale_self_observation_without_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(60)
            .build(&ctx)
            .await?;

    let material_id = Uuid::now_v7();
    let source_identifier = format!("sinex.self-observation.test#material={material_id}");
    let stale_start = Timestamp::now() - time::Duration::minutes(5);

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "self_observation",
            Some(&source_identifier),
            json!({}),
            stale_start,
        )
        .await?;

    assembler.reconcile_orphaned_sensing_materials().await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("self-observation material should exist");

    assert_eq!(material.status, MaterialStatus::RecoveredPartial);
    assert_eq!(
        material.metadata["recovery_info"]["recovery_reason"],
        json!("orphaned_self_observation_material_recovered_partial")
    );
    assert_eq!(
        material.metadata["orphaned_sensing_material"]["dlq_policy"],
        json!("suppressed_self_observation_restart_orphan")
    );
    Ok(())
}

#[sinex_test]
async fn orphan_reconcile_recovers_material_scoped_self_observation_identifier(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(60)
            .build(&ctx)
            .await?;

    let material_id = Uuid::now_v7();
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "self_observation",
            Some(&format!(
                "sinex.self-observation.browser.history#material={material_id}"
            )),
            json!({}),
            Timestamp::now() - time::Duration::minutes(5),
        )
        .await?;

    assembler.reconcile_orphaned_sensing_materials().await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("self-observation material should exist");

    assert_eq!(material.status, MaterialStatus::RecoveredPartial);
    assert_eq!(
        material.metadata["orphaned_sensing_material"]["dlq_policy"],
        json!("suppressed_self_observation_restart_orphan")
    );
    Ok(())
}

#[sinex_test]
async fn orphan_reconcile_recovers_globally_stale_zero_event_source_material_without_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(60)
            .build(&ctx)
            .await?;
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

    let material_id = Uuid::now_v7();
    let source_identifier = format!("desktop.window-manager#material={material_id}");
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "desktop.window-manager",
            Some(&source_identifier),
            json!({}),
            Timestamp::now() - time::Duration::minutes(5),
        )
        .await?;

    assembler.reconcile_orphaned_sensing_materials().await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("source material should exist");

    assert_eq!(material.status, MaterialStatus::RecoveredPartial);
    assert_eq!(
        material.metadata["recovery_info"]["recovery_reason"],
        json!("orphaned_zero_event_source_material_recovered_partial")
    );
    assert_eq!(
        material.metadata["orphaned_sensing_material"]["source_identifier"],
        json!(source_identifier)
    );
    assert_eq!(
        material.metadata["orphaned_sensing_material"]["parsed_event_count"],
        json!(0)
    );
    assert_eq!(
        material.metadata["orphaned_sensing_material"]["dlq_policy"],
        json!("suppressed_zero_event_source_material_restart_orphan")
    );
    assert!(
        timeout(Duration::from_millis(200), dlq_sub.next())
            .await
            .is_err(),
        "zero-event orphaned source material should not publish raw DLQ residue"
    );
    Ok(())
}

#[sinex_test]
async fn stale_eventful_timeout_recovers_partial_without_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(60)
            .build(&ctx)
            .await?;
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

    let material_id = Uuid::now_v7();
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "browser.history",
            Some(&format!("browser.history#material={material_id}")),
            json!({}),
            Timestamp::now() - time::Duration::minutes(5),
        )
        .await?;
    sqlx::query!(
        "UPDATE raw.source_material_registry SET parsed_event_count = 42 WHERE id = $1",
        material_id,
    )
    .execute(ctx.pool())
    .await?;

    assembler.process_stale_material(material_id, 61).await;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::RecoveredPartial);
    assert_eq!(
        material.metadata["recovery_info"]["recovery_reason"],
        json!("slice_arrival_timeout_with_admitted_events")
    );
    assert!(
        timeout(Duration::from_millis(200), dlq_sub.next())
            .await
            .is_err(),
        "eventful timeout recovery should not publish a DLQ message"
    );
    Ok(())
}

#[sinex_test]
async fn stale_zero_event_self_observation_timeout_recovers_partial_without_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(60)
            .build(&ctx)
            .await?;
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

    let material_id = Uuid::now_v7();
    let source_identifier = format!("sinex.self-observation.sinexd#material={material_id}");
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "self_observation",
            Some(&source_identifier),
            json!({}),
            Timestamp::now() - time::Duration::minutes(5),
        )
        .await?;
    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.source_identifier = source_identifier.clone();
    state.last_slice_received = Timestamp::now() - time::Duration::seconds(61);
    assembler.insert_state_handle(material_id, state);

    assembler.process_stale_material(material_id, 61).await;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::RecoveredPartial);
    assert_eq!(
        material.metadata["recovery_info"]["recovery_reason"],
        json!("slice_arrival_timeout_zero_event_self_observation_recovered_partial")
    );
    assert_eq!(
        material.metadata["slice_arrival_timeout_zero_event_self_observation"]["source_identifier"],
        json!(source_identifier)
    );
    assert_eq!(
        material.metadata["slice_arrival_timeout_zero_event_self_observation"]["parsed_event_count"],
        json!(0)
    );
    assert_eq!(
        material.metadata["slice_arrival_timeout_zero_event_self_observation"]["dlq_policy"],
        json!("suppressed_zero_event_self_observation_timeout")
    );
    assert!(
        timeout(Duration::from_millis(200), dlq_sub.next())
            .await
            .is_err(),
        "zero-event self-observation timeout recovery should not publish a DLQ message"
    );
    Ok(())
}

#[sinex_test]
async fn stale_zero_event_source_material_timeout_recovers_partial_without_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(60)
            .build(&ctx)
            .await?;
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.event_engine");
    let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

    let material_id = Uuid::now_v7();
    let source_identifier = format!("terminal.atuin-history#material={material_id}");
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "terminal.atuin-history",
            Some(&source_identifier),
            json!({}),
            Timestamp::now() - time::Duration::minutes(5),
        )
        .await?;
    let mut state = assembler.create_placeholder_state(material_id).await?;
    state.source_identifier = source_identifier.clone();
    state.last_slice_received = Timestamp::now() - time::Duration::seconds(61);
    assembler.insert_state_handle(material_id, state);

    assembler.process_stale_material(material_id, 61).await;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("material should exist");
    assert_eq!(material.status, MaterialStatus::RecoveredPartial);
    assert_eq!(
        material.metadata["recovery_info"]["recovery_reason"],
        json!("slice_arrival_timeout_zero_event_source_material_recovered_partial")
    );
    assert_eq!(
        material.metadata["slice_arrival_timeout_zero_event_source_material"]["source_identifier"],
        json!(source_identifier)
    );
    assert_eq!(
        material.metadata["slice_arrival_timeout_zero_event_source_material"]["parsed_event_count"],
        json!(0)
    );
    assert_eq!(
        material.metadata["slice_arrival_timeout_zero_event_source_material"]["dlq_policy"],
        json!("suppressed_zero_event_source_material_timeout")
    );
    assert!(
        timeout(Duration::from_millis(200), dlq_sub.next())
            .await
            .is_err(),
        "zero-event source-material timeout recovery should not publish a DLQ message"
    );
    Ok(())
}

/// sinex-ijps (finding 1): a single unexpected entry under the assembler
/// state root (an operator artifact, a corrupted folder -- anything not a
/// valid material-id-named directory) makes `check_orphaned_folder` return
/// `Err`, which propagates through `cleanup_orphaned_temp_files`'s `?` and
/// aborts the WHOLE cleanup pass. Every other genuinely orphaned folder in
/// that run is skipped until the bad entry is manually removed.
#[sinex_test]
#[ignore = "sinex-ijps open: one non-UUID-named directory under state_root aborts the entire orphan-cleanup pass instead of being skipped"]
async fn cleanup_orphaned_temp_files_one_bad_entry_does_not_abort_whole_pass(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("ijps-orphan-cleanup-test")
            .slice_timeout_secs(3_600)
            .build(&ctx)
            .await?;

    // An unrelated, non-UUID-named directory under state_root -- an
    // operator artifact or corrupted entry, not a material folder.
    std::fs::create_dir_all(state_dir.path().join("not-a-material-id"))?;

    // A genuinely orphaned material folder that IS a valid UUID and has no
    // corresponding live assembler state -- this should be cleanable.
    let orphan_material_id = Uuid::now_v7();
    std::fs::create_dir_all(state_dir.path().join(orphan_material_id.to_string()))?;

    let result = assembler.cleanup_orphaned_temp_files().await;

    assert!(
        result.is_ok(),
        "sinex-ijps: one malformed entry under state_root must not abort the whole \
         orphan-cleanup pass; got {result:?}"
    );
    Ok(())
}
