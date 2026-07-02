use super::*;
use serde_json::json;
use xtask::sandbox::prelude::*;

fn material_id() -> Uuid {
    Uuid::now_v7()
}

fn replayed_state() -> ReplayedState {
    ReplayedState {
        expected_offset: 42,
        slice_count: 1,
        started_at: "2026-04-22T00:00:00Z".to_string(),
        material_kind: "test".to_string(),
        source_identifier: "test://restore-plan".to_string(),
        metadata: json!({}),
        phase: AssemblyPhase::Accumulating,
        ..Default::default()
    }
}

#[sinex_test]
async fn missing_wal_without_artifacts_is_discarded_without_cleanup() -> TestResult<()> {
    let plan = derive_restore_plan(RestorePlanInput {
        material_id: material_id(),
        wal_present: false,
        has_state_artifacts: false,
        replay_corrupted: false,
        has_envelope_entries: false,
        has_non_empty_lines: false,
        material_terminal: false,
        file_progress_error: None,
        stale: false,
        replayed_state: None,
    });

    assert!(matches!(
        plan.classification,
        RestoreClassification::Discard {
            reason: RestoreDiscardReason::MissingWalWithoutArtifacts,
            cleanup_state: false,
        }
    ));
    assert!(!plan.cleanup_state());
    Ok(())
}

#[sinex_test]
async fn missing_wal_with_artifacts_is_quarantined() -> TestResult<()> {
    let plan = derive_restore_plan(RestorePlanInput {
        material_id: material_id(),
        wal_present: false,
        has_state_artifacts: true,
        replay_corrupted: false,
        has_envelope_entries: false,
        has_non_empty_lines: false,
        material_terminal: false,
        file_progress_error: None,
        stale: false,
        replayed_state: None,
    });

    assert!(matches!(
        plan.classification,
        RestoreClassification::Quarantine {
            reason: RestoreQuarantineReason::MissingWalWithArtifacts,
        }
    ));
    Ok(())
}

#[sinex_test]
async fn corrupt_wal_is_discarded_with_cleanup() -> TestResult<()> {
    let plan = derive_restore_plan(RestorePlanInput {
        material_id: material_id(),
        wal_present: true,
        has_state_artifacts: true,
        replay_corrupted: true,
        has_envelope_entries: false,
        has_non_empty_lines: true,
        material_terminal: false,
        file_progress_error: None,
        stale: false,
        replayed_state: None,
    });

    assert!(matches!(
        plan.classification,
        RestoreClassification::Discard {
            reason: RestoreDiscardReason::CorruptWal,
            cleanup_state: true,
        }
    ));
    assert!(plan.cleanup_state());
    Ok(())
}

#[sinex_test]
async fn empty_wal_is_discarded_with_cleanup() -> TestResult<()> {
    let plan = derive_restore_plan(RestorePlanInput {
        material_id: material_id(),
        wal_present: true,
        has_state_artifacts: true,
        replay_corrupted: false,
        has_envelope_entries: false,
        has_non_empty_lines: false,
        material_terminal: false,
        file_progress_error: None,
        stale: false,
        replayed_state: None,
    });

    assert!(matches!(
        plan.classification,
        RestoreClassification::Discard {
            reason: RestoreDiscardReason::EmptyWal,
            cleanup_state: true,
        }
    ));
    assert!(plan.cleanup_state());
    Ok(())
}

#[sinex_test]
async fn terminal_material_is_discarded_with_cleanup() -> TestResult<()> {
    let state = replayed_state();
    let plan = derive_restore_plan(RestorePlanInput {
        material_terminal: true,
        ..RestorePlanInput::from_replayed(material_id(), &state)
    });

    assert!(matches!(
        plan.classification,
        RestoreClassification::Discard {
            reason: RestoreDiscardReason::TerminalMaterial,
            cleanup_state: true,
        }
    ));
    Ok(())
}

#[sinex_test]
async fn file_progress_mismatch_is_discarded_with_cleanup() -> TestResult<()> {
    let state = replayed_state();
    let plan = derive_restore_plan(RestorePlanInput {
        file_progress_error: Some("staged file size mismatch".to_string()),
        ..RestorePlanInput::from_replayed(material_id(), &state)
    });

    assert!(matches!(
        plan.classification,
        RestoreClassification::Discard {
            reason: RestoreDiscardReason::FileProgressMismatch,
            cleanup_state: true,
        }
    ));
    Ok(())
}

#[sinex_test]
async fn stale_incomplete_state_is_discarded_with_cleanup() -> TestResult<()> {
    let state = replayed_state();
    let plan = derive_restore_plan(RestorePlanInput {
        stale: true,
        ..RestorePlanInput::from_replayed(material_id(), &state)
    });

    assert!(matches!(
        plan.classification,
        RestoreClassification::Discard {
            reason: RestoreDiscardReason::StaleIncompleteAssembly,
            cleanup_state: true,
        }
    ));
    Ok(())
}

#[sinex_test]
async fn pending_end_ready_state_is_classified_for_finalization() -> TestResult<()> {
    let mut state = replayed_state();
    state.pending_end = Some(MaterialEndMessage {
        material_id: material_id().to_string(),
        ended_at: "2026-04-22T00:01:00Z".to_string(),
        content_hash: "blake3:test".to_string(),
        total_slices: 1,
        total_size_bytes: 42,
        metadata: json!({}),
    });

    let plan = derive_restore_plan(RestorePlanInput::from_replayed(material_id(), &state));

    assert!(matches!(
        plan.classification,
        RestoreClassification::Finalize {
            reason: RestoreFinalizeReason::PendingEndReady,
        }
    ));
    assert!(plan.restores_state());
    Ok(())
}

#[sinex_test]
async fn missing_replayed_state_is_quarantined() -> TestResult<()> {
    let plan = derive_restore_plan(RestorePlanInput {
        material_id: material_id(),
        wal_present: true,
        has_state_artifacts: true,
        replay_corrupted: false,
        has_envelope_entries: true,
        has_non_empty_lines: true,
        material_terminal: false,
        file_progress_error: None,
        stale: false,
        replayed_state: None,
    });

    assert!(matches!(
        plan.classification,
        RestoreClassification::Quarantine {
            reason: RestoreQuarantineReason::MissingReplayedState,
        }
    ));
    assert!(!plan.restores_state());
    Ok(())
}

#[sinex_test]
async fn in_progress_state_is_kept() -> TestResult<()> {
    let state = replayed_state();
    let plan = derive_restore_plan(RestorePlanInput::from_replayed(material_id(), &state));

    assert!(matches!(
        plan.classification,
        RestoreClassification::Keep {
            reason: RestoreKeepReason::InProgress,
        }
    ));
    assert!(plan.restores_state());
    Ok(())
}
