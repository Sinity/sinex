use sinex_primitives::domain::{OperationKind, OperationStatus};
use sinex_primitives::views::{
    ActionAvailabilityState, OPERATION_JOB_LIST_SCHEMA_VERSION, OPERATION_VIEW_SCHEMA_VERSION,
    OperationJobListView, OperationView, VIEW_ENVELOPE_SCHEMA_VERSION, ViewEnvelope,
};
use xtask::sandbox::prelude::*;

// ─── OperationKind ───────────────────────────────────────────────────────────

#[sinex_test]
async fn operation_kind_known_variants_round_trip() -> TestResult<()> {
    let cases = [
        (OperationKind::Replay, "replay"),
        (OperationKind::Archive, "archive"),
        (OperationKind::Restore, "restore"),
        (OperationKind::Purge, "purge"),
        (OperationKind::Tombstone, "tombstone"),
        (OperationKind::DlqRequeue, "dlq.requeue"),
        (OperationKind::DlqPurge, "dlq.purge"),
        (OperationKind::RuntimeDrain, "runtime.drain"),
        (OperationKind::RuntimeResume, "runtime.resume"),
        (OperationKind::RuntimeSetHorizon, "runtime.set_horizon"),
        (OperationKind::CurationFinalize, "curation.finalize"),
        (OperationKind::PrivacyPrivateMode, "privacy.private_mode"),
        (
            OperationKind::ArchiveIntegrityMismatch,
            "archive.integrity_mismatch",
        ),
        (OperationKind::ProjectionRebuild, "projection-rebuild"),
    ];

    for (kind, expected_str) in cases {
        // as_str() returns the canonical string
        assert_eq!(kind.as_str(), expected_str, "as_str for {:?}", kind);
        // Display is the same string
        assert_eq!(kind.to_string(), expected_str, "Display for {:?}", kind);
        // JSON serializes as a plain string
        let json = serde_json::to_string(&kind)?;
        assert_eq!(json, format!("\"{expected_str}\""), "JSON for {:?}", kind);
        // Deserialization round-trips
        let back: OperationKind = serde_json::from_str(&json)?;
        assert_eq!(back, kind, "round-trip for {:?}", kind);
    }
    Ok(())
}

#[sinex_test]
async fn operation_kind_from_str_for_known() -> TestResult<()> {
    use std::str::FromStr;
    assert_eq!(
        OperationKind::from_str("replay").unwrap(),
        OperationKind::Replay
    );
    assert_eq!(
        OperationKind::from_str("archive").unwrap(),
        OperationKind::Archive
    );
    assert_eq!(
        OperationKind::from_str("restore").unwrap(),
        OperationKind::Restore
    );
    assert_eq!(
        OperationKind::from_str("purge").unwrap(),
        OperationKind::Purge
    );
    assert_eq!(
        OperationKind::from_str("tombstone").unwrap(),
        OperationKind::Tombstone
    );
    assert_eq!(
        OperationKind::from_str("dlq.requeue").unwrap(),
        OperationKind::DlqRequeue
    );
    assert_eq!(
        OperationKind::from_str("dlq.purge").unwrap(),
        OperationKind::DlqPurge
    );
    assert_eq!(
        OperationKind::from_str("runtime.drain").unwrap(),
        OperationKind::RuntimeDrain
    );
    assert_eq!(
        OperationKind::from_str("runtime.resume").unwrap(),
        OperationKind::RuntimeResume
    );
    assert_eq!(
        OperationKind::from_str("runtime.set_horizon").unwrap(),
        OperationKind::RuntimeSetHorizon
    );
    assert_eq!(
        OperationKind::from_str("curation.finalize").unwrap(),
        OperationKind::CurationFinalize
    );
    assert_eq!(
        OperationKind::from_str("privacy.private_mode").unwrap(),
        OperationKind::PrivacyPrivateMode
    );
    assert_eq!(
        OperationKind::from_str("archive.integrity_mismatch").unwrap(),
        OperationKind::ArchiveIntegrityMismatch
    );
    Ok(())
}

#[sinex_test]
async fn operation_kind_unknown_becomes_other() -> TestResult<()> {
    use std::str::FromStr;
    let kind = OperationKind::from_str("migration").unwrap();
    assert_eq!(kind, OperationKind::Other("migration".to_string()));
    assert_eq!(kind.as_str(), "migration");
    assert_eq!(kind.to_string(), "migration");
    Ok(())
}

#[sinex_test]
async fn operation_kind_other_serializes_as_plain_string() -> TestResult<()> {
    let kind = OperationKind::Other("custom-op".to_string());
    let json = serde_json::to_string(&kind)?;
    assert_eq!(json, r#""custom-op""#);
    let back: OperationKind = serde_json::from_str(&json)?;
    assert_eq!(back, kind);
    Ok(())
}

#[sinex_test]
async fn operation_kind_from_str_ref_and_string() -> TestResult<()> {
    let from_str_ref = OperationKind::from("replay");
    assert_eq!(from_str_ref, OperationKind::Replay);
    let from_string = OperationKind::from("tombstone".to_string());
    assert_eq!(from_string, OperationKind::Tombstone);
    Ok(())
}

// ─── OperationView ────────────────────────────────────────────────────────────

#[sinex_test]
async fn operation_view_from_rpc_known_type() -> TestResult<()> {
    let view = OperationView::from_rpc(
        "01HQ2KM0001ABC".to_string(),
        "replay",
        "operator@sinex".to_string(),
        OperationStatus::Running,
        None,
        None,
        None,
        None,
    );

    assert_eq!(view.id, "01HQ2KM0001ABC");
    assert_eq!(view.kind, OperationKind::Replay);
    assert_eq!(view.operator, "operator@sinex");
    assert_eq!(view.status, OperationStatus::Running);
    assert!(view.duration_ms.is_none());
    assert!(view.result_message.is_none());
    // Running operations should have a cancel action enabled
    let cancel = view.actions.iter().find(|a| a.id == "ops.cancel");
    assert!(cancel.is_some(), "cancel action should be present");
    Ok(())
}

#[sinex_test]
async fn operation_view_from_rpc_unknown_type() -> TestResult<()> {
    let view = OperationView::from_rpc(
        "01HQ2KM0002DEF".to_string(),
        "maintenance",
        "system".to_string(),
        OperationStatus::Success,
        Some(1234),
        Some("completed successfully".to_string()),
        None,
        None,
    );

    assert_eq!(view.kind, OperationKind::Other("maintenance".to_string()));
    assert_eq!(view.duration_ms, Some(1234));
    assert_eq!(
        view.result_message.as_deref(),
        Some("completed successfully")
    );
    Ok(())
}

#[sinex_test]
async fn operation_view_from_rpc_runtime_and_dlq_types() -> TestResult<()> {
    let dlq = OperationView::from_rpc(
        "01HQ2KM0002DLQ".to_string(),
        "dlq.requeue",
        "admin".to_string(),
        OperationStatus::Success,
        Some(42),
        None,
        None,
        None,
    );
    assert_eq!(dlq.kind, OperationKind::DlqRequeue);

    let runtime = OperationView::from_rpc(
        "01HQ2KM0002RUN".to_string(),
        "runtime.set_horizon",
        "operator".to_string(),
        OperationStatus::Running,
        None,
        None,
        None,
        None,
    );
    assert_eq!(runtime.kind, OperationKind::RuntimeSetHorizon);
    assert!(
        runtime
            .actions
            .iter()
            .any(|action| action.id == "ops.cancel"
                && action.state == ActionAvailabilityState::Enabled),
        "running runtime operations should still expose the shared cancel action"
    );

    Ok(())
}

#[sinex_test]
async fn operation_view_serializes_to_json() -> TestResult<()> {
    let view = OperationView::from_rpc(
        "01HQ2KM0003GHI".to_string(),
        "archive",
        "cli-operator".to_string(),
        OperationStatus::Failed,
        Some(500),
        Some("disk full".to_string()),
        None,
        None,
    );

    let json = serde_json::to_value(&view)?;
    assert_eq!(json["id"], "01HQ2KM0003GHI");
    assert_eq!(json["kind"], "archive");
    assert_eq!(json["status"], "failed");
    assert_eq!(json["duration_ms"], 500);
    assert_eq!(json["result_message"], "disk full");
    Ok(())
}

#[sinex_test]
async fn operation_view_optional_fields_skip_serialization() -> TestResult<()> {
    let view = OperationView::from_rpc(
        "01HQ2KM0004JKL".to_string(),
        "purge",
        "admin".to_string(),
        OperationStatus::Pending,
        None,
        None,
        None,
        None,
    );

    let json = serde_json::to_value(&view)?;
    // Optional fields that are None should be absent from JSON
    assert!(
        json.get("duration_ms").is_none(),
        "duration_ms should be absent when None"
    );
    assert!(
        json.get("result_message").is_none(),
        "result_message should be absent when None"
    );
    assert!(
        json.get("scope").is_none(),
        "scope should be absent when None"
    );
    assert!(
        json.get("preview_summary").is_none(),
        "preview_summary should be absent when None"
    );
    Ok(())
}

// ─── OperationJobListView + ViewEnvelope ─────────────────────────────────────

#[sinex_test]
async fn operation_job_list_view_new_sets_count() -> TestResult<()> {
    let views = vec![
        OperationView::from_rpc(
            "id1".to_string(),
            "replay",
            "op".to_string(),
            OperationStatus::Success,
            None,
            None,
            None,
            None,
        ),
        OperationView::from_rpc(
            "id2".to_string(),
            "archive",
            "op".to_string(),
            OperationStatus::Running,
            None,
            None,
            None,
            None,
        ),
    ];

    let list = OperationJobListView::new(views);
    assert_eq!(list.count, 2);
    assert_eq!(list.jobs.len(), 2);
    assert_eq!(list.schema_version, OPERATION_JOB_LIST_SCHEMA_VERSION);
    Ok(())
}

#[sinex_test]
async fn operation_job_list_view_empty() -> TestResult<()> {
    let list = OperationJobListView::new(vec![]);
    assert_eq!(list.count, 0);
    assert!(list.jobs.is_empty());
    Ok(())
}

#[sinex_test]
async fn view_envelope_wraps_operation_job_list() -> TestResult<()> {
    let views = vec![OperationView::from_rpc(
        "op-01".to_string(),
        "tombstone",
        "system".to_string(),
        OperationStatus::Cancelled,
        Some(100),
        Some("user cancelled".to_string()),
        None,
        None,
    )];
    let list = OperationJobListView::new(views);
    let envelope = ViewEnvelope::new("sinexctl.ops.jobs.list", list.clone());

    assert_eq!(envelope.schema_version, VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(envelope.source_surface, "sinexctl.ops.jobs.list");
    assert_eq!(envelope.payload.count, 1);
    assert_eq!(envelope.payload.jobs[0].kind, OperationKind::Tombstone);

    // The envelope round-trips through JSON
    let json = serde_json::to_string(&envelope)?;
    let back: ViewEnvelope<OperationJobListView> = serde_json::from_str(&json)?;
    assert_eq!(back.payload.count, 1);
    assert_eq!(
        back.payload.schema_version,
        OPERATION_JOB_LIST_SCHEMA_VERSION
    );
    Ok(())
}

#[sinex_test]
async fn view_envelope_with_query_echo() -> TestResult<()> {
    let list = OperationJobListView::new(vec![]);
    let envelope =
        ViewEnvelope::new("sinexctl.ops.jobs.list", list).with_query_echo(serde_json::json!({
            "kind": "replay",
            "status": null,
            "limit": 50,
        }));

    let json = serde_json::to_value(&envelope)?;
    assert!(
        json.get("query_echo").is_some(),
        "query_echo should be present"
    );
    assert_eq!(json["query_echo"]["kind"], "replay");
    assert_eq!(json["query_echo"]["limit"], 50);
    Ok(())
}

#[sinex_test]
async fn schema_version_constants_have_expected_format() -> TestResult<()> {
    assert!(VIEW_ENVELOPE_SCHEMA_VERSION.starts_with("sinex.view-envelope/v"));
    assert!(OPERATION_JOB_LIST_SCHEMA_VERSION.starts_with("sinex.operation-job-list/v"));
    assert!(OPERATION_VIEW_SCHEMA_VERSION.starts_with("sinex.operation-view/v"));
    Ok(())
}
