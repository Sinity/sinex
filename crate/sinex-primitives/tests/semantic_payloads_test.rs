use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    SemanticLaneDiffRecordedPayload, SemanticLaneOutputsDiscardedPayload,
    SemanticLaneStatusChangedPayload,
};
use sinex_primitives::{
    EntityRelationLaneOutputs, SemanticLaneStatus, Timestamp, Uuid, diff_entity_relation_lanes,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn semantic_payloads_publish_stable_event_names() -> TestResult<()> {
    assert_eq!(
        SemanticLaneStatusChangedPayload::SOURCE.as_str(),
        "semantics"
    );
    assert_eq!(
        SemanticLaneStatusChangedPayload::EVENT_TYPE.as_str(),
        "semantic.lane_status_changed"
    );
    assert_eq!(
        SemanticLaneDiffRecordedPayload::EVENT_TYPE.as_str(),
        "semantic.lane_diff_recorded"
    );
    assert_eq!(
        SemanticLaneOutputsDiscardedPayload::EVENT_TYPE.as_str(),
        "semantic.lane_outputs_discarded"
    );
    Ok(())
}

#[sinex_test]
async fn semantic_status_payload_records_explicit_discard() -> TestResult<()> {
    let payload = SemanticLaneStatusChangedPayload::test_discarded(Uuid::from_u128(1));

    assert_eq!(payload.previous_status, SemanticLaneStatus::Compared);
    assert_eq!(payload.new_status, SemanticLaneStatus::Discarded);
    assert!(payload.operation_id.is_some());
    assert!(payload.reason.contains("churn"));
    Ok(())
}

#[sinex_test]
async fn semantic_diff_payload_preserves_machine_report() -> TestResult<()> {
    let report = diff_entity_relation_lanes(
        Uuid::from_u128(2),
        Uuid::from_u128(3),
        "scope-hash",
        &EntityRelationLaneOutputs::default(),
        &EntityRelationLaneOutputs::default(),
        5,
    );
    let payload = SemanticLaneDiffRecordedPayload {
        diff_id: Uuid::from_u128(4),
        baseline_lane_id: Uuid::from_u128(5),
        candidate_lane_id: Uuid::from_u128(6),
        diff_kind: "entity_relation".to_string(),
        report,
        report_hash: "hash".to_string(),
        operation_id: Some(Uuid::from_u128(7)),
        created_at: Timestamp::UNIX_EPOCH,
    };

    assert_eq!(payload.diff_kind, "entity_relation");
    assert_eq!(payload.report.input_set_hash, "scope-hash");
    assert_eq!(payload.report.counts.entity_new, 0);
    Ok(())
}
