use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn lifecycle_audit_summary_samples_large_event_id_sets() -> TestResult<()> {
    let event_ids = (0..(EVENT_ID_AUDIT_SAMPLE_LIMIT + 2))
        .map(|_| Uuid::now_v7())
        .collect::<Vec<_>>();

    let summary = lifecycle_audit_summary(&event_ids, 2, event_ids.len(), event_ids.len(), true);

    assert_eq!(
        summary["affected_event_ids"].as_array().map(Vec::len),
        Some(EVENT_ID_AUDIT_SAMPLE_LIMIT)
    );
    assert_eq!(
        summary["affected_event_ids_count"].as_u64(),
        Some(event_ids.len() as u64)
    );
    assert_eq!(
        summary["affected_event_ids_sample_limit"].as_u64(),
        Some(EVENT_ID_AUDIT_SAMPLE_LIMIT as u64)
    );
    assert_eq!(summary["affected_event_ids_truncated"], true);
    Ok(())
}

#[sinex_test]
async fn parse_duration_to_timestamp_preserves_subsecond_precision() -> TestResult<()> {
    let before = Timestamp::now();
    let parsed = parse_duration_to_timestamp("500ms")
        .expect("500ms should parse")
        .expect("duration parsing should return a timestamp");
    let delta_ms = (before - parsed).whole_milliseconds();

    assert!(
        (400..1000).contains(&delta_ms),
        "expected roughly 500ms delta, got {delta_ms}ms"
    );
    Ok(())
}

#[sinex_test]
async fn tombstone_duration_ms_clamps_large_elapsed_values() -> TestResult<()> {
    let operation = TombstoneOperation {
        operation_id: "op-test".to_string(),
        phase: TombstoneOperationPhase::Executing,
        state: TombstoneOperationState::Executing,
        before: None,
        source: None,
        event_ids: None,
        limit: 1,
        reason: "test".to_string(),
        cascade_analysis: None,
        created_by: "tester".to_string(),
        created_at: "1900-01-01T00:00:00Z".to_string(),
        expires_at: "1900-01-01T01:00:00Z".to_string(),
        approved_by: None,
        approved_at: None,
        started_at: None,
        finished_at: None,
        tombstoned_count: None,
        error_details: None,
    };

    let duration_ms = tombstone_duration_ms(&operation, Timestamp::now())
        .expect("old timestamps should still produce a bounded duration");

    assert_eq!(duration_ms, Some(i32::MAX));
    Ok(())
}

#[sinex_test]
async fn matches_requested_tombstone_state_uses_reconciled_state() -> TestResult<()> {
    let operation = TombstoneOperation {
        operation_id: "op-test".to_string(),
        phase: TombstoneOperationPhase::Expired,
        state: TombstoneOperationState::Expired,
        before: None,
        source: None,
        event_ids: None,
        limit: 1,
        reason: "test".to_string(),
        cascade_analysis: None,
        created_by: "tester".to_string(),
        created_at: "1900-01-01T00:00:00Z".to_string(),
        expires_at: "1900-01-01T01:00:00Z".to_string(),
        approved_by: None,
        approved_at: None,
        started_at: None,
        finished_at: None,
        tombstoned_count: None,
        error_details: Some("Expired before approval".to_string()),
    };

    assert!(matches_requested_tombstone_state(None, &operation));
    assert!(!matches_requested_tombstone_state(
        Some(TombstoneOperationState::Previewed),
        &operation
    ));
    assert!(matches_requested_tombstone_state(
        Some(TombstoneOperationState::Expired),
        &operation
    ));
    Ok(())
}
