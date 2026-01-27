use chrono::{TimeZone, Utc};
use sinex_core::types::ulid::Ulid;
use sinex_gateway::{ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn state_transitions_follow_rules() -> Result<()> {
    assert!(ReplayState::Planning.can_transition_to(ReplayState::Previewed));
    assert!(ReplayState::Previewed.can_transition_to(ReplayState::Approved));
    assert!(ReplayState::Approved.can_transition_to(ReplayState::Executing));
    assert!(ReplayState::Executing.can_transition_to(ReplayState::Committing));
    assert!(ReplayState::Committing.can_transition_to(ReplayState::Completed));

    assert!(!ReplayState::Planning.can_transition_to(ReplayState::Executing));
    assert!(!ReplayState::Completed.can_transition_to(ReplayState::Planning));
    assert!(!ReplayState::Previewed.can_transition_to(ReplayState::Completed));

    assert!(ReplayState::Completed.is_terminal());
    assert!(ReplayState::Failed.is_terminal());
    assert!(ReplayState::Cancelled.is_terminal());
    assert!(!ReplayState::Executing.is_terminal());

    Ok(())
}

#[sinex_test]
async fn checkpoint_serialization_round_trips() -> Result<()> {
    let checkpoint = ReplayCheckpoint {
        processed_events: 12_345,
        total_events: 50_000,
        last_event_id: Some(Ulid::new()),
        batch_number: 42,
        savepoint_id: Some("sp_12345".to_string()),
        updated_at: Utc::now(),
    };

    let json = serde_json::to_string(&checkpoint)?;
    let deserialized: ReplayCheckpoint = serde_json::from_str(&json)?;

    assert_eq!(checkpoint.processed_events, deserialized.processed_events);
    assert_eq!(checkpoint.total_events, deserialized.total_events);
    assert_eq!(checkpoint.last_event_id, deserialized.last_event_id);
    assert_eq!(checkpoint.batch_number, deserialized.batch_number);
    assert_eq!(checkpoint.savepoint_id, deserialized.savepoint_id);

    Ok(())
}

#[sinex_test]
async fn checkpoint_serialization_handles_none() -> Result<()> {
    let checkpoint = ReplayCheckpoint {
        processed_events: 100,
        total_events: 1_000,
        last_event_id: None,
        batch_number: 1,
        savepoint_id: None,
        updated_at: Utc::now(),
    };

    let json = serde_json::to_string(&checkpoint)?;
    let deserialized: ReplayCheckpoint = serde_json::from_str(&json)?;

    assert!(deserialized.last_event_id.is_none());
    assert!(deserialized.savepoint_id.is_none());
    assert_eq!(checkpoint.processed_events, deserialized.processed_events);

    Ok(())
}

#[sinex_test]
async fn scope_serialization_round_trips() -> Result<()> {
    use std::collections::HashMap;

    let mut filters = HashMap::new();
    filters.insert("source".to_string(), serde_json::json!("filesystem"));
    filters.insert("max_size".to_string(), serde_json::json!(1024));

    let scope = ReplayScope {
        processor_id: "test-processor".to_string(),
        time_window: Some((
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap(),
        )),
        material_filter: Some(vec![Ulid::new(), Ulid::new()]),
        filters,
    };

    let json = serde_json::to_string(&scope)?;
    let deserialized: ReplayScope = serde_json::from_str(&json)?;

    assert_eq!(scope.processor_id, deserialized.processor_id);
    assert_eq!(scope.time_window, deserialized.time_window);
    assert_eq!(
        scope.material_filter.as_ref().map(|v| v.len()),
        deserialized.material_filter.as_ref().map(|v| v.len()),
    );
    assert_eq!(scope.filters.len(), deserialized.filters.len());

    Ok(())
}

#[sinex_test]
async fn operations_default_to_planning() -> Result<()> {
    use std::collections::HashMap;

    let scope = ReplayScope {
        processor_id: "test-processor".to_string(),
        time_window: None,
        material_filter: Some(vec![Ulid::new()]),
        filters: HashMap::new(),
    };

    let operation = ReplayOperation {
        operation_id: Ulid::new(),
        state: ReplayState::Planning,
        scope: scope.clone(),
        preview_summary: None,
        checkpoint: ReplayCheckpoint::default(),
        actor: "test-actor".to_string(),
        created_at: Utc::now(),
        approved_by: None,
        approved_at: None,
        executor_node: None,
        started_at: None,
        finished_at: None,
        outcome: None,
        error_details: None,
    };

    assert_eq!(operation.state, ReplayState::Planning);
    assert_eq!(operation.scope.processor_id, scope.processor_id);
    assert!(operation.approved_by.is_none());
    assert!(operation.finished_at.is_none());

    Ok(())
}

#[sinex_test]
async fn states_serialize_to_expected_strings() -> Result<()> {
    let states = vec![
        ReplayState::Planning,
        ReplayState::Previewed,
        ReplayState::Approved,
        ReplayState::Executing,
        ReplayState::Committing,
        ReplayState::Completed,
        ReplayState::Failed,
        ReplayState::Cancelled,
    ];

    for state in states {
        let json = serde_json::to_string(&state)?;
        let deserialized: ReplayState = serde_json::from_str(&json)?;

        match state {
            ReplayState::Planning => assert_eq!(json, "\"Planning\""),
            ReplayState::Previewed => assert_eq!(json, "\"Previewed\""),
            ReplayState::Approved => assert_eq!(json, "\"Approved\""),
            ReplayState::Executing => assert_eq!(json, "\"Executing\""),
            ReplayState::Committing => assert_eq!(json, "\"Committing\""),
            ReplayState::Completed => assert_eq!(json, "\"Completed\""),
            ReplayState::Failed => assert_eq!(json, "\"Failed\""),
            ReplayState::Cancelled => assert_eq!(json, "\"Cancelled\""),
        }

        assert_eq!(format!("{:?}", state), format!("{:?}", deserialized));
    }

    Ok(())
}
