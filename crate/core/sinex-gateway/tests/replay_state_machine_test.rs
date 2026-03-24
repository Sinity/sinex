use sinex_gateway::{ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState};
use sinex_primitives::domain::ReplayOutcome;
use sinex_primitives::{Uuid, temporal::Timestamp};
use std::collections::HashMap;
use time::{Date, Month, Time};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

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
        last_event_id: Some(Uuid::now_v7()),
        batch_number: 42,
        savepoint_id: Some("sp_12345".to_string()),
        updated_at: sinex_primitives::temporal::now(),
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
        updated_at: sinex_primitives::temporal::now(),
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
        node_id: "test-node".to_string(),
        time_window: Some((
            Timestamp::new(time::OffsetDateTime::new_utc(
                Date::from_calendar_date(2024, Month::January, 1).unwrap(),
                Time::MIDNIGHT,
            )),
            Timestamp::new(time::OffsetDateTime::new_utc(
                Date::from_calendar_date(2024, Month::December, 31).unwrap(),
                Time::from_hms(23, 59, 59).unwrap(),
            )),
        )),
        material_filter: Some(vec![Uuid::now_v7(), Uuid::now_v7()]),
        filters,
    };

    let json = serde_json::to_string(&scope)?;
    let deserialized: ReplayScope = serde_json::from_str(&json)?;

    assert_eq!(scope.node_id, deserialized.node_id);
    assert_eq!(scope.time_window, deserialized.time_window);
    assert_eq!(
        scope.material_filter.as_ref().map(std::vec::Vec::len),
        deserialized
            .material_filter
            .as_ref()
            .map(std::vec::Vec::len),
    );
    assert_eq!(scope.filters.len(), deserialized.filters.len());

    Ok(())
}

#[sinex_test]
async fn scope_normalized_filters_drop_empty_and_dedupe() -> Result<()> {
    let scope = ReplayScope {
        node_id: "test-node".to_string(),
        time_window: None,
        material_filter: Some(vec![
            Uuid::parse_str("018f3dd0-6ab6-7dd9-a0f2-2c6f99b67ed7")?,
            Uuid::parse_str("018f3dd0-6ab6-7dd9-a0f2-2c6f99b67ed7")?,
            Uuid::parse_str("018f3dd0-6ab6-7dd9-a0f2-2c6f99b67ed8")?,
        ]),
        filters: HashMap::from([(
            "event_types".to_string(),
            serde_json::json!(["file.created", "", "file.created", "file.modified", 7]),
        )]),
    };

    let normalized = scope.normalized_filters();

    assert_eq!(normalized.material_ids.as_ref().map(Vec::len), Some(2));
    assert_eq!(normalized.event_types.as_ref().map(Vec::len), Some(2));
    assert_eq!(
        normalized.event_types.clone().unwrap_or_default(),
        vec!["file.created".to_string(), "file.modified".to_string()]
    );

    Ok(())
}

#[sinex_test]
async fn operations_default_to_planning() -> Result<()> {
    let scope = ReplayScope {
        node_id: "test-node".to_string(),
        time_window: None,
        material_filter: Some(vec![Uuid::now_v7()]),
        filters: HashMap::new(),
    };

    let operation = ReplayOperation {
        operation_id: Uuid::now_v7(),
        state: ReplayState::Planning,
        scope: scope.clone(),
        preview_summary: None,
        checkpoint: ReplayCheckpoint::default(),
        actor: "test-actor".to_string(),
        created_at: sinex_primitives::temporal::now(),
        approved_by: None,
        approved_at: None,
        executor_node: None,
        started_at: None,
        finished_at: None,
        outcome: None,
        error_details: None,
    };

    assert_eq!(operation.state, ReplayState::Planning);
    assert_eq!(operation.scope.node_id, scope.node_id);
    assert!(operation.approved_by.is_none());
    assert!(operation.finished_at.is_none());

    Ok(())
}

#[sinex_test]
async fn preview_updates_do_not_regress_approved_state(ctx: TestContext) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "test-node".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    };

    let operation = replay
        .create_operation(scope, "test:planner".to_string())
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 2 }),
        )
        .await?;

    let updated = replay.load_operation(operation.operation_id).await?;
    assert_eq!(updated.state, ReplayState::Approved);
    assert_eq!(updated.approved_by.as_deref(), Some("admin:approver"));
    assert_eq!(
        updated
            .preview_summary
            .as_ref()
            .and_then(|preview| preview.get("total_events"))
            .and_then(serde_json::Value::as_i64),
        Some(2)
    );

    Ok(())
}

#[sinex_test]
async fn mark_failed_requires_valid_transition_path(ctx: TestContext) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "test-node".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    };

    let operation = replay
        .create_operation(scope, "test:planner".to_string())
        .await?;
    let invalid_err = replay
        .mark_failed(operation.operation_id, "pre-execution error".to_string())
        .await
        .expect_err("mark_failed should reject non-executing/non-committing states");
    assert!(
        invalid_err
            .to_string()
            .contains("cannot transition to Failed"),
        "unexpected error: {invalid_err}"
    );

    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;
    replay
        .transition(operation.operation_id, ReplayState::Executing)
        .await?;
    replay
        .mark_failed(operation.operation_id, "execution failed".to_string())
        .await?;

    let failed = replay.load_operation(operation.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(failed.outcome, Some(ReplayOutcome::Failed));
    assert_eq!(failed.error_details.as_deref(), Some("execution failed"));

    Ok(())
}

#[sinex_test]
async fn cancel_enforces_state_transition_rules(ctx: TestContext) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "test-node".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    };

    let operation = replay
        .create_operation(scope, "test:planner".to_string())
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;
    replay
        .transition(operation.operation_id, ReplayState::Executing)
        .await?;
    replay
        .transition(operation.operation_id, ReplayState::Committing)
        .await?;

    let invalid_err = replay
        .cancel(operation.operation_id, "too late".to_string())
        .await
        .expect_err("cancel should reject Committing state");
    assert!(
        invalid_err
            .to_string()
            .contains("cannot transition to Cancelled"),
        "unexpected error: {invalid_err}"
    );

    replay
        .transition(operation.operation_id, ReplayState::Completed)
        .await?;
    replay
        .cancel(
            operation.operation_id,
            "ignored terminal cancel".to_string(),
        )
        .await?;
    let completed = replay.load_operation(operation.operation_id).await?;
    assert_eq!(completed.state, ReplayState::Completed);
    assert_eq!(completed.outcome, Some(ReplayOutcome::Success));

    Ok(())
}

#[sinex_test]
async fn execution_lock_is_released_when_guard_drops(ctx: TestContext) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "test-node".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    };

    let operation = replay
        .create_operation(scope, "test:planner".to_string())
        .await?;

    let first_guard = replay
        .acquire_execution_lock(operation.operation_id)
        .await?;
    assert!(first_guard.is_some());
    let locked = replay.load_operation(operation.operation_id).await?;
    assert!(
        locked.executor_node.is_none(),
        "taking the advisory lock must not fabricate executor metadata"
    );

    let second_guard = replay
        .acquire_execution_lock(operation.operation_id)
        .await?;
    assert!(
        second_guard.is_none(),
        "lock should be exclusive while held"
    );

    drop(first_guard);

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let operation_id = operation.operation_id;
            async move {
                let replay = sinex_gateway::ReplayStateMachine::new(pool);
                let guard = replay.acquire_execution_lock(operation_id).await?;
                Ok::<bool, sinex_primitives::SinexError>(guard.is_some())
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn recover_stale_executing_clears_executor_node(ctx: TestContext) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "test-node".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    };

    let operation = replay
        .create_operation(scope, "test:planner".to_string())
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;
    replay
        .transition(operation.operation_id, ReplayState::Executing)
        .await?;

    let lock_guard = replay
        .acquire_execution_lock(operation.operation_id)
        .await?;
    assert!(lock_guard.is_some());
    drop(lock_guard);

    let stale_started_at = sinex_primitives::temporal::now() - time::Duration::hours(2);
    let executor_node = serde_json::to_value("node-a")?;
    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET preview_summary = jsonb_set(
                jsonb_set(
                    preview_summary,
                    '{started_at}',
                    to_jsonb($2::timestamptz),
                    true
                ),
                '{executor_node}',
                $3::jsonb,
                true
            )
        WHERE id = $1::uuid
        "#,
        operation.operation_id,
        *stale_started_at,
        executor_node
    )
    .execute(&ctx.pool)
    .await?;

    let recovered = replay
        .recover_stale_executing(std::time::Duration::from_secs(1))
        .await?;
    assert_eq!(recovered, 1);

    let failed = replay.load_operation(operation.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(failed.outcome, Some(ReplayOutcome::Failed));
    assert!(failed.executor_node.is_none());
    assert!(
        failed
            .error_details
            .as_deref()
            .is_some_and(|details| details.contains("stale executing state"))
    );

    Ok(())
}

#[sinex_test]
async fn recover_stale_committing_clears_executor_node(ctx: TestContext) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "test-node".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    };

    let operation = replay
        .create_operation(scope, "test:planner".to_string())
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;
    replay
        .transition(operation.operation_id, ReplayState::Executing)
        .await?;
    replay
        .set_executor_node(
            operation.operation_id,
            sinex_primitives::domain::NodeName::new("node-a"),
        )
        .await?;
    replay
        .transition(operation.operation_id, ReplayState::Committing)
        .await?;

    let stale_started_at = sinex_primitives::temporal::now() - time::Duration::hours(2);
    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET preview_summary = jsonb_set(
                preview_summary,
                '{started_at}',
                to_jsonb($2::timestamptz),
                true
            )
        WHERE id = $1::uuid
        "#,
        operation.operation_id,
        *stale_started_at,
    )
    .execute(&ctx.pool)
    .await?;

    let recovered = replay
        .recover_stale_executing(std::time::Duration::from_secs(1))
        .await?;
    assert_eq!(recovered, 1);

    let failed = replay.load_operation(operation.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(failed.outcome, Some(ReplayOutcome::Failed));
    assert!(failed.executor_node.is_none());
    assert!(
        failed
            .error_details
            .as_deref()
            .is_some_and(|details| details.contains("stale committing state"))
    );

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

        assert_eq!(format!("{state:?}"), format!("{:?}", deserialized));
    }

    Ok(())
}
