use sinex_gateway::{ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState};
use sinex_primitives::domain::ReplayOutcome;
use sinex_primitives::{Uuid, temporal::Timestamp};
use std::collections::HashMap;
use time::{Date, Month, Time};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn state_transitions_follow_rules() -> Result<()> {
    assert!(ReplayState::Planning.can_transition_to(ReplayState::Previewed));
    assert!(ReplayState::Previewed.can_transition_to(ReplayState::Approved));
    assert!(ReplayState::Approved.can_transition_to(ReplayState::Executing));
    assert!(ReplayState::Approved.can_transition_to(ReplayState::Failed));
    assert!(ReplayState::Executing.can_transition_to(ReplayState::Cancelling));
    assert!(ReplayState::Executing.can_transition_to(ReplayState::Committing));
    assert!(ReplayState::Cancelling.can_transition_to(ReplayState::Cancelled));
    assert!(ReplayState::Committing.can_transition_to(ReplayState::Completed));

    assert!(!ReplayState::Planning.can_transition_to(ReplayState::Executing));
    assert!(!ReplayState::Completed.can_transition_to(ReplayState::Planning));
    assert!(!ReplayState::Previewed.can_transition_to(ReplayState::Completed));

    assert!(ReplayState::Completed.is_terminal());
    assert!(ReplayState::Failed.is_terminal());
    assert!(ReplayState::Cancelled.is_terminal());
    assert!(!ReplayState::Cancelling.is_terminal());
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
async fn create_operation_persists_loadable_metadata_atomically(ctx: TestContext) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "atomic-create-node".to_string(),
        time_window: None,
        material_filter: Some(vec![Uuid::now_v7()]),
        filters: HashMap::from([(
            "event_types".to_string(),
            serde_json::json!(["file.created"]),
        )]),
    };

    let created = replay
        .create_operation(scope.clone(), "test:planner".to_string())
        .await?;
    let loaded = replay.load_operation(created.operation_id).await?;

    assert_eq!(loaded.state, ReplayState::Planning);
    assert_eq!(loaded.scope.node_id, scope.node_id);
    assert_eq!(loaded.scope.time_window, scope.time_window);
    assert_eq!(loaded.scope.material_filter, scope.material_filter);
    assert_eq!(loaded.scope.filters, scope.filters);
    assert_eq!(loaded.actor, "test:planner");
    assert_eq!(loaded.checkpoint.processed_events, 0);
    assert!(loaded.preview_summary.is_none());
    assert!(loaded.approved_by.is_none());
    assert!(loaded.executor_node.is_none());

    let persisted = sqlx::query!(
        r#"
        SELECT result_status, result_message, preview_summary
        FROM core.operations_log
        WHERE id = $1::uuid
        "#,
        created.operation_id
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(persisted.result_status, "running");
    assert_eq!(persisted.result_message.as_deref(), Some("planning"));
    assert!(persisted.preview_summary.is_some());

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
async fn submit_previewed_operation_sets_execution_metadata_atomically(
    ctx: TestContext,
) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "submit-node".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    };

    let operation = replay
        .create_operation(scope, "test:planner".to_string())
        .await?;
    let preview_start = Timestamp::now() - time::Duration::minutes(5);
    let preview_end = preview_start + time::Duration::minutes(1);
    let root_event_id = Uuid::now_v7();
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({
                "total_events": 1,
                "time_window": {
                    "start": preview_start.format_rfc3339(),
                    "end": preview_end.format_rfc3339(),
                },
                "root_event_ids": [root_event_id],
            }),
        )
        .await?;

    let submitted = replay
        .submit_previewed_for_execution(
            operation.operation_id,
            "admin:submitter".to_string(),
            sinex_primitives::domain::NodeName::new("gateway-node"),
        )
        .await?;

    assert_eq!(submitted.state, ReplayState::Executing);
    assert_eq!(submitted.approved_by.as_deref(), Some("admin:submitter"));
    assert!(submitted.approved_at.is_some());
    assert!(submitted.started_at.is_some());
    assert_eq!(submitted.executor_node.as_deref(), Some("gateway-node"));
    assert_eq!(submitted.outcome, None);
    assert_eq!(submitted.error_details, None);

    let loaded = replay.load_operation(operation.operation_id).await?;
    assert_eq!(loaded.state, ReplayState::Executing);
    assert_eq!(loaded.approved_by.as_deref(), Some("admin:submitter"));
    assert_eq!(loaded.executor_node.as_deref(), Some("gateway-node"));
    assert_eq!(loaded.approved_at, submitted.approved_at);
    assert_eq!(loaded.started_at, submitted.started_at);

    Ok(())
}

#[sinex_test]
async fn begin_execution_sets_execution_metadata_atomically(ctx: TestContext) -> Result<()> {
    let replay = sinex_gateway::ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        node_id: "execute-node".to_string(),
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
            serde_json::json!({
                "total_events": 1,
                "time_window": {
                    "start": (Timestamp::now() - time::Duration::minutes(1)).format_rfc3339(),
                    "end": Timestamp::now().format_rfc3339(),
                },
                "root_event_ids": [Uuid::now_v7()],
            }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;

    replay
        .begin_execution(
            operation.operation_id,
            sinex_primitives::domain::NodeName::new("gateway-node"),
        )
        .await?;

    let loaded = replay.load_operation(operation.operation_id).await?;
    assert_eq!(loaded.state, ReplayState::Executing);
    assert!(loaded.started_at.is_some());
    assert_eq!(loaded.executor_node.as_deref(), Some("gateway-node"));
    assert_eq!(loaded.outcome, None);
    assert_eq!(loaded.error_details, None);

    Ok(())
}

#[sinex_test]
async fn mark_failed_persists_pre_execution_and_execution_failures(ctx: TestContext) -> Result<()> {
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
        .expect_err("planning operations should not be directly markable as failed");
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
        .mark_failed(operation.operation_id, "pre-execution error".to_string())
        .await?;

    let failed_before_execution = replay.load_operation(operation.operation_id).await?;
    assert_eq!(failed_before_execution.state, ReplayState::Failed);
    assert_eq!(failed_before_execution.outcome, Some(ReplayOutcome::Failed));
    assert_eq!(
        failed_before_execution.error_details.as_deref(),
        Some("pre-execution error")
    );
    assert!(failed_before_execution.executor_node.is_none());

    let second_operation = replay
        .create_operation(
            ReplayScope {
                node_id: "test-node-2".to_string(),
                time_window: None,
                material_filter: None,
                filters: HashMap::new(),
            },
            "test:planner".to_string(),
        )
        .await?;
    replay
        .update_preview(
            second_operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;
    replay
        .approve(second_operation.operation_id, "admin:approver".to_string())
        .await?;
    replay
        .transition(second_operation.operation_id, ReplayState::Executing)
        .await?;
    replay
        .mark_failed(
            second_operation.operation_id,
            "execution failed".to_string(),
        )
        .await?;

    let failed = replay.load_operation(second_operation.operation_id).await?;
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
async fn cancel_marks_executing_operation_as_cancelling_until_finalized(
    ctx: TestContext,
) -> Result<()> {
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
        .cancel(
            operation.operation_id,
            "operator requested stop".to_string(),
        )
        .await?;

    let cancelling = replay.load_operation(operation.operation_id).await?;
    assert_eq!(cancelling.state, ReplayState::Cancelling);
    assert_eq!(
        cancelling.error_details.as_deref(),
        Some("operator requested stop")
    );
    assert!(cancelling.outcome.is_none());
    assert!(cancelling.finished_at.is_none());

    replay.finish_cancellation(operation.operation_id).await?;

    let cancelled = replay.load_operation(operation.operation_id).await?;
    assert_eq!(cancelled.state, ReplayState::Cancelled);
    assert_eq!(cancelled.outcome, Some(ReplayOutcome::Cancelled));
    assert_eq!(
        cancelled.error_details.as_deref(),
        Some("operator requested stop")
    );
    assert!(cancelled.finished_at.is_some());

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

    first_guard
        .expect("first execution lock guard should be present")
        .cleanup_now()
        .await;
    let third_guard = replay
        .acquire_execution_lock(operation.operation_id)
        .await?;
    assert!(
        third_guard.is_some(),
        "execution lock should be reacquirable immediately after awaited cleanup"
    );
    if let Some(third_guard) = third_guard {
        third_guard.cleanup_now().await;
    }

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
    if let Some(lock_guard) = lock_guard {
        lock_guard.cleanup_now().await;
    }

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
        ReplayState::Cancelling,
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
            ReplayState::Cancelling => assert_eq!(json, "\"Cancelling\""),
            ReplayState::Committing => assert_eq!(json, "\"Committing\""),
            ReplayState::Completed => assert_eq!(json, "\"Completed\""),
            ReplayState::Failed => assert_eq!(json, "\"Failed\""),
            ReplayState::Cancelled => assert_eq!(json, "\"Cancelled\""),
        }

        assert_eq!(format!("{state:?}"), format!("{:?}", deserialized));
    }

    Ok(())
}
