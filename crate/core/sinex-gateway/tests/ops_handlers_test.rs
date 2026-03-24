use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{
    handle_lifecycle_archive, handle_ops_cancel, handle_ops_get, handle_ops_list,
    handle_ops_start, handle_tombstone_create, handle_tombstone_status,
};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::domain::{OperationStatus, ReplayOutcome};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveResponse, TombstoneCreateResponse, TombstoneOperationState,
    TombstoneStatusResponse,
};
use sinex_primitives::rpc::ops::{
    OpsCancelResponse, OpsGetResponse, OpsListResponse, OpsStartResponse,
};
use sinex_gateway::{ReplayScope, ReplayState, ReplayStateMachine};
use std::collections::HashMap;
use xtask::sandbox::prelude::*;

fn system_auth() -> RpcAuthContext {
    RpcAuthContext::system()
}

async fn start_test_operation(
    ctx: &TestContext,
    auth: &RpcAuthContext,
    operation_type: &str,
    operator: &str,
) -> TestResult<OpsStartResponse> {
    let start_result = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": operation_type,
            "operator": operator,
        }),
        auth,
    )
    .await?;
    Ok(serde_json::from_value(start_result)?)
}

async fn get_operation(
    ctx: &TestContext,
    auth: &RpcAuthContext,
    operation_id: &str,
) -> TestResult<OpsGetResponse> {
    let result = handle_ops_get(ctx.pool(), json!({ "operation_id": operation_id }), auth).await?;
    Ok(serde_json::from_value(result)?)
}

async fn publish_event(
    ctx: &TestContext,
    source: &str,
    sequence: i64,
) -> TestResult<sinex_primitives::events::Event<serde_json::Value>> {
    let material_id = ctx.create_source_material(Some(source)).await?;
    Ok(ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(source, "test.ops", json!({ "sequence": sequence }))
                .from_material(material_id)
                .build()?,
        )
        .await?)
}

#[sinex_test]
async fn ops_start_creates_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let params = json!({
        "operation_type": "archive",
        "operator": "test-user",
        "scope": {"key": "value"},
    });

    let result = handle_ops_start(ctx.pool(), params, &auth).await?;
    let response: OpsStartResponse = serde_json::from_value(result)?;

    assert!(!response.operation.id.is_empty());
    assert_eq!(response.operation.operation_type, "archive");
    assert_eq!(response.operation.result_status, OperationStatus::Running);
    assert_eq!(response.operation.operator, "test-user");

    let persisted = get_operation(&ctx, &auth, &response.operation.id).await?;
    assert_eq!(persisted.operation.id, response.operation.id);
    assert_eq!(persisted.operation.operation_type, "archive");
    assert_eq!(persisted.operation.result_status, OperationStatus::Running);
    assert_eq!(persisted.operation.operator, "test-user");

    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_unknown_operation_type(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let err = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "test-operation",
            "operator": "test-user",
        }),
        &auth,
    )
    .await
    .expect_err("unknown operation type should be rejected before hitting the database");

    assert!(err.to_string().contains("Unsupported operation type"));
    Ok(())
}

#[sinex_test]
async fn ops_list_returns_operations(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();

    let started = start_test_operation(&ctx, &auth, "restore", "tester").await?;

    let result = handle_ops_list(ctx.pool(), json!({}), &auth).await?;
    let response: OpsListResponse = serde_json::from_value(result)?;

    assert!(!response.operations.is_empty());
    assert!(
        response
            .operations
            .iter()
            .any(|op| op.id == started.operation.id
                && op.operation_type == "restore"
                && op.result_status == OperationStatus::Running),
        "listed operations should include the started operation with running status"
    );

    Ok(())
}

#[sinex_test]
async fn ops_list_rejects_non_positive_limit(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();

    let err = handle_ops_list(ctx.pool(), json!({ "limit": 0 }), &auth)
        .await
        .expect_err("non-positive limits should be rejected explicitly");

    assert!(err.to_string().contains("limit must be positive"));
    Ok(())
}

#[sinex_test]
async fn ops_get_returns_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "purge", "tester").await?;
    let operation_id = &start_response.operation.id;

    let result = handle_ops_get(ctx.pool(), json!({ "operation_id": operation_id }), &auth).await?;
    let response: OpsGetResponse = serde_json::from_value(result)?;

    assert_eq!(response.operation.id, *operation_id);
    assert_eq!(response.operation.operation_type, "purge");
    assert_eq!(response.operation.operator, "tester");
    assert_eq!(response.operation.result_status, OperationStatus::Running);

    Ok(())
}

#[sinex_test]
async fn ops_cancel_stops_running_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "archive", "tester").await?;
    let operation_id = &start_response.operation.id;

    let result = handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": operation_id,
            "reason": "test cancellation",
        }),
        &auth,
    )
    .await?;

    let response: OpsCancelResponse = serde_json::from_value(result)?;

    assert_eq!(response.operation.result_status, OperationStatus::Cancelled);
    assert_eq!(
        response.operation.result_message,
        Some("test cancellation".to_string())
    );
    assert!(response.cancelled);

    let persisted = get_operation(&ctx, &auth, operation_id).await?;
    assert_eq!(
        persisted.operation.result_status,
        OperationStatus::Cancelled
    );
    assert_eq!(
        persisted.operation.result_message,
        Some("test cancellation".to_string())
    );

    Ok(())
}

#[sinex_test]
async fn ops_cancel_rejects_non_running_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "archive", "tester").await?;
    let operation_id = &start_response.operation.id;

    let first_cancel = handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": operation_id,
        }),
        &auth,
    )
    .await?;
    let first_response: OpsCancelResponse = serde_json::from_value(first_cancel)?;

    let err = handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": operation_id,
        }),
        &auth,
    )
    .await
    .expect_err("second cancel should fail");

    assert!(err.to_string().contains("cannot be cancelled"));

    let persisted = get_operation(&ctx, &auth, operation_id).await?;
    assert_eq!(
        persisted.operation.result_status,
        OperationStatus::Cancelled
    );
    assert!(
        persisted.operation.result_message == first_response.operation.result_message,
        "second cancel should not mutate stored cancellation payload"
    );

    Ok(())
}

#[sinex_test]
async fn ops_cancel_replay_updates_replay_state_machine(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let replay = ReplayStateMachine::new(ctx.pool.clone());
    let operation = replay
        .create_operation(
            ReplayScope {
                node_id: "ops-replay-node".to_string(),
                time_window: None,
                material_filter: None,
                filters: HashMap::new(),
            },
            "test:planner".to_string(),
        )
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;

    let operation_id = operation.operation_id.to_string();
    let response: OpsCancelResponse = serde_json::from_value(
        handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": operation_id,
                "reason": "cancel replay from ops",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(response.operation.result_status, OperationStatus::Cancelled);

    let replay_operation = replay.load_operation(operation.operation_id).await?;
    assert_eq!(replay_operation.state, ReplayState::Cancelled);
    assert_eq!(replay_operation.outcome, Some(ReplayOutcome::Cancelled));
    assert_eq!(
        replay_operation.error_details.as_deref(),
        Some("cancel replay from ops")
    );
    assert!(replay_operation.finished_at.is_some());

    let persisted = get_operation(&ctx, &auth, &response.operation.id).await?;
    assert_eq!(persisted.operation.result_status, OperationStatus::Cancelled);
    assert!(
        persisted.operation.duration_ms.is_some(),
        "terminal replay operations should persist duration_ms"
    );

    Ok(())
}

#[sinex_test]
async fn ops_cancel_tombstone_updates_scope_state(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let source = "test.ops.tombstone";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event.id.expect("published event should have an id").to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare tombstone for ops cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "ops cancel tombstone",
            }),
            &auth,
        )
        .await?,
    )?;
    let tombstone_operation_id = create.operation.operation_id.clone();

    let response: OpsCancelResponse = serde_json::from_value(
        handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": tombstone_operation_id,
                "reason": "cancel tombstone from ops",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(response.operation.result_status, OperationStatus::Cancelled);

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": create.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Cancelled);
    assert!(status.operation.finished_at.is_some());
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Cancelled: cancel tombstone from ops")
    );

    Ok(())
}
