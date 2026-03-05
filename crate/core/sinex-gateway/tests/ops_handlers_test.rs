use serde_json::json;
use sinex_gateway::handlers::{
    handle_ops_cancel, handle_ops_get, handle_ops_list, handle_ops_start,
};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::rpc::ops::{
    OpsCancelResponse, OpsGetResponse, OpsListResponse, OpsStartResponse,
};
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

#[sinex_test]
async fn ops_start_creates_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let params = json!({
        "operation_type": "test-operation",
        "operator": "test-user",
        "scope": {"key": "value"},
    });

    let result = handle_ops_start(ctx.pool(), params, &auth).await?;
    let response: OpsStartResponse = serde_json::from_value(result)?;

    assert!(!response.operation.id.is_empty());
    assert_eq!(response.operation.operation_type, "test-operation");
    assert_eq!(response.operation.result_status, OperationStatus::Running);

    Ok(())
}

#[sinex_test]
async fn ops_list_returns_operations(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();

    start_test_operation(&ctx, &auth, "test-op", "tester").await?;

    let result = handle_ops_list(ctx.pool(), json!({}), &auth).await?;
    let response: OpsListResponse = serde_json::from_value(result)?;

    assert!(!response.operations.is_empty());

    Ok(())
}

#[sinex_test]
async fn ops_get_returns_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "test-get", "tester").await?;
    let operation_id = &start_response.operation.id;

    let result = handle_ops_get(ctx.pool(), json!({ "operation_id": operation_id }), &auth).await?;
    let response: OpsGetResponse = serde_json::from_value(result)?;

    assert_eq!(response.operation.id, *operation_id);

    Ok(())
}

#[sinex_test]
async fn ops_cancel_stops_running_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "test-cancel", "tester").await?;
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

    Ok(())
}

#[sinex_test]
async fn ops_cancel_rejects_non_running_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "test-double-cancel", "tester").await?;
    let operation_id = &start_response.operation.id;

    handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": operation_id,
        }),
        &auth,
    )
    .await?;

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

    Ok(())
}
