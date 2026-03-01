use serde_json::Value;
use sinex_primitives::Id;
use sinex_primitives::SinexError;
use sinex_primitives::domain::OperationStatus;
use sinex_db::DbPoolExt;
use sqlx::PgPool;

// Re-export shared types
pub use sinex_primitives::rpc::ops::{
    Operation, OpsCancelRequest, OpsCancelResponse, OpsGetRequest, OpsGetResponse, OpsListRequest,
    OpsListResponse, OpsStartRequest, OpsStartResponse,
};

type Result<T> = std::result::Result<T, SinexError>;

fn default_ops_limit() -> i64 {
    100
}

/// Convert a repository OperationRecord to the RPC Operation type.
fn record_to_operation(record: sinex_db::repositories::OperationRecord) -> Operation {
    Operation {
        id: record.id.to_string(),
        operation_type: record.operation_type,
        operator: record.operator,
        scope: record.scope,
        result_status: record.result_status,
        result_message: record.result_message,
        preview_summary: record.preview_summary,
        duration_ms: record.duration_ms,
    }
}

/// Handle POST /ops/start - start a new operation
///
/// # Authorization
///
/// Write operations are logged for audit purposes.
pub async fn handle_ops_start(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;

    let request: OpsStartRequest = serde_json::from_value(params)?;
    let scope_jsonb = request.scope.unwrap_or(serde_json::json!({}));

    let record = pool
        .state()
        .start_operation(&request.operation_type, &request.operator, scope_jsonb)
        .await?;

    info!(
        token_prefix = %auth.token_prefix,
        operation_id = %record.id,
        operation_type = %request.operation_type,
        operator = %request.operator,
        "Operation started"
    );

    let response = OpsStartResponse {
        operation: record_to_operation(record),
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle GET /ops - list operations with optional filtering
///
/// # Authorization
///
/// Read-only operation. Auth context accepted for audit trail consistency.
pub async fn handle_ops_list(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::debug;

    debug!(token_prefix = %auth.token_prefix, "Operations list requested");

    let request: OpsListRequest = serde_json::from_value(params).unwrap_or_default();

    let limit = if request.limit > 0 {
        request.limit
    } else {
        default_ops_limit()
    };

    let records = pool
        .state()
        .list_operations(request.operation_type.as_deref(), request.status, limit)
        .await?;

    let response = OpsListResponse {
        operations: records.into_iter().map(record_to_operation).collect(),
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle GET /ops/{id} - get operation details
///
/// # Authorization
///
/// Read-only operation. Auth context accepted for audit trail consistency.
pub async fn handle_ops_get(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::debug;

    let request: OpsGetRequest = serde_json::from_value(params)?;

    debug!(
        token_prefix = %auth.token_prefix,
        operation_id = %request.operation_id,
        "Operation get requested"
    );

    let operation_id = request
        .operation_id
        .parse::<Id<Operation>>()
        .map_err(|e| SinexError::parse(format!("Invalid operation ID: {e}")))?;

    let record = pool
        .state()
        .get_operation(&operation_id)
        .await?
        .ok_or_else(|| SinexError::not_found(format!("Operation not found: {operation_id}")))?;

    let response = OpsGetResponse {
        operation: record_to_operation(record),
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle POST /ops/{id}/cancel - cancel a running operation
///
/// # Authorization
///
/// This is a dangerous operation that cancels a running system operation.
/// The auth context is logged for audit purposes.
pub async fn handle_ops_cancel(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;

    let request: OpsCancelRequest = serde_json::from_value(params)?;

    let operation_id = request
        .operation_id
        .parse::<Id<Operation>>()
        .map_err(|e| SinexError::parse(format!("Invalid operation ID: {e}")))?;

    info!(
        token_prefix = %auth.token_prefix,
        operation_id = %operation_id,
        "Operation cancel initiated"
    );

    let reason = request
        .reason
        .unwrap_or_else(|| "Cancelled by user".to_string());

    let record = pool
        .state()
        .cancel_operation(&operation_id, &reason)
        .await?;

    let response = OpsCancelResponse {
        operation: record_to_operation(record),
        cancelled: true,
    };

    Ok(serde_json::to_value(response)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn ops_start_creates_operation(ctx: &TestContext) -> TestResult<()> {
        let auth = crate::rpc_server::RpcAuthContext::system();
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
    async fn ops_list_returns_operations(ctx: &TestContext) -> TestResult<()> {
        let auth = crate::rpc_server::RpcAuthContext::system();

        // Create a test operation first
        let start_params = json!({
            "operation_type": "test-op",
            "operator": "tester",
        });
        handle_ops_start(ctx.pool(), start_params, &auth).await?;

        // List all operations
        let result = handle_ops_list(ctx.pool(), json!({}), &auth).await?;
        let response: OpsListResponse = serde_json::from_value(result)?;

        assert!(!response.operations.is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn ops_get_returns_operation(ctx: &TestContext) -> TestResult<()> {
        let auth = crate::rpc_server::RpcAuthContext::system();

        // Create a test operation
        let start_result = handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "test-get",
                "operator": "tester",
            }),
            &auth,
        )
        .await?;

        let start_response: OpsStartResponse = serde_json::from_value(start_result)?;
        let operation_id = &start_response.operation.id;

        // Get the operation
        let result =
            handle_ops_get(ctx.pool(), json!({ "operation_id": operation_id }), &auth).await?;
        let response: OpsGetResponse = serde_json::from_value(result)?;

        assert_eq!(response.operation.id, *operation_id);

        Ok(())
    }

    #[sinex_test]
    async fn ops_cancel_stops_running_operation(ctx: &TestContext) -> TestResult<()> {
        let auth = crate::rpc_server::RpcAuthContext::system();

        // Create a running operation
        let start_result = handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "test-cancel",
                "operator": "tester",
            }),
            &auth,
        )
        .await?;

        let start_response: OpsStartResponse = serde_json::from_value(start_result)?;
        let operation_id = &start_response.operation.id;

        // Cancel it
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
    async fn ops_cancel_rejects_non_running_operation(ctx: &TestContext) -> TestResult<()> {
        let auth = crate::rpc_server::RpcAuthContext::system();

        // Create and immediately cancel an operation
        let start_result = handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "test-double-cancel",
                "operator": "tester",
            }),
            &auth,
        )
        .await?;

        let start_response: OpsStartResponse = serde_json::from_value(start_result)?;
        let operation_id = &start_response.operation.id;

        // Cancel once
        handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": operation_id,
            }),
            &auth,
        )
        .await?;

        // Try to cancel again - should fail
        let err = handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": operation_id,
            }),
            &auth,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("cannot be cancelled"));

        Ok(())
    }
}
