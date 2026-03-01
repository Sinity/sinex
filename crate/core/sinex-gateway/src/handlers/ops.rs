use serde_json::Value;
use sinex_primitives::Id;
use sinex_primitives::SinexError;
use sinex_primitives::domain::OperationStatus;
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

/// Internal DB row type for operations
#[derive(Debug, sqlx::FromRow)]
struct OperationRow {
    id: Id<Operation>,
    operation_type: String,
    operator: String,
    scope: Option<Value>,
    result_status: OperationStatus,
    result_message: Option<String>,
    preview_summary: Option<Value>,
    duration_ms: Option<i32>,
}

impl From<OperationRow> for Operation {
    fn from(row: OperationRow) -> Self {
        Operation {
            id: row.id.to_string(),
            operation_type: row.operation_type,
            operator: row.operator,
            scope: row.scope,
            result_status: row.result_status,
            result_message: row.result_message,
            preview_summary: row.preview_summary,
            duration_ms: row.duration_ms,
        }
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

    // Parse scope as JSONB if provided
    let scope_jsonb = request.scope.unwrap_or(serde_json::json!({}));

    // Call the database function to start an operation
    let operation_uuid = sqlx::query_scalar!(
        r#"
        SELECT core.start_operation($1, $2, $3)::uuid as "id!"
        "#,
        request.operation_type,
        request.operator,
        scope_jsonb,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| SinexError::service(format!("Failed to start operation: {e}")))?;

    let operation_id = Id::<Operation>::from_uuid(operation_uuid);

    info!(
        token_prefix = %auth.token_prefix,
        operation_id = %operation_id,
        operation_type = %request.operation_type,
        operator = %request.operator,
        "Operation started"
    );

    // Fetch the created operation
    let row = sqlx::query_as!(
        OperationRow,
        r#"
        SELECT
            id::uuid as "id!: Id<Operation>",
            operation_type as "operation_type!",
            operator as "operator!",
            scope,
            result_status as "result_status!",
            result_message,
            preview_summary,
            duration_ms
        FROM core.operations_log
        WHERE id::uuid = $1
        "#,
        operation_id as _
    )
    .fetch_one(pool)
    .await
    .map_err(|e| SinexError::service(format!("Failed to fetch created operation: {e}")))?;

    let response = OpsStartResponse {
        operation: row.into(),
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

    // Convert typed status to string for sqlx binding (DB stores result_status as TEXT).
    let status_str: Option<String> = request.status.map(|s| s.to_string());

    // Build dynamic query based on filters
    let rows: Vec<OperationRow> = if request.operation_type.is_some() && status_str.is_some() {
        sqlx::query_as!(
            OperationRow,
            r#"
            SELECT
                id::uuid as "id!: Id<Operation>",
                operation_type as "operation_type!",
                operator as "operator!",
                scope,
                result_status as "result_status!",
                result_message,
                preview_summary,
                duration_ms
            FROM core.operations_log
            WHERE operation_type = $1
              AND result_status = $2
            ORDER BY id DESC
            LIMIT $3
            "#,
            request.operation_type,
            status_str,
            limit
        )
        .fetch_all(pool)
        .await
    } else if request.operation_type.is_some() {
        sqlx::query_as!(
            OperationRow,
            r#"
            SELECT
                id::uuid as "id!: Id<Operation>",
                operation_type as "operation_type!",
                operator as "operator!",
                scope,
                result_status as "result_status!",
                result_message,
                preview_summary,
                duration_ms
            FROM core.operations_log
            WHERE operation_type = $1
            ORDER BY id DESC
            LIMIT $2
            "#,
            request.operation_type,
            limit
        )
        .fetch_all(pool)
        .await
    } else if status_str.is_some() {
        sqlx::query_as!(
            OperationRow,
            r#"
            SELECT
                id::uuid as "id!: Id<Operation>",
                operation_type as "operation_type!",
                operator as "operator!",
                scope,
                result_status as "result_status!",
                result_message,
                preview_summary,
                duration_ms
            FROM core.operations_log
            WHERE result_status = $1
            ORDER BY id DESC
            LIMIT $2
            "#,
            status_str,
            limit
        )
        .fetch_all(pool)
        .await
    } else {
        sqlx::query_as!(
            OperationRow,
            r#"
            SELECT
                id::uuid as "id!: Id<Operation>",
                operation_type as "operation_type!",
                operator as "operator!",
                scope,
                result_status as "result_status!",
                result_message,
                preview_summary,
                duration_ms
            FROM core.operations_log
            ORDER BY id DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(pool)
        .await
    }
    .map_err(|e| SinexError::service(format!("Failed to list operations: {e}")))?;

    let response = OpsListResponse {
        operations: rows.into_iter().map(Into::into).collect(),
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

    let row = sqlx::query_as!(
        OperationRow,
        r#"
        SELECT
            id::uuid as "id!: Id<Operation>",
            operation_type as "operation_type!",
            operator as "operator!",
            scope,
            result_status as "result_status!",
            result_message,
            preview_summary,
            duration_ms
        FROM core.operations_log
        WHERE id::uuid = $1
        "#,
        operation_id as _
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| SinexError::service(format!("Failed to fetch operation: {e}")))?;

    match row {
        Some(row) => {
            let response = OpsGetResponse {
                operation: row.into(),
            };
            Ok(serde_json::to_value(response)?)
        }
        None => Err(SinexError::not_found(format!(
            "Operation not found: {operation_id}"
        ))),
    }
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

    // Check if operation exists and is running
    let operation = sqlx::query!(
        r#"
        SELECT result_status
        FROM core.operations_log
        WHERE id::uuid = $1
        "#,
        operation_id as _
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| SinexError::service(format!("Failed to check operation status: {e}")))?;

    let Some(op) = operation else {
        return Err(SinexError::not_found(format!(
            "Operation not found: {operation_id}"
        )));
    };

    if op.result_status != "running" {
        return Err(SinexError::invalid_state(format!(
            "Operation cannot be cancelled (status: {})",
            op.result_status
        )));
    }

    info!(
        token_prefix = %auth.token_prefix,
        operation_id = %operation_id,
        "Operation cancel initiated"
    );

    // Mark operation as cancelled
    let reason = request
        .reason
        .unwrap_or_else(|| "Cancelled by user".to_string());

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET result_status = 'cancelled',
            result_message = $2,
            duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - (id::timestamp)))::integer
        WHERE id::uuid = $1
        "#,
        operation_id as _,
        reason
    )
    .execute(pool)
    .await
    .map_err(|e| SinexError::service(format!("Failed to cancel operation: {e}")))?;

    // Fetch updated operation
    let row = sqlx::query_as!(
        OperationRow,
        r#"
        SELECT
            id::uuid as "id!: Id<Operation>",
            operation_type as "operation_type!",
            operator as "operator!",
            scope,
            result_status as "result_status!",
            result_message,
            preview_summary,
            duration_ms
        FROM core.operations_log
        WHERE id::uuid = $1
        "#,
        operation_id as _
    )
    .fetch_one(pool)
    .await
    .map_err(|e| SinexError::service(format!("Failed to fetch cancelled operation: {e}")))?;

    let response = OpsCancelResponse {
        operation: row.into(),
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
