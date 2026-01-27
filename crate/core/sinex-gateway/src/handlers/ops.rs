//! Generic operations log API handlers
//!
//! This module provides RPC endpoints for managing system operations:
//! - Start new operations
//! - List operations with filtering
//! - Get operation status
//! - Cancel running operations
//!
//! Reuses the existing core.operations_log table pattern.

use color_eyre::eyre::{eyre, Context, Result};
use serde_json::Value;
use sqlx::PgPool;

// Re-export shared types
pub use sinex_core::rpc::ops::{
    Operation, OpsCancelRequest, OpsCancelResponse, OpsGetRequest, OpsGetResponse, OpsListRequest,
    OpsListResponse, OpsStartRequest, OpsStartResponse,
};

fn default_ops_limit() -> i64 {
    100
}

/// Internal DB row type for operations
#[derive(Debug, sqlx::FromRow)]
struct OperationRow {
    id: String,
    operation_type: String,
    operator: String,
    scope: Option<Value>,
    result_status: String,
    result_message: Option<String>,
    preview_summary: Option<Value>,
    duration_ms: Option<i32>,
}

impl From<OperationRow> for Operation {
    fn from(row: OperationRow) -> Self {
        Operation {
            id: row.id,
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
pub async fn handle_ops_start(pool: &PgPool, params: Value) -> Result<Value> {
    let request: OpsStartRequest =
        serde_json::from_value(params).wrap_err("Invalid ops start parameters")?;

    // Parse scope as JSONB if provided
    let scope_jsonb = request.scope.unwrap_or(serde_json::json!({}));

    // Call the database function to start an operation
    let operation_id = sqlx::query_scalar!(
        r#"
        SELECT core.start_operation($1, $2, $3)::text
        "#,
        request.operation_type,
        request.operator,
        scope_jsonb,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| eyre!("Failed to start operation: {}", e))?;

    // Fetch the created operation
    let row = sqlx::query_as!(
        OperationRow,
        r#"
        SELECT
            id::text as "id!",
            operation_type as "operation_type!",
            operator as "operator!",
            scope,
            result_status as "result_status!",
            result_message,
            preview_summary,
            duration_ms
        FROM core.operations_log
        WHERE id::text = $1
        "#,
        operation_id
    )
    .fetch_one(pool)
    .await
    .map_err(|e| eyre!("Failed to fetch created operation: {}", e))?;

    let response = OpsStartResponse {
        operation: row.into(),
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle GET /ops - list operations with optional filtering
pub async fn handle_ops_list(pool: &PgPool, params: Value) -> Result<Value> {
    let request: OpsListRequest = serde_json::from_value(params).unwrap_or_default();

    let limit = if request.limit > 0 {
        request.limit
    } else {
        default_ops_limit()
    };

    // Build dynamic query based on filters
    let rows: Vec<OperationRow> = if request.operation_type.is_some() && request.status.is_some() {
        sqlx::query_as!(
            OperationRow,
            r#"
            SELECT
                id::text as "id!",
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
            request.status,
            limit
        )
        .fetch_all(pool)
        .await
    } else if request.operation_type.is_some() {
        sqlx::query_as!(
            OperationRow,
            r#"
            SELECT
                id::text as "id!",
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
    } else if request.status.is_some() {
        sqlx::query_as!(
            OperationRow,
            r#"
            SELECT
                id::text as "id!",
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
            request.status,
            limit
        )
        .fetch_all(pool)
        .await
    } else {
        sqlx::query_as!(
            OperationRow,
            r#"
            SELECT
                id::text as "id!",
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
    .map_err(|e| eyre!("Failed to list operations: {}", e))?;

    let response = OpsListResponse {
        operations: rows.into_iter().map(Into::into).collect(),
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle GET /ops/{id} - get operation details
pub async fn handle_ops_get(pool: &PgPool, params: Value) -> Result<Value> {
    let request: OpsGetRequest =
        serde_json::from_value(params).wrap_err("Invalid ops get parameters")?;

    let row = sqlx::query_as!(
        OperationRow,
        r#"
        SELECT
            id::text as "id!",
            operation_type as "operation_type!",
            operator as "operator!",
            scope,
            result_status as "result_status!",
            result_message,
            preview_summary,
            duration_ms
        FROM core.operations_log
        WHERE id::text = $1
        "#,
        request.operation_id
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| eyre!("Failed to fetch operation: {}", e))?;

    match row {
        Some(row) => {
            let response = OpsGetResponse {
                operation: row.into(),
            };
            Ok(serde_json::to_value(response)?)
        }
        None => Err(eyre!("Operation not found: {}", request.operation_id)),
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

    let request: OpsCancelRequest =
        serde_json::from_value(params).wrap_err("Invalid ops cancel parameters")?;

    // Check if operation exists and is running
    let operation = sqlx::query!(
        r#"
        SELECT result_status
        FROM core.operations_log
        WHERE id::text = $1
        "#,
        request.operation_id
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| eyre!("Failed to check operation status: {}", e))?;

    let Some(op) = operation else {
        return Err(eyre!("Operation not found: {}", request.operation_id));
    };

    if op.result_status != "running" {
        return Err(eyre!(
            "Operation cannot be cancelled (status: {})",
            op.result_status
        ));
    }

    info!(
        token_prefix = %auth.token_prefix,
        operation_id = %request.operation_id,
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
        WHERE id::text = $1
        "#,
        request.operation_id,
        reason
    )
    .execute(pool)
    .await
    .map_err(|e| eyre!("Failed to cancel operation: {}", e))?;

    // Fetch updated operation
    let row = sqlx::query_as!(
        OperationRow,
        r#"
        SELECT
            id::text as "id!",
            operation_type as "operation_type!",
            operator as "operator!",
            scope,
            result_status as "result_status!",
            result_message,
            preview_summary,
            duration_ms
        FROM core.operations_log
        WHERE id::text = $1
        "#,
        request.operation_id
    )
    .fetch_one(pool)
    .await
    .map_err(|e| eyre!("Failed to fetch cancelled operation: {}", e))?;

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
    use xtask::sandbox::{sinex_test, TestContext};

    #[sinex_test]
    async fn ops_start_creates_operation(ctx: &TestContext) -> TestResult<()> {
        let params = json!({
            "operation_type": "test-operation",
            "operator": "test-user",
            "scope": {"key": "value"},
        });

        let result = handle_ops_start(ctx.pool(), params).await?;
        let response: OpsStartResponse = serde_json::from_value(result)?;

        assert!(!response.operation.id.is_empty());
        assert_eq!(response.operation.operation_type, "test-operation");
        assert_eq!(response.operation.result_status, "running");

        Ok(())
    }

    #[sinex_test]
    async fn ops_list_returns_operations(ctx: &TestContext) -> TestResult<()> {
        // Create a test operation first
        let start_params = json!({
            "operation_type": "test-op",
            "operator": "tester",
        });
        handle_ops_start(ctx.pool(), start_params).await?;

        // List all operations
        let result = handle_ops_list(ctx.pool(), json!({})).await?;
        let response: OpsListResponse = serde_json::from_value(result)?;

        assert!(!response.operations.is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn ops_get_returns_operation(ctx: &TestContext) -> TestResult<()> {
        // Create a test operation
        let start_result = handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "test-get",
                "operator": "tester",
            }),
        )
        .await?;

        let start_response: OpsStartResponse = serde_json::from_value(start_result)?;
        let operation_id = &start_response.operation.id;

        // Get the operation
        let result = handle_ops_get(ctx.pool(), json!({ "operation_id": operation_id })).await?;
        let response: OpsGetResponse = serde_json::from_value(result)?;

        assert_eq!(response.operation.id, *operation_id);

        Ok(())
    }

    #[sinex_test]
    async fn ops_cancel_stops_running_operation(ctx: &TestContext) -> TestResult<()> {
        // Create a running operation
        let start_result = handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "test-cancel",
                "operator": "tester",
            }),
        )
        .await?;

        let start_response: OpsStartResponse = serde_json::from_value(start_result)?;
        let operation_id = &start_response.operation.id;

        // Cancel it
        let auth = crate::rpc_server::RpcAuthContext::system();
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

        assert_eq!(response.operation.result_status, "cancelled");
        assert_eq!(
            response.operation.result_message,
            Some("test cancellation".to_string())
        );
        assert!(response.cancelled);

        Ok(())
    }

    #[sinex_test]
    async fn ops_cancel_rejects_non_running_operation(ctx: &TestContext) -> TestResult<()> {
        // Create and immediately cancel an operation
        let start_result = handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "test-double-cancel",
                "operator": "tester",
            }),
        )
        .await?;

        let start_response: OpsStartResponse = serde_json::from_value(start_result)?;
        let operation_id = &start_response.operation.id;

        // Cancel once
        let auth = crate::rpc_server::RpcAuthContext::system();
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
