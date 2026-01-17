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
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;

/// Operation record from database
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Operation {
    pub id: String,
    pub operation_type: String,
    pub operator: String,
    pub scope: Option<Value>,
    pub result_status: String,
    pub result_message: Option<String>,
    pub preview_summary: Option<Value>,
    pub duration_ms: Option<i32>,
}

/// Parameters for starting a new operation
#[derive(Debug, Deserialize)]
struct OpsStartParams {
    operation_type: String,
    operator: String,
    scope: Option<Value>,
}

/// Parameters for listing operations
#[derive(Debug, Deserialize)]
struct OpsListParams {
    /// Filter by operation type
    operation_type: Option<String>,
    /// Filter by status
    status: Option<String>,
    /// Limit number of results
    #[serde(default = "default_ops_limit")]
    limit: i64,
}

fn default_ops_limit() -> i64 {
    100
}

/// Parameters for getting operation details
#[derive(Debug, Deserialize)]
struct OpsGetParams {
    operation_id: String,
}

/// Parameters for cancelling an operation
#[derive(Debug, Deserialize)]
struct OpsCancelParams {
    operation_id: String,
    reason: Option<String>,
}

/// Handle POST /ops/start - start a new operation
pub async fn handle_ops_start(pool: &PgPool, params: Value) -> Result<Value> {
    let start_params: OpsStartParams =
        serde_json::from_value(params).wrap_err("Invalid ops start parameters")?;

    // Parse scope as JSONB if provided
    let scope_jsonb = start_params.scope.unwrap_or(json!({}));

    // Call the database function to start an operation
    let operation_id = sqlx::query_scalar!(
        r#"
        SELECT core.start_operation($1, $2, $3)::text
        "#,
        start_params.operation_type,
        start_params.operator,
        scope_jsonb,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| eyre!("Failed to start operation: {}", e))?;

    // Fetch the created operation
    let operation = sqlx::query_as!(
        Operation,
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

    Ok(json!({
        "operation": operation,
    }))
}

/// Handle GET /ops - list operations with optional filtering
pub async fn handle_ops_list(pool: &PgPool, params: Value) -> Result<Value> {
    let list_params: OpsListParams =
        serde_json::from_value(params).unwrap_or_else(|_| OpsListParams {
            operation_type: None,
            status: None,
            limit: default_ops_limit(),
        });

    // Build dynamic query based on filters
    let operations = if list_params.operation_type.is_some() && list_params.status.is_some() {
        sqlx::query_as!(
            Operation,
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
            list_params.operation_type,
            list_params.status,
            list_params.limit
        )
        .fetch_all(pool)
        .await
    } else if list_params.operation_type.is_some() {
        sqlx::query_as!(
            Operation,
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
            list_params.operation_type,
            list_params.limit
        )
        .fetch_all(pool)
        .await
    } else if list_params.status.is_some() {
        sqlx::query_as!(
            Operation,
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
            list_params.status,
            list_params.limit
        )
        .fetch_all(pool)
        .await
    } else {
        sqlx::query_as!(
            Operation,
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
            list_params.limit
        )
        .fetch_all(pool)
        .await
    }
    .map_err(|e| eyre!("Failed to list operations: {}", e))?;

    Ok(json!({
        "operations": operations,
        "count": operations.len(),
    }))
}

/// Handle GET /ops/{id} - get operation details
pub async fn handle_ops_get(pool: &PgPool, params: Value) -> Result<Value> {
    let get_params: OpsGetParams =
        serde_json::from_value(params).wrap_err("Invalid ops get parameters")?;

    let operation = sqlx::query_as!(
        Operation,
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
        get_params.operation_id
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| eyre!("Failed to fetch operation: {}", e))?;

    match operation {
        Some(op) => Ok(json!({
            "operation": op,
        })),
        None => Err(eyre!("Operation not found: {}", get_params.operation_id)),
    }
}

/// Handle POST /ops/{id}/cancel - cancel a running operation
pub async fn handle_ops_cancel(pool: &PgPool, params: Value) -> Result<Value> {
    let cancel_params: OpsCancelParams =
        serde_json::from_value(params).wrap_err("Invalid ops cancel parameters")?;

    // Check if operation exists and is running
    let operation = sqlx::query!(
        r#"
        SELECT result_status
        FROM core.operations_log
        WHERE id::text = $1
        "#,
        cancel_params.operation_id
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| eyre!("Failed to check operation status: {}", e))?;

    let Some(op) = operation else {
        return Err(eyre!("Operation not found: {}", cancel_params.operation_id));
    };

    if op.result_status != "running" {
        return Err(eyre!(
            "Operation cannot be cancelled (status: {})",
            op.result_status
        ));
    }

    // Mark operation as cancelled
    let reason = cancel_params
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
        cancel_params.operation_id,
        reason
    )
    .execute(pool)
    .await
    .map_err(|e| eyre!("Failed to cancel operation: {}", e))?;

    // Fetch updated operation
    let updated_operation = sqlx::query_as!(
        Operation,
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
        cancel_params.operation_id
    )
    .fetch_one(pool)
    .await
    .map_err(|e| eyre!("Failed to fetch cancelled operation: {}", e))?;

    Ok(json!({
        "operation": updated_operation,
        "cancelled": true,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{sinex_test, TestContext};

    #[sinex_test]
    async fn ops_start_creates_operation(ctx: &TestContext) -> TestResult<()> {
        let params = json!({
            "operation_type": "test-operation",
            "operator": "test-user",
            "scope": {"key": "value"},
        });

        let result = handle_ops_start(ctx.pool(), params).await?;
        assert!(result["operation"]["id"].is_string());
        assert_eq!(result["operation"]["operation_type"], "test-operation");
        assert_eq!(result["operation"]["result_status"], "running");

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
        assert!(result["operations"].is_array());
        assert!(result["operations"].as_array().unwrap().len() > 0);

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

        let operation_id = start_result["operation"]["id"].as_str().unwrap();

        // Get the operation
        let result = handle_ops_get(ctx.pool(), json!({ "operation_id": operation_id })).await?;
        assert_eq!(result["operation"]["id"], operation_id);

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

        let operation_id = start_result["operation"]["id"].as_str().unwrap();

        // Cancel it
        let result = handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": operation_id,
                "reason": "test cancellation",
            }),
        )
        .await?;

        assert_eq!(result["operation"]["result_status"], "cancelled");
        assert_eq!(result["operation"]["result_message"], "test cancellation");
        assert_eq!(result["cancelled"], true);

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

        let operation_id = start_result["operation"]["id"].as_str().unwrap();

        // Cancel once
        handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": operation_id,
            }),
        )
        .await?;

        // Try to cancel again - should fail
        let err = handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": operation_id,
            }),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("cannot be cancelled"));

        Ok(())
    }
}
