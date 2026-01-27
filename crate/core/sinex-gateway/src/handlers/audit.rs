//! Audit trail endpoint handlers
//!
//! This module provides RPC endpoints for querying audit trails:
//! - Get audit trail for a specific operation
//! - Follow provenance links from operation to affected events

use color_eyre::eyre::{eyre, Context, Result};
use serde_json::Value;
use sqlx::PgPool;

// Re-export shared types
pub use sinex_core::rpc::audit::{
    AuditGetRequest, AuditGetResponse, AuditTrail, EventSummary, OperationRecord,
};

/// Internal DB row type for operation records
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

/// Handle GET /audit/{operation_id} - get audit trail for an operation
pub async fn handle_audit_get(pool: &PgPool, params: Value) -> Result<Value> {
    let request: AuditGetRequest =
        serde_json::from_value(params).wrap_err("Invalid audit parameters")?;

    // Fetch the operation record
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

    let Some(row) = row else {
        return Err(eyre!("Operation not found: {}", request.operation_id));
    };

    // Convert DB row to RPC type
    let operation = OperationRecord {
        id: row.id,
        operation_type: row.operation_type,
        operator: row.operator,
        scope: row.scope,
        result_status: row.result_status,
        result_message: row.result_message,
        preview_summary: row.preview_summary,
        duration_ms: row.duration_ms,
    };

    // TODO: Implement provenance tracking for audit trail
    // The events table doesn't have a provenance JSONB column yet
    // Need to design how operation_id links to events
    // For now, return empty array
    let affected_events: Vec<EventSummary> = Vec::new();

    let event_count = affected_events.len();
    let response = AuditGetResponse {
        audit_trail: AuditTrail {
            operation,
            affected_events,
        },
        event_count,
    };

    Ok(serde_json::to_value(response)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::{sinex_test, TestContext};

    #[sinex_test]
    async fn audit_get_returns_operation(ctx: &TestContext) -> TestResult<()> {
        // Create a test operation using the database function
        let operation_id: String = sqlx::query_scalar!(
            r#"
            SELECT core.start_operation('test-audit', 'test-user', '{}'::jsonb)::text
            "#
        )
        .fetch_one(ctx.pool())
        .await?
        .expect("operation_id should be returned");

        // Fetch audit trail
        let result = handle_audit_get(ctx.pool(), json!({ "operation_id": operation_id })).await?;

        // Parse as typed response
        let response: AuditGetResponse = serde_json::from_value(result)?;

        assert_eq!(response.audit_trail.operation.id, operation_id);
        assert_eq!(response.audit_trail.operation.operation_type, "test-audit");
        assert!(response.audit_trail.affected_events.is_empty());
        assert_eq!(response.event_count, 0);

        Ok(())
    }

    #[sinex_test]
    async fn audit_get_fails_for_missing_operation(ctx: &TestContext) -> TestResult<()> {
        let fake_id = "01HX1234567890ABCDEFGHJ000";

        let err = handle_audit_get(ctx.pool(), json!({ "operation_id": fake_id }))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Operation not found"));

        Ok(())
    }
}
