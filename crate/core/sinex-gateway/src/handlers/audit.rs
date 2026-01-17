//! Audit trail endpoint handlers
//!
//! This module provides RPC endpoints for querying audit trails:
//! - Get audit trail for a specific operation
//! - Follow provenance links from operation to affected events

use color_eyre::eyre::{eyre, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;

/// Audit trail record combining operation and provenance information
#[derive(Debug, Serialize)]
pub struct AuditTrail {
    pub operation: OperationRecord,
    pub affected_events: Vec<EventSummary>,
}

/// Operation record from database
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct OperationRecord {
    pub id: String,
    pub operation_type: String,
    pub operator: String,
    pub scope: Option<Value>,
    pub result_status: String,
    pub result_message: Option<String>,
    pub preview_summary: Option<Value>,
    pub duration_ms: Option<i32>,
}

/// Event summary for audit trail
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct EventSummary {
    pub id: String,
    pub source: String,
    pub event_type: String,
    pub ts_orig: Option<String>,
    pub ts_ingest: String,
    pub provenance_operation_id: Option<String>,
}

/// Parameters for fetching audit trail
#[derive(Debug, Deserialize)]
struct AuditGetParams {
    operation_id: String,
}

/// Handle GET /audit/{operation_id} - get audit trail for an operation
pub async fn handle_audit_get(pool: &PgPool, params: Value) -> Result<Value> {
    let audit_params: AuditGetParams =
        serde_json::from_value(params).wrap_err("Invalid audit parameters")?;

    // Fetch the operation record
    let operation = sqlx::query_as!(
        OperationRecord,
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
        audit_params.operation_id
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| eyre!("Failed to fetch operation: {}", e))?;

    let Some(operation) = operation else {
        return Err(eyre!("Operation not found: {}", audit_params.operation_id));
    };

    // TODO: Implement provenance tracking for audit trail
    // The events table doesn't have a provenance JSONB column yet
    // Need to design how operation_id links to events
    // For now, return empty array
    let affected_events: Vec<EventSummary> = Vec::new();

    let trail = AuditTrail {
        operation,
        affected_events,
    };

    Ok(json!({
        "audit_trail": trail,
        "event_count": trail.affected_events.len(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{sinex_test, TestContext};

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

        assert_eq!(result["audit_trail"]["operation"]["id"], operation_id);
        assert_eq!(
            result["audit_trail"]["operation"]["operation_type"],
            "test-audit"
        );
        assert!(result["audit_trail"]["affected_events"].is_array());

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
