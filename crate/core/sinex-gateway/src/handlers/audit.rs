//! Audit trail endpoint handlers
//!
//! This module provides RPC endpoints for querying audit trails:
//! - Get audit trail for a specific operation
//! - Follow provenance links from operation to affected events

use serde_json::Value;
use sinex_primitives::events::Event;
use sinex_primitives::rpc::ops::Operation;
use sinex_primitives::{Id, SinexError, Timestamp};
use sqlx::PgPool;

// Re-export shared types
pub use sinex_primitives::rpc::audit::{
    AuditGetRequest, AuditGetResponse, AuditTrail, EventSummary, OperationRecord,
};

type Result<T> = std::result::Result<T, SinexError>;

/// Maximum events to return in provenance query (pagination TODO)
const MAX_AFFECTED_EVENTS: i64 = 100;

/// Query events affected by an operation.
///
/// Operations affect events through archive operations. We find affected events by:
/// 1. Using the operation ULID's embedded timestamp as the start time
/// 2. Adding `duration_ms` (or a default buffer) to get the end time
/// 3. Querying `archived_events` whose `archived_at` falls within this window
async fn query_affected_events(
    pool: &PgPool,
    operation_id: &Id<Operation>,
    duration_ms: Option<i32>,
) -> Result<Vec<EventSummary>> {
    // The operation ULID contains its creation timestamp
    // We query archived events that were archived during the operation's execution
    let duration = duration_ms.unwrap_or(5000); // Default 5s buffer for short operations

    let rows = sqlx::query!(
        r#"
        SELECT
            id::uuid as "id!: Id<Event>",
            source,
            event_type,
            ts_orig as "ts_orig: Timestamp",
            ts_ingest as "ts_ingest: Timestamp"
        FROM audit.archived_events
        WHERE archived_at >= ($1::ulid)::timestamptz
          AND archived_at <= ($1::ulid)::timestamptz + ($2 || ' milliseconds')::interval
        ORDER BY ts_orig DESC
        LIMIT $3
        "#,
        operation_id as _,
        duration.to_string(),
        MAX_AFFECTED_EVENTS
    )
    .fetch_all(pool)
    .await
    .map_err(|e| SinexError::service("failed to query affected events").with_std_error(&e))?;

    let events = rows
        .into_iter()
        .map(|row| EventSummary {
            id: row.id,
            source: row.source,
            event_type: row.event_type,
            ts_orig: Some(row.ts_orig),
            ts_ingest: row.ts_ingest,
            provenance_operation_id: Some(*operation_id),
        })
        .collect();

    Ok(events)
}

/// Internal DB row type for operation records
#[derive(Debug, sqlx::FromRow)]
struct OperationRow {
    id: Id<Operation>,
    operation_type: String,
    operator: String,
    scope: Option<Value>,
    result_status: String,
    result_message: Option<String>,
    preview_summary: Option<Value>,
    duration_ms: Option<i32>,
}

/// Handle GET /`audit/{operation_id`} - get audit trail for an operation
pub async fn handle_audit_get(pool: &PgPool, params: Value) -> Result<Value> {
    let request: AuditGetRequest = serde_json::from_value(params)
        .map_err(|e| SinexError::serialization("invalid audit request").with_std_error(&e))?;

    let operation_id = request.operation_id;

    // Fetch the operation record
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
    .map_err(|e| SinexError::service("failed to fetch operation").with_std_error(&e))?;

    let Some(row) = row else {
        return Err(SinexError::not_found(format!(
            "Operation not found: {operation_id}"
        )));
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

    // Query affected events based on operation timeframe
    // Operations track events via timestamp correlation:
    // - Archived events: archived_at within operation's execution window
    // - The operation ULID contains its start timestamp
    let affected_events = query_affected_events(pool, &row.id, row.duration_ms).await?;

    let event_count = affected_events.len();
    let response = AuditGetResponse {
        audit_trail: AuditTrail {
            operation,
            affected_events,
        },
        event_count,
    };

    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("failed to serialize audit response").with_std_error(&e)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn audit_get_returns_operation(ctx: &TestContext) -> TestResult<()> {
        // Create a test operation using the database function
        let operation_uuid: uuid::Uuid = sqlx::query_scalar!(
            r#"
            SELECT core.start_operation('test-audit', 'test-user', '{}'::jsonb)::uuid as "id!"
            "#
        )
        .fetch_one(ctx.pool())
        .await?;

        let operation_id = Id::<Operation>::from_uuid(operation_uuid);

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
        let fake_id = Id::<Operation>::new();

        let err = handle_audit_get(ctx.pool(), json!({ "operation_id": fake_id }))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Operation not found"));

        Ok(())
    }
}
