//! Audit trail endpoint handlers
//!
//! This module provides RPC endpoints for querying audit trails:
//! - Get audit trail for a specific operation
//! - Follow provenance links from operation to affected events

use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation as DbOperation;
use sinex_primitives::events::Event;
use sinex_primitives::rpc::ops::Operation;
use sinex_primitives::{Id, SinexError, Timestamp};
use sqlx::PgPool;

// Re-export shared types
pub use sinex_primitives::rpc::audit::{
    AuditGetRequest, AuditGetResponse, AuditTrail, EventSummary, OperationRecord,
};

type Result<T> = std::result::Result<T, SinexError>;

/// Maximum allowed page size for affected events.
const MAX_AUDIT_PAGE_SIZE: i64 = 1000;

/// Row returned by affected-events queries.
///
/// Uses `uuid::Uuid` for the ID column so both cursor and non-cursor branches
/// return the same type (the UUIDv7-typed column is cast to UUID in the SELECT).
#[derive(Debug, sqlx::FromRow)]
struct AffectedEventRow {
    id: uuid::Uuid,
    source: String,
    event_type: String,
    ts_orig: Timestamp,
    ts_ingest: Timestamp,
}

/// Query events affected by an operation with optional cursor-based pagination.
///
/// NOTE: This query intentionally lives outside the repository pattern. It joins
/// `audit.archived_events` with time-window arithmetic derived from operation UUIDv7 IDs
/// and uses keyset pagination with a cursor ID. This logic is specific to the audit
/// RPC endpoint and doesn't generalize to other consumers.
///
/// Operations affect events through archive operations. We find affected events by:
/// 1. Using the operation UUIDv7's embedded timestamp as the start time
/// 2. Adding `duration_ms` (or a default buffer) to get the end time
/// 3. Querying `archived_events` whose `archived_at` falls within this window
///
/// Events are returned in descending UUIDv7 order. When `after_id` is supplied,
/// only events with `id < after_id` are returned (keyset pagination).
///
/// Returns `(events, has_more)` where `has_more` indicates whether additional
/// pages are available.
async fn query_affected_events(
    pool: &PgPool,
    operation_id: &Id<Operation>,
    duration_ms: Option<i32>,
    limit: i64,
    after_id: Option<&Id<Event>>,
) -> Result<(Vec<EventSummary>, bool)> {
    let page_size = limit.min(MAX_AUDIT_PAGE_SIZE);
    // Fetch one extra to detect whether more pages exist.
    let fetch_limit = page_size + 1;
    let duration_secs = f64::from(duration_ms.unwrap_or(5000)) / 1000.0;
    // Bind the operation UUIDv7 as a string; the query casts it with `$1::uuid`.
    let op_uuid = operation_id.as_uuid().to_string();

    let mut rows: Vec<AffectedEventRow> = if let Some(cursor) = after_id {
        // Cursor path: restrict to events before the cursor ID (keyset, descending).
        let cursor_uuid = cursor.to_uuid();
        sqlx::query_as(
            r"
            SELECT id::uuid AS id, source, event_type, ts_orig, ts_ingest
            FROM audit.archived_events
            WHERE archived_at >= ($1::uuid)::timestamptz
              AND archived_at <= ($1::uuid)::timestamptz + make_interval(secs => $2)
              AND id < $4::uuid
            ORDER BY id DESC
            LIMIT $3
            ",
        )
        .bind(&op_uuid)
        .bind(duration_secs)
        .bind(fetch_limit)
        .bind(cursor_uuid)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            SinexError::service("failed to query affected events (paged)").with_std_error(&e)
        })?
    } else {
        // First page: no cursor restriction.
        sqlx::query_as(
            r"
            SELECT id::uuid AS id, source, event_type, ts_orig, ts_ingest
            FROM audit.archived_events
            WHERE archived_at >= ($1::uuid)::timestamptz
              AND archived_at <= ($1::uuid)::timestamptz + make_interval(secs => $2)
            ORDER BY id DESC
            LIMIT $3
            ",
        )
        .bind(&op_uuid)
        .bind(duration_secs)
        .bind(fetch_limit)
        .fetch_all(pool)
        .await
        .map_err(|e| SinexError::service("failed to query affected events").with_std_error(&e))?
    };

    let has_more = rows.len() as i64 > page_size;
    if has_more {
        rows.truncate(page_size as usize);
    }

    let events = rows
        .into_iter()
        .map(|row| EventSummary {
            id: Id::from_uuid(row.id),
            source: row.source.into(),
            event_type: row.event_type.into(),
            ts_orig: Some(row.ts_orig),
            ts_ingest: row.ts_ingest,
            provenance_operation_id: Some(*operation_id),
        })
        .collect();

    Ok((events, has_more))
}

/// Handle GET /`audit/{operation_id`} - get audit trail for an operation
pub async fn handle_audit_get(pool: &PgPool, params: Value) -> Result<Value> {
    let request: AuditGetRequest = serde_json::from_value(params)
        .map_err(|e| SinexError::serialization("invalid audit request").with_std_error(&e))?;

    let operation_id = request.operation_id;

    // Convert RPC phantom type → DB phantom type for repository call
    let db_id = Id::<DbOperation>::from_uuid(*operation_id.as_uuid());

    // Fetch the operation record via repository
    let record = pool
        .state()
        .get_operation(&db_id)
        .await?
        .ok_or_else(|| SinexError::not_found(format!("Operation not found: {operation_id}")))?;

    let operation = OperationRecord {
        id: Id::from_uuid(*record.id.as_uuid()),
        operation_type: record.operation_type,
        operator: record.operator,
        scope: record.scope,
        result_status: record.result_status,
        result_message: record.result_message,
        preview_summary: record.preview_summary,
        duration_ms: record.duration_ms,
    };

    let limit = (request.limit as i64).min(MAX_AUDIT_PAGE_SIZE).max(1);
    let (affected_events, has_more) = query_affected_events(
        pool,
        &operation_id,
        record.duration_ms,
        limit,
        request.after_id.as_ref(),
    )
    .await?;

    let next_cursor = if has_more {
        affected_events.last().map(|e| e.id)
    } else {
        None
    };

    let event_count = affected_events.len();
    let response = AuditGetResponse {
        audit_trail: AuditTrail {
            operation,
            affected_events,
        },
        event_count,
        next_cursor,
        has_more,
    };

    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("failed to serialize audit response").with_std_error(&e)
    })
}
