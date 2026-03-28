//! Audit trail endpoint handlers
//!
//! This module provides RPC endpoints for querying audit trails:
//! - Get audit trail for a specific operation
//! - Follow provenance links from operation to affected events

use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation as DbOperation;
use sinex_primitives::domain::DataTier;
use sinex_primitives::events::Event;
use sinex_primitives::rpc::lifecycle::LifecycleOperationSummary;
use sinex_primitives::rpc::ops::Operation;
use sinex_primitives::{Id, SinexError, Timestamp};
use sqlx::PgPool;
use std::str::FromStr;

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
    ts_orig: Option<Timestamp>,
    ts_coided: Timestamp,
    tier: String,
}

fn uses_lifecycle_audit_summary(operation_type: &str) -> bool {
    matches!(operation_type, "archive" | "restore" | "purge" | "tombstone")
}

fn explicit_lifecycle_summary(
    operation_type: &str,
    summary: Option<&Value>,
) -> Result<Option<LifecycleOperationSummary>> {
    if !uses_lifecycle_audit_summary(operation_type) {
        return Ok(None);
    }

    let Some(summary) = summary else {
        return Ok(None);
    };

    let summary =
        serde_json::from_value::<LifecycleOperationSummary>(summary.clone()).map_err(|error| {
            SinexError::invalid_state(format!(
                "invalid lifecycle preview_summary for {operation_type} audit trail"
            ))
            .with_std_error(&error)
        })?;

    if summary.affected_event_ids.is_empty() {
        return Ok(None);
    }

    Ok(Some(summary))
}

fn parse_affected_event_tier(operation_id: &Id<Operation>, tier: &str) -> Result<Option<DataTier>> {
    DataTier::from_str(tier).map(Some).map_err(|error| {
        SinexError::invalid_state(format!(
            "operation {operation_id} returned invalid affected-event tier '{tier}'"
        ))
        .with_context("reason", error)
    })
}

async fn query_affected_events_by_operation_links(
    pool: &PgPool,
    operation_id: &Id<Operation>,
    limit: i64,
    after_id: Option<&Id<Event>>,
) -> Result<(Vec<EventSummary>, bool)> {
    let page_size = limit.min(MAX_AUDIT_PAGE_SIZE);
    let fetch_limit = page_size + 1;
    let cursor_uuid = after_id.map(|id| id.to_uuid());

    let mut rows = sqlx::query_as::<_, AffectedEventRow>(
        r#"
        WITH tiered_events AS (
            SELECT
                e.id::uuid AS id,
                e.source,
                e.event_type,
                e.ts_orig,
                e.ts_coided,
                'live'::text AS tier,
                0 AS tier_rank
            FROM core.events e
            WHERE e.created_by_operation_id = $1::uuid

            UNION ALL

            SELECT
                t.id::uuid AS id,
                t.source,
                t.event_type,
                t.ts_orig,
                uuid_extract_timestamp(t.id)::timestamptz AS ts_coided,
                'tombstone'::text AS tier,
                2 AS tier_rank
            FROM core.event_tombstones t
            WHERE t.purge_operation_id = $1::uuid
        ),
        ranked AS (
            SELECT DISTINCT ON (id) id, source, event_type, ts_orig, ts_coided, tier, tier_rank
            FROM tiered_events
            ORDER BY id, tier_rank
        )
        SELECT id, source, event_type, ts_orig, ts_coided, tier
        FROM ranked
        WHERE ($2::uuid IS NULL OR id < $2::uuid)
        ORDER BY id DESC
        LIMIT $3
        "#,
    )
    .bind(operation_id.to_uuid())
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| {
        SinexError::service("failed to query operation-linked affected events").with_std_error(&e)
    })?;

    let has_more = rows.len() as i64 > page_size;
    if has_more {
        rows.truncate(page_size as usize);
    }

    let events = rows
        .into_iter()
        .map(|row| {
            Ok(EventSummary {
                id: Id::from_uuid(row.id),
                source: row.source.into(),
                event_type: row.event_type.into(),
                ts_orig: row.ts_orig,
                ts_coided: row.ts_coided,
                tier: parse_affected_event_tier(operation_id, &row.tier)?,
                provenance_operation_id: Some(*operation_id),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((events, has_more))
}

async fn query_affected_events_by_ids(
    pool: &PgPool,
    operation_id: &Id<Operation>,
    affected_event_ids: &[uuid::Uuid],
    limit: i64,
    after_id: Option<&Id<Event>>,
) -> Result<(Vec<EventSummary>, bool)> {
    let page_size = limit.min(MAX_AUDIT_PAGE_SIZE);
    let fetch_limit = page_size + 1;
    let cursor_uuid = after_id.map(|id| id.to_uuid());
    let mut rows = sqlx::query_as::<_, AffectedEventRow>(
        r#"
        WITH affected_ids AS (
            SELECT unnest($1::uuid[]) AS id
        ),
        tiered_events AS (
            SELECT
                e.id::uuid AS id,
                e.source,
                e.event_type,
                e.ts_orig,
                e.ts_coided,
                'live'::text AS tier,
                0 AS tier_rank
            FROM core.events e
            INNER JOIN affected_ids ids ON ids.id = e.id

            UNION ALL

            SELECT
                ae.id::uuid AS id,
                ae.source,
                ae.event_type,
                ae.ts_orig,
                ae.ts_coided,
                'archive'::text AS tier,
                1 AS tier_rank
            FROM audit.archived_events ae
            INNER JOIN affected_ids ids ON ids.id = ae.id

            UNION ALL

            SELECT
                t.id::uuid AS id,
                t.source,
                t.event_type,
                t.ts_orig,
                uuid_extract_timestamp(t.id)::timestamptz AS ts_coided,
                'tombstone'::text AS tier,
                2 AS tier_rank
            FROM core.event_tombstones t
            INNER JOIN affected_ids ids ON ids.id = t.id
        ),
        ranked AS (
            SELECT DISTINCT ON (id) id, source, event_type, ts_orig, ts_coided, tier, tier_rank
            FROM tiered_events
            ORDER BY id, tier_rank
        )
        SELECT id, source, event_type, ts_orig, ts_coided, tier
        FROM ranked
        WHERE ($2::uuid IS NULL OR id < $2::uuid)
        ORDER BY id DESC
        LIMIT $3
        "#,
    )
    .bind(affected_event_ids)
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| SinexError::service("failed to query explicit affected events").with_std_error(&e))?;

    let has_more = rows.len() as i64 > page_size;
    if has_more {
        rows.truncate(page_size as usize);
    }

    let events = rows
        .into_iter()
        .map(|row| {
            Ok(EventSummary {
                id: Id::from_uuid(row.id),
                source: row.source.into(),
                event_type: row.event_type.into(),
                ts_orig: row.ts_orig,
                ts_coided: row.ts_coided,
                tier: parse_affected_event_tier(operation_id, &row.tier)?,
                provenance_operation_id: Some(*operation_id),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((events, has_more))
}

/// Query events affected by an operation with optional cursor-based pagination.
///
/// Prefer explicit lifecycle summaries persisted in `preview_summary`. Older
/// operations without that data fall back to the legacy archive-window heuristic.
async fn query_affected_events(
    pool: &PgPool,
    operation_type: &str,
    operation_id: &Id<Operation>,
    duration_ms: Option<i32>,
    limit: i64,
    after_id: Option<&Id<Event>>,
    preview_summary: Option<&Value>,
) -> Result<(Vec<EventSummary>, bool)> {
    if let Some(summary) = explicit_lifecycle_summary(operation_type, preview_summary)? {
        let affected_event_ids = summary
            .affected_event_ids
            .iter()
            .map(|raw| {
                uuid::Uuid::parse_str(raw).map_err(|error| {
                    SinexError::invalid_state(format!(
                        "invalid affected_event_id '{raw}' in lifecycle preview summary: {error}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        return query_affected_events_by_ids(
            pool,
            operation_id,
            &affected_event_ids,
            limit,
            after_id,
        )
        .await;
    }

    let linked_events =
        query_affected_events_by_operation_links(pool, operation_id, limit, after_id).await?;
    if !linked_events.0.is_empty() || linked_events.1 {
        return Ok(linked_events);
    }

    let page_size = limit.min(MAX_AUDIT_PAGE_SIZE);
    let fetch_limit = page_size + 1;
    let duration_secs = f64::from(duration_ms.unwrap_or(5000)) / 1000.0;
    let op_uuid = operation_id.to_uuid();

    let mut rows: Vec<AffectedEventRow> = if let Some(cursor) = after_id {
        let cursor_uuid = cursor.to_uuid();
        sqlx::query_as(
            r"
            SELECT
                id::uuid AS id,
                source,
                event_type,
                ts_orig,
                ts_coided,
                'archive'::text AS tier
            FROM audit.archived_events
            WHERE archived_at >= uuid_extract_timestamp($1::uuid)::timestamptz
              AND archived_at <= uuid_extract_timestamp($1::uuid)::timestamptz + make_interval(secs => $2)
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
        sqlx::query_as(
            r"
            SELECT
                id::uuid AS id,
                source,
                event_type,
                ts_orig,
                ts_coided,
                'archive'::text AS tier
            FROM audit.archived_events
            WHERE archived_at >= uuid_extract_timestamp($1::uuid)::timestamptz
              AND archived_at <= uuid_extract_timestamp($1::uuid)::timestamptz + make_interval(secs => $2)
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
        .map(|row| {
            Ok(EventSummary {
                id: Id::from_uuid(row.id),
                source: row.source.into(),
                event_type: row.event_type.into(),
                ts_orig: row.ts_orig,
                ts_coided: row.ts_coided,
                tier: parse_affected_event_tier(operation_id, &row.tier)?,
                provenance_operation_id: Some(*operation_id),
            })
        })
        .collect::<Result<Vec<_>>>()?;

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

    let preview_summary = record.preview_summary.clone();
    let operation_type = record.operation_type.clone();
    let operation = OperationRecord {
        id: Id::from_uuid(*record.id.as_uuid()),
        operation_type,
        operator: record.operator,
        scope: record.scope,
        result_status: record.result_status,
        result_message: record.result_message,
        preview_summary: preview_summary.clone(),
        duration_ms: record.duration_ms,
    };

    let limit = (request.limit as i64).min(MAX_AUDIT_PAGE_SIZE).max(1);
    let (affected_events, has_more) = query_affected_events(
        pool,
        &operation.operation_type,
        &operation_id,
        record.duration_ms,
        limit,
        request.after_id.as_ref(),
        preview_summary.as_ref(),
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
