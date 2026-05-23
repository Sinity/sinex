//! Composable event query, provenance lineage, and event-annotation handlers.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::query::{EventQuery, EventQueryResult, LineageQuery, LineageResult};
use sinex_primitives::rpc::events::{EventsAnnotateRequest, EventsAnnotateResponse};
use sinex_primitives::{Id, Result, SinexError};
use sqlx::PgPool;
use std::str::FromStr;

pub async fn handle_events_query(pool: &PgPool, query: EventQuery) -> Result<EventQueryResult> {
    pool.events().query(query).await
}

pub async fn handle_events_lineage(pool: &PgPool, query: LineageQuery) -> Result<LineageResult> {
    pool.events().lineage(query).await
}

/// `events.annotate` (#1172 AC-9): write a typed annotation to
/// `core.event_annotations` against an existing event id.
///
/// Distinct from `sources.annotate` (material-level annotation).
pub async fn handle_events_annotate(
    pool: &PgPool,
    req: EventsAnnotateRequest,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<EventsAnnotateResponse> {
    let event_id_str = req.event_id.as_str();
    let annotation_type = req.annotation_type.as_str();
    let content = req.content.as_str();
    let metadata = req.metadata.unwrap_or_else(|| json!({}));

    if annotation_type.trim().is_empty() {
        return Err(SinexError::validation(
            "events.annotate: annotation_type must not be empty",
        ));
    }
    if content.trim().is_empty() {
        return Err(SinexError::validation(
            "events.annotate: content must not be empty",
        ));
    }

    let event_uuid = sinex_primitives::Uuid::from_str(event_id_str).map_err(|error| {
        SinexError::validation("events.annotate: invalid event_id UUID")
            .with_context("event_id", event_id_str)
            .with_std_error(&error)
    })?;
    let event_id =
        Id::<sinex_primitives::events::Event<sinex_primitives::JsonValue>>::from_uuid(event_uuid);

    let record = pool
        .events()
        .add_annotation(
            event_id,
            annotation_type,
            content,
            metadata,
            auth.actor_id(),
        )
        .await
        .map_err(|error| {
            SinexError::database("events.annotate: failed to record annotation")
                .with_source(error.to_string())
        })?;

    Ok(EventsAnnotateResponse {
        id: record.id.as_uuid().to_string(),
        event_id: record.event_id.as_uuid().to_string(),
        annotation_type: record.annotation_type,
        content: record.content,
        metadata: record.metadata,
        created_by: record.created_by,
        created_at: record.created_at.format_rfc3339(),
        updated_at: record.updated_at.format_rfc3339(),
    })
}
