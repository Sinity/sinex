//! Composable event query, provenance lineage, and event-annotation handlers.

use serde_json::{Value, json};
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::query::{EventQuery, LineageQuery};
use sinex_primitives::{Id, Result, SinexError};
use sqlx::PgPool;
use std::str::FromStr;

pub async fn handle_events_query(pool: &PgPool, params: Value) -> Result<Value> {
    let query: EventQuery = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid event query parameters").with_std_error(&error)
    })?;
    let result = pool.events().query(query).await?;
    serde_json::to_value(&result).map_err(|error| {
        SinexError::serialization("failed to serialize events.query response")
            .with_std_error(&error)
    })
}

pub async fn handle_events_lineage(pool: &PgPool, params: Value) -> Result<Value> {
    let query: LineageQuery = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid lineage query parameters").with_std_error(&error)
    })?;
    let result = pool.events().lineage(query).await?;
    serde_json::to_value(&result).map_err(|error| {
        SinexError::serialization("failed to serialize events.lineage response")
            .with_std_error(&error)
    })
}

/// `events.annotate` (#1172 AC-9): write a typed annotation to
/// `core.event_annotations` against an existing event id.
///
/// Distinct from `sources.annotate` (material-level annotation).
pub async fn handle_events_annotate(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    let event_id_str = params
        .get("event_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            SinexError::validation("events.annotate: missing event_id (string)")
        })?;
    let annotation_type = params
        .get("annotation_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            SinexError::validation("events.annotate: missing annotation_type (string)")
        })?;
    let content = params
        .get("content")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            SinexError::validation("events.annotate: missing content (string)")
        })?;
    let metadata = params.get("metadata").cloned().unwrap_or_else(|| json!({}));

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
    let event_id = Id::<
        sinex_primitives::events::Event<sinex_primitives::JsonValue>,
    >::from_uuid(event_uuid);

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

    serde_json::to_value(json!({
        "id": record.id.as_uuid().to_string(),
        "event_id": record.event_id.as_uuid().to_string(),
        "annotation_type": record.annotation_type,
        "content": record.content,
        "metadata": record.metadata,
        "created_by": record.created_by,
        "created_at": record.created_at.format_rfc3339(),
        "updated_at": record.updated_at.format_rfc3339(),
    }))
    .map_err(|error| {
        SinexError::serialization("events.annotate: failed to serialize response")
            .with_std_error(&error)
    })
}
