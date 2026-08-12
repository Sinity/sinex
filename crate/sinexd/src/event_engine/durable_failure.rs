//! Durable failure evidence shared by the raw and source-material DLQ routes.
//!
//! JetStream is a bounded delivery/recovery surface.  A failure that unlocks
//! progress therefore needs a Postgres witness before the caller settles its
//! message.  The existing `sinex_schemas.dlq_events` table is the operator-
//! visible witness; this module is its narrow event-engine writer.

use sinex_primitives::{JsonValue, Uuid};
use sqlx::PgPool;

use super::{EventEngineResult, SinexError};

/// Header carried by every DLQ message whose terminal settlement is backed by
/// a row in `sinex_schemas.dlq_events`.
pub(crate) const DURABLE_FAILURE_ID_HEADER: &str = "Sinex-Durable-Failure-Id";

/// Write a failure witness before any DLQ publish or progress-unlocking
/// settlement.  The caller supplies a metadata-only payload for retryable
/// failures; terminal routes may pass the already-redacted original payload.
pub(crate) async fn persist_failure_evidence(
    pool: &PgPool,
    failed_event_id: Uuid,
    automaton_name: &str,
    source: &str,
    event_type: &str,
    error_category: &str,
    failure_reason: &str,
    original_event_payload: JsonValue,
    additional_metadata: JsonValue,
    retry_count: i32,
) -> EventEngineResult<Uuid> {
    let row = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.dlq_events (
            failed_event_id,
            automaton_name,
            source,
            event_type,
            error_category,
            failure_reason,
            original_event_payload,
            additional_metadata,
            retry_count
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING dlq_id
        "#,
        failed_event_id,
        automaton_name,
        source,
        event_type,
        error_category,
        failure_reason,
        original_event_payload,
        additional_metadata,
        retry_count,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to persist durable failure evidence")
            .with_context("failed_event_id", failed_event_id.to_string())
            .with_context("automaton_name", automaton_name.to_string())
            .with_context("error_category", error_category.to_string())
            .with_source(error)
    })?;

    Ok(row.dlq_id)
}
