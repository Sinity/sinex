//! Repository for durable dead-letter evidence.

use super::common::{DbResult, Repository, db_error};
use crate::JsonValue;
use sinex_primitives::Uuid;
use sqlx::PgPool;

/// Postgres authority for terminal DLQ and durable-debt evidence.
pub struct DlqEventRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for DlqEventRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl DlqEventRepository<'_> {
    /// Insert the witness that must exist before a raw message is settled.
    pub async fn insert_failure_evidence(
        &self,
        failed_event_id: Uuid,
        automaton_name: &str,
        source: &str,
        event_type: &str,
        error_category: &str,
        failure_reason: &str,
        original_event_payload: JsonValue,
        additional_metadata: JsonValue,
        retry_count: i32,
    ) -> DbResult<Uuid> {
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
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "insert durable DLQ evidence"))?;

        Ok(row.dlq_id)
    }
}
