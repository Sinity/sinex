use super::conversions::{EventRecordExt, records_to_events};
use super::persistence::{
    BatchViolation, EventAnnotation, EventRepository, InvalidPayloadEvent, InvalidTimestamp,
    SuspiciousEvent,
};
use crate::EventRecord;
use crate::JsonValue;
use crate::models::Event;
use crate::repositories::common::{DbResult, db_error};
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::{Id, Pagination};
use sqlx::types::Json;
use tracing::{instrument, warn};

impl EventRepository<'_> {
    #[instrument(skip(self), fields(event_id = %id))]
    pub async fn get_by_id(&self, id: Id<Event<JsonValue>>) -> DbResult<Option<Event<JsonValue>>> {
        let record = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE id = $1"
        ))
        .bind(id.to_uuid())
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get event by id"))?;

        record.map(|r| r.try_to_event()).transpose()
    }

    #[instrument(skip(self))]
    pub async fn count_all(&self) -> DbResult<i64> {
        let result = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "count all events"))?;

        Ok(result.unwrap_or(0))
    }

    #[instrument(skip(self))]
    pub async fn count_all_estimate(&self) -> DbResult<i64> {
        let estimate = sqlx::query_scalar!(
            r#"
            SELECT COALESCE(reltuples::bigint, 0) as "estimate!"
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'core' AND c.relname = 'events'
            "#
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "estimate event count"))?;

        Ok(estimate)
    }

    #[instrument(skip(self), fields(limit = limit))]
    pub async fn get_recent(&self, limit: i64) -> DbResult<Vec<Event<JsonValue>>> {
        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events ORDER BY ts_coided DESC LIMIT $1"
        ))
        .bind(limit)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent events"))?;

        records_to_events(records)
    }

    #[instrument(
        skip(self),
        fields(source = %source, limit = pagination.limit(), offset = pagination.offset())
    )]
    pub async fn get_by_source(
        &self,
        source: &EventSource,
        pagination: Pagination,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let (limit, offset) = pagination.as_tuple();

        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE source = $1 ORDER BY ts_coided DESC LIMIT $2 OFFSET $3"
        ))
        .bind(source.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by source"))?;

        records_to_events(records)
    }

    #[instrument(
        skip(self),
        fields(event_type = %event_type, limit = pagination.limit(), offset = pagination.offset())
    )]
    pub async fn get_by_event_type(
        &self,
        event_type: &EventType,
        pagination: Pagination,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let (limit, offset) = pagination.as_tuple();

        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE event_type = $1 ORDER BY ts_coided DESC LIMIT $2 OFFSET $3"
        ))
        .bind(event_type.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by type"))?;

        records_to_events(records)
    }

    #[instrument(skip(self), fields(source = %source))]
    pub async fn count_by_source(&self, source: &EventSource) -> DbResult<i64> {
        let result = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.events WHERE source = $1",
            source.as_str()
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by source"))?;

        Ok(result.unwrap_or(0))
    }

    #[instrument(skip(self), fields(source = %source))]
    pub async fn estimate_count_by_source(&self, source: &EventSource) -> DbResult<i64> {
        // EXPLAIN output shape is not supported by sqlx macros; use runtime query.
        let plan: Json<serde_json::Value> = sqlx::query_scalar(
            r"
            EXPLAIN (FORMAT JSON)
            SELECT 1 FROM core.events WHERE source = $1
            ",
        )
        .bind(source.as_str())
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "estimate event count by source"))?;

        Ok(extract_plan_rows(plan.0))
    }

    #[instrument(skip(self), fields(event_type = %event_type))]
    pub async fn count_by_event_type(&self, event_type: &EventType) -> DbResult<i64> {
        let result = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.events WHERE event_type = $1",
            event_type.as_str()
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by type"))?;

        Ok(result.unwrap_or(0))
    }

    #[instrument(skip(self), fields(event_type = %event_type))]
    pub async fn estimate_count_by_event_type(&self, event_type: &EventType) -> DbResult<i64> {
        // EXPLAIN output shape is not supported by sqlx macros; use runtime query.
        let plan: Json<serde_json::Value> = sqlx::query_scalar(
            r"
            EXPLAIN (FORMAT JSON)
            SELECT 1 FROM core.events WHERE event_type = $1
            ",
        )
        .bind(event_type.as_str())
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "estimate event count by type"))?;

        Ok(extract_plan_rows(plan.0))
    }

    #[instrument(
        skip(self),
        fields(
            start = %start,
            end = %end,
            limit = pagination.limit(),
            offset = pagination.offset()
        )
    )]
    pub async fn get_by_time_range(
        &self,
        start: Timestamp,
        end: Timestamp,
        pagination: Pagination,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let (limit, offset) = pagination.as_tuple();

        // Use index hint for TimescaleDB optimization on time-range queries
        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE ts_coided >= $1 AND ts_coided <= $2 ORDER BY ts_coided DESC LIMIT $3 OFFSET $4"
        ))
        .bind(start)
        .bind(end)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by time range"))?;

        records_to_events(records)
    }

    #[instrument(skip(self), fields(start = %start, end = %end))]
    pub async fn estimate_count_by_time_range(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> DbResult<i64> {
        // EXPLAIN output shape is not supported by sqlx macros; use runtime query.
        let plan: Json<serde_json::Value> = sqlx::query_scalar(
            r"
            EXPLAIN (FORMAT JSON)
            SELECT 1 FROM core.events WHERE ts_coided >= $1 AND ts_coided <= $2
            ",
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "estimate event count by time range"))?;

        Ok(extract_plan_rows(plan.0))
    }

    pub async fn get_process_heartbeats(
        &self,
        source: &EventSource,
        start: Timestamp,
        end: Timestamp,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE source = $1 AND event_type = 'process.heartbeat' AND ts_coided >= $2 AND ts_coided <= $3 ORDER BY ts_coided ASC LIMIT 10000"
        ))
        .bind(source.as_str())
        .bind(start)
        .bind(end)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get process heartbeats"))?;

        records_to_events(records)
    }

    // ========== Event Annotations ==========

    /// Get annotations for an event
    pub async fn get_annotations(
        &self,
        id: Id<Event<JsonValue>>,
    ) -> DbResult<Vec<EventAnnotation>> {
        sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT
                id as "id!: Id<EventAnnotation>",
                event_id::uuid as "event_id!: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at: Timestamp",
                updated_at as "updated_at: Timestamp"
            FROM core.event_annotations
            WHERE event_id::uuid = $1
            ORDER BY created_at DESC
            "#,
            *id.as_uuid() as _
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get annotations"))
    }

    /// Get annotations by type
    pub async fn get_annotations_by_type(
        &self,
        annotation_type: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<EventAnnotation>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT
                id as "id!: Id<EventAnnotation>",
                event_id::uuid as "event_id!: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at: Timestamp",
                updated_at as "updated_at: Timestamp"
            FROM core.event_annotations
            WHERE annotation_type = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
            annotation_type,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get annotations by type"))
    }

    /// Search annotations by content
    ///
    /// # Performance Note
    /// This query uses `ILIKE '%term%'` which requires a full table scan and cannot use indexes.
    /// For large annotation tables, this may be slow. Consider:
    /// - Adding a GIN index with pg_trgm for LIKE queries: `CREATE INDEX ON event_annotations USING gin (content gin_trgm_ops);`
    /// - Or using full-text search with tsvector if semantic search is needed
    /// - Limiting usage to small datasets or adding additional filters (annotation_type, date range)
    pub async fn search_annotations(
        &self,
        query: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<EventAnnotation>> {
        let limit = limit.unwrap_or(100);

        let rows = sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT
                id as "id!: Id<EventAnnotation>",
                event_id::uuid as "event_id!: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at: Timestamp",
                updated_at as "updated_at: Timestamp"
            FROM core.event_annotations
            WHERE content ILIKE $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
            format!("%{}%", query),
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "search annotations"))?;

        Ok(rows)
    }

    // ========== Data Quality Checks (replacing validation queries) ==========

    /// Find events with null or empty payloads
    pub async fn find_invalid_payloads(&self, limit: i64) -> DbResult<Vec<InvalidPayloadEvent>> {
        sqlx::query!(
            r#"
            SELECT
                id::uuid as "id!",
                source as "source!",
                event_type as "event_type!",
                ts_coided as "ts_coided: Timestamp",
                payload as "payload!"
            FROM core.events
            WHERE payload IS NULL OR payload = 'null'::jsonb OR payload = '{}'::jsonb
            ORDER BY ts_coided DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find invalid payloads"))
        .map(|rows| {
            rows.into_iter()
                .map(|row| InvalidPayloadEvent {
                    event_id: Id::<Event<JsonValue>>::from_uuid(row.id),
                    source: row.source.into(),
                    event_type: row.event_type.into(),
                    ts_coided: row.ts_coided,
                    payload: row.payload,
                })
                .collect()
        })
    }

    /// Find events with timestamp regressions
    pub async fn find_timestamp_regressions(
        &self,
        limit: i64,
    ) -> DbResult<
        Vec<(
            Id<Event<JsonValue>>,
            Id<Event<JsonValue>>,
            Timestamp,
            Timestamp,
        )>,
    > {
        let rows = sqlx::query!(
            r#"
            WITH ordered_events AS (
                SELECT
                    id,
                    ts_orig,
                    LAG(id) OVER (PARTITION BY source ORDER BY id) as prev_id,
                    LAG(ts_orig) OVER (PARTITION BY source ORDER BY id) as prev_ts
                FROM core.events
                WHERE ts_orig IS NOT NULL
            )
            SELECT
                id::uuid as "id!",
                prev_id::uuid as "prev_id!",
                ts_orig as "ts_orig: Timestamp",
                prev_ts as "prev_ts: Timestamp"
            FROM ordered_events
            WHERE prev_ts IS NOT NULL AND ts_orig < prev_ts
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find timestamp regressions"))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    Id::<Event<JsonValue>>::from_uuid(r.id),
                    Id::<Event<JsonValue>>::from_uuid(r.prev_id),
                    r.ts_orig,
                    r.prev_ts.unwrap_or(r.ts_orig),
                )
            })
            .collect())
    }

    // ========== Data Integrity Checks ==========

    /// Find batch monotonicity violations
    pub async fn find_batch_violations(
        &self,
        days_back: i32,
        max_violations: i64,
    ) -> DbResult<Vec<BatchViolation>> {
        let rows = sqlx::query_as!(
            BatchViolation,
            r#"
            WITH event_batches AS (
                SELECT
                    id,
                    ts_orig,
                    source,
                    ROW_NUMBER() OVER (ORDER BY id) as row_num,
                    LAG(id) OVER (ORDER BY id) as prev_event_id,
                    LAG(ts_orig) OVER (ORDER by id) as prev_ts_orig
                FROM core.events
                WHERE ts_coided > NOW() - INTERVAL '1 day' * $1
                ORDER BY id DESC
                LIMIT 10000
            )
            SELECT
                id as "event_id?: Id<Event<JsonValue>>",
                prev_event_id as "prev_event_id?: Id<Event<JsonValue>>",
                ts_orig as "ts_orig: Timestamp",
                prev_ts_orig as "prev_ts_orig: Timestamp",
                source,
                row_num
            FROM event_batches
            WHERE prev_event_id IS NOT NULL
              AND (ts_orig < prev_ts_orig OR id < prev_event_id)
            LIMIT $2
            "#,
            days_back as f64,
            max_violations
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find batch violations"))?;

        Ok(rows)
    }

    /// Find events with suspicious payloads
    pub async fn find_suspicious_events(
        &self,
        days_back: i32,
        size_threshold: i32,
    ) -> DbResult<Vec<SuspiciousEvent>> {
        let rows = sqlx::query_as!(
            SuspiciousEvent,
            r#"
            SELECT
                id::uuid as "event_id!: Id<Event<JsonValue>>",
                source as "source!",
                event_type as "event_type!",
                payload as "payload!",
                jsonb_typeof(payload) as payload_type,
                pg_column_size(payload) as payload_size
            FROM core.events
            WHERE ts_coided > NOW() - INTERVAL '1 day' * $1
              AND (
                jsonb_typeof(payload) NOT IN ('object', 'array')
                OR pg_column_size(payload) > $2
                OR payload = '{}'::jsonb
                OR payload = 'null'::jsonb
              )
            ORDER BY ts_coided DESC
            LIMIT 100
            "#,
            days_back as f64,
            size_threshold
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find suspicious events"))?;

        Ok(rows)
    }

    /// Find events with invalid timestamps (too far in future/past)
    pub async fn find_invalid_timestamps(&self, limit: i64) -> DbResult<Vec<InvalidTimestamp>> {
        let rows = sqlx::query_as!(
            InvalidTimestamp,
            r#"
            SELECT
                id::uuid as "event_id!: Id<Event<JsonValue>>",
                ts_orig as "ts_orig: Timestamp",
                ts_coided as "ts_coided: Timestamp"
            FROM core.events
            WHERE ts_orig > NOW() + INTERVAL '1 hour'
               OR ts_orig < '2020-01-01'::timestamptz
               OR ts_coided > NOW() + INTERVAL '1 hour'
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find invalid timestamps"))?;

        Ok(rows)
    }

    // ========== Test Support Operations ==========

    /// Get a test event by ID
    pub async fn get_test_event(
        &self,
        id: Id<Event<JsonValue>>,
    ) -> DbResult<Option<Event<JsonValue>>> {
        self.get_by_id(id).await
    }

    /// Get events by multiple IDs efficiently (prevents N+1 queries)
    pub async fn get_by_ids(
        &self,
        ids: &[Id<Event<JsonValue>>],
    ) -> DbResult<Vec<Event<JsonValue>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let ids = if ids.len() > 1000 {
            warn!(
                count = ids.len(),
                "get_by_ids called with too many IDs, clamping to 1000"
            );
            &ids[..1000]
        } else {
            ids
        };

        let uuids: Vec<uuid::Uuid> = ids.iter().map(|id| id.to_uuid()).collect();

        let records = sqlx::query_as::<_, EventRecord>(&format!(
            "SELECT {} FROM core.events WHERE id::uuid = ANY($1::uuid[]) ORDER BY ts_coided DESC",
            event_select_columns!()
        ))
        .bind(&uuids)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by ids"))?;

        records_to_events(records)
    }
}

pub(crate) fn extract_plan_rows(plan: serde_json::Value) -> i64 {
    plan.get(0)
        .and_then(|entry| entry.get("Plan"))
        .and_then(|entry| entry.get("Plan Rows"))
        .and_then(|rows| rows.as_i64())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::extract_plan_rows;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn extract_plan_rows_reads_estimate() -> TestResult<()> {
        let plan = serde_json::json!([{"Plan": {"Plan Rows": 42}}]);
        assert_eq!(extract_plan_rows(plan), 42);
        Ok(())
    }
}
