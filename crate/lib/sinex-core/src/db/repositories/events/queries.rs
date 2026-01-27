use super::conversions::{records_to_events, EventRecordExt, EventSearchRow};
use super::event_select_columns;
use super::persistence::{
    BatchViolation, CommandCount, EventAnnotation, EventRepository, EventTypeCount,
    InvalidPayloadEvent, InvalidTimestamp, SourceActivity, SuspiciousEvent,
};
use crate::models::{Event, JsonValue};
use crate::query_helpers::ulid_to_uuid;
use crate::repositories::common::{db_error, DbResult, EventSearchFilters, TimeBucketResult};
use crate::types::domain::{EventSource, EventType};
use crate::types::{Id, Pagination};
use crate::EventRecord;
use chrono::{DateTime, Utc};
use sqlx::{types::Json, Postgres, QueryBuilder, Row};
use tracing::{instrument, warn};

impl<'a> EventRepository<'a> {
    #[instrument(skip(self), fields(event_id = %id))]
    pub async fn get_by_id(&self, id: Id<Event<JsonValue>>) -> DbResult<Option<Event<JsonValue>>> {
        let record = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE id::uuid = $1"
        ))
        .bind(ulid_to_uuid(*id.as_ulid()))
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get event by id"))?;

        Ok(record.map(|r| r.try_to_event()).transpose()?)
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
            " FROM core.events ORDER BY ts_ingest DESC LIMIT $1"
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
            " FROM core.events WHERE source = $1 ORDER BY ts_ingest DESC LIMIT $2 OFFSET $3"
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
            " FROM core.events WHERE event_type = $1 ORDER BY ts_ingest DESC LIMIT $2 OFFSET $3"
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
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT 1 FROM core.events WHERE source = $1
            "#,
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
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT 1 FROM core.events WHERE event_type = $1
            "#,
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
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        pagination: Pagination,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let (limit, offset) = pagination.as_tuple();

        // Use index hint for TimescaleDB optimization on time-range queries
        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE ts_ingest >= $1 AND ts_ingest <= $2 ORDER BY ts_ingest DESC LIMIT $3 OFFSET $4"
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
    pub async fn count_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<i64> {
        // Use approximate count for better performance on large time ranges
        let result = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM core.events
            WHERE ts_ingest >= $1 AND ts_ingest <= $2
            "#,
            start,
            end
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by time range"))?;

        Ok(result.unwrap_or(0))
    }

    #[instrument(skip(self), fields(start = %start, end = %end))]
    pub async fn estimate_count_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<i64> {
        // EXPLAIN output shape is not supported by sqlx macros; use runtime query.
        let plan: Json<serde_json::Value> = sqlx::query_scalar(
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT 1 FROM core.events WHERE ts_ingest >= $1 AND ts_ingest <= $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "estimate event count by time range"))?;

        Ok(extract_plan_rows(plan.0))
    }

    pub async fn get_events_by_type_and_time_range(
        &self,
        event_type: &EventType,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        pagination: Pagination,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let (limit, offset) = pagination.as_tuple();

        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE event_type = $1 AND ts_ingest >= $2 AND ts_ingest <= $3 ORDER BY ts_ingest DESC LIMIT $4 OFFSET $5"
        ))
        .bind(event_type.as_str())
        .bind(start)
        .bind(end)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by type and time range"))?;

        records_to_events(records)
    }

    pub async fn get_process_heartbeats(
        &self,
        source: &EventSource,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE source = $1 AND event_type = 'process.heartbeat' AND ts_ingest >= $2 AND ts_ingest <= $3 ORDER BY ts_ingest ASC LIMIT 10000"
        ))
        .bind(source.as_str())
        .bind(start)
        .bind(end)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get process heartbeats"))?;

        records_to_events(records)
    }

    #[instrument(
        skip(self, filters),
        fields(
            limit = filters.pagination.limit(),
            offset = filters.pagination.offset(),
            sources = filters.sources.len(),
            event_types = filters.event_types.len(),
            has_text = filters.text_query.is_some()
        )
    )]
    pub async fn search(&self, filters: EventSearchFilters) -> DbResult<Vec<EventSearchRow>> {
        let EventSearchFilters {
            sources,
            event_types,
            host,
            payload_contains,
            text_query,
            time_range,
            pagination,
        } = filters;

        let text_query = text_query.and_then(|text| {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT id::uuid AS id, source, event_type, host, ts_ingest, payload",
        );

        if let Some(text) = text_query.as_deref() {
            query.push(
                ", ts_rank_cd(to_tsvector('simple', payload::text), websearch_to_tsquery('simple', ",
            );
            query.push_bind(text);
            query.push("))::float8 AS score");
            query.push(", ts_headline('simple', payload::text, websearch_to_tsquery('simple', ");
            query.push_bind(text);
            query.push("), 'MaxFragments=2, MinWords=8, MaxWords=24') AS snippet");
        } else {
            query.push(", NULL::float8 AS score, NULL::text AS snippet");
        }

        query.push(" FROM core.events");

        query.push(" WHERE TRUE");

        if !sources.is_empty() {
            let values: Vec<String> = sources.iter().map(|s| s.as_str().to_string()).collect();
            query.push(" AND source = ANY(");
            query.push_bind(values);
            query.push(")");
        }

        if !event_types.is_empty() {
            let values: Vec<String> = event_types.iter().map(|t| t.as_str().to_string()).collect();
            query.push(" AND event_type = ANY(");
            query.push_bind(values);
            query.push(")");
        }

        if let Some(host) = host {
            query.push(" AND host = ");
            query.push_bind(host.into_string());
        }

        if let Some(range) = time_range {
            if let Some(start) = range.start() {
                query.push(" AND ts_orig >= ");
                query.push_bind(start);
            }
            if let Some(end) = range.end() {
                query.push(" AND ts_orig <= ");
                query.push_bind(end);
            }
        }

        if let Some(payload_filter) = payload_contains {
            query.push(" AND payload @> ");
            query.push_bind(payload_filter);
        }

        if let Some(text) = text_query.as_deref() {
            query.push(
                " AND to_tsvector('simple', payload::text) @@ websearch_to_tsquery('simple', ",
            );
            query.push_bind(text);
            query.push(")");
        }

        if text_query.is_some() {
            query.push(" ORDER BY score DESC, ts_ingest DESC, id DESC");
        } else {
            query.push(" ORDER BY ts_ingest DESC, id DESC");
        }
        query.push(" LIMIT ");
        query.push_bind(pagination.limit());
        query.push(" OFFSET ");
        query.push_bind(pagination.offset());

        query
            .build_query_as::<EventSearchRow>()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "search events"))
    }

    #[instrument(skip(self), fields(interval = interval, start = %start, end = %end))]
    pub async fn time_series_aggregate(
        &self,
        interval: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<Vec<TimeBucketResult>> {
        let mut builder = QueryBuilder::<Postgres>::new("SELECT time_bucket(");
        builder.push_bind(interval);
        builder.push("::interval, ts_ingest) AS bucket, COUNT(id) AS count FROM core.events WHERE ts_ingest >= ");
        builder.push_bind(start);
        builder.push(" AND ts_ingest <= ");
        builder.push_bind(end);
        builder.push(" GROUP BY bucket ORDER BY bucket ASC");

        builder
            .build_query_as::<TimeBucketResult>()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "time series aggregate"))
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
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            FROM core.event_annotations
            WHERE event_id = $1
            ORDER BY created_at DESC
            "#,
            *id.as_ulid() as _
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
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
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
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
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
                ts_ingest as "ts_ingest!",
                payload as "payload!"
            FROM core.events
            WHERE payload IS NULL OR payload = 'null'::jsonb OR payload = '{}'::jsonb
            ORDER BY ts_ingest DESC
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
                    source: row.source,
                    event_type: row.event_type,
                    ts_ingest: row.ts_ingest,
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
            DateTime<Utc>,
            DateTime<Utc>,
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
                ts_orig as "ts_orig!",
                prev_ts as "prev_ts!"
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
                    r.prev_ts,
                )
            })
            .collect())
    }

    // ========== Data Integrity Checks (from old integrity module) ==========

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
                WHERE ts_ingest > NOW() - INTERVAL '1 day' * $1
                ORDER BY id DESC
                LIMIT 10000
            )
            SELECT
                id as "event_id?: Id<Event<JsonValue>>",
                prev_event_id as "prev_event_id?: Id<Event<JsonValue>>",
                ts_orig,
                prev_ts_orig,
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
                id as "event_id: Id<Event<JsonValue>>",
                source as "source!",
                event_type as "event_type!",
                payload as "payload!",
                jsonb_typeof(payload) as payload_type,
                pg_column_size(payload) as payload_size
            FROM core.events
            WHERE ts_ingest > NOW() - INTERVAL '1 day' * $1
              AND (
                jsonb_typeof(payload) NOT IN ('object', 'array')
                OR pg_column_size(payload) > $2
                OR payload = '{}'::jsonb
                OR payload = 'null'::jsonb
              )
            ORDER BY ts_ingest DESC
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
                id as "event_id: Id<Event<JsonValue>>",
                ts_orig,
                ts_ingest as "ts_ingest!"
            FROM core.events
            WHERE ts_orig > NOW() + INTERVAL '1 hour'
               OR ts_orig < '2020-01-01'::timestamptz
               OR ts_ingest > NOW() + INTERVAL '1 hour'
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

    // ========== Analytics Queries ==========

    /// Get top terminal commands
    pub async fn top_commands(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: i64,
    ) -> DbResult<Vec<CommandCount>> {
        let rows = sqlx::query(
            r#"
            SELECT
                payload->>'command' as command,
                COUNT(*) as count
            FROM core.events
            WHERE event_type IN ('command.executed','terminal.command','command.imported')
              AND ts_orig >= $1
              AND ts_orig < $2
              AND payload->>'command' IS NOT NULL
            GROUP BY payload->>'command'
            ORDER BY count DESC
            LIMIT $3
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(limit)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "top commands"))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let command = r
                    .try_get::<Option<String>, _>("command")
                    .unwrap_or(None)
                    .unwrap_or_default();
                let count = r
                    .try_get::<Option<i64>, _>("count")
                    .unwrap_or(Some(0))
                    .unwrap_or(0);
                CommandCount { command, count }
            })
            .collect())
    }

    /// Get top commands all time
    pub async fn top_commands_all_time(&self, limit: i64) -> DbResult<Vec<CommandCount>> {
        let rows = sqlx::query(
            r#"
            SELECT
                payload->>'command' as command,
                COUNT(*) as count
            FROM core.events
            WHERE event_type IN ('command.executed','terminal.command','command.imported')
              AND payload->>'command' IS NOT NULL
            GROUP BY payload->>'command'
            ORDER BY count DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "top commands all time"))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let command = r
                    .try_get::<Option<String>, _>("command")
                    .unwrap_or(None)
                    .unwrap_or_default();
                let count = r
                    .try_get::<Option<i64>, _>("count")
                    .unwrap_or(Some(0))
                    .unwrap_or(0);
                CommandCount { command, count }
            })
            .collect())
    }

    /// Get source activity statistics with proper pagination
    pub async fn get_source_activity(
        &self,
        since: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> DbResult<Vec<SourceActivity>> {
        let limit = limit.unwrap_or(100); // Default limit to prevent unbounded queries

        let rows = sqlx::query!(
            r#"
            SELECT
                source,
                COUNT(*) as event_count,
                MIN(ts_orig) as first_event,
                MAX(ts_orig) as last_event
            FROM core.events
            WHERE ts_orig >= $1
              AND ($2::timestamptz IS NULL OR ts_orig <= $2)
            GROUP BY source
            ORDER BY event_count DESC
            LIMIT $3
            "#,
            since,
            end,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get source activity"))?;

        Ok(rows
            .into_iter()
            .map(|r| SourceActivity {
                source: r.source,
                event_count: r.event_count.unwrap_or(0),
                first_event: r.first_event,
                last_event: r.last_event,
            })
            .collect())
    }

    /// Count events by type in time range
    pub async fn count_by_type_in_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<Vec<EventTypeCount>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                event_type,
                COUNT(*) as count
            FROM core.events
            WHERE ts_orig >= $1 AND ts_orig < $2
            GROUP BY event_type
            ORDER BY count DESC
            "#,
            start,
            end
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "count by type in range"))?;

        Ok(rows
            .into_iter()
            .map(|r| EventTypeCount {
                event_type: r.event_type,
                count: r.count.unwrap_or(0),
            })
            .collect())
    }

    /// Count events by type all time with proper pagination
    pub async fn count_by_type_all_time(
        &self,
        limit: Option<i64>,
    ) -> DbResult<Vec<EventTypeCount>> {
        let limit = limit.unwrap_or(100); // Prevent unbounded queries

        let rows = sqlx::query!(
            r#"
            SELECT
                event_type,
                COUNT(*) as count
            FROM core.events
            GROUP BY event_type
            ORDER BY count DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "count by type all time"))?;

        Ok(rows
            .into_iter()
            .map(|r| EventTypeCount {
                event_type: r.event_type,
                count: r.count.unwrap_or(0),
            })
            .collect())
    }

    /// Get events over time using TimescaleDB time buckets with proper limits
    ///
    /// This uses raw SQL for TimescaleDB time_bucket function
    pub async fn get_events_over_time(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        interval: sqlx::postgres::types::PgInterval,
        limit: Option<i64>,
    ) -> DbResult<Vec<TimeBucketResult>> {
        let limit = limit.unwrap_or(1000); // Reasonable default for time series data

        let rows = sqlx::query_as!(
            TimeBucketResult,
            r#"
            SELECT
                time_bucket($1::interval, COALESCE(ts_orig, ts_ingest)) as "bucket!",
                COUNT(*) as "count!"
            FROM core.events
            WHERE COALESCE(ts_orig, ts_ingest) >= $2 AND COALESCE(ts_orig, ts_ingest) <= $3
            GROUP BY time_bucket($1::interval, COALESCE(ts_orig, ts_ingest))
            ORDER BY time_bucket($1::interval, COALESCE(ts_orig, ts_ingest)) ASC
            LIMIT $4
            "#,
            interval,
            start_time,
            end_time,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events over time"))?;

        Ok(rows)
    }

    /// Get activity heatmap using TimescaleDB time buckets
    ///
    /// This uses raw SQL for TimescaleDB time_bucket function
    pub async fn get_activity_heatmap(
        &self,
        interval: sqlx::postgres::types::PgInterval,
        limit: i64,
    ) -> DbResult<Vec<TimeBucketResult>> {
        let rows = sqlx::query_as!(
            TimeBucketResult,
            r#"
            SELECT
                time_bucket($1::interval, COALESCE(ts_orig, ts_ingest)) as "bucket!",
                COUNT(*) as "count!"
            FROM core.events
            GROUP BY time_bucket($1::interval, COALESCE(ts_orig, ts_ingest))
            ORDER BY COUNT(*) DESC
            LIMIT $2
            "#,
            interval,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get activity heatmap"))?;

        Ok(rows)
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

        let uuids: Vec<uuid::Uuid> = ids.iter().map(|id| ulid_to_uuid(*id.as_ulid())).collect();

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT
                id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids
            FROM core.events
            WHERE id::uuid = ANY($1::uuid[])
            ORDER BY ts_ingest DESC
            "#,
        )
        .bind(&uuids)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by ids"))?;

        records_to_events(records)
    }

    /// Get recent events for multiple sources efficiently
    pub async fn get_recent_by_sources(
        &self,
        sources: &[EventSource],
        limit_per_source: i64,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        if sources.is_empty() {
            return Ok(Vec::new());
        }

        // Clamp limit per source to prevent massive result sets
        let limit_per_source = limit_per_source.min(1000).max(1);
        // Total hard limit for entire query
        const TOTAL_MAX_ROWS: i64 = 10000;

        let source_strings: Vec<String> = sources.iter().map(|s| s.as_str().to_string()).collect();

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT
                id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY source ORDER BY ts_ingest DESC) as rn
            FROM core.events
            WHERE source = ANY($1::text[])
            ) ranked_events
            WHERE rn <= $2
            ORDER BY source, ts_ingest DESC
            LIMIT $3
            "#,
        )
        .bind(&source_strings)
        .bind(limit_per_source)
        .bind(TOTAL_MAX_ROWS)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent by sources"))?;

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
