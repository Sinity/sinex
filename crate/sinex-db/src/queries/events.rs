//! Event query registry for centralized event operations
//!
//! This module provides all database queries related to raw event storage,
//! retrieval, and management. All queries automatically handle ULID/UUID
//! conversion and provide consistent error handling.

use crate::constants::tables;
use crate::query_builder::{QueryBuilder, QueryParam};
use crate::query_helpers::{db_error, DbResult};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_events::constants::event_types;
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Event query registry with centralized event operations
pub struct EventQueries;

/// Standard column projection for event queries
const EVENT_COLUMNS: &[&str] = &[
    "event_id::uuid as \"id!\"",
    "source as \"source!\"",
    "event_type as \"event_type!\"",
    "ts_ingest as \"ts_ingest!\"",
    "ts_orig",
    "host as \"host!\"",
    "ingestor_version",
    "payload_schema_id::uuid as \"payload_schema_id\"",
    "payload as \"payload!\"",
    "source_event_ids::ulid[] as \"source_event_ids\"",
    "source_material_id::uuid as \"source_material_id\"",
    "source_material_offset_start",
    "source_material_offset_end",
    "anchor_byte",
    "associated_blob_ids::uuid[] as \"associated_blob_ids\"",
];

impl EventQueries {
    /// Get an event by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<RawEvent>(pool)`
    pub fn get_by_id(event_id: Ulid) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(EVENT_COLUMNS)
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Insert a new event
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<RawEvent>(pool)`
    pub fn insert_event(
        source: String,
        event_type: String,
        host: String,
        payload: JsonValue,
        ts_orig: Option<DateTime<Utc>>,
        ingestor_version: Option<String>,
        payload_schema_id: Option<Ulid>,
        source_event_ids: Option<Vec<Ulid>>,
    ) -> QueryBuilder {
        QueryBuilder::insert(tables::EVENTS)
            .columns(&[
                "source",
                "event_type",
                "host",
                "payload",
                "ts_orig",
                "ingestor_version",
                "payload_schema_id",
                "source_event_ids",
            ])
            .values(&[
                QueryParam::String(source),
                QueryParam::String(event_type),
                QueryParam::String(host),
                QueryParam::Json(payload),
                QueryParam::OptionalTimestamp(ts_orig),
                QueryParam::OptionalString(ingestor_version),
                QueryParam::OptionalUlid(payload_schema_id),
                QueryParam::OptionalUlidArray(source_event_ids),
            ])
            .returning(EVENT_COLUMNS)
    }

    /// Insert event with ULID array handling
    pub fn insert_event_with_source_ids(
        source: String,
        event_type: String,
        host: String,
        payload: JsonValue,
        ts_orig: Option<DateTime<Utc>>,
        ingestor_version: Option<String>,
        payload_schema_id: Option<Ulid>,
        source_event_ids: Option<Vec<Ulid>>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::insert(tables::EVENTS).columns(&[
            "source",
            "event_type",
            "host",
            "payload",
            "ts_orig",
            "ingestor_version",
            "payload_schema_id",
            "source_event_ids",
        ]);

        let values = vec![
            QueryParam::String(source),
            QueryParam::String(event_type),
            QueryParam::String(host),
            QueryParam::Json(payload),
            QueryParam::OptionalTimestamp(ts_orig),
            QueryParam::OptionalString(ingestor_version),
            QueryParam::OptionalUlid(payload_schema_id),
            // Handle ULID array properly
            QueryParam::OptionalUlidArray(source_event_ids),
        ];

        builder = builder.values(&values);

        builder.returning(EVENT_COLUMNS)
    }

    /// Count total events
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_all() -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS).columns(&["COUNT(*) as count"])
    }

    /// Count events by source
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_by_source(source: String) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["COUNT(*) as count"])
            .where_eq("source", QueryParam::String(source))
    }

    /// Count events by event type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_by_event_type(event_type: String) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["COUNT(*) as count"])
            .where_eq("event_type", QueryParam::String(event_type))
    }

    /// Count events by source and time range
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_by_source_and_time_range(
        source: String,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["COUNT(*) as count"])
            .where_eq("source", QueryParam::String(source))
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(start_time))
            .where_op("ts_ingest", "<=", QueryParam::Timestamp(end_time))
    }

    /// Count events by time range
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_by_time_range(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["COUNT(*) as count"])
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(start_time))
            .where_op("ts_ingest", "<=", QueryParam::Timestamp(end_time))
    }

    /// Get recent events with pagination
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<RawEvent>(pool)`
    pub fn get_recent(limit: Option<i64>, offset: Option<i64>) -> QueryBuilder {
        let mut builder = QueryBuilder::select(tables::EVENTS)
            .columns(EVENT_COLUMNS)
            .order_by("ts_ingest", "DESC");

        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }

        if let Some(offset) = offset {
            builder = builder.offset(offset);
        }

        builder
    }

    /// Get events by source with pagination
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<RawEvent>(pool)`
    pub fn get_by_source(source: String, limit: Option<i64>, offset: Option<i64>) -> QueryBuilder {
        let mut builder = QueryBuilder::select(tables::EVENTS)
            .columns(EVENT_COLUMNS)
            .where_eq("source", QueryParam::String(source))
            .order_by("ts_ingest", "DESC");

        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }

        if let Some(offset) = offset {
            builder = builder.offset(offset);
        }

        builder
    }

    /// Get events by event type with pagination
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<RawEvent>(pool)`
    pub fn get_by_event_type(
        event_type: String,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::select(tables::EVENTS)
            .columns(EVENT_COLUMNS)
            .where_eq("event_type", QueryParam::String(event_type))
            .order_by("ts_ingest", "DESC");

        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }

        if let Some(offset) = offset {
            builder = builder.offset(offset);
        }

        builder
    }

    /// Get events by multiple IDs
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<RawEvent>(pool)`
    pub fn get_by_ids(event_ids: Vec<Ulid>) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(EVENT_COLUMNS)
            .where_in("event_id", QueryParam::UlidArray(event_ids))
            .order_by("ts_ingest", "DESC")
    }

    /// Get events within time range
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<RawEvent>(pool)`
    pub fn get_by_time_range(
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::select(tables::EVENTS)
            .columns(EVENT_COLUMNS)
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(start_time))
            .where_op("ts_ingest", "<=", QueryParam::Timestamp(end_time))
            .order_by("ts_ingest", "DESC");

        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }

        if let Some(offset) = offset {
            builder = builder.offset(offset);
        }

        builder
    }

    /// Update event payload
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_payload(event_id: Ulid, payload: JsonValue) -> QueryBuilder {
        QueryBuilder::update(tables::EVENTS)
            .set("payload", QueryParam::Json(payload))
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Update event source event IDs
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_source_event_ids(event_id: Ulid, source_event_ids: Vec<Ulid>) -> QueryBuilder {
        QueryBuilder::update(tables::EVENTS)
            .set("source_event_ids", QueryParam::UlidArray(source_event_ids))
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Delete an event by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_by_id(event_id: Ulid) -> QueryBuilder {
        QueryBuilder::delete(tables::EVENTS).where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Delete events by source
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_by_source(source: String) -> QueryBuilder {
        QueryBuilder::delete(tables::EVENTS).where_eq("source", QueryParam::String(source))
    }

    /// Delete events older than timestamp
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_older_than(timestamp: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::delete(tables::EVENTS).where_op(
            "ts_ingest",
            "<",
            QueryParam::Timestamp(timestamp),
        )
    }

    /// Get events with associated blobs
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<RawEvent>(pool)`
    pub fn get_with_blobs(limit: Option<i64>, offset: Option<i64>) -> QueryBuilder {
        let mut builder = QueryBuilder::select(tables::EVENTS)
            .columns(EVENT_COLUMNS)
            .where_op(
                "associated_blob_ids",
                "IS NOT",
                QueryParam::OptionalUlid(None),
            )
            .order_by("ts_ingest", "DESC");

        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }

        if let Some(offset) = offset {
            builder = builder.offset(offset);
        }

        builder
    }

    /// Attach blob to event
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn attach_blob(event_id: Ulid, blob_id: Ulid) -> QueryBuilder {
        QueryBuilder::update(tables::EVENTS)
            .set("associated_blob_ids", QueryParam::UlidArray(vec![blob_id]))
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Detach blob from event
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn detach_blob(event_id: Ulid) -> QueryBuilder {
        QueryBuilder::update(tables::EVENTS)
            .set("associated_blob_ids", QueryParam::OptionalUlid(None))
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Insert event with attached blob
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<EventRecord>(pool)`
    pub fn insert_event_with_blob(
        source: String,
        event_type: String,
        host: String,
        payload: JsonValue,
        ts_orig: Option<DateTime<Utc>>,
        ingestor_version: Option<String>,
        payload_schema_id: Option<Ulid>,
        blob_id: Ulid,
        source_event_ids: Option<Vec<Ulid>>,
    ) -> QueryBuilder {
        QueryBuilder::insert(tables::EVENTS)
            .columns(&[
                "source",
                "event_type",
                "host",
                "payload",
                "ts_orig",
                "ingestor_version",
                "payload_schema_id",
                "associated_blob_ids",
                "source_event_ids",
            ])
            .values(&[
                QueryParam::String(source),
                QueryParam::String(event_type),
                QueryParam::String(host),
                QueryParam::Json(payload),
                QueryParam::OptionalTimestamp(ts_orig),
                QueryParam::OptionalString(ingestor_version),
                QueryParam::OptionalUlid(payload_schema_id),
                QueryParam::UlidArray(vec![blob_id]),
                QueryParam::OptionalUlidArray(source_event_ids),
            ])
            .returning(EVENT_COLUMNS)
    }

    /// Find canonical command event by time window and command text
    ///
    /// This uses custom SQL for JSON path queries and complex WHERE conditions
    pub async fn find_canonical_command_by_time_and_text(
        pool: &PgPool,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        command_text: String,
    ) -> DbResult<Option<String>> {
        let row = sqlx::query!(
            r#"
            SELECT event_id::text as event_id
            FROM core.events
            WHERE source = 'canonical.terminal'
                AND event_type = 'command.canonical'
                AND ts_ingest >= $1
                AND ts_ingest <= $2
                AND payload->>'command' = $3
                AND source_event_ids IS NOT NULL
            ORDER BY ts_ingest ASC
            LIMIT 1
            "#,
            start_time,
            end_time,
            command_text
        )
        .fetch_optional(pool)
        .await
        .map_err(|e| db_error(e, "find canonical command by time and text"))?;

        Ok(row.and_then(|r| r.event_id))
    }

    /// Get event payload by event ID (text format)
    ///
    /// This uses custom SQL for text to UUID casting
    pub async fn get_payload_by_event_id_text(
        pool: &PgPool,
        event_id_text: String,
    ) -> DbResult<JsonValue> {
        let row = sqlx::query!(
            r#"
            SELECT payload
            FROM core.events
            WHERE event_id::text = $1
            "#,
            event_id_text
        )
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, "get payload by event ID text"))?;

        Ok(row.payload)
    }

    /// Update event payload by event ID (text format)
    ///
    /// This uses custom SQL for text to UUID casting
    pub async fn update_payload_by_event_id_text(
        pool: &PgPool,
        event_id_text: String,
        payload: JsonValue,
    ) -> DbResult<sqlx::postgres::PgQueryResult> {
        let result = sqlx::query!(
            r#"
            UPDATE core.events 
            SET payload = $1
            WHERE event_id::text = $2
            "#,
            payload,
            event_id_text
        )
        .execute(pool)
        .await
        .map_err(|e| db_error(e, "update payload by event ID text"))?;

        Ok(result)
    }

    // ========================================================================
    // Analytics queries
    // ========================================================================

    /// Count events by type within a time range
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<(String, i64)>(pool)`
    pub fn count_by_type_in_range(
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["event_type", "COUNT(*) as count"])
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(start_time))
            .where_op("ts_ingest", "<=", QueryParam::Timestamp(end_time))
            .group_by("event_type")
            .order_by("count", "DESC")
    }

    /// Count events by type for all time
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<(String, i64)>(pool)`
    pub fn count_by_type_all_time() -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["event_type", "COUNT(*) as count"])
            .group_by("event_type")
            .order_by("count", "DESC")
    }

    /// Get top commands within a time range
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CommandCountRecord>(pool)`
    pub fn top_commands_in_range(
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: i64,
    ) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["payload->>'command' as command", "COUNT(*) as count"])
            .where_eq(
                "source",
                QueryParam::String("canonical.terminal".to_string()),
            )
            .where_eq(
                "event_type",
                QueryParam::String("command.canonical".to_string()),
            )
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(start_time))
            .where_op("ts_ingest", "<=", QueryParam::Timestamp(end_time))
            .where_op(
                "payload->>'command'",
                "IS NOT",
                QueryParam::String("NULL".to_string()),
            )
            .group_by("payload->>'command'")
            .order_by("count", "DESC")
            .limit(limit)
    }

    /// Get top commands for all time
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CommandCountRecord>(pool)`
    pub fn top_commands_all_time(limit: i64) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["payload->>'command' as command", "COUNT(*) as count"])
            .where_eq(
                "source",
                QueryParam::String("canonical.terminal".to_string()),
            )
            .where_eq(
                "event_type",
                QueryParam::String("command.canonical".to_string()),
            )
            .where_op(
                "payload->>'command'",
                "IS NOT",
                QueryParam::String("NULL".to_string()),
            )
            .group_by("payload->>'command'")
            .order_by("count", "DESC")
            .limit(limit)
    }

    /// Get process heartbeats within a time range
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<HeartbeatRecord>(pool)`
    pub fn get_process_heartbeats(
        process_name: String,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(EVENT_COLUMNS)
            .where_eq("source", QueryParam::String(process_name))
            .where_eq(
                "event_type",
                QueryParam::String(event_types::sinex::PROCESS_HEARTBEAT.to_string()),
            )
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(start_time))
            .where_op("ts_ingest", "<=", QueryParam::Timestamp(end_time))
            .order_by("ts_ingest", "DESC")
    }

    // ========================================================================
    // Time-series analytics queries (require TimescaleDB)
    // ========================================================================

    /// Get events over time using time buckets
    ///
    /// This uses raw SQL for TimescaleDB time_bucket function
    pub async fn get_events_over_time(
        pool: &PgPool,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        interval: sqlx::postgres::types::PgInterval,
    ) -> DbResult<Vec<TimeBucketRecord>> {
        let rows = sqlx::query_as!(
            TimeBucketRecord,
            r#"
            SELECT 
                time_bucket($1::interval, ts_ingest) as "bucket!",
                COUNT(*) as "count!"
            FROM core.events
            WHERE ts_ingest >= $2 AND ts_ingest <= $3
            GROUP BY time_bucket($1::interval, ts_ingest)
            ORDER BY time_bucket($1::interval, ts_ingest) ASC
            "#,
            interval,
            start_time,
            end_time
        )
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, "get events over time"))?;

        Ok(rows)
    }

    /// Get activity heatmap using time buckets
    ///
    /// This uses raw SQL for TimescaleDB time_bucket function
    pub async fn get_activity_heatmap(
        pool: &PgPool,
        interval: sqlx::postgres::types::PgInterval,
        limit: i64,
    ) -> DbResult<Vec<TimeBucketRecord>> {
        let rows = sqlx::query_as!(
            TimeBucketRecord,
            r#"
            SELECT 
                time_bucket($1::interval, ts_ingest) as "bucket!",
                COUNT(*) as "count!"
            FROM core.events
            GROUP BY time_bucket($1::interval, ts_ingest)
            ORDER BY COUNT(*) DESC
            LIMIT $2
            "#,
            interval,
            limit
        )
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, "get activity heatmap"))?;

        Ok(rows)
    }
}

/// Record type for time bucket results
#[derive(Debug, sqlx::FromRow)]
pub struct TimeBucketRecord {
    pub bucket: DateTime<Utc>,
    pub count: i64,
}
