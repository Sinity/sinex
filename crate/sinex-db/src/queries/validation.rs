//! Validation query registry for data integrity checks
//!
//! This module provides queries for validating data consistency,
//! schema compliance, and integrity constraints.

use crate::constants::tables;
use crate::query_builder::{QueryBuilder, QueryParam};
use chrono::{DateTime, Utc};

/// Validation query registry
pub struct ValidationQueries;

impl ValidationQueries {
    /// Get recent events for schema validation
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<RawEventRecord>(pool)`
    pub fn get_recent_events(limit: i64) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&[
                "event_id::uuid as \"id!\"",
                "source",
                "event_type",
                "ts_orig",
                "ts_ingest",
                "host",
                "payload",
                "source_event_ids::uuid[] as \"source_event_ids?\"",
                "source_material_id::uuid as \"source_material_id?\"",
                "source_material_offset_start",
                "source_material_offset_end",
                "anchor_byte",
                "associated_blob_ids::uuid[] as \"associated_blob_ids?\"",
                "ingestor_version",
                "payload_schema_id::uuid as \"payload_schema_id?\"",
            ])
            .where_op(
                "ts_ingest",
                ">",
                QueryParam::Raw("NOW() - INTERVAL '1 hour'".to_string()),
            )
            .order_by("ts_ingest", "DESC")
            .limit(limit)
    }

    /// Get all automaton checkpoints
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CheckpointRecord>(pool)`
    pub fn get_all_checkpoints() -> QueryBuilder {
        QueryBuilder::select(tables::AUTOMATON_CHECKPOINTS).columns(&[
            "automaton_name",
            "last_processed_id::uuid as last_processed_id",
            "processed_count",
            "last_activity",
            "checkpoint_data as state_data",
        ])
    }

    /// Check if an event exists by UUID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<(i32,)>(pool)`
    pub fn event_exists(event_id_uuid: sqlx::types::Uuid) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["1"])
            .where_eq("event_id::uuid", QueryParam::Uuid(event_id_uuid))
            .limit(1)
    }

    /// Count events newer than a given UUID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_newer_events(event_id_uuid: sqlx::types::Uuid) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["COUNT(*)::bigint as count"])
            .where_op("event_id::uuid", ">", QueryParam::Uuid(event_id_uuid))
            .where_op(
                "ts_ingest",
                "<",
                QueryParam::Raw("NOW() - INTERVAL '5 minutes'".to_string()),
            )
    }

    /// Find events with null or empty payloads
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ValidationRecord>(pool)`
    pub fn find_null_payloads(limit: i64) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["event_id::uuid as id", "source", "event_type"])
            .where_op("payload", "IS", QueryParam::Raw("NULL".to_string()))
            // TODO: Implement or_where for multiple conditions
            // .or_where("payload", "=", QueryParam::Raw("'null'::jsonb".to_string()))
            .limit(limit)
    }

    /// Find events with invalid ULIDs
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ValidationRecord>(pool)`
    pub fn find_invalid_ulids(limit: i64) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["event_id::text as id_str", "source", "event_type"])
            .where_op(
                "LENGTH(event_id::text)",
                "!=",
                QueryParam::Raw("36".to_string()),
            )
            .limit(limit)
    }

    /// Find events with encoding issues in text fields
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ValidationRecord>(pool)`
    pub fn find_encoding_issues(limit: i64) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["event_id::uuid as id", "source", "event_type"])
            .where_op(
                "source",
                "~",
                QueryParam::Raw(r"'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]'".to_string()),
            )
            // TODO: Implement or_where for multiple conditions
            // .or_where("event_type", "~", QueryParam::Raw(r"'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]'".to_string()))
            // .or_where("host", "~", QueryParam::Raw(r"'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]'".to_string()))
            .limit(limit)
    }

    /// Count total events
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_total_events() -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS).columns(&["COUNT(*)::bigint as count"])
    }
}

// Special queries that need raw SQL due to complex window functions

use crate::query_helpers::{db_error, DbResult};
use sqlx::PgPool;

/// Find timestamp regressions using window functions
///
/// This uses raw SQL for complex window functions
pub async fn find_timestamp_regressions(
    pool: &PgPool,
    start_time: DateTime<Utc>,
    limit: i64,
) -> DbResult<Vec<TimestampRegressionRecord>> {
    let rows = sqlx::query_as!(
        TimestampRegressionRecord,
        r#"
        WITH ordered_events AS (
            SELECT 
                event_id::uuid as id,
                ts_orig,
                ts_ingest,
                LAG(event_id::uuid) OVER (ORDER BY event_id) as prev_id,
                LAG(ts_orig) OVER (ORDER BY event_id) as prev_ts_orig,
                LAG(ts_ingest) OVER (ORDER BY event_id) as prev_ts_ingest
            FROM core.events
            WHERE ts_ingest > $1
            ORDER BY event_id
            LIMIT 10000
        )
        SELECT 
            id, ts_orig, ts_ingest as "ts_ingest!",
            prev_id, prev_ts_orig, prev_ts_ingest as "prev_ts_ingest!"
        FROM ordered_events
        WHERE prev_id IS NOT NULL
          AND (ts_orig < prev_ts_orig OR ts_ingest < prev_ts_ingest)
        LIMIT $2
        "#,
        start_time,
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|e| db_error(e, "find timestamp regressions"))?;

    Ok(rows)
}

#[derive(sqlx::FromRow, Debug)]
pub struct TimestampRegressionRecord {
    pub id: Option<sqlx::types::Uuid>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub ts_ingest: DateTime<Utc>,
    pub prev_id: Option<sqlx::types::Uuid>,
    pub prev_ts_orig: Option<DateTime<Utc>>,
    pub prev_ts_ingest: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
pub struct InvalidTimestampRecord {
    pub id: sqlx::types::Uuid,
    pub ts_orig: Option<DateTime<Utc>>,
    pub ts_ingest: DateTime<Utc>,
}

/// Find invalid timestamps (too far in future/past)
///
/// This uses raw SQL for timestamp arithmetic
pub async fn find_invalid_timestamps(
    pool: &PgPool,
    limit: i64,
) -> DbResult<Vec<InvalidTimestampRecord>> {
    let rows = sqlx::query_as!(
        InvalidTimestampRecord,
        r#"
        SELECT event_id::uuid as "id!", ts_orig, ts_ingest as "ts_ingest!"
        FROM core.events
        WHERE ts_orig > NOW() + INTERVAL '1 hour'
           OR ts_orig < '2020-01-01'::timestamptz
           OR ts_ingest > NOW() + INTERVAL '1 hour'
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|e| db_error(e, "find invalid timestamps"))?;

    Ok(rows)
}
