//! Integrity query registry for data consistency checks
//!
//! This module provides queries for checking data integrity,
//! monotonicity, and checkpoint consistency.

use crate::constants::tables;
use crate::query_builder::{QueryBuilder, QueryParam};
use chrono::{DateTime, Utc};

/// Integrity query registry
pub struct IntegrityQueries;

impl IntegrityQueries {
    /// Get checkpoint details for an automaton
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<CheckpointDetail>(pool)`
    pub fn get_checkpoint(automaton_name: String) -> QueryBuilder {
        QueryBuilder::select(tables::AUTOMATON_CHECKPOINTS)
            .columns(&[
                "last_processed_id::uuid",
                "processed_count",
                "last_activity",
            ])
            .where_eq("automaton_name", QueryParam::String(automaton_name))
    }

    /// Check if an event exists
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<(i32,)>(pool)`
    pub fn event_exists(event_id_uuid: sqlx::types::Uuid) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["1"])
            .where_eq("event_id::uuid", QueryParam::Uuid(event_id_uuid))
            .limit(1)
    }

    /// Get expected automatons from processor manifests
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<(String,)>(pool)`
    pub fn get_expected_automatons() -> QueryBuilder {
        QueryBuilder::select("core.processor_manifests")
            .columns(&["DISTINCT processor_name"])
            .where_eq("processor_type", QueryParam::String("automaton".to_string()))
    }
}

// Special queries that need raw SQL due to complex window functions

use sqlx::PgPool;
use crate::query_helpers::{db_error, DbResult};

#[derive(sqlx::FromRow)]
pub struct BatchViolationRecord {
    pub event_id: Option<sqlx::types::Uuid>,
    pub prev_event_id: Option<sqlx::types::Uuid>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub prev_ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub row_num: Option<i64>,
}

/// Find batch monotonicity violations using window functions
///
/// This uses raw SQL for complex window functions
pub async fn find_batch_violations(
    pool: &PgPool,
    days_back: i32,
    max_violations: i64,
) -> DbResult<Vec<BatchViolationRecord>> {

    let rows = sqlx::query_as!(
        BatchViolationRecord,
        r#"
        WITH event_batches AS (
            SELECT 
                event_id::uuid as event_id,
                ts_orig,
                source,
                ROW_NUMBER() OVER (ORDER BY event_id) as row_num,
                LAG(event_id::uuid) OVER (ORDER BY event_id) as prev_event_id,
                LAG(ts_orig) OVER (ORDER BY event_id) as prev_ts_orig
            FROM core.events
            WHERE ts_ingest > NOW() - INTERVAL '1 day' * $1
            ORDER BY event_id DESC
            LIMIT 10000
        )
        SELECT 
            event_id,
            prev_event_id,
            ts_orig,
            prev_ts_orig,
            source,
            row_num
        FROM event_batches
        WHERE prev_event_id IS NOT NULL
          AND (ts_orig < prev_ts_orig OR event_id < prev_event_id)
        LIMIT $2
        "#,
        days_back as f64,
        max_violations
    )
    .fetch_all(pool)
    .await
    .map_err(|e| db_error(e, "find batch violations"))?;

    Ok(rows)
}

#[derive(sqlx::FromRow)]
pub struct SuspiciousEventRecord {
    pub event_id: sqlx::types::Uuid,
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub payload_type: Option<String>,
    pub payload_size: Option<i32>,
}

/// Find events with suspicious payloads
///
/// This uses raw SQL for payload analysis
pub async fn find_suspicious_events(
    pool: &PgPool,
    days_back: i32,
    size_threshold: i32,
) -> DbResult<Vec<SuspiciousEventRecord>> {

    let rows = sqlx::query_as!(
        SuspiciousEventRecord,
        r#"
        SELECT 
            event_id::uuid as "event_id!",
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
            OR payload @> '{}'::jsonb
            OR payload = 'null'::jsonb
          )
        ORDER BY ts_ingest DESC
        LIMIT 100
        "#,
        days_back as f64,
        size_threshold
    )
    .fetch_all(pool)
    .await
    .map_err(|e| db_error(e, "find suspicious events"))?;

    Ok(rows)
}

#[derive(sqlx::FromRow)]
pub struct CheckpointGapRecord {
    pub automaton_name: String,
    pub last_processed_id: Option<sqlx::types::Uuid>,
    pub processed_count: Option<i64>,
    pub last_activity: Option<DateTime<Utc>>,
    pub events_after_checkpoint: Option<i64>,
    pub first_unprocessed_event_time: Option<DateTime<Utc>>,
    pub last_unprocessed_event_time: Option<DateTime<Utc>>,
    pub processing_delay_seconds: Option<i64>,
}

/// Analyze checkpoint gaps and processing delays
///
/// This uses raw SQL for complex aggregations
pub async fn analyze_checkpoint_gaps(pool: &PgPool) -> DbResult<Vec<CheckpointGapRecord>> {

    let rows = sqlx::query_as!(
        CheckpointGapRecord,
        r#"
        WITH checkpoint_analysis AS (
            SELECT 
                ac.automaton_name,
                ac.last_processed_id::uuid as last_processed_id,
                ac.processed_count,
                ac.last_activity,
                COUNT(e.event_id) as events_after_checkpoint,
                MIN(e.ts_ingest) as first_unprocessed_event_time,
                MAX(e.ts_ingest) as last_unprocessed_event_time
            FROM core.automaton_checkpoints ac
            LEFT JOIN core.events e ON e.event_id::uuid > ac.last_processed_id::uuid
            GROUP BY ac.automaton_name, ac.last_processed_id, ac.processed_count, ac.last_activity
        )
        SELECT 
            automaton_name as "automaton_name!",
            last_processed_id,
            processed_count,
            last_activity,
            events_after_checkpoint,
            first_unprocessed_event_time,
            last_unprocessed_event_time,
            CASE 
                WHEN first_unprocessed_event_time IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (NOW() - first_unprocessed_event_time))::bigint
                ELSE NULL
            END as processing_delay_seconds
        FROM checkpoint_analysis
        WHERE events_after_checkpoint > 0
        ORDER BY events_after_checkpoint DESC
        "#
    )
    .fetch_all(pool)
    .await
    .map_err(|e| db_error(e, "analyze checkpoint gaps"))?;

    Ok(rows)
}