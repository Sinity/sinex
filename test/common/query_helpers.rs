//! Test-specific query helpers that wrap production query builders
//!
//! This module provides a simplified interface for common database operations
//! in tests, ensuring consistent use of the centralized query builder system
//! and proper ULID/UUID conversion.

use crate::common::prelude::*;
use sinex_db::queries::{CheckpointQueries, EventQueries, OperationQueries, SchemaQueries};
use sinex_db::RawEvent;
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use chrono::{DateTime, Utc};

/// Test-specific query helpers
pub struct TestQueries;

impl TestQueries {
    /// Insert a test event with minimal required fields
    pub async fn insert_test_event(
        pool: &DbPool,
        source: &str,
        event_type: &str,
        payload: JsonValue,
    ) -> AnyhowResult<RawEvent> {
        EventQueries::insert_event(
            source.to_string(),
            event_type.to_string(),
            gethostname::gethostname().to_string_lossy().to_string(),
            payload,
            None, // ts_orig
            None, // ingestor_version
            None, // payload_schema_id
            None, // source_event_ids
        )
        .fetch_one(pool)
        .await
        .map_err(Into::into)
    }

    /// Insert a test event with all fields
    pub async fn insert_full_event(
        pool: &DbPool,
        source: &str,
        event_type: &str,
        host: &str,
        payload: JsonValue,
        ts_orig: Option<DateTime<Utc>>,
        ingestor_version: Option<String>,
        payload_schema_id: Option<Ulid>,
        source_event_ids: Option<Vec<Ulid>>,
    ) -> AnyhowResult<RawEvent> {
        EventQueries::insert_event(
            source.to_string(),
            event_type.to_string(),
            host.to_string(),
            payload,
            ts_orig,
            ingestor_version,
            payload_schema_id,
            source_event_ids,
        )
        .fetch_one(pool)
        .await
        .map_err(Into::into)
    }

    /// Get an event by ID
    pub async fn get_event(pool: &DbPool, event_id: Ulid) -> AnyhowResult<RawEvent> {
        EventQueries::get_by_id(event_id)
            .fetch_one(pool)
            .await
            .map_err(Into::into)
    }

    /// Get events by source
    pub async fn get_events_by_source(
        pool: &DbPool,
        source: &str,
        limit: Option<i64>,
    ) -> AnyhowResult<Vec<RawEvent>> {
        EventQueries::get_by_source(source.to_string(), limit, None)
            .fetch_all(pool)
            .await
            .map_err(Into::into)
    }

    /// Get events by type
    pub async fn get_events_by_type(
        pool: &DbPool,
        event_type: &str,
        limit: Option<i64>,
    ) -> AnyhowResult<Vec<RawEvent>> {
        EventQueries::get_by_event_type(event_type.to_string(), limit, None)
            .fetch_all(pool)
            .await
            .map_err(Into::into)
    }

    /// Count events by source
    pub async fn count_events_by_source(pool: &DbPool, source: &str) -> AnyhowResult<i64> {
        let (count,) = EventQueries::count_by_source(source.to_string())
            .fetch_one::<(i64,)>(pool)
            .await?;
        Ok(count)
    }

    /// Get checkpoint for an automaton
    pub async fn get_checkpoint(
        pool: &DbPool,
        automaton_name: &str,
    ) -> AnyhowResult<Option<CheckpointRecord>> {
        Self::get_checkpoint_full(
            pool,
            automaton_name,
            &format!("{}-group", automaton_name),
            &format!("{}-consumer", automaton_name),
        )
        .await
    }

    /// Get checkpoint with full specification
    pub async fn get_checkpoint_full(
        pool: &DbPool,
        automaton_name: &str,
        consumer_group: &str,
        consumer_name: &str,
    ) -> AnyhowResult<Option<CheckpointRecord>> {
        CheckpointQueries::get_checkpoint(
            automaton_name.to_string(),
            consumer_group.to_string(),
            consumer_name.to_string(),
        )
        .fetch_optional(pool)
        .await
        .map_err(Into::into)
    }

    /// Upsert a test checkpoint
    pub async fn upsert_checkpoint(
        pool: &DbPool,
        automaton_name: &str,
        last_processed_id: Option<String>,
        processed_count: i64,
    ) -> AnyhowResult<()> {
        CheckpointQueries::upsert_checkpoint(
            Ulid::new(),
            automaton_name.to_string(),
            format!("{}-group", automaton_name),
            format!("{}-consumer", automaton_name),
            last_processed_id,
            processed_count,
            Utc::now(),
            None, // state_data
            1,    // checkpoint_version
            None, // checkpoint_data
            Utc::now(),
            Utc::now(),
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Delete all test events
    pub async fn cleanup_test_events(pool: &DbPool) -> AnyhowResult<()> {
        EventQueries::delete_by_source("test%".to_string())
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Delete all test checkpoints
    pub async fn cleanup_test_checkpoints(pool: &DbPool) -> AnyhowResult<()> {
        // Delete checkpoints matching pattern
        sqlx::query!("DELETE FROM core.automaton_checkpoints WHERE automaton_name LIKE 'test%'")
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Count checkpoints by automaton name
    pub async fn count_checkpoints_by_automaton(
        pool: &DbPool,
        automaton_name: &str,
    ) -> AnyhowResult<i64> {
        let result = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.automaton_checkpoints WHERE automaton_name = $1",
            automaton_name
        )
        .fetch_one(pool)
        .await?;
        Ok(result.unwrap_or(0))
    }

    /// Get recent events
    pub async fn get_recent_events(
        pool: &DbPool,
        limit: i64,
    ) -> AnyhowResult<Vec<RawEvent>> {
        EventQueries::get_recent(Some(limit), None)
            .fetch_all(pool)
            .await
            .map_err(Into::into)
    }

    /// Get events in time range
    pub async fn get_events_in_range(
        pool: &DbPool,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
    ) -> AnyhowResult<Vec<RawEvent>> {
        EventQueries::get_by_time_range(start, end, limit, None)
            .fetch_all(pool)
            .await
            .map_err(Into::into)
    }

    /// Create a test operation
    pub async fn create_test_operation(
        pool: &DbPool,
        operation_type: &str,
        description: &str,
    ) -> AnyhowResult<Ulid> {
        let id = Ulid::new();
        OperationQueries::insert_operation(
            id,
            operation_type.to_string(),
            description.to_string(),
            json!({}),
            "test".to_string(),
            Utc::now(),
        )
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// Complete a test operation
    pub async fn complete_operation(
        pool: &DbPool,
        operation_id: Ulid,
    ) -> AnyhowResult<()> {
        OperationQueries::update_operation_status(
            operation_id,
            "completed".to_string(),
            Utc::now(),
            None,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Register a test schema
    pub async fn register_test_schema(
        pool: &DbPool,
        schema_name: &str,
        schema_version: &str,
        event_types: Vec<String>,
        schema_content: JsonValue,
    ) -> AnyhowResult<Ulid> {
        let id = Ulid::new();
        // Insert schema for each event type
        for event_type in event_types {
            SchemaQueries::insert_schema(
                event_type,
                schema_version.parse::<i32>().unwrap_or(1),
                schema_content.clone(),
            )
            .execute(pool)
            .await?;
        }
        Ok(id)
    }
}

/// Checkpoint record type for test queries
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CheckpointRecord {
    #[sqlx(rename = "id")]
    pub id: sqlx::types::Uuid,
    pub automaton_name: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub last_processed_id: Option<String>,
    pub processed_count: i64,
    pub last_activity: DateTime<Utc>,
    pub state_data: Option<JsonValue>,
    pub checkpoint_version: i32,
    pub checkpoint_data: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Test operation record
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OperationRecord {
    #[sqlx(rename = "id")]
    pub id: sqlx::types::Uuid,
    pub operation_type: String,
    pub description: String,
    pub metadata: JsonValue,
    pub status: String,
    pub operator: String,
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}