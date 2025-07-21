//! Verification query registry for preflight and integration testing
//!
//! This module provides specialized queries for system verification,
//! integration testing, and preflight checks. These queries are used
//! during system startup and testing scenarios.

use crate::constants::tables;
use crate::query_builder::{QueryBuilder, QueryParam};
use crate::query_helpers::{db_error, DbResult};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Verification query registry for testing and preflight checks
pub struct VerificationQueries;

impl VerificationQueries {
    /// Insert a test event for verification
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<EventIdRecord>(pool)`
    pub fn insert_test_event(
        source: String,
        event_type: String,
        host: String,
        payload: JsonValue,
    ) -> QueryBuilder {
        QueryBuilder::insert(tables::EVENTS)
            .columns(&["source", "event_type", "host", "payload"])
            .values(&[
                QueryParam::String(source),
                QueryParam::String(event_type),
                QueryParam::String(host),
                QueryParam::Json(payload),
            ])
            .returning(&["event_id::uuid as \"id!\""])
    }

    /// Get test event by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<TestEventRecord>(pool)`
    pub fn get_test_event(event_id: Ulid) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&[
                "event_id::uuid as \"id!\"",
                "source",
                "event_type",
                "payload",
            ])
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Update test event payload
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_test_event(event_id: Ulid, payload: JsonValue) -> QueryBuilder {
        QueryBuilder::update(tables::EVENTS)
            .set("payload", QueryParam::Json(payload))
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Delete test event
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_test_event(event_id: Ulid) -> QueryBuilder {
        QueryBuilder::delete(tables::EVENTS)
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Delete test events by source and event type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn cleanup_test_events(source: String, event_type: String) -> QueryBuilder {
        QueryBuilder::delete(tables::EVENTS)
            .where_eq("source", QueryParam::String(source))
            .where_eq("event_type", QueryParam::String(event_type))
    }

    /// Delete test events by source
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn cleanup_by_source(source: String) -> QueryBuilder {
        QueryBuilder::delete(tables::EVENTS)
            .where_eq("source", QueryParam::String(source))
    }

    /// Count events by source and phase
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<CountRecord>(pool)`
    pub fn count_by_source_and_phase(
        source: String,
        event_type: String,
        phase: String,
    ) -> QueryBuilder {
        QueryBuilder::select(tables::EVENTS)
            .columns(&["COUNT(*) as count"])
            .where_eq("source", QueryParam::String(source))
            .where_eq("event_type", QueryParam::String(event_type))
            .where_eq("payload->>'phase'", QueryParam::String(phase))
    }

    /// Insert test checkpoint
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<CheckpointIdRecord>(pool)`
    pub fn insert_test_checkpoint(
        automaton_name: String,
        consumer_group: String,
        consumer_name: String,
        processed_count: i64,
        state_data: JsonValue,
    ) -> QueryBuilder {
        QueryBuilder::insert(tables::AUTOMATON_CHECKPOINTS)
            .columns(&[
                "automaton_name",
                "consumer_group",
                "consumer_name",
                "last_processed_id",
                "processed_count",
                "state_data",
            ])
            .values(&[
                QueryParam::String(automaton_name),
                QueryParam::String(consumer_group),
                QueryParam::String(consumer_name),
                QueryParam::OptionalUlid(None), // NULL::ulid
                QueryParam::Integer(processed_count),
                QueryParam::Json(state_data),
            ])
            .returning(&["id::uuid as \"id!\""])
    }

    /// Delete test checkpoint
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_test_checkpoint(checkpoint_id: sqlx::types::Uuid) -> QueryBuilder {
        QueryBuilder::delete(tables::AUTOMATON_CHECKPOINTS)
            .where_eq("id", QueryParam::Uuid(checkpoint_id))
    }

    // ========================================================================
    // Extension testing queries (require raw SQL)
    // ========================================================================

    /// Test UUID generation functionality
    pub async fn test_uuid_generation(pool: &PgPool) -> DbResult<sqlx::types::Uuid> {
        let row = sqlx::query!("SELECT gen_random_uuid() as test_uuid")
            .fetch_one(pool)
            .await
            .map_err(|e| db_error(e, "test UUID generation"))?;

        row.test_uuid
            .ok_or_else(|| db_error(sqlx::Error::RowNotFound, "UUID generation returned NULL"))
    }

    /// Test ULID generation functionality
    pub async fn test_ulid_generation(pool: &PgPool) -> DbResult<String> {
        let row = sqlx::query!("SELECT gen_ulid()::text as test_ulid")
            .fetch_one(pool)
            .await
            .map_err(|e| db_error(e, "test ULID generation"))?;

        row.test_ulid
            .ok_or_else(|| db_error(sqlx::Error::RowNotFound, "ULID generation returned NULL"))
    }

    /// Check TimescaleDB extension version
    pub async fn get_timescaledb_version(pool: &PgPool) -> DbResult<Option<String>> {
        let row = sqlx::query!("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            .fetch_optional(pool)
            .await
            .map_err(|e| db_error(e, "check TimescaleDB version"))?;

        Ok(row.map(|r| r.extversion))
    }

    /// Test JSON schema validation functionality
    pub async fn test_json_schema_validation(pool: &PgPool) -> DbResult<bool> {
        let row = sqlx::query!(r#"SELECT json_matches_schema('{"type": "object"}', '{}') as valid"#)
            .fetch_one(pool)
            .await
            .map_err(|e| db_error(e, "test JSON schema validation"))?;

        Ok(row.valid.unwrap_or(false))
    }

    /// Check if a table exists
    pub async fn table_exists(pool: &PgPool, schema: &str, table_name: &str) -> DbResult<bool> {
        let row = sqlx::query!(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = $1 
                AND table_name = $2
            ) as exists
            "#,
            schema,
            table_name
        )
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, "check table existence"))?;

        Ok(row.exists.unwrap_or(false))
    }
}

/// Record type for event ID results
#[derive(Debug, sqlx::FromRow)]
pub struct EventIdRecord {
    pub id: sqlx::types::Uuid,
}

/// Record type for test event results
#[derive(Debug, sqlx::FromRow)]
pub struct TestEventRecord {
    pub id: sqlx::types::Uuid,
    pub source: String,
    pub event_type: String,
    pub payload: JsonValue,
}

/// Record type for count results
#[derive(Debug, sqlx::FromRow)]
pub struct CountRecord {
    pub count: Option<i64>,
}

/// Record type for checkpoint ID results
#[derive(Debug, sqlx::FromRow)]
pub struct CheckpointIdRecord {
    pub id: sqlx::types::Uuid,
}