//! Database integration tests
//!
//! This module contains comprehensive integration tests for database functionality,
//! including TimescaleDB features, ULID handling, JSON schema validation,
//! work queue management, and connection pool behavior.
//!
//! # Test Coverage
//! - Basic database operations and transactions
//! - TimescaleDB hypertable functionality
//! - ULID primary key integration
//! - JSON schema validation with pg_jsonschema
//! - Work queue operations and TTL
//! - Connection pool edge cases and limits
//! - Query performance and optimization
//! - Data integrity and consistency

/// Core database integration tests
pub mod database_integration_tests;

/// TimescaleDB-specific functionality tests
pub mod timescaledb_tests;

/// ULID integration and conversion tests
pub mod ulid_integration_tests;

/// JSON schema validation tests (pg_jsonschema)
pub mod jsonschema_validation_tests;

/// Schema validation and management tests
pub mod schema_validation_tests;

/// Work queue functionality tests
pub mod work_queue_tests;

/// Work queue TTL and cleanup tests
pub mod work_queue_ttl_tests;

/// Routing cache functionality tests
pub mod routing_cache_tests;

/// Queue metrics and monitoring tests
pub mod queue_metrics_tests;

/// Connection pool edge cases and stress tests
pub mod connection_pool_edge_cases_test;

/// Common utilities for database testing
pub mod utils {
    use crate::common::prelude::*;
    // use chrono::{DateTime, Utc};

    /// Create test schema for validation
    pub async fn create_test_schema(
        pool: &DbPool,
        source: &str,
        event_type: &str,
        schema: serde_json::Value,
    ) -> Result<Ulid> {
        let schema_id = Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas
            (id, event_source, event_type, schema_version, json_schema_definition, description)
            VALUES ($1::uuid::ulid, $2, $3, '1.0', $4, $5)
            "#,
            schema_id.to_uuid(),
            source,
            event_type,
            schema,
            format!("Test schema for {}.{}", source, event_type)
        )
        .execute(pool)
        .await?;

        Ok(schema_id)
    }

    /// Verify TimescaleDB hypertable exists
    pub async fn verify_hypertable_exists(pool: &DbPool, table_name: &str) -> Result<bool> {
        let exists = sqlx::query_scalar!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM timescaledb_information.hypertables
                WHERE hypertable_name = $1
            )
            "#,
            table_name
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(false);

        Ok(exists)
    }

    /// Create test work queue items
    pub async fn create_test_work_items(
        pool: &DbPool,
        agent_name: &str,
        count: usize,
    ) -> Result<Vec<Ulid>> {
        let mut queue_ids = Vec::new();

        for i in 0..count {
            let queue_id = Ulid::new();
            let raw_event_id = Ulid::new();

            sqlx::query!(
                r#"
                INSERT INTO sinex_schemas.work_queue
                (queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, created_at)
                VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 'pending', 0, 3, NOW())
                "#,
                queue_id.to_uuid(),
                raw_event_id.to_uuid(),
                format!("{}_item_{}", agent_name, i)
            )
            .execute(pool)
            .await?;

            queue_ids.push(queue_id);
        }

        Ok(queue_ids)
    }

    /// Measure query performance
    pub async fn measure_query_performance<F, Fut, T>(
        operation: F,
        iterations: usize,
    ) -> Result<(Vec<std::time::Duration>, T)>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut durations = Vec::new();
        let mut last_result = None;

        for _ in 0..iterations {
            let start = std::time::Instant::now();
            let result = operation().await?;
            durations.push(start.elapsed());
            last_result = Some(result);
        }

        Ok((durations, last_result.unwrap()))
    }

    /// Get database connection statistics
    pub async fn get_connection_stats(pool: &DbPool) -> Result<DatabaseConnectionStats> {
        let row = sqlx::query!(
            r#"
            SELECT
                COUNT(*) as total_connections,
                COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
                COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections
            FROM pg_stat_activity
            WHERE datname = current_database()
            "#
        )
        .fetch_one(pool)
        .await?;

        Ok(DatabaseConnectionStats {
            total: row.total_connections.unwrap_or(0),
            active: row.active_connections.unwrap_or(0),
            idle: row.idle_connections.unwrap_or(0),
        })
    }

    /// Database connection statistics
    #[derive(Debug, Clone)]
    pub struct DatabaseConnectionStats {
        pub total: i64,
        pub active: i64,
        pub idle: i64,
    }

    /// Verify database constraints and indexes
    pub async fn verify_database_integrity(pool: &DbPool) -> Result<IntegrityReport> {
        // Check for foreign key violations
        let fk_violations = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) FROM (
                SELECT 1 FROM sinex_schemas.work_queue wq
                LEFT JOIN raw.events e ON wq.raw_event_id::uuid = e.id::uuid
                WHERE e.id IS NULL
            ) violations
            "#
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        // Check for orphaned records
        let orphaned_schemas = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM sinex_schemas.event_payload_schemas WHERE json_schema_definition IS NULL"
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        Ok(IntegrityReport {
            foreign_key_violations: fk_violations,
            orphaned_records: orphaned_schemas,
            integrity_ok: fk_violations == 0 && orphaned_schemas == 0,
        })
    }

    /// Database integrity report
    #[derive(Debug, Clone)]
    pub struct IntegrityReport {
        pub foreign_key_violations: i64,
        pub orphaned_records: i64,
        pub integrity_ok: bool,
    }
}
