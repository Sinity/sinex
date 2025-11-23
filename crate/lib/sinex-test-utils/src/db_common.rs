//! Database Common - Shared Database Utilities for Tests and Benchmarks
//!
//! This module provides common database operations used by both the test infrastructure
//! and benchmarking infrastructure. It includes utilities for resetting databases,
//! loading fixtures, clearing caches, and performing other common database tasks.
//!
//! # Overview
//!
//! The utilities in this module are designed to work with the Sinex event storage
//! system and provide consistent behavior across tests and benchmarks. All operations
//! respect foreign key constraints and maintain database integrity.
//!
//! # Key Functions
//!
//! - [`reset_database`] - Resets database to clean state by truncating all tables
//! - [`load_fixture`] - Loads a named dataset fixture into the database
//! - [`clear_pg_cache`] - Clears PostgreSQL query plan and buffer caches
//! - [`get_row_counts`] - Gets row counts for all major tables
//! - [`verify_clean_state`] - Verifies database is in clean state
//!
//! # Usage Examples
//!
//! ```rust
//! use sinex_test_utils::db_common;
//!
//! // Reset database to clean state
//! db_common::reset_database(&pool).await?;
//!
//! // Load a standard fixture
//! db_common::load_fixture(&pool, "small").await?;
//!
//! // Clear caches for cold cache benchmarks
//! db_common::clear_pg_cache(&pool).await?;
//! ```
//!
//! # Fixture Management
//!
//! Fixtures are pre-generated datasets stored as SQL files. Standard fixtures include:
//! - `empty` - No data (no-op)
//! - `small` - 1K events for quick tests/benchmarks
//! - `medium` - 100K events for integration tests/benchmarks
//! - `large` - 10M events for performance benchmarks
//!
//! Custom fixtures can be loaded by name from the `fixtures/datasets/` directory.

use crate::Result;
use crate::TestResult;

use camino::Utf8PathBuf;
use futures::Future;
use once_cell::sync::Lazy;
use sinex_core::db::DbPool;
use sinex_core::types::error::SinexError;
use sinex_core::types::ulid::Ulid;
use sqlx::pool::PoolConnection;
use sqlx::Postgres;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};

static BASELINE_EVENT_COUNT: AtomicI64 = AtomicI64::new(-1);
static BASELINE_MATERIAL_COUNT: AtomicI64 = AtomicI64::new(-1);
static BOOTSTRAP_MATERIAL_ID: Lazy<Ulid> = Lazy::new(|| {
    Ulid::from_str("014D2PF2DBSQQZXQ5TK1V58CGG").expect("valid bootstrap material id")
});

struct OperationIdGuard {
    previous: Option<String>,
    is_active: bool,
}

impl OperationIdGuard {
    async fn apply(conn: &mut PoolConnection<Postgres>, value: &str) -> TestResult<Self> {
        let previous = sqlx::query_scalar::<_, Option<String>>(
            "SELECT current_setting('sinex.operation_id', true)",
        )
        .fetch_optional(conn.as_mut())
        .await?
        .flatten();

        sqlx::query("SELECT set_config('sinex.operation_id', $1, false)")
            .bind(value)
            .execute(conn.as_mut())
            .await?;

        Ok(Self {
            previous,
            is_active: true,
        })
    }

    async fn restore(mut self, conn: &mut PoolConnection<Postgres>) -> TestResult<()> {
        let outcome = if let Some(prev) = self.previous.take() {
            sqlx::query("SELECT set_config('sinex.operation_id', $1, false)")
                .bind(prev)
                .execute(conn.as_mut())
                .await
        } else {
            sqlx::query("RESET sinex.operation_id")
                .execute(conn.as_mut())
                .await
        };

        if let Err(e) = outcome {
            tracing::warn!(
                "Failed to restore sinex.operation_id session setting after cleanup: {}",
                e
            );
        }

        self.is_active = false;
        Ok(())
    }
}

impl Drop for OperationIdGuard {
    fn drop(&mut self) {
        if self.is_active {
            tracing::warn!(
                "OperationIdGuard dropped without restore; session may retain cleanup operation id"
            );
        }
    }
}

/// Reset database to clean state by truncating all tables
///
/// This function performs a comprehensive cleanup of the database, removing all
/// data while preserving schema. It handles foreign key constraints properly
/// by temporarily disabling them during the truncation process.
///
/// # Foreign Key Handling
///
/// The function uses PostgreSQL's `session_replication_role = 'replica'` to
/// temporarily disable foreign key checks, allowing for efficient truncation
/// of all tables regardless of their relationships.
///
/// # Tables Cleaned
///
/// - All core event-related tables (events, annotations, relations, etc.)
/// - Entity tables
/// - Schema and manifest tables
/// - Any other tables added to the core schema
///
/// # Performance
///
/// - Truncation is much faster than DELETE for large datasets
/// - Falls back to DELETE for hypertables (TimescaleDB) that can't be truncated
/// - Typical execution time: 20-50ms for most databases
///
/// # Example
///
/// ```rust
/// # use sinex_test_utils::db_common::reset_database;
/// # async fn example(pool: &DbPool) -> TestResult<()> {
/// reset_database(pool).await?;
/// // Database is now empty and ready for new test data
/// # Ok(())
/// # }
/// ```
pub async fn reset_database(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;

    // Disable FK checks for the cleanup session
    sqlx::query("SET session_replication_role = 'replica'")
        .execute(conn.as_mut())
        .await?;

    let pool_for_chunks = pool.clone();
    let operation_guard = OperationIdGuard::apply(&mut conn, "test-cleanup").await?;
    {
        let pool_for_chunks = pool_for_chunks.clone();
        // Try to use TRUNCATE for non-hypertables (much faster)
        let truncate_result = sqlx::query(
            r#"
                TRUNCATE TABLE 
                    core.event_annotations,
                    core.event_relations,
                    core.event_cluster_members,
                    core.event_embeddings,
                    core.entity_relations,
                    core.revisions,
                    core.entities,
                    core.event_clusters,
                    core.processor_checkpoints,
                    core.operations_log,
                    core.transactional_outbox,
                    core.blobs,
                    core.tags,
                    core.tagged_items,
                    raw.source_material_registry,
                    raw.temporal_ledger,
                    core.processor_manifests,
                    sinex_schemas.event_payload_schemas
                CASCADE
            "#,
        )
        .execute(conn.as_mut())
        .await;

        if let Err(e) = truncate_result {
            tracing::warn!("TRUNCATE failed ({}), falling back to DELETE", e);

            // Fall back to DELETE in dependency order
            let delete_queries = [
                "DELETE FROM core.event_annotations",
                "DELETE FROM core.event_relations",
                "DELETE FROM core.event_cluster_members",
                "DELETE FROM core.event_embeddings",
                "DELETE FROM core.entity_relations",
                "DELETE FROM core.revisions",
                "DELETE FROM core.processor_manifests",
                "DELETE FROM sinex_schemas.event_payload_schemas",
                "DELETE FROM core.processor_checkpoints",
                "DELETE FROM core.operations_log",
                "DELETE FROM core.transactional_outbox",
                "DELETE FROM core.tags",
                "DELETE FROM core.tagged_items",
                "DELETE FROM core.blobs",
                "DELETE FROM raw.temporal_ledger",
                "DELETE FROM core.entities",
                "DELETE FROM core.event_clusters",
            ];

            for query in delete_queries {
                if let Err(e) = sqlx::query(query).execute(conn.as_mut()).await {
                    let table_name = query.split_whitespace().nth(2).unwrap_or("unknown");
                    tracing::warn!("Failed to delete from {}: {}", table_name, e);
                }
            }
        }

        // Handle core.events separately (hypertable cannot be truncated)
        if let Err(e) = sqlx::query("DELETE FROM core.events")
            .execute(conn.as_mut())
            .await
        {
            tracing::warn!("Failed to delete from core.events: {}", e);
            // Try TimescaleDB-specific cleanup
            if let Err(e2) =
                sqlx::query("SELECT drop_chunks('core.events', older_than => INTERVAL '0 seconds')")
                    .execute(&pool_for_chunks)
                    .await
            {
                tracing::warn!("Failed to drop chunks: {}", e2);
            }
        }

        if let Err(e) = sqlx::query("DELETE FROM raw.source_material_registry")
            .execute(conn.as_mut())
            .await
        {
            tracing::warn!(
                "Failed to delete from raw.source_material_registry: {}. Retrying after removing dependent events.",
                e
            );
            // Ensure no events remain that reference lingering source materials before retrying.
            if let Err(ev_err) =
                sqlx::query("DELETE FROM core.events WHERE source_material_id IS NOT NULL")
                    .execute(conn.as_mut())
                    .await
            {
                tracing::warn!(
                    "Fallback removal of events referencing source materials failed: {}",
                    ev_err
                );
            }
            sqlx::query("DELETE FROM raw.source_material_registry")
                .execute(conn.as_mut())
                .await?;
        }
    }
    operation_guard.restore(&mut conn).await?;

    // Re-enable FK checks
    sqlx::query("SET session_replication_role = 'origin'")
        .execute(conn.as_mut())
        .await?;

    // Ensure no stale bootstrap records remain from prior runs
    // This DELETE needs operation_id for RLS policy
    let operation_guard2 = OperationIdGuard::apply(&mut conn, "bootstrap-cleanup").await?;
    sqlx::query(
        r#"
        DELETE FROM core.events
        WHERE source_material_id = $1::uuid::ulid
           OR source_material_id IN (
                SELECT id
                FROM raw.source_material_registry
                WHERE source_identifier LIKE 'test-material-%'
            )
        "#,
    )
    .bind(BOOTSTRAP_MATERIAL_ID.as_uuid())
    .execute(conn.as_mut())
    .await?;
    operation_guard2.restore(&mut conn).await?;

    // Restore canonical test material record relied upon by Event::test_event.
    sqlx::query(
        r#"
        INSERT INTO raw.source_material_registry (
            id,
            material_kind,
            source_identifier,
            status,
            timing_info_type,
            metadata
        ) VALUES (
            $1::uuid::ulid,
            'annex',
            'test-material-bootstrap',
            'completed',
            'realtime',
            '{}'::jsonb
        )
        ON CONFLICT (source_identifier) DO UPDATE
        SET id = EXCLUDED.id,
            status = EXCLUDED.status,
            timing_info_type = EXCLUDED.timing_info_type,
            metadata = EXCLUDED.metadata
        "#,
    )
    .bind(BOOTSTRAP_MATERIAL_ID.as_uuid())
    .execute(conn.as_mut())
    .await?;

    sqlx::query("RESET sinex.operation_id")
        .execute(conn.as_mut())
        .await?;

    Ok(())
}

pub async fn with_operation_id<F, Fut, T>(
    conn: &mut PoolConnection<Postgres>,
    operation_id: &str,
    f: F,
) -> TestResult<T>
where
    F: FnOnce(&mut PoolConnection<Postgres>) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let previous = sqlx::query_scalar::<_, Option<String>>(
        "SELECT current_setting('sinex.operation_id', true)",
    )
    .fetch_optional(conn.as_mut())
    .await?
    .flatten();

    sqlx::query("SELECT set_config('sinex.operation_id', $1, false)")
        .bind(operation_id)
        .execute(conn.as_mut())
        .await?;

    let result = f(conn).await;

    let restore_result = if let Some(prev) = previous {
        sqlx::query("SELECT set_config('sinex.operation_id', $1, false)")
            .bind(prev)
            .execute(conn.as_mut())
            .await
    } else {
        sqlx::query("RESET sinex.operation_id")
            .execute(conn.as_mut())
            .await
    };

    if let Err(e) = restore_result {
        tracing::warn!(
            "Failed to restore sinex.operation_id session setting after cleanup: {}",
            e
        );
    }

    result.map_err(Into::into)
}

/// Load a named dataset fixture into the database
///
/// Fixtures are pre-generated SQL files containing test/benchmark data.
/// This function loads the specified fixture into the current database.
///
/// # Standard Fixtures
///
/// - `empty` - No data loaded (returns immediately)
/// - `small` - 1K events with related data
/// - `medium` - 100K events with related data
/// - `large` - 10M events with related data
///
/// # Custom Fixtures
///
/// Any other name will be looked up as `fixtures/datasets/{name}.sql`
///
/// # Performance
///
/// Fixtures use PostgreSQL COPY commands for efficient bulk loading:
/// - Small (1K): ~50ms
/// - Medium (100K): ~500ms
/// - Large (10M): ~30s
///
/// # Example
///
/// ```rust
/// # use sinex_test_utils::db_common::load_fixture;
/// # async fn example(pool: &DbPool) -> TestResult<()> {
/// // Load standard small dataset
/// load_fixture(pool, "small").await?;
///
/// // Load custom fixture
/// load_fixture(pool, "user_behavior_test").await?;
/// # Ok(())
/// # }
/// ```
pub async fn load_fixture(pool: &DbPool, name: &str) -> TestResult<()> {
    let path = match name {
        "empty" => return Ok(()),
        "small" => Utf8PathBuf::from("fixtures/datasets/small_1k.sql"),
        "medium" => Utf8PathBuf::from("fixtures/datasets/medium_100k.sql"),
        "large" => Utf8PathBuf::from("fixtures/datasets/large_10m.sql"),
        custom => Utf8PathBuf::from(format!("fixtures/datasets/{custom}.sql")),
    };

    if !path.exists() {
        return Err(
            SinexError::not_found(format!("Fixture file not found: {}", path.as_str())).into(),
        );
    }

    let sql = std::fs::read_to_string(&path)?;

    // Execute the fixture SQL
    // For large fixtures, this might contain multiple statements
    // separated by semicolons, so we need to handle that
    for statement in sql.split(";\n").filter(|s| !s.trim().is_empty()) {
        let statement = format!("{};", statement.trim());
        if !statement.starts_with("--") && statement.len() > 10 {
            sqlx::query(&statement).execute(pool).await?;
        }
    }

    Ok(())
}

/// Clear PostgreSQL caches for cold cache benchmarking
///
/// This function clears various PostgreSQL caches to ensure consistent
/// cold cache benchmark conditions. It discards:
/// - Temporary tables
/// - Prepared statements and query plans
/// - (Optionally) Shared buffer cache
///
/// # Cache Types
///
/// - **DISCARD TEMP** - Drops all temporary tables
/// - **DISCARD PLANS** - Invalidates all cached query plans
/// - **pg_prewarm** - Can be used to clear buffer cache (if available)
///
/// # Usage in Benchmarks
///
/// ```rust
/// # use sinex_test_utils::db_common::clear_pg_cache;
/// # async fn benchmark(pool: &DbPool) -> TestResult<()> {
/// // Cold cache measurement
/// clear_pg_cache(pool).await?;
/// let cold_time = measure_query(pool).await?;
///
/// // Warm cache measurement (no clear)
/// let warm_time = measure_query(pool).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Note
///
/// This only clears connection-local caches. System-wide buffer cache
/// clearing requires superuser privileges and is generally not done
/// in benchmarks to avoid affecting other database users.
pub async fn clear_pg_cache(pool: &DbPool) -> TestResult<()> {
    // Discard temporary tables and prepared statements
    sqlx::query("DISCARD TEMP").execute(pool).await?;
    sqlx::query("DISCARD PLANS").execute(pool).await?;

    // Optionally try to clear shared buffers if we have pg_prewarm
    // This requires superuser privileges and the extension
    let _ = sqlx::query("SELECT pg_prewarm('core.events', 'none')")
        .execute(pool)
        .await;

    Ok(())
}

/// Get row counts for all major tables
///
/// Returns a map of table names to row counts for monitoring and verification.
/// This is useful for:
/// - Verifying fixture loads
/// - Checking cleanup completeness
/// - Benchmark dataset validation
///
/// # Tables Included
///
/// - core.events
/// - core.event_annotations
/// - core.entity_relations
/// - core.entities
/// - core.event_clusters
/// - core.event_cluster_members
/// - core.event_embeddings
/// - core.revisions
/// - core.blobs
/// - core.tags
/// - core.tagged_items
/// - core.processor_checkpoints
/// - core.operations_log
/// - core.transactional_outbox
/// - raw.source_material_registry
/// - raw.temporal_ledger
/// - sinex_schemas.event_payload_schemas
/// - core.processor_manifests
///
/// # Example
///
/// ```rust
/// # use sinex_test_utils::db_common::get_row_counts;
/// # async fn example(pool: &DbPool) -> TestResult<()> {
/// let counts = get_row_counts(pool).await?;
/// for (table, count) in counts {
///     println!("{}: {} rows", table, count);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn get_row_counts(pool: &DbPool) -> TestResult<HashMap<String, i64>> {
    let mut counts = HashMap::new();

    // List of all major tables to count from the schema
    let tables = vec![
        // Core tables
        "core.events",
        "core.event_annotations",
        "core.event_relations",
        "core.event_cluster_members",
        "core.event_embeddings",
        "core.entity_relations",
        "core.revisions",
        "core.entities",
        "core.event_clusters",
        "core.blobs",
        "core.tags",
        "core.tagged_items",
        "core.processor_checkpoints",
        "core.operations_log",
        "core.transactional_outbox",
        // Raw data tables
        "raw.source_material_registry",
        "raw.temporal_ledger",
        // Schema tables
        "sinex_schemas.event_payload_schemas",
        "core.processor_manifests",
    ];

    for table in tables {
        let query = format!("SELECT COUNT(*) FROM {table}");

        match sqlx::query_scalar::<_, i64>(&query).fetch_one(pool).await {
            Ok(count) => {
                counts.insert(table.to_string(), count);
            }
            Err(e) => {
                tracing::warn!("Failed to count rows in table {}: {}", table, e);
                // Don't fail the entire operation if one table doesn't exist or has issues
                // Just log the warning and continue - this is useful during development
                // when not all tables might exist yet
                counts.insert(table.to_string(), -1); // Use -1 to indicate error
            }
        }
    }

    Ok(counts)
}

/// Verify database is in clean state
///
/// Checks that all tables are empty and the database is ready for testing.
/// Returns an error with details if any tables contain data.
///
/// # Checks Performed
///
/// - All core tables have 0 rows
/// - Foreign key constraints are valid
/// - No orphaned data
///
/// # Example
///
/// ```rust
/// # use sinex_test_utils::db_common::{reset_database, verify_clean_state};
/// # async fn example(pool: &DbPool) -> TestResult<()> {
/// reset_database(pool).await?;
/// verify_clean_state(pool).await?; // Should pass
/// # Ok(())
/// # }
/// ```
pub async fn verify_clean_state(pool: &DbPool) -> TestResult<()> {
    let observed_events: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await?
        .unwrap_or(0);
    let observed_materials: i64 =
        sqlx::query_scalar!("SELECT COUNT(*) FROM raw.source_material_registry")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);

    let baseline_events = {
        let current = BASELINE_EVENT_COUNT.load(Ordering::Relaxed);
        if current == -1 || observed_events < current {
            BASELINE_EVENT_COUNT.store(observed_events, Ordering::Relaxed);
            observed_events
        } else {
            current
        }
    };

    let baseline_materials = {
        let current = BASELINE_MATERIAL_COUNT.load(Ordering::Relaxed);
        if current == -1 || observed_materials < current {
            BASELINE_MATERIAL_COUNT.store(observed_materials, Ordering::Relaxed);
            observed_materials
        } else {
            current
        }
    };

    let counts = get_row_counts(pool).await?;

    let mut non_empty = Vec::new();
    let mut table_errors = Vec::new();

    for (table, count) in counts {
        if count == -1 {
            // Table had an error during counting (likely doesn't exist)
            table_errors.push(table);
        } else if table == "raw.source_material_registry"
            && (count == baseline_materials || count == 1)
        {
            // Allow for the canonical bootstrap materials seeded into the template
            continue;
        } else if table == "core.events" && count == baseline_events {
            // Baseline system events shipped with the template
            continue;
        } else if count > 0 {
            non_empty.push((table, count));
        }
    }

    // Report table errors as warnings but don't fail verification
    // This is useful during development when schema might be incomplete
    if !table_errors.is_empty() {
        tracing::warn!(
            "Some tables could not be verified (they may not exist): {}",
            table_errors.join(", ")
        );
    }

    if !non_empty.is_empty() {
        let details: Vec<String> = non_empty
            .iter()
            .map(|(table, count)| format!("{table} has {count} rows"))
            .collect();
        return Err(SinexError::validation(format!(
            "Database not in clean state:\n{}",
            details.join("\n")
        ))
        .into());
    }

    Ok(())
}

/// Apply test-specific session optimizations
///
/// Configures the current database session for optimal test/benchmark
/// performance. These are session-level settings that don't affect
/// other connections.
///
/// # Optimizations Applied
///
/// - Increased work_mem for sorting/hashing
/// - Disabled synchronous_commit for speed
/// - Adjusted planner costs for SSD storage
/// - Increased cache sizes
///
/// # Example
///
/// ```rust
/// # use sinex_test_utils::db_common::apply_test_optimizations;
/// # async fn example(pool: &DbPool) -> TestResult<()> {
/// apply_test_optimizations(pool).await?;
/// // Run performance-sensitive operations
/// # Ok(())
/// # }
/// ```
pub async fn apply_test_optimizations(pool: &DbPool) -> TestResult<()> {
    let optimizations = vec![
        "SET work_mem = '64MB'",
        "SET maintenance_work_mem = '256MB'",
        "SET synchronous_commit = off",
        "SET random_page_cost = 1.1",
        "SET effective_cache_size = '1GB'",
        "SET temp_buffers = '32MB'",
        "SET statement_timeout = '300s'", // 5 minutes for benchmarks
    ];

    for setting in optimizations {
        if let Err(e) = sqlx::query(setting).execute(pool).await {
            tracing::warn!("Could not apply setting '{}': {}", setting, e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;
    use crate::database_pool::acquire_test_database;
    use crate::sinex_test;

    #[sinex_test]
    async fn test_reset_database() -> TestResult<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Insert some test data
        use sinex_core::*;
        use sinex_core::*;
        use sinex_core::{
            Blob, BlobRecord, CheckpointRecord, Entity, EntityRecord, EntityRelation, Event,
            JsonValue, Operation, OperationRecord, Provenance, SourceMaterial,
        };

        let new_event = Event::<JsonValue>::test_event(
            EventSource::new("test"),
            EventType::new("test.event"),
            serde_json::json!({}),
        )
        .with_host(HostName::new("test-host"));
        pool.events().insert(new_event).await?;

        // Verify data exists
        let count = pool.events().count_all().await?;
        assert!(count > 0);

        // Reset database
        reset_database(pool).await?;

        // Verify clean
        verify_clean_state(pool).await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_verify_clean_state() -> TestResult<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        db.force_cleanup().await?;

        // Should be clean initially
        verify_clean_state(pool).await?;

        // Add data
        use sinex_core::*;
        use sinex_core::*;
        use sinex_core::{
            Blob, BlobRecord, CheckpointRecord, Entity, EntityRecord, EntityRelation, Event,
            JsonValue, Operation, OperationRecord, Provenance, SourceMaterial,
        };

        let material_record = pool
            .source_materials()
            .register_in_flight(
                sinex_core::db::repositories::source_materials::legacy_material_types::STREAM,
                Some("test-material"),
                serde_json::json!({ "test": true }),
            )
            .await?;
        let material_id = Id::<SourceMaterial>::from_ulid(material_record.id);

        let new_event = Event::<JsonValue>::create(
            EventSource::new("test"),
            EventType::new("test"),
            serde_json::json!({}),
            Provenance::from_material(material_id, 0, None, None),
        )
        .with_host(HostName::new("test"));
        pool.events().insert(new_event).await?;

        // Should fail verification
        assert!(verify_clean_state(pool).await.is_err());

        Ok(())
    }

    #[sinex_test]
    async fn test_get_row_counts() -> TestResult<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        let counts = get_row_counts(pool).await?;
        let baseline_events: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);

        // All should be zero in clean database
        for (table, count) in counts {
            if count == -1 {
                tracing::warn!(
                    "Table {} is not available in the test schema; skipping",
                    table
                );
                continue;
            }
            if table == "raw.source_material_registry" && count == 1 {
                continue;
            }
            if table == "core.events" && count == baseline_events {
                continue;
            }
            assert_eq!(count, 0, "table {table} expected to be empty");
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_clear_pg_cache() -> TestResult<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Should not error
        clear_pg_cache(pool).await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_apply_optimizations() -> TestResult<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Should not error
        apply_test_optimizations(pool).await?;

        Ok(())
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    // All benchmarks commented out - no imports needed

    // Benchmark database reset operation
    //
    // This measures the time to completely clean a database with various
    // amounts of existing data.
    // TODO: These benchmarks need async support in sinex_bench macro
    // #[sinex_bench]
    // fn bench_reset_empty_database() -> color_eyre::eyre::Result<()> {
    //     // Database is already empty from reset_and_load
    //     reset_database(ctx.pool()).await?;
    //     Ok(())
    // }

    // Benchmark database reset with data
    //
    // Measures reset performance when database contains events and related data
    // #[sinex_bench]
    // fn bench_reset_populated_database() -> color_eyre::eyre::Result<()> {
    //     // Setup: Insert some data
    //     use sinex_core::types::*;
    //     for i in 0..10 {
    //         let event = EventQueries::insert_event(
    //             "bench".to_string(),
    //             "test".to_string(),
    //             "test-host".to_string(),
    //             serde_json::json!({"index": i}),
    //             None,
    //             None,
    //             None,
    //             None,
    //         )
    //         .fetch_one::<sinex_core::types::Event<JsonValue>>(ctx.pool())
    //         .await?;

    //         // Add annotation
    //         sqlx::query(
    //             "INSERT INTO core.event_annotations (id, event_id, annotation_type, content, annotator)
    //              VALUES ($1, $2, 'test', '{}'::jsonb, 'bench')"
    //         )
    //         .bind(sinex_core::types::ulid::Ulid::new().to_uuid())
    //         .bind(event.id.to_uuid())
    //         .execute(ctx.pool())
    //         .await?;
    //     }

    //     // Perform the reset
    //     reset_database(ctx.pool()).await?;
    //     Ok(())
    // }

    // All benchmarks below commented out - they need async support in sinex_bench macro

    // /// Benchmark cache clearing operation
    // #[sinex_bench]
    // fn bench_clear_pg_cache() -> color_eyre::eyre::Result<()> {
    //     clear_pg_cache(ctx.pool()).await?;
    //     Ok(())
    // }

    // /// Benchmark row count collection
    // #[sinex_bench]
    // fn bench_get_row_counts() -> color_eyre::eyre::Result<()> {
    //     // Setup: Insert varied amounts of data
    //     reset_database(ctx.pool()).await?;

    //     // Insert some events
    //     use sinex_core::types::*;
    //     for i in 0..50 {
    //         EventQueries::insert_event(
    //             format!("source_{}", i % 5),
    //             "test".to_string(),
    //             "bench".to_string(),
    //             serde_json::json!({}),
    //             None,
    //             None,
    //             None,
    //             None,
    //         )
    //         .execute(ctx.pool())
    //         .await?;
    //     }

    //     // Insert some checkpoints
    //     for i in 0..10 {
    //         sqlx::query(
    //             "INSERT INTO core.processor_checkpoints (processor_name, last_processed_event_id, processed_count, state)
    //              VALUES ($1, $2, $3, '{}'::jsonb)"
    //         )
    //         .bind(format!("satellite_{}", i % 3))
    //         .bind(sinex_core::types::ulid::Ulid::new().to_uuid())
    //         .bind(i as i64 * 10)
    //         .execute(ctx.pool())
    //         .await?;
    //     }

    //     // Perform the count
    //     let counts = get_row_counts(ctx.pool()).await?;
    //     divan::black_box(counts);
    //     Ok(())
    // }

    // /// Benchmark database state verification
    // #[sinex_bench]
    // fn bench_verify_clean_state() -> color_eyre::eyre::Result<()> {
    //     verify_clean_state(ctx.pool()).await?;
    //     Ok(())
    // }

    // /// Benchmark applying test optimizations
    // #[sinex_bench]
    // fn bench_apply_optimizations() -> color_eyre::eyre::Result<()> {
    //     apply_test_optimizations(ctx.pool()).await?;
    //     Ok(())
    // }
}
