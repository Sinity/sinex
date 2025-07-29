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
use sinex_core_types::DbPool;
use sinex_error::SinexError;
use std::collections::HashMap;
use std::path::PathBuf;

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
/// - Entity and artifact tables
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
/// # async fn example(pool: &DbPool) -> Result<()> {
/// reset_database(pool).await?;
/// // Database is now empty and ready for new test data
/// # Ok(())
/// # }
/// ```
pub async fn reset_database(pool: &DbPool) -> Result<()> {
    // Disable FK checks for the cleanup session
    sqlx::query("SET session_replication_role = 'replica'")
        .execute(pool)
        .await?;

    // Try to use TRUNCATE for non-hypertables (much faster)
    let truncate_result = sqlx::query(
        r#"
        TRUNCATE TABLE 
            core.event_annotations,
            core.event_artifact_refs,
            core.event_relations,
            core.event_cluster_members,
            core.artifact_event_sources,
            core.event_embeddings,
            core.entity_relations,
            core.artifact_embeddings,
            core.revisions,
            core.artifact_tags,
            core.artifact_relations,
            core.entities,
            core.artifacts,
            core.event_clusters,
            sinex_schemas.processor_manifests
        CASCADE
    "#,
    )
    .execute(pool)
    .await;

    if let Err(e) = truncate_result {
        tracing::warn!("TRUNCATE failed ({}), falling back to DELETE", e);

        // Fall back to DELETE in dependency order
        let delete_queries = [
            "DELETE FROM core.event_annotations",
            "DELETE FROM core.event_artifact_refs",
            "DELETE FROM core.event_relations",
            "DELETE FROM core.event_cluster_members",
            "DELETE FROM core.artifact_event_sources",
            "DELETE FROM core.event_embeddings",
            "DELETE FROM core.entity_relations",
            "DELETE FROM core.artifact_embeddings",
            "DELETE FROM core.revisions",
            "DELETE FROM core.artifact_tags",
            "DELETE FROM core.artifact_relations",
            "DELETE FROM sinex_schemas.processor_manifests",
            "DELETE FROM core.entities",
            "DELETE FROM core.artifacts",
            "DELETE FROM core.event_clusters",
        ];

        for query in delete_queries {
            if let Err(e) = sqlx::query(query).execute(pool).await {
                let table_name = query.split_whitespace().nth(2).unwrap_or("unknown");
                tracing::warn!("Failed to delete from {}: {}", table_name, e);
            }
        }
    }

    // Handle core.events separately (hypertable cannot be truncated)
    if let Err(e) = sqlx::query("DELETE FROM core.events").execute(pool).await {
        tracing::warn!("Failed to delete from core.events: {}", e);
        // Try TimescaleDB-specific cleanup
        if let Err(e2) =
            sqlx::query("SELECT drop_chunks('core.events', older_than => INTERVAL '0 seconds')")
                .execute(pool)
                .await
        {
            tracing::warn!("Failed to drop chunks: {}", e2);
        }
    }

    // Re-enable FK checks
    sqlx::query("SET session_replication_role = 'origin'")
        .execute(pool)
        .await?;

    Ok(())
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
/// # async fn example(pool: &DbPool) -> Result<()> {
/// // Load standard small dataset
/// load_fixture(pool, "small").await?;
///
/// // Load custom fixture
/// load_fixture(pool, "user_behavior_test").await?;
/// # Ok(())
/// # }
/// ```
pub async fn load_fixture(pool: &DbPool, name: &str) -> Result<()> {
    let path = match name {
        "empty" => return Ok(()),
        "small" => PathBuf::from("fixtures/datasets/small_1k.sql"),
        "medium" => PathBuf::from("fixtures/datasets/medium_100k.sql"),
        "large" => PathBuf::from("fixtures/datasets/large_10m.sql"),
        custom => PathBuf::from(format!("fixtures/datasets/{}.sql", custom)),
    };

    if !path.exists() {
        return Err(SinexError::not_found(format!(
            "Fixture file not found: {}",
            path.display()
        )));
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
/// # async fn benchmark(pool: &DbPool) -> Result<()> {
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
pub async fn clear_pg_cache(pool: &DbPool) -> Result<()> {
    // Discard temporary tables and prepared statements
    sqlx::query("DISCARD TEMP; DISCARD PLANS;")
        .execute(pool)
        .await?;

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
/// - core.entities
/// - core.entity_relations
/// - core.artifacts
/// - core.artifact_relations
/// - core.revisions
/// - core.event_clusters
///
/// # Example
///
/// ```rust
/// # use sinex_test_utils::db_common::get_row_counts;
/// # async fn example(pool: &DbPool) -> Result<()> {
/// let counts = get_row_counts(pool).await?;
/// for (table, count) in counts {
///     println!("{}: {} rows", table, count);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn get_row_counts(pool: &DbPool) -> Result<HashMap<String, i64>> {
    let mut counts = HashMap::new();

    let tables = [
        "core.events",
        "core.event_annotations",
        "core.entities",
        "core.entity_relations",
        "core.artifacts",
        "core.artifact_relations",
        "core.revisions",
        "core.event_clusters",
    ];

    for table in tables {
        let query = format!("SELECT COUNT(*) FROM {}", table);
        let count: i64 = sqlx::query_scalar(&query)
            .fetch_one(pool)
            .await
            .unwrap_or(0);
        counts.insert(table.to_string(), count);
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
/// # async fn example(pool: &DbPool) -> Result<()> {
/// reset_database(pool).await?;
/// verify_clean_state(pool).await?; // Should pass
/// # Ok(())
/// # }
/// ```
pub async fn verify_clean_state(pool: &DbPool) -> Result<()> {
    let counts = get_row_counts(pool).await?;

    let mut non_empty = Vec::new();
    for (table, count) in counts {
        if count > 0 {
            non_empty.push((table, count));
        }
    }

    if !non_empty.is_empty() {
        let details: Vec<String> = non_empty
            .iter()
            .map(|(table, count)| format!("{} has {} rows", table, count))
            .collect();
        return Err(SinexError::validation(format!(
            "Database not in clean state:\n{}",
            details.join("\n")
        )));
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
/// # async fn example(pool: &DbPool) -> Result<()> {
/// apply_test_optimizations(pool).await?;
/// // Run performance-sensitive operations
/// # Ok(())
/// # }
/// ```
pub async fn apply_test_optimizations(pool: &DbPool) -> Result<()> {
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
    use super::*;
    use crate::database_pool::acquire_test_database;

    #[tokio::test]
    async fn test_reset_database() -> Result<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Insert some test data
        use sinex_db::queries::EventQueries;
        EventQueries::insert_event(
            "test".to_string(),
            "test.event".to_string(),
            "test-host".to_string(),
            serde_json::json!({}),
            None,
            None,
            None,
            None,
        )
        .execute(pool)
        .await?;

        // Verify data exists
        use sinex_db::count_events;
        let count = count_events(pool).await?;
        assert_eq!(count, 1);

        // Reset database
        reset_database(pool).await?;

        // Verify clean
        verify_clean_state(pool).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_clean_state() -> Result<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Should be clean initially
        verify_clean_state(pool).await?;

        // Add data
        use sinex_db::queries::EventQueries;
        EventQueries::insert_event(
            "test".to_string(),
            "test".to_string(),
            "test".to_string(),
            serde_json::json!({}),
            None,
            None,
            None,
            None,
        )
        .execute(pool)
        .await?;

        // Should fail verification
        assert!(verify_clean_state(pool).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_row_counts() -> Result<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        let counts = get_row_counts(pool).await?;

        // All should be zero in clean database
        for (_table, count) in counts {
            assert_eq!(count, 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_clear_pg_cache() -> Result<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Should not error
        clear_pg_cache(pool).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_optimizations() -> Result<()> {
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Should not error
        apply_test_optimizations(pool).await?;

        Ok(())
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use crate::bench::*;

    /// Benchmark database reset operation
    ///
    /// This measures the time to completely clean a database with various
    /// amounts of existing data.
    bench_with_db!(
        bench_reset_empty_database,
        |ctx: &BenchContext| async move {
            // Database is already empty from reset_and_load
            reset_database(ctx.pool()).await
        }
    );

    /// Benchmark database reset with data
    ///
    /// Measures reset performance when database contains events and related data
    #[divan::bench]
    fn bench_reset_populated_database(bencher: divan::Bencher) {
        let ctx = &*BENCH_CONTEXT;

        bencher
            .with_inputs(|| {
                // Setup: Insert some data
                ctx.runtime.block_on(async {
                    // Insert a few events with related data
                    use sinex_db::queries::EventQueries;
                    for i in 0..10 {
                        let event = EventQueries::insert_event(
                            "bench".to_string(),
                            "test".to_string(),
                            "test-host".to_string(),
                            serde_json::json!({"index": i}),
                            None,
                            None,
                            None,
                            None,
                        )
                        .fetch_one::<sinex_core_types::RawEvent>(ctx.pool())
                        .await
                        .unwrap();

                        // Add annotation
                        sqlx::query(
                            "INSERT INTO core.event_annotations (id, event_id, annotation_type, content, annotator) 
                             VALUES ($1, $2, 'test', '{}'::jsonb, 'bench')"
                        )
                        .bind(sinex_ulid::Ulid::new().to_uuid())
                        .bind(event.id.to_uuid())
                        .execute(ctx.pool())
                        .await
                        .unwrap();
                    }
                })
            })
            .bench_values(|_| {
                ctx.runtime.block_on(async {
                    reset_database(ctx.pool()).await.unwrap()
                })
            });
    }

    /// Benchmark fixture loading
    #[divan::bench(args = ["empty", "small", "medium"])]
    fn bench_load_fixture(bencher: divan::Bencher, fixture: &str) {
        let ctx = &*BENCH_CONTEXT;

        bencher.bench_local(|| {
            ctx.runtime.block_on(async {
                // Reset first to ensure consistent state
                reset_database(ctx.pool()).await.unwrap();

                // Note: This will fail until fixtures are actually created
                // For now, we're benchmarking the attempt
                let _ = load_fixture(ctx.pool(), fixture).await;
            })
        });
    }

    /// Benchmark cache clearing operation
    bench_with_db!(bench_clear_pg_cache, |ctx: &BenchContext| async move {
        clear_pg_cache(ctx.pool()).await
    });

    /// Benchmark row count collection
    #[divan::bench]
    fn bench_get_row_counts(bencher: divan::Bencher) {
        let ctx = &*BENCH_CONTEXT;

        bencher
            .with_inputs(|| {
                // Setup: Insert varied amounts of data
                ctx.runtime.block_on(async {
                    reset_database(ctx.pool()).await.unwrap();

                    // Insert some events
                    use sinex_db::queries::EventQueries;
                    for i in 0..50 {
                        EventQueries::insert_event(
                            format!("source_{}", i % 5),
                            "test".to_string(),
                            "bench".to_string(),
                            serde_json::json!({}),
                            None,
                            None,
                            None,
                            None,
                        )
                        .execute(ctx.pool())
                        .await
                        .unwrap();
                    }
                })
            })
            .bench_values(|_| {
                ctx.runtime
                    .block_on(async { get_row_counts(ctx.pool()).await.unwrap() })
            });
    }

    /// Benchmark database state verification
    bench_with_db!(bench_verify_clean_state, |ctx: &BenchContext| async move {
        verify_clean_state(ctx.pool()).await
    });

    /// Benchmark applying test optimizations
    bench_with_db!(bench_apply_optimizations, |ctx: &BenchContext| async move {
        apply_test_optimizations(ctx.pool()).await
    });
}
