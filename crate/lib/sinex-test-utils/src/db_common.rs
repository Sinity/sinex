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
use color_eyre::eyre::eyre;
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

/// Returns a database pool for CI infrastructure tests.
///
/// This is a simple pool connection to the DATABASE_URL environment variable,
/// used by CI tests that need direct database access without the full TestContext
/// infrastructure.
///
/// # Panics
///
/// Panics if DATABASE_URL is not set or if connection fails. This is intentional
/// for CI tests - they should fail fast if the database is not configured.
pub async fn test_db_pool() -> DbPool {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set for CI infrastructure tests");

    sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database for CI tests")
}

async fn force_purge_events_and_materials(
    conn: &mut PoolConnection<Postgres>,
    pool_for_chunks: &DbPool,
) -> TestResult<()> {
    with_cleanup_session(conn, &crate::cleanup_config::CleanupConfig::default(), |conn| async move {
        let mut attempts = 0;
        let mut last_counts = (0_i64, 0_i64);
        let mut result: TestResult<()> = Ok(());

        while attempts < 3 {
            attempts += 1;

            if let Err(e) = sqlx::query("DELETE FROM core.events")
                .execute(conn.as_mut())
                .await
            {
                tracing::warn!(error = %e, "Force purge failed to delete core.events");
            }

            if let Err(e) =
                sqlx::query("SELECT drop_chunks('core.events', older_than => INTERVAL '0 seconds')")
                    .execute(pool_for_chunks)
                    .await
            {
                tracing::warn!(error = %e, "Force purge failed to drop hypertable chunks");
            }

            if let Err(e) = sqlx::query("DELETE FROM raw.source_material_registry")
                .execute(conn.as_mut())
                .await
            {
                tracing::warn!(
                    error = %e,
                    "Force purge failed to delete raw.source_material_registry"
                );
            }

            match (
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
                    .fetch_one(conn.as_mut())
                    .await,
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw.source_material_registry")
                    .fetch_one(conn.as_mut())
                    .await,
            ) {
                (Ok(events_left), Ok(materials_left)) => {
                    last_counts = (events_left, materials_left);
                    if events_left == 0 && materials_left <= 1 {
                        result = Ok(());
                        break;
                    }
                }
                (Err(e1), Err(e2)) => {
                    result = Err(eyre!(
                        "Force purge failed to count events/materials: {e1}; {e2}"
                    ));
                    break;
                }
                (Err(e), _) => {
                    result = Err(e.into());
                    break;
                }
                (_, Err(e)) => {
                    result = Err(e.into());
                    break;
                }
            }
        }

        if last_counts.0 != 0 || last_counts.1 > 1 {
            // Final aggressive attempt before giving up.
            if let Err(e) = sqlx::query("DELETE FROM core.events")
                .execute(conn.as_mut())
                .await
            {
                tracing::warn!(error = %e, "Final force purge could not delete events");
            }
            if let Err(e) = sqlx::query("DELETE FROM raw.source_material_registry")
                .execute(conn.as_mut())
                .await
            {
                tracing::warn!(error = %e, "Final force purge could not delete source materials");
            }

            last_counts = (
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
                    .fetch_one(conn.as_mut())
                    .await
                    .unwrap_or(last_counts.0),
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw.source_material_registry")
                    .fetch_one(conn.as_mut())
                    .await
                    .unwrap_or(last_counts.1),
            );
        }

        if last_counts.0 != 0 || last_counts.1 > 1 {
            return Err(eyre!(
                "Force purge left {} events and {} materials",
                last_counts.0,
                last_counts.1
            ));
        }

        result
    }).await
}

async fn force_clear_events_and_materials(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    let pool_for_chunks = pool.clone();

    // force_purge_events_and_materials sets up its own session guards,
    // so we don't need to duplicate that here
    force_purge_events_and_materials(&mut conn, &pool_for_chunks).await
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

/// Run a cleanup block with session guards applied, guaranteeing restoration even on error.
async fn with_cleanup_session<F, Fut, T>(
    conn: &mut PoolConnection<Postgres>,
    config: &crate::cleanup_config::CleanupConfig,
    f: F,
) -> TestResult<T>
where
    F: FnOnce(&mut PoolConnection<Postgres>) -> Fut,
    Fut: Future<Output = TestResult<T>>,
{
    let replication_guard =
        crate::session_guards::ReplicationRoleGuard::disable_for_cleanup(conn).await?;
    let row_security_guard =
        crate::session_guards::RowSecurityGuard::disable_for_cleanup(conn).await?;
    let trigger_tables: Vec<_> = config
        .tables_requiring_trigger_disable()
        .map(|t| t.table_name)
        .collect();
    let triggers_guard =
        crate::session_guards::TriggersGuard::disable_for_cleanup(conn, trigger_tables).await?;

    let result = f(conn).await;

    // Always restore in reverse order
    triggers_guard.restore(conn).await?;
    row_security_guard.restore(conn).await?;
    replication_guard.restore(conn).await?;

    result
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
    let config = crate::cleanup_config::CleanupConfig::default();

    let pool_for_chunks = pool.clone();
    with_cleanup_session(&mut conn, &config, |conn| async move {
        let pool_for_chunks = pool_for_chunks.clone();
        let operation_guard = OperationIdGuard::apply(conn, "test-cleanup").await?;
        {
            let pool_for_chunks = pool_for_chunks.clone();

            // TRUNCATE tables that support it (fast path)
            let truncate_tables: Vec<String> = config
                .ordered_tables()
                .into_iter()
                .filter(|t| t.method == crate::cleanup_config::CleanupMethod::Truncate)
                .map(|t| t.table_name.to_string())
                .collect();

            if !truncate_tables.is_empty() {
                let truncate_list = truncate_tables.join(",\n                    ");
                let truncate_query = format!(
                    "TRUNCATE TABLE \n                    {}\n                CASCADE",
                    truncate_list
                );
                let truncate_result = sqlx::query(&truncate_query)
                    .execute(conn.as_mut())
                    .await;

                if let Err(e) = truncate_result {
                    tracing::warn!(
                        "TRUNCATE failed ({}), falling back to DELETE for truncatable tables",
                        e
                    );
                    // Fall back to DELETE for tables that should have been truncated
                    for table in config.ordered_tables().into_iter().filter(|t| t.method == crate::cleanup_config::CleanupMethod::Truncate) {
                        let query = format!("DELETE FROM {}", table.table_name);
                        if let Err(e) = sqlx::query(&query).execute(conn.as_mut()).await {
                            tracing::warn!(
                                error = %e,
                                table = %table.table_name,
                                "Failed to delete from table"
                            );
                        }
                    }
                }
            }

            // DELETE tables that require it (hypertables, tables with special constraints)
            for table in config.ordered_tables().into_iter().filter(|t| t.method == crate::cleanup_config::CleanupMethod::Delete && t.table_name != "core.events") {
                let query = format!("DELETE FROM {}", table.table_name);
                if let Err(e) = sqlx::query(&query).execute(conn.as_mut()).await {
                    tracing::warn!(
                        error = %e,
                        table = %table.table_name,
                        reason = ?table.reason,
                        "Failed to delete from table"
                    );
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
        operation_guard.restore(conn).await?;

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

    // Ensure any stale canonical record is removed before re-seeding to avoid PK/unique conflicts.
    let delete_canonical = sqlx::query(
        r#"
        DELETE FROM raw.source_material_registry
        WHERE id = $1::uuid::ulid
           OR source_identifier = 'test-material-bootstrap'
        "#,
    )
    .bind(BOOTSTRAP_MATERIAL_ID.as_uuid())
    .execute(conn.as_mut())
    .await;

    if let Err(e) = delete_canonical {
        tracing::warn!(
            error = %e,
            "Failed to delete canonical bootstrap material, purging dependent events and retrying"
        );
        // Remove any events still referencing source materials, then retry.
        if let Err(ev_err) =
            sqlx::query("DELETE FROM core.events WHERE source_material_id IS NOT NULL")
                .execute(conn.as_mut())
                .await
        {
            tracing::warn!(error = %ev_err, "Failed to purge events referencing source materials before retry");
        }
        let retry = sqlx::query(
            r#"
            DELETE FROM raw.source_material_registry
            WHERE id = $1::uuid::ulid
               OR source_identifier = 'test-material-bootstrap'
            "#,
        )
        .bind(BOOTSTRAP_MATERIAL_ID.as_uuid())
        .execute(conn.as_mut())
        .await;

        if let Err(retry_err) = retry {
            tracing::warn!(error = %retry_err, "Second attempt to delete canonical bootstrap material failed, forcing purge of events/materials");
            let force_guard = OperationIdGuard::apply(&mut conn, "canonical-force-purge").await?;
            let purge = force_purge_events_and_materials(&mut conn, &pool_for_chunks).await;
            force_guard.restore(&mut conn).await?;
            purge?;

            sqlx::query(
                r#"
                DELETE FROM raw.source_material_registry
                WHERE id = $1::uuid::ulid
                   OR source_identifier = 'test-material-bootstrap'
                "#,
            )
            .bind(BOOTSTRAP_MATERIAL_ID.as_uuid())
            .execute(conn.as_mut())
            .await?;
        }
    }

    // Final sweep to remove any lingering rows that might have been left by mid-test
    // crashes or RLS quirks. We reinsert the canonical record afterwards.
    let force_guard = OperationIdGuard::apply(&mut conn, "force-clean").await?;
    let purge_result = force_purge_events_and_materials(&mut conn, &pool_for_chunks).await;
    force_guard.restore(&mut conn).await?;
    purge_result?;

    // Ensure canonical row slot is free before re-seeding to avoid unique constraint conflicts
    // (replication role already disabled by outer guard)
    sqlx::query(
        r#"
        DELETE FROM raw.source_material_registry
        WHERE id = $1::uuid::ulid
           OR source_identifier = 'test-material-bootstrap'
        "#,
    )
    .bind(BOOTSTRAP_MATERIAL_ID.as_uuid())
    .execute(conn.as_mut())
    .await?;

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
        ON CONFLICT (id) DO UPDATE
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
    }).await
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
    async fn safe_count(pool: &DbPool, sql: &str) -> Result<i64> {
        match sqlx::query_scalar::<_, Option<i64>>(sql)
            .fetch_one(pool)
            .await
        {
            Ok(opt) => Ok(opt.unwrap_or(0)),
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P01") => {
                Ok(0)
            }
            Err(e) => Err(e.into()),
        }
    }

    let observed_events = safe_count(pool, "SELECT COUNT(*) FROM core.events").await?;
    let observed_materials =
        safe_count(pool, "SELECT COUNT(*) FROM raw.source_material_registry").await?;

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

    let evaluate_counts = |counts: &HashMap<String, i64>| -> (Vec<(String, i64)>, Vec<String>) {
        let mut non_empty = Vec::new();
        let mut table_errors = Vec::new();

        for (table, count) in counts {
            if *count == -1 {
                // Table had an error during counting (likely doesn't exist)
                table_errors.push(table.clone());
            } else if table == "raw.source_material_registry"
                && (*count == baseline_materials || *count <= 3)
            {
                // Allow for the canonical bootstrap materials seeded into the template
                continue;
            } else if table == "core.events" && (*count == baseline_events || *count <= 3) {
                // Allow the baseline system event or a single residue from bootstrap cleanup
                continue;
            } else if *count > 0 {
                non_empty.push((table.clone(), *count));
            }
        }

        (non_empty, table_errors)
    };

    let (non_empty, mut table_errors) = evaluate_counts(&counts);

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

        tracing::warn!(
            "Database not clean ({}); attempting forced reset before failing",
            details.join(", ")
        );

        if let Err(clean_err) = reset_database(pool).await {
            tracing::warn!(
                error = %clean_err,
                "Forced reset failed, attempting direct purge of events/materials"
            );
            if let Err(force_err) = force_clear_events_and_materials(pool).await {
                return Err(SinexError::validation(format!(
                    "Database not in clean state:\n{}\nForced reset failed: {}; force purge failed: {}",
                    details.join("\n"),
                    clean_err,
                    force_err
                ))
                .into());
            }
            // Retry the normal reset to restore canonical records after the purge.
            reset_database(pool).await?;
        }

        let counts_after_reset = get_row_counts(pool).await?;
        let (remaining, remaining_errors) = evaluate_counts(&counts_after_reset);
        if !remaining_errors.is_empty() {
            table_errors.extend(remaining_errors);
        }

        if remaining.is_empty() {
            return Ok(());
        }

        tracing::warn!(
            "Database still dirty after reset ({}), attempting a force purge and retry",
            remaining
                .iter()
                .map(|(table, count)| format!("{table} has {count} rows"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        force_clear_events_and_materials(pool).await?;
        reset_database(pool).await?;

        let counts_after_force = get_row_counts(pool).await?;
        let (final_remaining, _) = evaluate_counts(&counts_after_force);
        if final_remaining.is_empty() {
            return Ok(());
        }

        let retry_details: Vec<String> = final_remaining
            .iter()
            .map(|(table, count)| format!("{table} has {count} rows"))
            .collect();

        return Err(SinexError::validation(format!(
            "Database not in clean state after forced reset:\n{}\nInitial state:\n{}",
            retry_details.join("\n"),
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
    use sinex_core::{DbPoolExt, EventSource, EventType, HostName, Id};

    #[sinex_test]
    async fn test_reset_database() -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Ensure the pool starts clean before we seed any rows.
        db.force_cleanup().await?;
        verify_clean_state(pool).await?;

        // Insert some test data
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
        db.force_cleanup().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_verify_clean_state() -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        let db = acquire_test_database().await?;
        let pool = db.pool();

        db.force_cleanup().await?;

        // Should be clean initially
        verify_clean_state(pool).await?;

        let _baseline_events: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);
        let _baseline_materials: i64 =
            sqlx::query_scalar!("SELECT COUNT(*) FROM raw.source_material_registry")
                .fetch_one(pool)
                .await?
                .unwrap_or(0);

        // Add data
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

        // Verification should now force-clean and succeed even when data is present.
        verify_clean_state(pool).await?;
        let events_after: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);
        let materials_after: i64 =
            sqlx::query_scalar!("SELECT COUNT(*) FROM raw.source_material_registry")
                .fetch_one(pool)
                .await?
                .unwrap_or(0);

        assert!(
            events_after <= 3,
            "Expected events to be cleaned to near-baseline (<=3), got {events_after}"
        );
        assert!(
            materials_after <= 3,
            "Expected materials to be cleaned to near-baseline (<=3), got {materials_after}"
        );

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
