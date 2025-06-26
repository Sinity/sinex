//! Database helper functions and macros for test standardization
//!
//! Provides standardized patterns for database operations in tests,
//! reducing boilerplate and ensuring consistency.

use crate::common::prelude::*;
use serde_json::json;
use tokio::sync::OnceCell;

/// Process-wide shared test pool using async-safe OnceCell
static SHARED_TEST_POOL: OnceCell<DbPool> = OnceCell::const_new();

/// Get or create the shared test database pool
///
/// This uses tokio::sync::OnceCell which is designed for async initialization
/// and works correctly across different test threads and runtimes.
pub async fn get_shared_test_pool() -> Result<DbPool> {
    Ok(SHARED_TEST_POOL
        .get_or_init(|| async {
            let database_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
                
            // Conservative pool sizing for tests
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(50) // Reasonable for parallel tests
                .min_connections(5)
                .acquire_timeout(Duration::from_secs(30))
                .idle_timeout(Duration::from_secs(300))
                .test_before_acquire(false)
                .connect(&database_url)
                .await
                .expect("Failed to create test database pool");
                
            // Apply migrations to ensure schema is current
            run_migrations(&pool).await.expect("Failed to run migrations");
            
            pool
        })
        .await
        .clone())
}

/// Create a test database pool (for tests that need isolated pools)
///
/// Most tests should use get_shared_test_pool() with transaction isolation.
/// Only use this for tests that specifically need their own connection pool.
pub async fn create_test_pool() -> Result<DbPool> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
        
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5) // Single test doesn't need many connections
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(10))
        .test_before_acquire(false)
        .connect(&database_url)
        .await?;
        
    // Apply migrations to ensure schema is current
    run_migrations(&pool).await?;
    
    Ok(pool)
}

/// Get a database transaction for test isolation
///
/// Each test gets its own transaction that automatically rolls back,
/// providing perfect test isolation without database cleanup overhead.
/// 
/// Usage:
/// ```rust
/// #[tokio::test]
/// async fn my_test() -> Result<(), anyhow::Error> {
///     let mut tx = test_transaction().await?;
///     // Use &mut tx instead of &pool
///     sqlx::query!("INSERT INTO ...").execute(&mut *tx).await?;
///     // Transaction automatically rolls back at end
/// }
/// ```
pub async fn test_transaction() -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    let pool = get_shared_test_pool().await?;
    let tx = pool.begin().await?;
    Ok(tx)
}


/// Create multiple test work queue items for a given agent
pub async fn create_test_work_items(
    pool: &DbPool,
    agent_name: &str,
    count: usize,
) -> Result<Vec<Ulid>> {
    let mut items = Vec::new();
    for i in 0..count {
        let queue_id = Ulid::new();
        let event_id = Ulid::new();
        
        // First create a raw event for the foreign key constraint
        sqlx::query!(
            "INSERT INTO raw.events (id, source, event_type, payload, ts_orig, host) 
             VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6)",
            event_id.to_uuid(), 
            "test_source", 
            format!("test.event.{}", i),
            serde_json::json!({"test": true, "index": i}),
            chrono::Utc::now(),
            "test_host"
        ).execute(pool).await?;
        
        // Then create the work queue item
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue (queue_id, raw_event_id, target_agent_name, status) 
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, $4)",
            queue_id.to_uuid(), event_id.to_uuid(), 
            agent_name, "pending"
        ).execute(pool).await?;
        items.push(queue_id);
    }
    Ok(items)
}

/// Register a test agent with unique name
pub async fn register_test_agent(pool: &DbPool, suffix: &str) -> Result<String> {
    let agent_name = format!("test_agent_{}_{}", suffix, Ulid::new());
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, status) 
         VALUES ($1, $2, $3, $4)",
        agent_name, "1.0.0", "Test agent", "running"
    ).execute(pool).await?;
    Ok(agent_name)
}



/// Insert a batch of test events efficiently
pub async fn insert_test_event_batch(
    pool: &DbPool,
    events: &[RawEvent],
) -> Result<Vec<Ulid>> {
    let mut event_ids = Vec::new();
    
    for event in events {
        let inserted = queries::insert_event(&pool, event).await?;
        event_ids.push(inserted.id);
    }
    
    Ok(event_ids)
}

/// Create test events and work items in a single transaction
pub async fn setup_test_workload(
    pool: &DbPool,
    agent_name: &str,
    event_count: usize,
) -> Result<(Vec<Ulid>, Vec<Ulid>)> {
    // Create test events
    let test_events: Vec<_> = (0..event_count)
        .map(|i| events::adversarial_test_event(
            "workload.test", 
            json!({"sequence": i, "batch": "workload"})
        ))
        .collect();
    
    let event_ids = insert_test_event_batch(pool, &test_events).await?;
    let work_item_ids = create_test_work_items(pool, agent_name, event_count).await?;
    
    Ok((event_ids, work_item_ids))
}

/// Create a simple test event
pub async fn create_test_event(source: &str, event_type: &str) -> RawEvent {
    RawEventBuilder::new(source, event_type, json!({"test": true})).build()
}

/// Create a test agent with minimal fields
pub async fn create_test_agent(pool: &DbPool, agent_name: &str) -> Result<()> {
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, status) 
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (agent_name) DO NOTHING",
        agent_name, "1.0.0", "Test agent", "running"
    ).execute(pool).await?;
    Ok(())
}

/// Purge old work queue items (TTL cleanup function)
pub async fn purge_old_work_queue_items(pool: &DbPool) -> Result<u64> {
    // Purge succeeded items older than 90 days
    let result = sqlx::query!(
        "DELETE FROM sinex_schemas.work_queue 
         WHERE status = 'succeeded' 
         AND processed_at < NOW() - INTERVAL '90 days'"
    )
    .execute(pool)
    .await?;
    
    Ok(result.rows_affected())
}

/// Macro for tests that need a clean database pool
#[macro_export]
macro_rules! test_with_pool {
    ($test_name:ident, $pool_name:ident, $test_body:block) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let $pool_name = crate::common::database::TestPool::new().await?;
            $test_body
        }
    };
}

/// Macro for integration tests with migrations
#[macro_export]
macro_rules! integration_test {
    ($test_name:ident, $pool_name:ident, $test_body:block) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let $pool_name = crate::common::database::TestPool::new().await?;
            $test_body
        }
    };
}

/// Macro for tests that need an agent registered
#[macro_export]
macro_rules! test_with_agent {
    ($test_name:ident, $pool_name:ident, $agent_name:ident, $test_body:block) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let $pool_name = crate::common::database::TestPool::new().await?;
            let $agent_name = crate::common::database_helpers::register_test_agent(
                &$pool_name, 
                &format!("{}_{}", stringify!($test_name), line!())
            ).await?;
            $test_body
        }
    };
}

/// Macro for workload tests with events and work items
#[macro_export]
macro_rules! workload_test {
    ($test_name:ident, $pool_name:ident, $agent_name:ident, $event_count:expr, $test_body:block) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let $pool_name = crate::common::database::TestPool::new().await?;
            let $agent_name = crate::common::database_helpers::register_test_agent(
                &$pool_name, 
                &format!("{}_{}", stringify!($test_name), line!())
            ).await?;
            let (_event_ids, _work_item_ids) = crate::common::database_helpers::setup_test_workload(
                &$pool_name, &$agent_name, $event_count
            ).await?;
            $test_body
        }
    };
}

/// Macro for tests using shared pool with transaction isolation
/// 
/// This is the RECOMMENDED pattern for new tests. Provides automatic:
/// - Shared pool (resource efficient)
/// - Transaction isolation (perfect test isolation)
/// - Automatic rollback (no cleanup needed)
/// 
/// Usage:
/// ```rust
/// test_with_transaction!(test_name, tx, {
///     sqlx::query!("INSERT INTO ...").execute(&mut *tx).await?;
///     // Test automatically isolated and cleaned up
/// });
/// ```
#[macro_export]
macro_rules! test_with_transaction {
    ($test_name:ident, $tx_name:ident, $test_body:block) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let mut $tx_name = crate::common::database_helpers::test_transaction().await?;
            $test_body
            // Transaction automatically rolls back
        }
    };
}

/// Macro for tests that need shared pool but not transaction isolation
/// 
/// Use this for tests that need to share data across multiple operations
/// or when transaction semantics interfere with testing (e.g., testing transaction behavior itself).
#[macro_export]
macro_rules! test_with_shared_pool {
    ($test_name:ident, $pool_name:ident, $test_body:block) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let $pool_name = crate::common::database_helpers::get_shared_test_pool().await?;
            $test_body
        }
    };
}

/// Macro for transaction-isolated agent tests
/// 
/// Combines transaction isolation with agent registration. Perfect test isolation
/// with automatic cleanup and no resource waste.
#[macro_export]
macro_rules! test_with_transaction_agent {
    ($test_name:ident, $tx_name:ident, $agent_name:ident, $test_body:block) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let pool = crate::common::database_helpers::get_shared_test_pool().await?;
            let mut $tx_name = pool.begin().await?;
            let $agent_name = format!("test_agent_{}_{}_{}", stringify!($test_name), line!(), sinex_ulid::Ulid::new());
            
            sqlx::query!(
                "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, status) 
                 VALUES ($1, $2, $3, $4)",
                $agent_name, "1.0.0", "Test agent", "running"
            ).execute(&mut $tx_name).await?;
            
            $test_body
            // Transaction automatically rolls back, cleaning up agent
        }
    };
}