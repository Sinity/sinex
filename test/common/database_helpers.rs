//! Database helper functions and macros for test standardization
//!
//! Provides standardized patterns for database operations in tests,
//! reducing boilerplate and ensuring consistency.

use crate::common::prelude::*;

/// Create multiple test work queue items for a given agent
pub async fn create_test_work_items(
    pool: &PgPool,
    agent_name: &str,
    count: usize,
) -> Result<Vec<Ulid>> {
    let mut items = Vec::new();
    for i in 0..count {
        let queue_id = Ulid::new();
        let event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue (queue_id, event_id, route_key, agent_name, status) 
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, $4, $5)",
            queue_id.to_uuid(), event_id.to_uuid(), 
            format!("test_route_{}", i), agent_name, "pending"
        ).execute(pool).await?;
        items.push(queue_id);
    }
    Ok(items)
}

/// Register a test agent with unique name
pub async fn register_test_agent(pool: &PgPool, suffix: &str) -> Result<String> {
    let agent_name = format!("test_agent_{}_{}", suffix, Ulid::new());
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, status) 
         VALUES ($1, $2, $3, $4)",
        agent_name, "1.0.0", "Test agent", "running"
    ).execute(pool).await?;
    Ok(agent_name)
}

/// Get a clean test database pool with automatic cleanup
pub async fn get_clean_test_pool() -> Result<PgPool> {
    let pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
    
    // Clean up any leftover test data
    sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE agent_name LIKE 'test_%'")
        .execute(&pool).await?;
    sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name LIKE 'test_%'")
        .execute(&pool).await?;
        
    Ok(pool)
}

/// Get an integration test pool with migrations applied
pub async fn get_integration_test_pool() -> Result<PgPool> {
    let pool = get_clean_test_pool().await?;
    run_migrations(&pool).await?;
    Ok(pool)
}

/// Insert a batch of test events efficiently
pub async fn insert_test_event_batch(
    pool: &PgPool,
    events: &[RawEvent],
) -> Result<Vec<Ulid>> {
    let mut event_ids = Vec::new();
    
    for event in events {
        let inserted = queries::insert_event(pool, event).await?;
        event_ids.push(inserted.id);
    }
    
    Ok(event_ids)
}

/// Create test events and work items in a single transaction
pub async fn setup_test_workload(
    pool: &PgPool,
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

/// Macro for tests that need a clean database pool
#[macro_export]
macro_rules! test_with_pool {
    ($test_name:ident, $pool_name:ident, $test_body:block) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let $pool_name = crate::common::database_helpers::get_clean_test_pool().await?;
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
            let $pool_name = crate::common::database_helpers::get_integration_test_pool().await?;
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
            let $pool_name = crate::common::database_helpers::get_clean_test_pool().await?;
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
            let $pool_name = crate::common::database_helpers::get_clean_test_pool().await?;
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