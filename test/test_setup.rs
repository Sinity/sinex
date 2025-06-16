use sqlx::PgPool;
use std::sync::{Arc, OnceLock};

static TEST_DB: OnceLock<Arc<PgPool>> = OnceLock::new();

/// Get or create a shared test database pool
pub async fn get_test_db() -> Arc<PgPool> {
    if let Some(pool) = TEST_DB.get() {
        return pool.clone();
    }
    
    let pool = setup_test_database().await;
    let arc_pool = Arc::new(pool);
    TEST_DB.set(arc_pool.clone()).ok();
    arc_pool
}

async fn setup_test_database() -> PgPool {
    // Use DATABASE_URL if available (from nix shell), otherwise fall back to test URL
    let database_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .unwrap_or_else(|_| {
            // Try to use the ephemeral database if available
            if let Ok(ephemeral_url) = std::env::var("DATABASE_URL") {
                ephemeral_url
            } else {
                // Fall back to local PostgreSQL
                "postgresql:///sinex_test?host=/run/postgresql".to_string()
            }
        });

    // Use the high-concurrency test pool
    let pool = sinex_db::create_test_pool(&database_url)
        .await
        .expect("Failed to connect to test database");

    // Ensure migrations are run
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    pool
}

/// Macro to replace sqlx::test for our environment
#[macro_export]
macro_rules! db_test {
    (async fn $name:ident($pool:ident: PgPool) -> $ret:ty $body:block) => {
        #[tokio::test]
        async fn $name() -> $ret {
            let pool_arc = crate::test_setup::get_test_db().await;
            let $pool = pool_arc.as_ref().clone();
            
            // Clean up before test to ensure clean state
            let _ = crate::test_setup::cleanup_test_data(&$pool).await;
            
            // Clone pool for cleanup after
            let cleanup_pool = $pool.clone();
            
            // Run the test body
            let result = async move $body.await;
            
            // Clean up after test (best effort, don't fail the test if cleanup fails)
            let _ = crate::test_setup::cleanup_test_data(&cleanup_pool).await;
            
            result
        }
    };
}

/// Macro for tests that need transaction isolation
#[macro_export]
macro_rules! db_test_tx {
    (async fn $name:ident($tx:ident: sqlx::Transaction<'_, sqlx::Postgres>) -> $ret:ty $body:block) => {
        #[tokio::test]
        async fn $name() -> $ret {
            let pool_arc = crate::test_setup::get_test_db().await;
            let pool = pool_arc.as_ref().clone();
            
            // Clean up before test to ensure clean state
            let _ = crate::test_setup::cleanup_test_data(&pool).await;
            
            // Start a transaction that will be rolled back
            let mut $tx = pool.begin().await.expect("Failed to start transaction");
            
            // Run the test body
            let result = async move $body.await;
            
            // Always rollback to ensure test isolation
            let _ = $tx.rollback().await;
            
            result
        }
    };
}

/// Clean up test data (if needed between test runs)
#[allow(dead_code)]
pub async fn cleanup_test_data(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Clean up in dependency order (foreign keys)
    
    // First clean tables that reference other tables
    // Clean DLQ entries that reference work queue
    sqlx::query!("DELETE FROM sinex_schemas.dlq_events WHERE agent_name LIKE 'test%' OR agent_name LIKE 'pipeline_test%' OR agent_name LIKE 'error_test%' OR agent_name = 'concurrency_test_agent'")
        .execute(pool)
        .await?;
    
    // Clean work queue entries (references raw.events)
    sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE target_agent_name LIKE 'test%' OR target_agent_name LIKE 'pipeline_test%' OR target_agent_name LIKE 'error_test%' OR target_agent_name = 'concurrency_test_agent' OR target_agent_name = 'test_worker'")
        .execute(pool)
        .await?;
    
    // Clean test events
    sqlx::query!("DELETE FROM raw.events WHERE source LIKE 'test%' OR source LIKE 'pipeline_test%' OR source LIKE 'error_test%' OR source = 'concurrency_test' OR source = 'slow_source' OR (source = 'filesystem' AND payload->>'path' LIKE '/test/%')")
        .execute(pool)
        .await?;
    
    // Clean test agent manifests
    sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name LIKE 'test%' OR agent_name LIKE 'pipeline_test%' OR agent_name LIKE 'error_test%' OR agent_name = 'concurrency_test_agent'")
        .execute(pool)
        .await?;
    
    // Clean test schemas
    sqlx::query!("DELETE FROM sinex_schemas.event_payload_schemas WHERE event_source LIKE 'test%' OR event_source = 'test'")
        .execute(pool)
        .await?;
    
    Ok(())
}