use sqlx::PgPool;
use std::sync::{Arc, OnceLock};
use std::collections::HashMap;
use tokio::sync::RwLock;

static TEST_DB: OnceLock<Arc<PgPool>> = OnceLock::new();
static POOL_CACHE: OnceLock<Arc<RwLock<HashMap<String, Arc<PgPool>>>>> = OnceLock::new();

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

/// Get or create a cached database pool for specific test configurations
pub async fn get_cached_test_pool(config_key: &str) -> Arc<PgPool> {
    let cache = POOL_CACHE.get_or_init(|| Arc::new(RwLock::new(HashMap::new())));
    
    // Check if pool already exists
    {
        let cache_read = cache.read().await;
        if let Some(pool) = cache_read.get(config_key) {
            return pool.clone();
        }
    }
    
    // Create new pool
    let pool = setup_test_database().await;
    let arc_pool = Arc::new(pool);
    
    // Cache the pool
    {
        let mut cache_write = cache.write().await;
        cache_write.insert(config_key.to_string(), arc_pool.clone());
    }
    
    arc_pool
}

/// Get a high-performance pool for concurrent tests
pub async fn get_high_performance_test_pool() -> Arc<PgPool> {
    get_cached_test_pool("high_performance").await
}

/// Clear the pool cache (for cleanup)
pub async fn clear_pool_cache() {
    if let Some(cache) = POOL_CACHE.get() {
        let mut cache_write = cache.write().await;
        cache_write.clear();
    }
}

async fn setup_test_database() -> PgPool {
    setup_test_database_with_config(None).await
}

async fn setup_test_database_with_config(config: Option<&str>) -> PgPool {
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

    // Use the high-concurrency test pool with optimized settings
    let pool = match config {
        Some("high_performance") => create_high_performance_test_pool(&database_url).await.expect("Failed to create high performance test pool"),
        _ => sinex_db::create_test_pool(&database_url).await.expect("Failed to create test pool"),
    };

    // Ensure migrations are run
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    pool
}

async fn create_high_performance_test_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    use sqlx::postgres::{PgPoolOptions, PgConnectOptions};
    use sqlx::ConnectOptions;
    use std::str::FromStr;
    
    let connect_options = PgConnectOptions::from_str(database_url)?
        .statement_cache_capacity(1000);  // Larger statement cache
    
    PgPoolOptions::new()
        .max_connections(50)  // Higher connection limit for concurrent tests
        .min_connections(10)  // Keep minimum connections ready
        .acquire_timeout(std::time::Duration::from_secs(5))
        .idle_timeout(Some(std::time::Duration::from_secs(30)))
        .max_lifetime(Some(std::time::Duration::from_secs(300)))
        .connect_with(connect_options)
        .await
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