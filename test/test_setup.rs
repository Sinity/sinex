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
    sqlx::migrate!("./migration")
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
            
            // Run the test body
            let result = async move $body.await;
            
            result
        }
    };
}

/// Clean up test data (if needed between test runs)
#[allow(dead_code)]
pub async fn cleanup_test_data(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Only clean up data that might interfere with tests
    // Don't drop tables as other tests might be using them
    sqlx::query!("DELETE FROM raw.events WHERE source LIKE 'test_%'")
        .execute(pool)
        .await?;
    
    sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name LIKE 'test_%'")
        .execute(pool)
        .await?;
    
    Ok(())
}