pub mod models;
pub mod pool;
pub mod queries;
pub mod validation;
pub mod metrics;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::{migrate::MigrateDatabase, PgPool, Postgres};
use std::time::Duration;
use tracing::info;

/// Create a database connection pool with default settings
pub async fn create_pool(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(500)  // Massive pool size
        .min_connections(50)
        .acquire_timeout(Duration::from_secs(120))  // Very long timeout
        .idle_timeout(Duration::from_secs(1800))
        .connect(database_url)
        .await?;

    info!("Database pool created successfully");
    Ok(pool)
}

/// Create a database connection pool optimized for testing with high concurrency
pub async fn create_test_pool(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(2000)  // Even more massive limit for concurrent tests
        .min_connections(200)
        .acquire_timeout(Duration::from_secs(600))  // 10 minute timeout
        .idle_timeout(Duration::from_secs(1200))
        .test_before_acquire(false)  // Skip connection testing for speed
        .connect(database_url)
        .await?;

    info!("Test database pool created successfully with ultra-high concurrency settings");
    Ok(pool)
}

/// Create database if it doesn't exist
pub async fn create_database_if_not_exists(database_url: &str) -> Result<()> {
    if !Postgres::database_exists(database_url).await? {
        info!("Creating database...");
        Postgres::create_database(database_url).await?;
    }
    Ok(())
}

/// Run database migrations
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("../../migrations")
        .run(pool)
        .await?;
    
    info!("Database migrations completed");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests don't currently use anything from super

    #[tokio::test]
    async fn test_pool_creation() {
        // This would require a test database
        // For now, just ensure the function compiles
    }
}