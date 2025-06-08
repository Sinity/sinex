pub mod models;
pub mod pool;
pub mod queries;
pub mod queries_macro_safe;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::{migrate::MigrateDatabase, PgPool, Postgres};
use std::time::Duration;
use tracing::info;

/// Create a database connection pool with default settings
pub async fn create_pool(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .min_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(600))
        .connect(database_url)
        .await?;

    info!("Database pool created successfully");
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
    sqlx::migrate!("../../migration")
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