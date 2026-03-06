use crate::DbPool;
use sinex_primitives::error::{Result, SinexError};
use tracing::info;

/// Apply declarative schema using the given pool.
pub async fn run_migrations(pool: &DbPool) -> Result<()> {
    info!("Applying declarative database schema...");
    sinex_schema::apply::apply(pool)
        .await
        .map_err(|e| SinexError::database(format!("Schema apply failed: {e}")))?;
    info!("Database schema apply completed");
    Ok(())
}

/// Apply declarative schema for a given database URL by creating a temporary connection.
pub async fn run_migrations_for_url(database_url: &str) -> Result<()> {
    use crate::pool::create_pool;

    let pool = create_pool(database_url)
        .await
        .map_err(|e| SinexError::database(format!("Failed to create pool for schema apply: {e}")))?;

    run_migrations(&pool).await?;
    pool.close().await;
    Ok(())
}
