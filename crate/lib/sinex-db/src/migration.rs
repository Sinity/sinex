use crate::DbPool;
use sea_orm::SqlxPostgresConnector;
use sinex_primitives::error::{Result, SinexError};
use sinex_schema::{Migrator, MigratorTrait};
use tracing::info;

/// Run migrations using the given pool
pub async fn run_migrations(pool: &DbPool) -> Result<()> {
    info!("Running database migrations...");
    let db = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
    Migrator::up(&db, None)
        .await
        .map_err(|e| SinexError::database(format!("Migration failed: {e}")))?;
    info!("Database migrations completed");
    Ok(())
}

/// Run migrations for a given database URL by creating a temporary connection
pub async fn run_migrations_for_url(database_url: &str) -> Result<()> {
    use crate::pool::create_pool;

    let pool = create_pool(database_url)
        .await
        .map_err(|e| SinexError::database(format!("Failed to create pool for migration: {e}")))?;

    run_migrations(&pool).await?;
    pool.close().await;
    Ok(())
}
