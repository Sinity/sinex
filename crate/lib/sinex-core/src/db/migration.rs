//! Database migration utilities for SeaORM integration

use color_eyre::eyre::Result;
use sea_orm_migration::prelude::*;
use sinex_schema;
use sqlx::PgPool;

/// Run database migrations using SeaORM migration system
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    use sea_orm_migration::sea_orm::SqlxPostgresConnector;

    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
    sinex_schema::Migrator::up(&conn, None).await?;
    Ok(())
}

pub async fn run_migrations_for_url(database_url: &str) -> Result<()> {
    use sea_orm_migration::sea_orm::{ConnectOptions, Database, DatabaseConnection};

    let mut opt = ConnectOptions::new(database_url.to_string());
    opt.sqlx_logging(false);

    let conn: DatabaseConnection = Database::connect(opt).await?;
    sinex_schema::Migrator::up(&conn, None).await?;

    Ok(())
}

/// Check for pending migrations
pub async fn get_pending_migrations(pool: &PgPool) -> Result<Vec<String>> {
    use sea_orm_migration::sea_orm::SqlxPostgresConnector;

    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
    let pending = sinex_schema::Migrator::get_pending_migrations(&conn).await?;
    Ok(pending.iter().map(|m| m.name().to_string()).collect())
}

/// Get applied migrations
pub async fn get_applied_migrations(pool: &PgPool) -> Result<Vec<String>> {
    use sea_orm_migration::sea_orm::SqlxPostgresConnector;

    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
    let applied = sinex_schema::Migrator::get_applied_migrations(&conn).await?;
    Ok(applied.iter().map(|m| m.name().to_string()).collect())
}
