//! Database migration utilities for SeaORM integration

#[cfg(feature = "migration")]
use color_eyre::eyre::Result;
#[cfg(feature = "migration")]
use sea_orm_migration::prelude::*;
#[cfg(feature = "migration")]
use sinex_db_migration;
#[cfg(feature = "migration")]
use sqlx::PgPool;

/// Run database migrations using SeaORM migration system
#[cfg(feature = "migration")]
pub async fn run_migrations(_pool: &PgPool) -> Result<()> {
    use sea_orm_migration::sea_orm::{ConnectOptions, Database, DatabaseConnection};

    // Get database URL from pool
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    // Create SeaORM connection
    let mut opt = ConnectOptions::new(database_url);
    opt.sqlx_logging(false); // We already have sqlx logging

    let conn: DatabaseConnection = Database::connect(opt).await?;

    // Run migrations
    sinex_db_migration::Migrator::up(&conn, None).await?;

    Ok(())
}

/// Check for pending migrations
#[cfg(feature = "migration")]
pub async fn get_pending_migrations(_pool: &PgPool) -> Result<Vec<String>> {
    use sea_orm_migration::sea_orm::{ConnectOptions, Database, DatabaseConnection};

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    let mut opt = ConnectOptions::new(database_url);
    opt.sqlx_logging(false);

    let conn: DatabaseConnection = Database::connect(opt).await?;

    let pending = sinex_db_migration::Migrator::get_pending_migrations(&conn).await?;
    Ok(pending.iter().map(|m| m.name().to_string()).collect())
}

/// Get applied migrations
#[cfg(feature = "migration")]
pub async fn get_applied_migrations(_pool: &PgPool) -> Result<Vec<String>> {
    use sea_orm_migration::sea_orm::{ConnectOptions, Database, DatabaseConnection};

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    let mut opt = ConnectOptions::new(database_url);
    opt.sqlx_logging(false);

    let conn: DatabaseConnection = Database::connect(opt).await?;

    let applied = sinex_db_migration::Migrator::get_applied_migrations(&conn).await?;
    Ok(applied.iter().map(|m| m.name().to_string()).collect())
}

// Stub implementations when migration feature is not enabled
#[cfg(not(feature = "migration"))]
pub async fn run_migrations(_pool: &PgPool) -> Result<()> {
    Err(eyre!(
        "Migration feature not enabled. Add 'migration' feature to sinex-db"
    ))
}

#[cfg(not(feature = "migration"))]
pub async fn get_pending_migrations(_pool: &PgPool) -> Result<Vec<String>> {
    Err(eyre!(
        "Migration feature not enabled. Add 'migration' feature to sinex-db"
    ))
}

#[cfg(not(feature = "migration"))]
pub async fn get_applied_migrations(_pool: &PgPool) -> Result<Vec<String>> {
    Err(eyre!(
        "Migration feature not enabled. Add 'migration' feature to sinex-db"
    ))
}
