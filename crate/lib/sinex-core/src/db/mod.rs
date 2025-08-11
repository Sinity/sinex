//! Database module for sinex-core
//!
//! This module contains all database-related functionality that was previously in sinex-db.

pub mod models;
pub mod pool;
pub mod query_helpers;
pub mod sanitization;
pub mod security;

// Core modules
pub mod constants;
pub mod distributed_locking;

// Repository pattern - the new way to access data
pub mod replay;
pub mod repositories;

// Database schema definitions using SeaQuery
pub use sinex_migrations::schema;
pub mod schema_migrations;
pub mod seaquery_helpers;

// Migration support
#[cfg(feature = "migration")]
pub mod migration;

// Re-export query helpers for easier access
pub use query_helpers::{
    count, db_error, exists, from_db, is_retryable_db_error, opt_from_db, opt_to_db,
    opt_vec_from_db, opt_vec_to_db, to_db, ulid_to_uuid, uuid_to_ulid, with_retry_transaction,
    with_transaction, DbUuidCollectionExt, DbUuidExt, RetryConfig, UlidArrayExt, UlidExt,
};

// Re-export SeaQuery ULID helpers
pub use seaquery_helpers::SeaQueryUlidExt;

// Telemetry module (optional feature)
#[cfg(feature = "telemetry")]
pub mod telemetry;

// Re-export repository pattern
pub use repositories::{
    Checkpoint, DbPoolExt, DbResult as RepoResult, EventPayloadSchema, EventSearchFilters,
    NewSchema,
};

use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{migrate::MigrateDatabase, PgPool, Postgres, Row};
use std::env;
use std::time::Duration;
use tracing::{info, warn};
use validator::Validate;

// Common type aliases for database operations
pub type DbPool = PgPool;
pub type DbPoolRef<'a> = &'a PgPool;

// Re-export PgPool for external crates (avoiding naming conflict)
pub use sqlx::PgPool as SqlxPgPool;

// Import type aliases from types module
pub use crate::types::{ulid::Timestamp, JsonValue};
pub type OptionalTimestamp = Option<Timestamp>;

/// Configuration for database connection pool
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct PoolConfig {
    #[validate(range(min = 1, max = 1000))]
    pub max_connections: u32,

    #[validate(range(min = 0, max = 100))]
    pub min_connections: u32,

    #[validate(range(min = 1, max = 300))]
    pub acquire_timeout_secs: u64,

    #[validate(range(min = 0, max = 3600))]
    pub idle_timeout_secs: u64,

    pub validate_against_postgres_max: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 25, // Conservative default
            min_connections: 5,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 300, // 5 minutes
            validate_against_postgres_max: true,
        }
    }
}

/// Create a database connection pool with default settings
pub async fn create_pool(database_url: &str) -> Result<DbPool> {
    let config = PoolConfig::default();
    create_pool_with_config(database_url, &config).await
}

/// Create a database connection pool with custom configuration
pub async fn create_pool_with_config(database_url: &str, config: &PoolConfig) -> Result<DbPool> {
    // Validate configuration using validator crate
    config
        .validate()
        .map_err(|e| eyre!("Invalid pool configuration: {}", e))?;

    // Validate configuration against PostgreSQL limits if requested
    if config.validate_against_postgres_max {
        if let Err(e) = validate_pool_config_against_postgres(database_url, config).await {
            warn!("Pool configuration validation failed: {}", e);
            warn!("Proceeding anyway - this may cause connection exhaustion in production");
        }
    }

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
        .connect(database_url)
        .await?;

    info!(
        max_connections = config.max_connections,
        min_connections = config.min_connections,
        acquire_timeout_secs = config.acquire_timeout_secs,
        "Database pool created successfully"
    );
    Ok(pool)
}

/// Get database URL from environment - DATABASE_URL required
pub fn get_database_url() -> Result<String> {
    env::var("DATABASE_URL").map_err(|_| {
        eyre!(
            "DATABASE_URL environment variable is required. Set it like: \
             export DATABASE_URL=postgresql:///sinex_dev?host=/run/postgresql"
        )
    })
}

/// Create a database connection pool
pub async fn create_pool_strict() -> Result<DbPool> {
    let database_url = get_database_url()?;
    create_pool(&database_url).await
}

/// Create a database connection pool with custom configuration
pub async fn create_pool_with_config_strict(config: &PoolConfig) -> Result<DbPool> {
    let database_url = get_database_url()?;
    create_pool_with_config(&database_url, config).await
}

/// Validate pool configuration against PostgreSQL server limits
async fn validate_pool_config_against_postgres(
    database_url: &str,
    config: &PoolConfig,
) -> Result<()> {
    // Create a temporary minimal connection to check PostgreSQL settings
    let temp_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    // Query PostgreSQL max_connections setting
    let max_connections_row = sqlx::query("SHOW max_connections")
        .fetch_one(&temp_pool)
        .await?;

    let postgres_max_connections: i32 = max_connections_row.try_get("max_connections")?;

    // Validate our pool size against PostgreSQL limits
    if config.max_connections as i32 > postgres_max_connections {
        return Err(eyre!(
            "Pool max_connections ({}) exceeds PostgreSQL max_connections ({}). \
             This will cause connection exhaustion. Consider reducing pool size or \
             increasing PostgreSQL max_connections setting.",
            config.max_connections,
            postgres_max_connections
        ));
    }

    // Warn if we're using more than 80% of available connections
    let usage_percentage =
        (config.max_connections as f64 / postgres_max_connections as f64) * 100.0;
    if usage_percentage > 80.0 {
        warn!(
            "Pool is configured to use {:.1}% of PostgreSQL max_connections. \
             Consider leaving more headroom for other applications.",
            usage_percentage
        );
    }

    info!(
        pool_max = config.max_connections,
        postgres_max = postgres_max_connections,
        usage_percent = format!("{:.1}%", usage_percentage),
        "Pool configuration validated against PostgreSQL limits"
    );

    temp_pool.close().await;
    Ok(())
}

/// Create a database connection pool optimized for testing with high concurrency
pub async fn create_test_pool(database_url: &str) -> Result<DbPool> {
    let test_config = PoolConfig {
        max_connections: 100, // High concurrency for tests
        min_connections: 10,
        acquire_timeout_secs: 30,
        idle_timeout_secs: 300,
        validate_against_postgres_max: false, // Skip validation in tests
    };

    let pool = PgPoolOptions::new()
        .max_connections(test_config.max_connections)
        .min_connections(test_config.min_connections)
        .acquire_timeout(Duration::from_secs(test_config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(test_config.idle_timeout_secs))
        .test_before_acquire(false) // Skip connection testing for speed
        .connect(database_url)
        .await?;

    info!("Test database pool created successfully with optimized concurrency settings");
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
///
/// This uses the new sea-orm-migration system. The migration feature must be enabled
/// in Cargo.toml to use this function.
#[cfg(feature = "migration")]
pub async fn run_migrations(pool: DbPoolRef<'_>) -> Result<()> {
    // Use the new migration system
    migration::run_migrations(pool).await?;
    info!("Database migrations completed");
    Ok(())
}

/// Run database migrations (stub when migration feature is disabled)
#[cfg(not(feature = "migration"))]
pub async fn run_migrations(_pool: DbPoolRef<'_>) -> Result<()> {
    Err(eyre!(
        "Database migration feature is not enabled. \
         To enable migrations, add to your Cargo.toml:\n\
         sinex-core = {{ version = \"*\", features = [\"migration\"] }}\n\n\
         Or run migrations manually with: just migrate"
    ))
}
