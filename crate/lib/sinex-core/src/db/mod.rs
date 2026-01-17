//! Database module for sinex-core
//!
//! This module contains all database-related functionality that was previously in sinex-db.

pub mod advisory_lock;
pub mod integrity;
pub mod models;
pub mod pool;
pub mod query_helpers;
pub mod sanitization;
pub mod security;
pub mod validation;

// Core modules
// Repository pattern - the new way to access data
pub mod replay;
pub mod repositories;

// Database schema definitions using SeaQuery
pub use sinex_schema::schema;

// Migration support
pub mod migration;
pub use migration::run_migrations_for_url;

// Re-export query helpers for easier access
pub use query_helpers::{
    count, db_error, exists, is_retryable_db_error, with_retry_transaction_idempotent,
    with_transaction, IdempotentTransaction, RetryConfig,
};

// Re-export ULID conversion utilities from sinex-schema
pub use sinex_schema::ulid_conversions::{
    from_db, opt_from_db, opt_to_db, opt_vec_from_db, opt_vec_to_db, to_db, ulid_to_uuid,
    uuid_to_ulid, DbUuidCollectionExt, DbUuidExt, UlidArrayExt, UlidExt,
};

// Re-export repository pattern
pub use repositories::{
    DbPoolExt, DbResult as RepoResult, EventPayloadSchema, EventSearchFilters, NewSchema,
};

use crate::types::Seconds;
use crate::SinexError;
use color_eyre::eyre::{eyre, Result};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgPoolOptions;
use sqlx::{migrate::MigrateDatabase, PgPool, Postgres};
use std::env;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use validator::Validate;

// Common type aliases for database operations
pub type DbPool = PgPool;
pub type DbPoolRef<'a> = &'a PgPool;

// Re-export PgPool for external crates (avoiding naming conflict)
pub use sqlx::PgPool as SqlxPgPool;

// Import type aliases from types module
pub use crate::{JsonValue, Timestamp};
pub type OptionalTimestamp = Option<Timestamp>;

/// Acquire a database connection with a hard timeout.
pub async fn acquire_with_timeout(
    pool: &DbPool,
    timeout: Duration,
) -> std::result::Result<PoolConnection<Postgres>, SinexError> {
    let warn_threshold = pool_acquire_warn_threshold();
    let start = Instant::now();
    let result = tokio::time::timeout(timeout, pool.acquire()).await;
    let elapsed = start.elapsed();
    if !warn_threshold.is_zero() && elapsed >= warn_threshold {
        warn!(
            acquire_latency_ms = elapsed.as_millis(),
            pool_size = pool.size(),
            pool_idle = pool.num_idle(),
            warn_threshold_ms = warn_threshold.as_millis(),
            "Database pool acquire latency exceeded threshold"
        );
    }

    match result {
        Ok(result) => result.map_err(SinexError::from),
        Err(_) => Err(SinexError::timeout(format!(
            "Timed out acquiring database connection after {timeout:?}"
        ))),
    }
}

const DEFAULT_POOL_ACQUIRE_WARN_MS: u64 = 100;
static POOL_ACQUIRE_WARN_MS: OnceCell<u64> = OnceCell::new();
const DEFAULT_POOL_ACQUIRE_TIMEOUT_SECS: Seconds = Seconds::from_secs(30);
static POOL_ACQUIRE_TIMEOUT_SECS: OnceCell<Seconds> = OnceCell::new();

fn pool_acquire_warn_threshold() -> Duration {
    let ms = *POOL_ACQUIRE_WARN_MS.get_or_init(|| {
        std::env::var("SINEX_POOL_ACQUIRE_WARN_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_POOL_ACQUIRE_WARN_MS)
    });
    Duration::from_millis(ms)
}

/// Default per-call hard timeout for pool acquisition.
pub fn pool_acquire_timeout() -> Duration {
    let secs = *POOL_ACQUIRE_TIMEOUT_SECS.get_or_init(|| {
        env::var("SINEX_POOL_ACQUIRE_TIMEOUT_SECS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(Seconds::from_secs)
            .unwrap_or(DEFAULT_POOL_ACQUIRE_TIMEOUT_SECS)
    });
    Duration::from_secs(secs.as_secs())
}

/// Configuration for database connection pool
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct PoolConfig {
    #[validate(range(min = 1, max = 1000))]
    pub max_connections: u32,

    #[validate(range(min = 0, max = 100))]
    pub min_connections: u32,

    #[validate(custom(function = "validate_acquire_timeout_secs"))]
    pub acquire_timeout_secs: Seconds,

    #[validate(custom(function = "validate_idle_timeout_secs"))]
    pub idle_timeout_secs: Seconds,

    pub validate_against_postgres_max: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 100, // Increased for high-throughput node cluster
            min_connections: 10,  // Increased minimum to handle baseline load
            acquire_timeout_secs: Seconds::from_secs(30),
            idle_timeout_secs: Seconds::from_secs(300), // 5 minutes
            validate_against_postgres_max: true,
        }
    }
}

fn validate_acquire_timeout_secs(value: &Seconds) -> Result<(), validator::ValidationError> {
    let secs = value.as_secs();
    if !(1..=300).contains(&secs) {
        return Err(validator::ValidationError::new("range"));
    }
    Ok(())
}

fn validate_idle_timeout_secs(value: &Seconds) -> Result<(), validator::ValidationError> {
    let secs = value.as_secs();
    if secs > 3600 {
        return Err(validator::ValidationError::new("range"));
    }
    Ok(())
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

    // Keep environment behavior unchanged; do not force SQLx simple protocol here.

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
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs.as_secs()))
        .idle_timeout(Duration::from_secs(config.idle_timeout_secs.as_secs()))
        .connect(database_url)
        .await?;

    info!(
        max_connections = config.max_connections,
        min_connections = config.min_connections,
        acquire_timeout_secs = config.acquire_timeout_secs.as_secs(),
        "Database pool created successfully"
    );
    Ok(pool)
}

/// Get database URL from environment with environment namespacing
///
/// This function gets the DATABASE_URL and applies environment-specific namespacing
/// to ensure proper isolation between dev/staging/prod environments.
pub fn get_database_url() -> Result<String> {
    use crate::environment::environment;

    let base_url = env::var("DATABASE_URL").map_err(|_| {
        eyre!(
            "DATABASE_URL environment variable is required. Set it like: \
             export DATABASE_URL=postgresql:///sinex?host=/run/postgresql \
             (database name will be automatically namespaced for environment)"
        )
    })?;

    // Apply environment namespacing to ensure isolation
    Ok(environment().database_url(&base_url)?)
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
    let max_connections_str = sqlx::query_scalar!("SHOW max_connections")
        .fetch_one(&temp_pool)
        .await?
        .unwrap_or_else(|| "0".to_string());

    let postgres_max_connections: i32 = max_connections_str
        .parse()
        .map_err(|e| eyre!("Failed to parse max_connections value: {e}"))?;

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
        acquire_timeout_secs: Seconds::from_secs(30),
        idle_timeout_secs: Seconds::from_secs(300),
        validate_against_postgres_max: false, // Skip validation in tests
    };

    // Keep environment behavior unchanged during tests.

    let pool = PgPoolOptions::new()
        .max_connections(test_config.max_connections)
        .min_connections(test_config.min_connections)
        .acquire_timeout(Duration::from_secs(
            test_config.acquire_timeout_secs.as_secs(),
        ))
        .idle_timeout(Duration::from_secs(test_config.idle_timeout_secs.as_secs()))
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

/// Run database migrations using the SeaORM migration system.
pub async fn run_migrations(pool: DbPoolRef<'_>) -> Result<()> {
    // Use the new migration system
    migration::run_migrations(pool).await?;
    info!("Database migrations completed");
    Ok(())
}
