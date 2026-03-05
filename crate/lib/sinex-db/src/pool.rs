//! Database connection pool management for Sinex

use serde::{Deserialize, Serialize};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::temporal::Duration;
use sinex_primitives::units::Seconds;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres};
use std::env;
use std::sync::OnceLock;
use std::time::Instant;
use tracing::{info, warn};
use validator::Validate;

/// Common type aliases for database operations
pub type DbPool = PgPool;

/// Acquire a database connection with a hard timeout.
#[tracing::instrument(
    level = "trace",
    skip(pool),
    fields(
        pool_size = pool.size(),
        pool_idle = pool.num_idle(),
        timeout_ms = timeout.whole_milliseconds() as u64,
        acquire_ms = tracing::field::Empty
    )
)]
pub async fn acquire_with_timeout(
    pool: &DbPool,
    timeout: Duration,
) -> Result<PoolConnection<Postgres>> {
    let warn_threshold = pool_acquire_warn_threshold();
    let start = Instant::now();
    let std_timeout = std::time::Duration::from_millis(timeout.whole_milliseconds() as u64);
    let result = tokio::time::timeout(std_timeout, pool.acquire()).await;
    let elapsed = start.elapsed();

    // Record acquire latency in current span
    tracing::Span::current().record("acquire_ms", elapsed.as_millis() as u64);

    if !warn_threshold.is_zero() && elapsed >= warn_threshold {
        warn!(
            acquire_latency_ms = elapsed.as_millis(),
            pool_size = pool.size(),
            pool_idle = pool.num_idle(),
            warn_threshold_ms = warn_threshold.as_millis(),
            "Database pool acquire latency exceeded threshold"
        );
    }

    if let Ok(result) = result {
        result.map_err(SinexError::from)
    } else {
        tracing::error!(
            timeout_ms = timeout.whole_milliseconds(),
            pool_size = pool.size(),
            pool_idle = pool.num_idle(),
            "Database pool acquire timed out"
        );
        Err(SinexError::timeout(format!(
            "Timed out acquiring database connection after {timeout:?}"
        )))
    }
}

const DEFAULT_POOL_ACQUIRE_WARN_MS: u64 = 100;
static POOL_ACQUIRE_WARN_MS: OnceLock<u64> = OnceLock::new();

fn pool_acquire_warn_threshold() -> std::time::Duration {
    let ms = *POOL_ACQUIRE_WARN_MS.get_or_init(|| {
        std::env::var("SINEX_POOL_ACQUIRE_WARN_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_POOL_ACQUIRE_WARN_MS)
    });
    std::time::Duration::from_millis(ms)
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

    #[validate(custom(function = "validate_statement_timeout_secs"))]
    pub statement_timeout_secs: Seconds,

    pub validate_against_postgres_max: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            min_connections: 10,
            acquire_timeout_secs: Seconds::from_secs(30),
            idle_timeout_secs: Seconds::from_secs(300),
            statement_timeout_secs: Seconds::from_secs(60),
            validate_against_postgres_max: true,
        }
    }
}

impl PoolConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = env::var("SINEX_DB_MAX_CONNECTIONS") {
            if let Ok(num) = val.parse() {
                config.max_connections = num;
            }
        }

        if let Ok(val) = env::var("SINEX_DB_MIN_CONNECTIONS") {
            if let Ok(num) = val.parse() {
                config.min_connections = num;
            }
        }

        if let Ok(val) = env::var("SINEX_DB_ACQUIRE_TIMEOUT_SECS") {
            if let Ok(num) = val.parse() {
                config.acquire_timeout_secs = Seconds::from_secs(num);
            }
        }

        config
    }
}

fn validate_acquire_timeout_secs(
    value: &Seconds,
) -> std::result::Result<(), validator::ValidationError> {
    let secs = value.as_secs();
    if !(1..=300).contains(&secs) {
        return Err(validator::ValidationError::new("range"));
    }
    Ok(())
}

fn validate_idle_timeout_secs(
    value: &Seconds,
) -> std::result::Result<(), validator::ValidationError> {
    let secs = value.as_secs();
    if secs > 3600 {
        return Err(validator::ValidationError::new("range"));
    }
    Ok(())
}

fn validate_statement_timeout_secs(
    value: &Seconds,
) -> std::result::Result<(), validator::ValidationError> {
    let secs = value.as_secs();
    if secs != 0 && secs > 3600 {
        return Err(validator::ValidationError::new("range"));
    }
    Ok(())
}

pub async fn create_pool(database_url: &str) -> Result<DbPool> {
    let config = PoolConfig::default();
    create_pool_with_config(database_url, &config).await
}

pub async fn create_pool_with_config(database_url: &str, config: &PoolConfig) -> Result<DbPool> {
    config
        .validate()
        .map_err(|e| SinexError::validation("pool config validation failed").with_std_error(&e))?;

    let statement_timeout_secs = config.statement_timeout_secs.as_secs();
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(std::time::Duration::from_secs(
            config.acquire_timeout_secs.as_secs(),
        ))
        .idle_timeout(std::time::Duration::from_secs(
            config.idle_timeout_secs.as_secs(),
        ))
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                let timeout_value = if statement_timeout_secs == 0 {
                    "0".to_string()
                } else {
                    format!("{statement_timeout_secs}s")
                };
                sqlx::query(&format!("SET statement_timeout = '{timeout_value}'"))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .map_err(SinexError::from)?;

    info!("Database pool created successfully");
    Ok(pool)
}

pub fn get_database_url() -> Result<String> {
    use sinex_primitives::environment::environment;

    let base_url = env::var("DATABASE_URL")
        .map_err(|_| SinexError::configuration("DATABASE_URL environment variable is required"))?;

    environment().database_url(&base_url).map_err(|e| {
        SinexError::configuration("failed to construct database URL").with_std_error(&e)
    })
}

pub async fn create_pool_strict() -> Result<DbPool> {
    let database_url = get_database_url()?;
    create_pool(&database_url).await
}

pub async fn create_pool_with_config_strict(config: &PoolConfig) -> Result<DbPool> {
    let database_url = get_database_url()?;
    create_pool_with_config(&database_url, config).await
}

pub async fn create_test_pool(database_url: &str) -> Result<DbPool> {
    let test_config = PoolConfig {
        max_connections: 100,
        min_connections: 10,
        acquire_timeout_secs: Seconds::from_secs(30),
        idle_timeout_secs: Seconds::from_secs(300),
        statement_timeout_secs: Seconds::from_secs(60),
        validate_against_postgres_max: false,
    };
    create_pool_with_config(database_url, &test_config).await
}

pub async fn create_database_if_not_exists(database_url: &str) -> Result<()> {
    use sqlx::migrate::MigrateDatabase;
    if !Postgres::database_exists(database_url)
        .await
        .map_err(SinexError::from)?
    {
        info!("Creating database...");
        Postgres::create_database(database_url)
            .await
            .map_err(SinexError::from)?;
    }
    Ok(())
}
