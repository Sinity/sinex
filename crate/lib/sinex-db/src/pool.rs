//! Database connection pool management for Sinex

use serde::{Deserialize, Serialize};
use sinex_primitives::env as shared_env;
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::temporal::Duration;
use sinex_primitives::units::Seconds;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool, Postgres};
use std::env;
use std::str::FromStr;
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

fn env_parse_override<T>(var: &str, context: &str) -> Option<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    shared_env::parse_optional(var, context)
}

fn env_parse_with_default<T>(var: &str, default: T, context: &str) -> T
where
    T: FromStr + Clone,
    T::Err: std::fmt::Display,
{
    shared_env::parse_or(var, default, context)
}

fn pool_acquire_warn_threshold() -> std::time::Duration {
    let ms = *POOL_ACQUIRE_WARN_MS.get_or_init(|| {
        env_parse_with_default(
            "SINEX_POOL_ACQUIRE_WARN_MS",
            DEFAULT_POOL_ACQUIRE_WARN_MS,
            "database pool acquire warn threshold",
        )
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
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Some(num) =
            env_parse_override("SINEX_DB_MAX_CONNECTIONS", "database pool max connections")
        {
            config.max_connections = num;
        }

        if let Some(num) =
            env_parse_override("SINEX_DB_MIN_CONNECTIONS", "database pool min connections")
        {
            config.min_connections = num;
        }

        if let Some(num) = env_parse_override(
            "SINEX_DB_ACQUIRE_TIMEOUT_SECS",
            "database pool acquire timeout",
        ) {
            config.acquire_timeout_secs = Seconds::from_secs(num);
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

    if config.validate_against_postgres_max {
        validate_pool_size_against_postgres_max(database_url, config.max_connections).await?;
    }

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
                configure_session_timeout(conn, statement_timeout_secs).await?;
                Ok(())
            })
        })
        .before_acquire(move |conn, _meta| {
            Box::pin(async move {
                if let Err(error) = configure_session_timeout(conn, statement_timeout_secs).await {
                    warn!(error = %error, "Database pooled connection failed session preflight");
                    return Ok(false);
                }

                Ok(true)
            })
        })
        .connect(database_url)
        .await
        .map_err(SinexError::from)?;

    info!("Database pool created successfully");
    Ok(pool)
}

pub fn resolve_effective_database_url(base_url: &str) -> Result<String> {
    if base_url.trim().is_empty() {
        return Err(SinexError::configuration("DATABASE_URL cannot be empty"));
    }
    PgConnectOptions::from_str(base_url).map_err(|error| {
        SinexError::configuration("failed to parse DATABASE_URL").with_std_error(&error)
    })?;
    Ok(base_url.to_string())
}

pub fn get_database_url() -> Result<String> {
    let base_url = env::var("DATABASE_URL")
        .map_err(|_| SinexError::configuration("DATABASE_URL environment variable is required"))?;

    resolve_effective_database_url(&base_url)
}

async fn validate_pool_size_against_postgres_max(
    database_url: &str,
    configured_max_connections: u32,
) -> Result<()> {
    let mut connection = PgConnection::connect(database_url)
        .await
        .map_err(SinexError::from)?;

    let postgres_max_connections = sqlx::query_scalar::<_, String>("SHOW max_connections")
        .fetch_one(&mut connection)
        .await
        .map_err(SinexError::from)?;
    let postgres_max_connections = postgres_max_connections.parse::<u32>().map_err(|error| {
        SinexError::configuration("failed to parse PostgreSQL max_connections")
            .with_std_error(&error)
            .with_context("value", postgres_max_connections.clone())
    })?;

    if configured_max_connections > postgres_max_connections {
        return Err(SinexError::configuration(format!(
            "configured pool max_connections ({configured_max_connections}) exceeds PostgreSQL max_connections ({postgres_max_connections})"
        )));
    }

    Ok(())
}

async fn configure_session_timeout(
    conn: &mut PgConnection,
    statement_timeout_secs: u64,
) -> sqlx::Result<()> {
    let timeout_value = if statement_timeout_secs == 0 {
        "0".to_string()
    } else {
        format!("{statement_timeout_secs}s")
    };

    sqlx::query("SELECT pg_catalog.set_config('statement_timeout', $1, false)")
        .bind(timeout_value)
        .execute(conn)
        .await?;
    Ok(())
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

#[cfg(test)]
mod tests {
    // Inline because this covers local env parsing semantics in the pool module.
    use super::{
        DEFAULT_POOL_ACQUIRE_WARN_MS, PoolConfig, env_parse_override, env_parse_with_default,
    };
    use xtask::sandbox::sinex_serial_test;

    use xtask::sandbox::EnvGuard;

    #[test]
    fn env_parse_override_rejects_invalid_numeric_values() {
        let mut env = EnvGuard::new();
        env.set("SINEX_UNUSED", "bogus");
        let parsed = env_parse_override::<u64>("SINEX_UNUSED", "test context");
        assert!(parsed.is_none());
    }

    #[test]
    fn env_parse_with_default_keeps_default_without_override() {
        let parsed =
            env_parse_with_default("SINEX_UNUSED", DEFAULT_POOL_ACQUIRE_WARN_MS, "test context");
        assert_eq!(parsed, DEFAULT_POOL_ACQUIRE_WARN_MS);
    }

    #[sinex_serial_test]
    async fn pool_config_from_env_ignores_invalid_overrides() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_DB_MAX_CONNECTIONS", "bogus");
        env.set("SINEX_DB_MIN_CONNECTIONS", "bogus");
        env.set("SINEX_DB_ACQUIRE_TIMEOUT_SECS", "bogus");

        let config = PoolConfig::from_env();

        assert_eq!(
            config.max_connections,
            PoolConfig::default().max_connections
        );
        assert_eq!(
            config.min_connections,
            PoolConfig::default().min_connections
        );
        assert_eq!(
            config.acquire_timeout_secs.as_secs(),
            PoolConfig::default().acquire_timeout_secs.as_secs()
        );
        Ok(())
    }
}
