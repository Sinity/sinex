//! Configuration for the ingestion daemon

use crate::{IngestdError, IngestdResult};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::info;
use validator::Validate;

/// Configuration for the ingestion daemon
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct IngestdConfig {
    /// Database URL for PostgreSQL connection
    #[validate(url(message = "Invalid database URL"))]
    #[validate(custom(
        function = "validate_postgres_url",
        message = "Must be a PostgreSQL URL"
    ))]
    pub database_url: String,

    /// Database connection pool size
    #[validate(range(min = 1, max = 1000, message = "Pool size must be between 1 and 1000"))]
    pub database_pool_size: u32,

    /// Redis URL for message bus
    #[validate(url(message = "Invalid Redis URL"))]
    #[validate(custom(function = "validate_redis_url", message = "Must be a Redis URL"))]
    pub redis_url: String,

    /// Unix Domain Socket path for gRPC server
    #[validate(custom(function = "validate_socket_path", message = "Invalid socket path"))]
    pub socket_path: String,

    /// Batch size for database writes
    #[validate(range(min = 1, message = "Batch size must be greater than 0"))]
    pub batch_size: usize,

    /// Batch timeout in seconds
    #[validate(range(min = 1, message = "Batch timeout must be greater than 0"))]
    pub batch_timeout_secs: u64,

    /// Enable dry-run mode (no database writes)
    pub dry_run: bool,

    /// Enable schema validation
    pub validate_schemas: bool,

    /// Working directory for temporary files
    #[validate(custom(function = "validate_work_dir", message = "Invalid work directory"))]
    pub work_dir: PathBuf,

    /// Maximum message size in bytes
    #[validate(range(
        min = 1024,
        max = 1073741824,
        message = "Max message size must be between 1KB and 1GB"
    ))]
    pub max_message_size: usize,

    /// Redis stream prefix for topics
    #[validate(length(min = 1, message = "Redis stream prefix cannot be empty"))]
    pub redis_stream_prefix: String,
}

impl IngestdConfig {
    /// Create configuration from command line arguments
    pub fn from_args(
        database_url: Option<String>,
        redis_url: String,
        socket_path: String,
        pool_size: u32,
        batch_size: usize,
        batch_timeout_secs: u64,
        dry_run: bool,
    ) -> IngestdResult<Self> {
        let database_url = database_url.unwrap_or_else(|| {
            std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string())
        });

        let work_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("sinex")
            .join("ingestd");

        Ok(Self {
            database_url,
            database_pool_size: pool_size,
            redis_url,
            socket_path,
            batch_size,
            batch_timeout_secs,
            dry_run,
            validate_schemas: true,
            work_dir,
            max_message_size: 16 * 1024 * 1024, // 16MB
            redis_stream_prefix: "sinex:events".to_string(),
        })
    }

    /// Validate the configuration
    pub async fn validate(&self) -> IngestdResult<()> {
        use validator::Validate as ValidateTrait;

        // Run validator crate validation first
        ValidateTrait::validate(self)
            .map_err(|e| IngestdError::Config(format!("Validation failed: {}", e)))?;

        // Additional runtime validation - create directories if needed
        if let Some(parent) = PathBuf::from(&self.socket_path).parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    IngestdError::Config(format!(
                        "Cannot create socket directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
        }

        if !self.work_dir.exists() {
            tokio::fs::create_dir_all(&self.work_dir)
                .await
                .map_err(|e| {
                    IngestdError::Config(format!(
                        "Cannot create work directory {}: {}",
                        self.work_dir.display(),
                        e
                    ))
                })?;
        }

        // Test database connection
        self.test_database_connection().await?;

        // Test Redis connection
        self.test_redis_connection().await?;

        info!("Configuration validation passed");
        Ok(())
    }

    /// Test database connection
    async fn test_database_connection(&self) -> IngestdResult<()> {
        use sqlx::postgres::PgPoolOptions;

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.database_url)
            .await?;

        // Test basic query with a simple SELECT 1
        sqlx::query("SELECT 1")
            .fetch_one(&pool)
            .await
            .map_err(|e| IngestdError::Config(format!("Database connection test failed: {}", e)))?;

        pool.close().await;
        info!("Database connection test passed");
        Ok(())
    }

    /// Test Redis connection
    async fn test_redis_connection(&self) -> IngestdResult<()> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_async_connection().await?;

        // Test basic command
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| IngestdError::Config(format!("Redis connection test failed: {}", e)))?;

        info!("Redis connection test passed");
        Ok(())
    }

    /// Get Redis stream topic name for an event source
    pub fn get_stream_topic(&self, source: &str) -> String {
        format!("{}:{}", self.redis_stream_prefix, source.replace('.', ":"))
    }

    /// Get database connection options
    pub fn get_db_options(&self) -> sqlx::postgres::PgPoolOptions {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(self.database_pool_size)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .idle_timeout(std::time::Duration::from_secs(600))
            .max_lifetime(std::time::Duration::from_secs(1800))
    }
}

impl Default for IngestdConfig {
    fn default() -> Self {
        Self {
            database_url: "postgresql:///sinex_dev?host=/run/postgresql".to_string(),
            database_pool_size: 50,
            redis_url: "redis://localhost:6379".to_string(),
            socket_path: "/run/sinex/ingest.sock".to_string(),
            batch_size: 1000,
            batch_timeout_secs: 5,
            dry_run: false,
            validate_schemas: true,
            work_dir: PathBuf::from("/tmp/sinex/ingestd"),
            max_message_size: 16 * 1024 * 1024,
            redis_stream_prefix: "sinex:events".to_string(),
        }
    }
}

// Custom validator functions

fn validate_postgres_url(url: &str) -> Result<(), validator::ValidationError> {
    if url.starts_with("postgresql://") || url.starts_with("postgres://") {
        Ok(())
    } else {
        Err(validator::ValidationError::new("not_postgres_url"))
    }
}

fn validate_redis_url(url: &str) -> Result<(), validator::ValidationError> {
    if url.starts_with("redis://") || url.starts_with("rediss://") {
        Ok(())
    } else {
        Err(validator::ValidationError::new("not_redis_url"))
    }
}

fn validate_socket_path(path: &str) -> Result<(), validator::ValidationError> {
    use sinex_validation::validate_path;

    validate_path(path)
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_socket_path"))
}

fn validate_work_dir(path: &PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_validation::validate_path;

    let path_str = path
        .to_str()
        .ok_or_else(|| validator::ValidationError::new("non_utf8_path"))?;

    validate_path(path_str)
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_work_dir"))
}
