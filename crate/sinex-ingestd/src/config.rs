//! Configuration for the ingestion daemon

use crate::{IngestdError, IngestdResult};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::info;

/// Configuration for the ingestion daemon
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestdConfig {
    /// Database URL for PostgreSQL connection
    pub database_url: String,

    /// Database connection pool size
    pub database_pool_size: u32,

    /// Redis URL for message bus
    pub redis_url: String,

    /// Unix Domain Socket path for gRPC server
    pub socket_path: String,

    /// Batch size for database writes
    pub batch_size: usize,

    /// Batch timeout in seconds
    pub batch_timeout_secs: u64,

    /// Enable dry-run mode (no database writes)
    pub dry_run: bool,

    /// Enable schema validation
    pub validate_schemas: bool,

    /// Working directory for temporary files
    pub work_dir: PathBuf,

    /// Maximum message size in bytes
    pub max_message_size: usize,

    /// Redis stream prefix for topics
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
        // Validate database URL format
        if !self.database_url.starts_with("postgresql://")
            && !self.database_url.starts_with("postgres://")
        {
            return Err(IngestdError::Config(
                "Database URL must be a PostgreSQL connection string".to_string(),
            ));
        }

        // Validate Redis URL format
        if !self.redis_url.starts_with("redis://") && !self.redis_url.starts_with("rediss://") {
            return Err(IngestdError::Config(
                "Redis URL must be a valid Redis connection string".to_string(),
            ));
        }

        // Validate batch configuration
        if self.batch_size == 0 {
            return Err(IngestdError::Config(
                "Batch size must be greater than 0".to_string(),
            ));
        }

        if self.batch_timeout_secs == 0 {
            return Err(IngestdError::Config(
                "Batch timeout must be greater than 0".to_string(),
            ));
        }

        if self.database_pool_size == 0 {
            return Err(IngestdError::Config(
                "Database pool size must be greater than 0".to_string(),
            ));
        }

        // Validate socket path directory exists or can be created
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

        // Validate work directory exists or can be created
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
        use sinex_db::queries::OperationQueries;
        use sqlx::postgres::PgPoolOptions;

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.database_url)
            .await?;

        // Test basic query using the query system
        OperationQueries::health_check()
            .fetch_one::<()>(&pool)
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
