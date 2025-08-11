//! Configuration for the ingestion daemon

use crate::{IngestdResult, SinexError};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_core::types::{deserialize_validated_utf8_path, validate_path};
use tracing::{error, info};
use validator::Validate;

/// Configuration for the ingestion daemon
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct IngestdConfig {
    /// Database URL for PostgreSQL connection
    #[validate(url(message = "Invalid database URL"))]
    #[validate(custom(
        function = "validate_postgres_url",
        message = "Must be a PostgreSQL URL"
    ))]
    #[builder(default = default_database_url())]
    pub database_url: String,

    /// Database connection pool size
    #[validate(range(min = 1, max = 1000, message = "Pool size must be between 1 and 1000"))]
    #[builder(default = 50)]
    pub database_pool_size: u32,

    /// NATS URL for message bus
    #[validate(length(min = 1, message = "NATS URL cannot be empty"))]
    #[builder(default = String::from("nats://localhost:4222"))]
    pub nats_url: String,

    /// Unix Domain Socket path for gRPC server
    #[validate(custom(function = "validate_socket_path", message = "Invalid socket path"))]
    #[builder(default = String::from("/run/sinex/ingest.sock"))]
    pub socket_path: String,

    /// Batch size for database writes
    #[validate(range(min = 1, message = "Batch size must be greater than 0"))]
    #[builder(default = 1000)]
    pub batch_size: usize,

    /// Batch timeout in seconds
    #[validate(range(min = 1, message = "Batch timeout must be greater than 0"))]
    #[builder(default = 5)]
    pub batch_timeout_secs: u64,

    /// Enable dry-run mode (no database writes)
    #[builder(default = false)]
    pub dry_run: bool,

    /// Enable schema validation
    #[builder(default = true)]
    pub validate_schemas: bool,

    /// Working directory for temporary files
    #[serde(deserialize_with = "deserialize_validated_utf8_path")]
    #[validate(custom(function = "validate_work_dir", message = "Invalid work directory"))]
    #[builder(default = default_work_dir())]
    pub work_dir: Utf8PathBuf,

    /// Maximum message size in bytes
    #[validate(range(
        min = 1024,
        max = 1073741824,
        message = "Max message size must be between 1KB and 1GB"
    ))]
    #[builder(default = 16 * 1024 * 1024)]
    pub max_message_size: usize,

    /// NATS stream name for events
    #[validate(length(min = 1, message = "NATS stream name cannot be empty"))]
    #[builder(default = String::from("EVENTS"))]
    pub nats_stream_name: String,

    /// NATS consumer durable name
    #[validate(length(min = 1, message = "NATS consumer name cannot be empty"))]
    #[builder(default = String::from("ingestd"))]
    pub nats_consumer_name: String,
}

impl IngestdConfig {
    /// Create configuration from command line arguments using the builder
    pub fn from_args(
        database_url: Option<String>,
        nats_url: String,
        socket_path: String,
        pool_size: u32,
        batch_size: usize,
        batch_timeout_secs: u64,
        dry_run: bool,
    ) -> IngestdResult<Self> {
        let builder = Self::builder()
            .nats_url(nats_url)
            .socket_path(socket_path)
            .database_pool_size(pool_size)
            .batch_size(batch_size)
            .batch_timeout_secs(batch_timeout_secs)
            .dry_run(dry_run);

        let db_url = database_url.unwrap_or_else(default_database_url);
        let builder = builder.database_url(db_url);

        Ok(builder.build())
    }

    /// Validate configuration and exit with appropriate status code
    pub async fn validate_and_exit(&self) -> ! {
        info!("Validating configuration...");
        match self.validate().await {
            Ok(()) => {
                info!("✅ Configuration is valid");
                std::process::exit(0);
            }
            Err(e) => {
                error!("❌ Configuration validation failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    /// Validate the configuration
    pub async fn validate(&self) -> IngestdResult<()> {
        use validator::Validate as ValidateTrait;

        // Run validator crate validation first
        ValidateTrait::validate(self).map_err(|e| {
            SinexError::configuration(format!("Validation failed: {}", e))
                .with_operation("config.validate_connection_strings")
        })?;

        // Additional runtime validation - create directories if needed
        if let Some(parent) = Utf8PathBuf::from(&self.socket_path).parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    SinexError::configuration(format!(
                        "Cannot create socket directory {}: {}",
                        parent.as_str(),
                        e
                    ))
                })?;
            }
        }

        if !self.work_dir.exists() {
            tokio::fs::create_dir_all(&self.work_dir)
                .await
                .map_err(|e| {
                    SinexError::configuration(format!(
                        "Cannot create work directory {}: {}",
                        self.work_dir.as_str(),
                        e
                    ))
                })?;
        }

        // Test database connection
        self.test_database_connection().await?;

        // Test NATS connection
        self.test_nats_connection().await?;

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
            .map_err(|e| {
                SinexError::configuration(format!("Database connection test failed: {}", e))
                    .with_operation("config.test_database_connection")
                    .with_context("database_url", self.database_url.clone())
            })?;

        pool.close().await;
        info!("Database connection test passed");
        Ok(())
    }

    /// Test NATS connection
    async fn test_nats_connection(&self) -> IngestdResult<()> {
        use async_nats::ConnectOptions;

        let client = ConnectOptions::new()
            .name("ingestd-test")
            .connect(&self.nats_url)
            .await
            .map_err(|e| {
                SinexError::configuration(format!("NATS connection test failed: {}", e))
                    .with_operation("config.test_nats_connection")
                    .with_context("nats_url", self.nats_url.clone())
            })?;

        // Connection successful
        info!("NATS connection test passed");
        drop(client);
        Ok(())
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
            nats_url: "nats://localhost:4222".to_string(),
            socket_path: "/run/sinex/ingest.sock".to_string(),
            batch_size: 1000,
            batch_timeout_secs: 5,
            dry_run: false,
            validate_schemas: true,
            work_dir: Utf8PathBuf::from("/tmp/sinex/ingestd"),
            max_message_size: 16 * 1024 * 1024,
            nats_stream_name: "EVENTS".to_string(),
            nats_consumer_name: "ingestd".to_string(),
        }
    }
}

// Helper functions

/// Default database URL
fn default_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string())
}

/// Default work directory for ingestd (validated)
fn default_work_dir() -> Utf8PathBuf {
    let base_dir = dirs::cache_dir()
        .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())
        .unwrap_or_else(|| Utf8PathBuf::from("/tmp"));

    let work_dir = base_dir.join("sinex").join("ingestd");

    // Validate the default path
    match validate_path(work_dir.as_str()) {
        Ok(validated) => validated,
        Err(_) => {
            // Fallback to a safe default if validation fails
            Utf8PathBuf::from("/tmp/sinex/ingestd")
        }
    }
}

// Custom validator functions

fn validate_postgres_url(url: &str) -> Result<(), validator::ValidationError> {
    match url::Url::parse(url) {
        Ok(parsed_url) => {
            if matches!(parsed_url.scheme(), "postgresql" | "postgres") {
                Ok(())
            } else {
                Err(validator::ValidationError::new("not_postgres_url"))
            }
        }
        Err(_) => Err(validator::ValidationError::new("invalid_url")),
    }
}

fn validate_socket_path(path: &str) -> Result<(), validator::ValidationError> {
    use sinex_core::types::validate_path;

    validate_path(path)
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_socket_path"))
}

fn validate_work_dir(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_core::types::validate_path;

    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_work_dir"))
}
