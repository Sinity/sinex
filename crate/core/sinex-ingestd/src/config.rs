#![doc = include_str!("../doc/config.md")]

//! Configuration helpers for the ingestion daemon.

use crate::{IngestdResult, SinexError};
use camino::Utf8PathBuf;
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use sinex_core::{
    environment::environment,
    types::{deserialize_validated_utf8_path, validate_path},
};
use tracing::{debug, error, info, warn};
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

    /// Skip schema synchronization on startup (useful for tests)
    #[builder(default = false)]
    pub skip_schema_sync: bool,

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
    #[builder(default = default_nats_stream_name())]
    pub nats_stream_name: String,

    /// NATS consumer durable name
    #[validate(length(min = 1, message = "NATS consumer name cannot be empty"))]
    #[builder(default = String::from("ingestd"))]
    pub nats_consumer_name: String,

    /// git-annex repository path for assembled materials
    #[serde(deserialize_with = "deserialize_validated_utf8_path")]
    #[validate(custom(
        function = "validate_annex_path",
        message = "Invalid annex repository path"
    ))]
    #[builder(default = default_annex_repo_path())]
    pub annex_repo_path: Utf8PathBuf,

    /// Directory used to persist in-flight assembler state between restarts
    #[serde(deserialize_with = "deserialize_validated_utf8_path")]
    #[validate(custom(
        function = "validate_state_dir",
        message = "Invalid assembler state directory"
    ))]
    #[builder(default = default_assembler_state_dir())]
    pub assembler_state_dir: Utf8PathBuf,
}

impl IngestdConfig {
    /// Build a Figment instance with defaults, config files, and environment overrides.
    fn build_figment_base() -> Figment {
        Figment::from(Serialized::defaults(Self::default()))
            .merge(Toml::file("ingestd.toml").nested())
            .merge(Toml::file("/etc/sinex/ingestd.toml").nested())
    }

    /// Add shared environment variable layers for ingestd configuration.
    fn add_env(figment: Figment) -> Figment {
        figment
            .merge(Env::prefixed("INGESTD_").split('_'))
            .merge(Env::raw().only(&["DATABASE_URL"]))
    }

    /// Load configuration from defaults, files, and environment overrides.
    pub fn load() -> Result<Self, figment::Error> {
        Self::add_env(Self::build_figment_base()).extract()
    }

    /// Load configuration including a specific config file.
    pub fn load_from_path(path: impl AsRef<str>) -> Result<Self, figment::Error> {
        let figment = Self::build_figment_base().merge(Toml::file(path.as_ref()).nested());
        Self::add_env(figment).extract()
    }

    /// Load configuration from an existing Figment instance.
    pub fn from_figment(figment: Figment) -> Result<Self, figment::Error> {
        Self::add_env(figment).extract()
    }

    /// Create configuration from command line arguments using the builder
    pub fn from_args(
        database_url: Option<String>,
        nats_url: String,
        pool_size: u32,
        batch_size: usize,
        batch_timeout_secs: u64,
        dry_run: bool,
        annex_repo_path: Option<String>,
        assembler_state_dir: Option<String>,
    ) -> Self {
        let skip_schema_sync = env_flag("SINEX_SKIP_SCHEMA_SYNC").unwrap_or(false);
        let validate_schemas = env_flag("SINEX_VALIDATE_SCHEMAS").unwrap_or(true);

        let builder = Self::builder()
            .nats_url(nats_url)
            .database_pool_size(pool_size)
            .batch_size(batch_size)
            .batch_timeout_secs(batch_timeout_secs)
            .dry_run(dry_run)
            .skip_schema_sync(skip_schema_sync)
            .validate_schemas(validate_schemas);

        let db_url = database_url.unwrap_or_else(default_database_url);
        let builder = builder.database_url(db_url);

        let mut config = builder.build();

        if let Some(path) = annex_repo_path {
            config.annex_repo_path = Utf8PathBuf::from(path);
        }

        if let Some(path) = assembler_state_dir {
            config.assembler_state_dir = Utf8PathBuf::from(path);
        }

        config
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
            SinexError::configuration(format!("Validation failed: {e}"))
                .with_operation("config.validate_connection_strings")
        })?;

        // Ensure work directory exists using atomic create_dir_all
        match tokio::fs::create_dir_all(&self.work_dir).await {
            Ok(()) => {
                debug!("Ensured work directory exists: {}", self.work_dir.as_str());
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Directory already exists, this is fine
                debug!("Work directory already exists: {}", self.work_dir.as_str());
            }
            Err(e) => {
                return Err(SinexError::configuration(format!(
                    "Cannot create work directory {}: {}",
                    self.work_dir.as_str(),
                    e
                )));
            }
        }

        if tokio::fs::metadata(&self.annex_repo_path).await.is_err() {
            warn!(
                path = %self.annex_repo_path,
                "Annex repository path does not exist; git-annex will attempt initialization"
            );
        }

        if let Err(e) = tokio::fs::create_dir_all(&self.assembler_state_dir).await {
            return Err(SinexError::configuration(format!(
                "Cannot create assembler state directory {}: {}",
                self.assembler_state_dir.as_str(),
                e
            )));
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
                SinexError::configuration(format!("Database connection test failed: {e}"))
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
                SinexError::configuration(format!("NATS connection test failed: {e}"))
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

fn env_flag(name: &str) -> Option<bool> {
    match std::env::var(name) {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            Some(matches!(normalized.as_str(), "1" | "true" | "yes" | "on"))
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                env = name,
                "Environment variable is not valid UTF-8; ignoring"
            );
            None
        }
        Err(std::env::VarError::NotPresent) => None,
    }
}

impl Default for IngestdConfig {
    fn default() -> Self {
        let env = environment();
        Self {
            database_url: default_database_url(),
            database_pool_size: 50,
            nats_url: "nats://localhost:4222".to_string(),
            batch_size: 1000,
            batch_timeout_secs: 5,
            dry_run: false,
            validate_schemas: true,
            skip_schema_sync: false,
            work_dir: default_work_dir(),
            max_message_size: 16 * 1024 * 1024,
            nats_stream_name: default_nats_stream_name(),
            nats_consumer_name: format!("ingestd-{}", env.name()),
            annex_repo_path: default_annex_repo_path(),
            assembler_state_dir: default_assembler_state_dir(),
        }
    }
}

// Helper functions

/// Default database URL with environment namespacing
fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        let env = environment();
        let base_name = env.database_name("sinex");
        format!("postgresql:///{base_name}?host=/run/postgresql")
    })
}

/// Default work directory for ingestd with environment namespacing
fn default_work_dir() -> Utf8PathBuf {
    let env = environment();
    let base_dir = dirs::cache_dir()
        .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())
        .unwrap_or_else(|| Utf8PathBuf::from("/tmp"));

    let work_dir = env.work_directory(base_dir.join("sinex").join("ingestd"));

    // Validate the default path
    match validate_path(work_dir.to_str().unwrap_or("/tmp/sinex/ingestd")) {
        Ok(validated) => validated,
        Err(_) => {
            // Fallback to a safe default if validation fails
            Utf8PathBuf::from_path_buf(env.work_directory("/tmp/sinex/ingestd"))
                .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex/ingestd"))
        }
    }
}

/// Default NATS stream name with environment namespacing
fn default_nats_stream_name() -> String {
    let env = environment();
    env.nats_stream_name("SINEX_RAW_EVENTS")
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

fn validate_work_dir(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_core::types::validate_path;

    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_work_dir"))
}

fn default_annex_repo_path() -> Utf8PathBuf {
    use sinex_core::types::validate_path;

    if let Ok(path) = std::env::var("SINEX_ANNEX_PATH") {
        if let Ok(validated) = validate_path(&path) {
            return validated;
        }
    }

    let annex = default_work_dir().join("annex");
    validate_path(annex.as_str()).unwrap_or(annex)
}

fn default_assembler_state_dir() -> Utf8PathBuf {
    use sinex_core::types::validate_path;

    let state_dir = default_work_dir().join("assembler_state");
    validate_path(state_dir.as_str()).unwrap_or(state_dir)
}

fn validate_annex_path(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_core::types::validate_path;
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_annex_path"))
}

fn validate_state_dir(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_core::types::validate_path;
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_state_dir"))
}
