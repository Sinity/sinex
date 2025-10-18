#![doc = include_str!("../doc/figment_config.md")]

//! Figment bindings for ingestion configuration.

use camino::Utf8PathBuf;

// Default configuration constants
const DEFAULT_POOL_SIZE: u32 = 50;
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_BATCH_TIMEOUT: u64 = 5;
const DEFAULT_VALIDATE_SCHEMAS: bool = true;
const DEFAULT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB
const DEFAULT_SOCKET_PATH: &str = "/tmp/sinex-ingestd.sock";
const DEFAULT_NATS_CONSUMER_NAME: &str = "ingestd";
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Configuration for the ingestion daemon using Figment
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct IngestdFigmentConfig {
    /// Database URL for PostgreSQL connection
    #[validate(url(message = "Invalid database URL"))]
    #[validate(custom(
        function = "validate_postgres_url",
        message = "Must be a PostgreSQL URL"
    ))]
    pub database_url: String,

    /// Database connection pool size
    #[validate(range(min = 1, max = 1000, message = "Pool size must be between 1 and 1000"))]
    #[serde(default = "default_pool_size")]
    pub database_pool_size: u32,

    /// NATS URL for message bus
    #[validate(length(min = 1, message = "NATS URL cannot be empty"))]
    pub nats_url: String,

    /// Unix Domain Socket path for gRPC server
    #[validate(custom(function = "validate_socket_path", message = "Invalid socket path"))]
    #[serde(default = "default_socket_path")]
    pub socket_path: String,

    /// Batch size for database writes
    #[validate(range(min = 1, message = "Batch size must be greater than 0"))]
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Batch timeout in seconds
    #[validate(range(min = 1, message = "Batch timeout must be greater than 0"))]
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_secs: u64,

    /// Enable dry-run mode (no database writes)
    #[serde(default)]
    pub dry_run: bool,

    /// Enable schema validation
    #[serde(default = "default_validate_schemas")]
    pub validate_schemas: bool,

    /// Working directory for temporary files
    #[validate(custom(function = "validate_work_dir", message = "Invalid work directory"))]
    #[serde(default = "default_work_dir")]
    pub work_dir: Utf8PathBuf,

    /// Maximum message size in bytes
    #[validate(range(
        min = 1024,
        max = 1073741824,
        message = "Max message size must be between 1KB and 1GB"
    ))]
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// NATS stream name for events
    #[validate(length(min = 1, message = "NATS stream name cannot be empty"))]
    #[serde(default = "default_nats_stream_name")]
    pub nats_stream_name: String,

    /// NATS consumer durable name
    #[validate(length(min = 1, message = "NATS consumer name cannot be empty"))]
    #[serde(default = "default_nats_consumer_name")]
    pub nats_consumer_name: String,
}

// Const default functions for serde
const fn default_pool_size() -> u32 {
    DEFAULT_POOL_SIZE
}
const fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
const fn default_batch_timeout() -> u64 {
    DEFAULT_BATCH_TIMEOUT
}
const fn default_validate_schemas() -> bool {
    DEFAULT_VALIDATE_SCHEMAS
}
const fn default_max_message_size() -> usize {
    DEFAULT_MAX_MESSAGE_SIZE
}

fn default_socket_path() -> String {
    DEFAULT_SOCKET_PATH.to_string()
}
fn default_nats_stream_name() -> String {
    "EVENTS".to_string()
}
fn default_nats_consumer_name() -> String {
    DEFAULT_NATS_CONSUMER_NAME.to_string()
}

fn default_work_dir() -> Utf8PathBuf {
    dirs::cache_dir()
        .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())
        .unwrap_or_else(|| Utf8PathBuf::from("/tmp"))
        .join("sinex")
        .join("ingestd")
}

impl Default for IngestdFigmentConfig {
    fn default() -> Self {
        Self {
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string()),
            database_pool_size: default_pool_size(),
            nats_url: "nats://localhost:4222".to_string(),
            socket_path: default_socket_path(),
            batch_size: default_batch_size(),
            batch_timeout_secs: default_batch_timeout(),
            dry_run: false,
            validate_schemas: default_validate_schemas(),
            work_dir: default_work_dir(),
            max_message_size: default_max_message_size(),
            nats_stream_name: default_nats_stream_name(),
            nats_consumer_name: default_nats_consumer_name(),
        }
    }
}

impl IngestdFigmentConfig {
    /// Build a figment with common configuration layers
    fn build_figment_base() -> Result<Figment, Box<figment::Error>> {
        let default_toml = toml::to_string(&Self::default()).map_err(|e| {
            Box::new(figment::Error::from(figment::error::Kind::Message(
                format!("Failed to serialize default config: {e}",),
            )))
        })?;

        Ok(Figment::new()
            // Start with defaults
            .merge(Toml::string(&default_toml)))
    }

    /// Add common environment variable layers to a figment
    fn add_env_layers(figment: Figment) -> Figment {
        figment
            .merge(Env::prefixed("INGESTD_").split("_"))
            .merge(Env::raw().only(&["DATABASE_URL"]))
    }

    /// Load configuration from multiple sources
    pub fn load() -> Result<Self, Box<figment::Error>> {
        let figment = Self::build_figment_base()?
            // Load from config file if exists
            .merge(Toml::file("ingestd.toml").nested())
            .merge(Toml::file("/etc/sinex/ingestd.toml").nested());

        Self::add_env_layers(figment).extract().map_err(Box::new)
    }

    /// Load configuration with custom config file
    pub fn load_from(config_file: &str) -> Result<Self, Box<figment::Error>> {
        let figment = Self::build_figment_base()?
            // Load from specified config file
            .merge(Toml::file(config_file).nested());

        Self::add_env_layers(figment).extract().map_err(Box::new)
    }

    /// Validate the configuration
    pub fn validate_config(&self) -> Result<(), validator::ValidationErrors> {
        use validator::Validate as ValidateTrait;
        ValidateTrait::validate(self)
    }

    /// Convert from command line arguments (for backward compatibility)
    pub fn from_args(
        database_url: Option<String>,
        nats_url: String,
        socket_path: String,
        pool_size: u32,
        batch_size: usize,
        batch_timeout_secs: u64,
        dry_run: bool,
    ) -> Self {
        let mut config = Self::default();

        if let Some(url) = database_url {
            config.database_url = url;
        }
        config.nats_url = nats_url;
        config.socket_path = socket_path;
        config.database_pool_size = pool_size;
        config.batch_size = batch_size;
        config.batch_timeout_secs = batch_timeout_secs;
        config.dry_run = dry_run;

        config
    }
}

// Validation functions
fn validate_postgres_url(url: &str) -> Result<(), validator::ValidationError> {
    if !url.starts_with("postgres://") && !url.starts_with("postgresql://") {
        return Err(validator::ValidationError::new("not_postgres_url"));
    }
    Ok(())
}

fn validate_socket_path(path: &str) -> Result<(), validator::ValidationError> {
    if path.is_empty() {
        return Err(validator::ValidationError::new("empty_socket_path"));
    }
    // Check parent directory exists
    if let Some(parent) = camino::Utf8Path::new(path).parent() {
        if !parent.exists() && parent != camino::Utf8Path::new("") {
            return Err(validator::ValidationError::new("parent_dir_not_found"));
        }
    }
    Ok(())
}

fn validate_work_dir(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    // Work dir will be created if it doesn't exist, so just check it's not empty
    if path.as_os_str().is_empty() {
        return Err(validator::ValidationError::new("empty_work_dir"));
    }
    Ok(())
}
