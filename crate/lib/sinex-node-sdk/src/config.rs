//! Configuration management for node services.
//!
//! This module provides environment-based configuration with the following precedence:
//! 1. Command-line arguments (highest priority)
//! 2. Environment variables
//! 3. Default values (lowest priority)
//!
//! # Configuration Loading
//!
//! All node services use environment-based configuration only:
//!
//! ```rust
//! use sinex_node_sdk::NodeConfig;
//!
//! // Load from environment variables and defaults
//! let config = NodeConfig::load_from_env("my-service");
//! ```
//!
//! # Environment Variables
//!
//! - `SINEX_LOG_LEVEL`: Log level (trace, debug, info, warn, error)
//! - `SINEX_NATS_URL`: NATS server URL for event ingestion
//! - `DATABASE_URL`: PostgreSQL database connection string
//! - `SINEX_DB_POOL_SIZE`: Database connection pool size
//! - `SINEX_WORK_DIR`: Working directory for temporary files
//! - `SINEX_DRY_RUN`: Enable dry-run mode (true/false)
//! - `SINEX_<SERVICE>_LOG_LEVEL`: Per-node override (service name uppercased with `_`)
//!
//! # Validation
//!
//! All configuration is validated on load. Common validation rules:
//! - Service names must be non-empty
//! - Log levels must be valid (trace, debug, info, warn, error)
//! - Directory paths must exist or be creatable
//! - Batch sizes must be greater than 0
//! - URLs must be well-formed

use camino::Utf8PathBuf;
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use sinex_primitives::{environment::environment, units::Seconds, validation::validate_path};
use std::collections::HashMap;
use uncased::{Uncased, UncasedStr};
use validator::Validate;

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] toml::de::Error),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Missing required field: {0}")]
    MissingField(String),
}

/// Base configuration for all node services.
///
/// This structure contains common configuration fields shared by all
/// node services (both ingestors and automata). Service-specific
/// configuration should extend this via `EventSourceConfig` or `AutomatonConfig`.
///
/// # Field Defaults
/// Most fields have sensible defaults provided by corresponding `default_*` functions.
/// See individual field documentation for specific default values.
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct NodeConfig {
    /// Service name (used for logging and identification)
    #[validate(length(min = 1, message = "Service name cannot be empty"))]
    pub service_name: String,

    /// Log level
    #[serde(default = "default_log_level")]
    #[validate(custom(function = "validate_log_level", message = "Invalid log level"))]
    #[builder(default = default_log_level())]
    pub log_level: String,

    /// NATS connection configuration.
    #[builder(default)]
    #[cfg(feature = "messaging")]
    pub nats: sinex_primitives::nats::NatsConnectionConfig,

    /// Database URL for direct database access (automata only).
    ///
    /// PostgreSQL connection string for automata that need direct database access.
    /// Format: `postgresql://username:password@hostname:port/database`
    ///
    /// This field is optional - not all automata require database access.
    /// Ingestors typically don't need this as they communicate via gRPC.
    #[validate(url(message = "Invalid database URL"))]
    pub database_url: Option<String>,

    /// Database connection pool size.
    ///
    /// Maximum number of concurrent database connections to maintain.
    /// Higher values improve concurrent query performance but consume more resources.
    ///
    /// Default: `10` (see `default_pool_size()`)
    #[serde(default = "default_pool_size")]
    #[validate(range(min = 1, max = 1000, message = "Pool size must be between 1 and 1000"))]
    #[builder(default = default_pool_size())]
    pub database_pool_size: u32,

    /// Working directory for temporary files
    #[serde(default = "default_work_dir")]
    #[validate(custom(function = "validate_work_dir", message = "Invalid work directory"))]
    #[builder(default = default_work_dir())]
    pub work_dir: Utf8PathBuf,

    /// Enable dry-run mode (no actual operations)
    #[serde(default)]
    #[builder(default = false)]
    pub dry_run: bool,

    /// Replay mode configuration
    #[validate(nested)]
    pub replay: Option<ReplayConfig>,
}

/// Configuration for event source nodes
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct EventSourceConfig {
    #[serde(flatten)]
    #[validate(nested)]
    pub base: NodeConfig,

    /// Batch size for event submission
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, message = "Batch size must be greater than 0"))]
    #[builder(default = default_batch_size())]
    pub batch_size: usize,

    /// Maximum batch wait time in seconds
    #[serde(default = "default_batch_timeout")]
    #[validate(custom(function = "validate_seconds_nonzero"))]
    #[builder(default = default_batch_timeout())]
    pub batch_timeout_secs: Seconds,

    /// Source-specific configuration
    #[builder(default = HashMap::new())]
    pub source_config: HashMap<String, serde_json::Value>,
}

/// Configuration for automaton nodes
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct AutomatonConfig {
    #[serde(flatten)]
    #[validate(nested)]
    pub base: NodeConfig,

    /// Stream consumer group name
    #[validate(length(min = 1, message = "Consumer group cannot be empty"))]
    pub consumer_group: String,

    /// Stream consumer name (usually hostname + process ID)
    #[validate(length(min = 1, message = "Consumer name cannot be empty"))]
    pub consumer_name: String,

    /// Topics to subscribe to
    #[validate(length(min = 1, message = "At least one topic must be specified"))]
    pub topics: Vec<String>,

    /// Maximum number of messages to process per batch
    #[serde(default = "default_processing_batch_size")]
    #[validate(range(min = 1, message = "Processing batch size must be greater than 0"))]
    pub processing_batch_size: usize,

    /// Checkpoint interval in seconds
    #[serde(default = "default_checkpoint_interval")]
    #[validate(custom(function = "validate_seconds_nonzero"))]
    pub checkpoint_interval_secs: Seconds,

    /// Automaton-specific configuration
    pub automaton_config: HashMap<String, serde_json::Value>,
}

/// Configuration for replay mode
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ReplayConfig {
    /// Enable replay mode
    pub enabled: bool,

    /// Start time for replay (RFC 3339 format)
    #[validate(custom(function = "validate_rfc3339", message = "Invalid RFC 3339 timestamp"))]
    pub start_time: Option<String>,

    /// End time for replay (RFC 3339 format)
    #[validate(custom(function = "validate_rfc3339", message = "Invalid RFC 3339 timestamp"))]
    pub end_time: Option<String>,

    /// Event sources to replay (empty = all)
    pub sources: Vec<String>,

    /// Event types to replay (empty = all)
    pub event_types: Vec<String>,

    /// Maximum events per batch during replay
    #[serde(default = "default_replay_batch_size")]
    #[validate(range(min = 1, message = "Replay batch size must be greater than 0"))]
    pub replay_batch_size: usize,
}

impl NodeConfig {
    fn defaults(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            log_level: default_log_level(),
            #[cfg(feature = "messaging")]
            nats: sinex_primitives::nats::NatsConnectionConfig::default(),
            database_url: None,
            database_pool_size: default_pool_size(),
            work_dir: default_work_dir(),
            dry_run: false,
            replay: None,
        }
    }

    fn figment_base(service_name: &str) -> Figment {
        Figment::from(Serialized::defaults(Self::defaults(service_name)))
            .merge(Toml::file("node.toml").nested())
            .merge(Toml::file("/etc/sinex/node.toml").nested())
            .merge(Toml::file(format!("{service_name}.toml")).nested())
            .merge(Toml::file(format!("/etc/sinex/{service_name}.toml")).nested())
    }

    fn env_prefix(service_name: &str) -> String {
        service_name.to_uppercase().replace('-', "_")
    }

    fn apply_env(figment: Figment, service_name: &str) -> Figment {
        let env_prefix = Self::env_prefix(service_name);
        figment
            .merge(Env::raw().only(&["DATABASE_URL"]))
            .merge(Env::prefixed("SINEX_").map(map_env_key))
            .merge(Env::prefixed(&format!("SINEX_{env_prefix}_")).map(map_env_key))
    }

    /// Load configuration using Figment from defaults, config files, and environment.
    pub fn load(service_name: &str) -> Result<Self, figment::Error> {
        Self::apply_env(Self::figment_base(service_name), service_name).extract()
    }

    /// Load configuration from a specific TOML file merged with defaults and environment.
    pub fn load_from_path(
        service_name: &str,
        path: impl AsRef<str>,
    ) -> Result<Self, figment::Error> {
        let figment = Self::figment_base(service_name).merge(Toml::file(path.as_ref()).nested());
        Self::apply_env(figment, service_name).extract()
    }

    /// Load configuration from an existing Figment instance.
    pub fn from_figment(service_name: &str, figment: Figment) -> Result<Self, figment::Error> {
        Self::apply_env(figment, service_name).extract()
    }

    /// Load configuration from environment and defaults.
    ///
    /// Creates a configuration using environment variables with fallback to
    /// default values. This is the preferred method for production deployments.
    ///
    /// # Environment Variables
    /// - `SINEX_LOG_LEVEL`: Log level (default: "info")
    /// - `DATABASE_URL`: PostgreSQL URL (optional)
    /// - `SINEX_DB_POOL_SIZE`: Pool size (default: 10)
    /// - `SINEX_WORK_DIR`: Work directory (default: system cache dir)
    /// - `SINEX_DRY_RUN`: Dry run mode (default: false)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sinex_node_sdk::NodeConfig;
    ///
    /// // Load with defaults
    /// let config = NodeConfig::load_from_env("my-service");
    ///
    /// // With environment variables set:
    /// // SINEX_LOG_LEVEL=debug
    /// // SINEX_DRY_RUN=true
    /// std::env::set_var("SINEX_LOG_LEVEL", "debug");
    /// std::env::set_var("SINEX_DRY_RUN", "true");
    /// let config = NodeConfig::load_from_env("debug-service");
    /// assert_eq!(config.log_level, "debug");
    /// assert!(config.dry_run);
    /// ```
    pub fn load_from_env(service_name: &str) -> Self {
        let defaults = Self::defaults(service_name);
        Self {
            service_name: defaults.service_name,
            log_level: env_var_or_default("SINEX_LOG_LEVEL", default_log_level),
            #[cfg(feature = "messaging")]
            nats: sinex_primitives::nats::NatsConnectionConfig::from_env(),
            database_url: std::env::var("DATABASE_URL")
                .ok()
                .map(|url| environment().database_url(&url).unwrap_or(url)),
            database_pool_size: std::env::var("SINEX_DB_POOL_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(defaults.database_pool_size),
            work_dir: std::env::var("SINEX_WORK_DIR")
                .map_or(defaults.work_dir, |s| sanitize_work_dir(&s)),
            dry_run: std::env::var("SINEX_DRY_RUN")
                .map_or(defaults.dry_run, |s| s.parse().unwrap_or(false)),
            replay: None,
        }
    }

    /// Validate configuration
    pub fn validate_config(&self) -> Result<(), ConfigError> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self)
            .map_err(|e| ConfigError::Validation(format!("Validation failed: {e}")))?;

        // Additional runtime validation - check if parent directory exists
        if let Some(parent) = self.work_dir.parent() {
            if !parent.exists() {
                return Err(ConfigError::Validation(format!(
                    "Work directory parent does not exist: {parent}"
                )));
            }
        }

        Ok(())
    }
}

impl EventSourceConfig {
    fn defaults(service_name: &str) -> Self {
        Self {
            base: NodeConfig::defaults(service_name),
            batch_size: default_batch_size(),
            batch_timeout_secs: default_batch_timeout(),
            source_config: HashMap::new(),
        }
    }

    fn figment_base(service_name: &str) -> Figment {
        Figment::from(Serialized::defaults(Self::defaults(service_name)))
            .merge(Toml::file(format!("{service_name}.toml")).nested())
            .merge(Toml::file(format!("/etc/sinex/{service_name}.toml")).nested())
            .merge(Toml::file("event-source.toml").nested())
            .merge(Toml::file("/etc/sinex/event-source.toml").nested())
    }

    /// Load configuration for an event source ingestor using Figment.
    pub fn load(service_name: &str) -> Result<Self, figment::Error> {
        NodeConfig::apply_env(Self::figment_base(service_name), service_name).extract()
    }

    /// Load configuration for an event source ingestor from a specific file.
    pub fn load_from_path(
        service_name: &str,
        path: impl AsRef<str>,
    ) -> Result<Self, figment::Error> {
        let figment = Self::figment_base(service_name).merge(Toml::file(path.as_ref()).nested());
        NodeConfig::apply_env(figment, service_name).extract()
    }

    /// Validate event source configuration
    pub fn validate_config(&self) -> Result<(), ConfigError> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self)
            .map_err(|e| ConfigError::Validation(format!("Validation failed: {e}")))?;

        // Base validation includes runtime checks
        self.base.validate_config()?;

        Ok(())
    }
}

impl AutomatonConfig {
    fn defaults(service_name: &str) -> Self {
        Self {
            base: NodeConfig::defaults(service_name),
            consumer_group: format!("{service_name}-group"),
            consumer_name: Self::default_consumer_name(),
            topics: Vec::new(),
            processing_batch_size: default_processing_batch_size(),
            checkpoint_interval_secs: default_checkpoint_interval(),
            automaton_config: HashMap::new(),
        }
    }

    fn figment_base(service_name: &str) -> Figment {
        Figment::from(Serialized::defaults(Self::defaults(service_name)))
            .merge(Toml::file(format!("{service_name}.toml")).nested())
            .merge(Toml::file(format!("/etc/sinex/{service_name}.toml")).nested())
            .merge(Toml::file("automaton.toml").nested())
            .merge(Toml::file("/etc/sinex/automaton.toml").nested())
    }

    /// Load configuration for an automaton using Figment.
    pub fn load(service_name: &str) -> Result<Self, figment::Error> {
        NodeConfig::apply_env(Self::figment_base(service_name), service_name).extract()
    }

    /// Load configuration for an automaton from a specific file.
    pub fn load_from_path(
        service_name: &str,
        path: impl AsRef<str>,
    ) -> Result<Self, figment::Error> {
        let figment = Self::figment_base(service_name).merge(Toml::file(path.as_ref()).nested());
        NodeConfig::apply_env(figment, service_name).extract()
    }

    /// Validate automaton configuration
    pub fn validate_config(&self) -> Result<(), ConfigError> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self)
            .map_err(|e| ConfigError::Validation(format!("Validation failed: {e}")))?;

        // Base validation includes runtime checks
        self.base.validate_config()?;

        Ok(())
    }

    /// Generate default consumer name from hostname, process ID, and random suffix.
    ///
    /// The random suffix ensures uniqueness even if a process restarts with the same PID
    /// within the same second (which would otherwise cause NATS consumer name collisions).
    pub fn default_consumer_name() -> String {
        use uuid::Uuid;
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let pid = std::process::id();
        // Use last 8 chars of UUID for brevity while maintaining uniqueness
        let uuid_suffix = Uuid::new_v4().to_string();
        let suffix = &uuid_suffix[uuid_suffix.len().saturating_sub(8)..];
        format!("{hostname}-{pid}-{suffix}")
    }
}

// Default value functions
fn default_log_level() -> String {
    "info".to_string()
}

fn default_pool_size() -> u32 {
    10
}

fn default_work_dir() -> Utf8PathBuf {
    let env = environment();
    let work_dir = env.work_directory(get_cache_dir_or_fallback().join("sinex"));

    // Validate the default path
    match validate_path(work_dir.to_string_lossy().as_ref()) {
        Ok(validated) => validated,
        Err(_) => {
            // Fallback to a safe default if validation fails
            Utf8PathBuf::from_path_buf(env.work_directory("/tmp/sinex"))
                .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex"))
        }
    }
}

fn get_cache_dir_or_fallback() -> Utf8PathBuf {
    dirs::cache_dir().map_or_else(
        || Utf8PathBuf::from("/tmp"),
        |p| Utf8PathBuf::from_path_buf(p).unwrap_or_else(|_| Utf8PathBuf::from("/tmp")),
    )
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout() -> Seconds {
    Seconds::from_secs(5)
}

fn default_processing_batch_size() -> usize {
    50
}

fn default_checkpoint_interval() -> Seconds {
    Seconds::from_secs(30)
}

fn default_replay_batch_size() -> usize {
    1000
}

// Custom validator functions

/// Sanitize a work directory path by making it absolute and removing traversal sequences.
///
/// This function:
/// 1. Converts relative paths to absolute by joining with current_dir
/// 2. Normalizes the path by removing `.` and `..` components
/// 3. Ensures the result doesn't contain path traversal sequences
fn sanitize_work_dir(path_str: &str) -> Utf8PathBuf {
    use std::path::{Component, PathBuf};

    let path = PathBuf::from(path_str);

    // Make absolute if relative
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    };

    // Clean the path by processing components
    let mut components = Vec::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {} // Skip .
            Component::ParentDir => {
                // Pop if possible, but never go above root
                if let Some(last) = components.last() {
                    if !matches!(last, Component::RootDir | Component::Prefix(_)) {
                        components.pop();
                    }
                }
                // If we can't pop, just skip the ..
            }
            _ => components.push(component),
        }
    }

    let cleaned: PathBuf = components.iter().collect();
    Utf8PathBuf::try_from(cleaned).unwrap_or_else(|e| {
        // Fallback: lossy conversion if path contains non-UTF8
        Utf8PathBuf::from(e.into_path_buf().to_string_lossy().to_string())
    })
}

fn validate_log_level(level: &str) -> Result<(), validator::ValidationError> {
    if matches!(level, "trace" | "debug" | "info" | "warn" | "error") {
        Ok(())
    } else {
        Err(validator::ValidationError::new("invalid_log_level"))
    }
}

fn validate_work_dir(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    let path_str = path.as_str();

    validate_path(path_str)
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_work_dir"))
}

fn validate_rfc3339(timestamp: &str) -> Result<(), validator::ValidationError> {
    sinex_primitives::temporal::Timestamp::parse_rfc3339(timestamp)
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_rfc3339"))
}

fn validate_seconds_nonzero(value: &Seconds) -> Result<(), validator::ValidationError> {
    if value.as_secs() == 0 {
        return Err(validator::ValidationError::new("min"));
    }
    Ok(())
}

fn map_env_key(key: &UncasedStr) -> Uncased<'_> {
    let raw = key.as_str();
    let upper = raw.to_ascii_uppercase();
    let mapped = if let Some(rest) = upper.strip_prefix("NATS_") {
        map_nats_key(rest)
    } else if let Some(rest) = upper.strip_prefix("REPLAY_") {
        map_replay_key(rest)
    } else {
        match upper.as_str() {
            "LOG_LEVEL" => "log_level".to_string(),
            "DB_POOL_SIZE" | "DATABASE_POOL_SIZE" => "database_pool_size".to_string(),
            "DATABASE_URL" => "database_url".to_string(),
            "WORK_DIR" => "work_dir".to_string(),
            "DRY_RUN" => "dry_run".to_string(),
            other => other.to_ascii_lowercase(),
        }
    };

    Uncased::from_owned(mapped)
}

fn map_nats_key(suffix: &str) -> String {
    let field = match suffix {
        "URL" => "url".to_string(),
        "NAME" => "name".to_string(),
        "REQUIRE_TLS" => "require_tls".to_string(),
        "CA_CERT" => "ca_cert".to_string(),
        "CLIENT_CERT" => "client_cert".to_string(),
        "CLIENT_KEY" => "client_key".to_string(),
        "CREDS" | "CREDS_FILE" => "creds_file".to_string(),
        "NKEY" | "NKEY_SEED" => "nkey_file".to_string(),
        "TOKEN" => "token".to_string(),
        other => other.to_ascii_lowercase(),
    };

    format!("nats.{field}")
}

fn map_replay_key(suffix: &str) -> String {
    let field = match suffix {
        "BATCH_SIZE" => "replay_batch_size".to_string(),
        other => other.to_ascii_lowercase(),
    };

    format!("replay.{field}")
}

/// Helper function for environment variable parsing with default values
fn env_var_or_default<F>(key: &str, default_fn: F) -> String
where
    F: FnOnce() -> String,
{
    std::env::var(key).unwrap_or_else(|_| default_fn())
}
