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
//! let config = NodeConfig::load_from_env("my-service")?;
//! # Ok::<(), sinex_node_sdk::config::ConfigError>(())
//! ```
//!
//! # Environment Variables
//!
//! - `SINEX_LOG_LEVEL`: Log level (trace, debug, info, warn, error)
//! - `SINEX_NATS_URL`: NATS server URL for event ingestion
//! - `DATABASE_URL`: `PostgreSQL` database connection string
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
use serde::{Deserialize, Serialize};
use sinex_primitives::{environment::environment, units::Seconds, validation::validate_path};
use std::collections::HashMap;
use validator::Validate;

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

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
    /// `PostgreSQL` connection string for automata that need direct database access.
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
        }
    }

    fn env_prefix(service_name: &str) -> String {
        service_name.to_uppercase().replace('-', "_")
    }

    /// Load configuration from environment and defaults.
    ///
    /// Creates a configuration using environment variables with fallback to
    /// default values. This is the preferred method for production deployments.
    ///
    /// # Environment Variables
    /// - `SINEX_LOG_LEVEL`: Log level (default: "info")
    /// - `DATABASE_URL`: `PostgreSQL` URL (optional)
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
    /// let config = NodeConfig::load_from_env("my-service")?;
    ///
    /// // With environment variables set:
    /// // SINEX_LOG_LEVEL=debug
    /// // SINEX_DRY_RUN=true
    /// unsafe { std::env::set_var("SINEX_LOG_LEVEL", "debug"); }
    /// unsafe { std::env::set_var("SINEX_DRY_RUN", "true"); }
    /// let config = NodeConfig::load_from_env("debug-service")?;
    /// assert_eq!(config.log_level, "debug");
    /// assert!(config.dry_run);
    /// # Ok::<(), sinex_node_sdk::config::ConfigError>(())
    /// ```
    pub fn load_from_env(service_name: &str) -> Result<Self, ConfigError> {
        let defaults = Self::defaults(service_name);
        let env_prefix = Self::env_prefix(service_name);
        Ok(Self {
            service_name: defaults.service_name,
            log_level: service_or_global_env_string(&env_prefix, "LOG_LEVEL")?
                .unwrap_or_else(default_log_level),
            #[cfg(feature = "messaging")]
            nats: nats_config_from_env(&env_prefix)?,
            database_url: service_or_global_env_string(&env_prefix, "DATABASE_URL")?,
            database_pool_size: match service_or_global_env_parse(&env_prefix, "DB_POOL_SIZE")? {
                Some(value) => value,
                None => service_or_global_env_parse(&env_prefix, "DATABASE_POOL_SIZE")?
                    .unwrap_or(defaults.database_pool_size),
            },
            work_dir: service_or_global_env_string(&env_prefix, "WORK_DIR")?
                .map_or(defaults.work_dir, |s| sanitize_work_dir(&s)),
            dry_run: service_or_global_env_bool(&env_prefix, "DRY_RUN")?.unwrap_or(defaults.dry_run),
        })
    }

    /// Validate configuration
    pub fn validate_config(&self) -> Result<(), ConfigError> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self)
            .map_err(|e| ConfigError::Validation(format!("Validation failed: {e}")))?;

        // Additional runtime validation - check if parent directory exists
        if let Some(parent) = self.work_dir.parent()
            && !parent.exists()
        {
            return Err(ConfigError::Validation(format!(
                "Work directory parent does not exist: {parent}"
            )));
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

    /// Load configuration for an event source ingestor from environment and defaults.
    pub fn load_from_env(service_name: &str) -> Result<Self, ConfigError> {
        let defaults = Self::defaults(service_name);
        let env_prefix = NodeConfig::env_prefix(service_name);

        Ok(Self {
            base: NodeConfig::load_from_env(service_name)?,
            batch_size: service_or_global_env_parse(&env_prefix, "BATCH_SIZE")?
                .unwrap_or(defaults.batch_size),
            batch_timeout_secs: service_or_global_env_parse::<u64>(
                &env_prefix,
                "BATCH_TIMEOUT_SECS",
            )?
            .map_or(defaults.batch_timeout_secs, Seconds::from_secs),
            source_config: HashMap::new(),
        })
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

    /// Load configuration for an automaton from environment and defaults.
    pub fn load_from_env(service_name: &str) -> Result<Self, ConfigError> {
        let defaults = Self::defaults(service_name);
        let env_prefix = NodeConfig::env_prefix(service_name);

        Ok(Self {
            base: NodeConfig::load_from_env(service_name)?,
            consumer_group: service_or_global_env_string(&env_prefix, "CONSUMER_GROUP")?
                .unwrap_or(defaults.consumer_group),
            consumer_name: service_or_global_env_string(&env_prefix, "CONSUMER_NAME")?
                .unwrap_or(defaults.consumer_name),
            topics: service_or_global_env_list(&env_prefix, "TOPICS")?.unwrap_or(defaults.topics),
            processing_batch_size: service_or_global_env_parse(
                &env_prefix,
                "PROCESSING_BATCH_SIZE",
            )?
            .unwrap_or(defaults.processing_batch_size),
            checkpoint_interval_secs: service_or_global_env_parse::<u64>(
                &env_prefix,
                "CHECKPOINT_INTERVAL_SECS",
            )?
            .map_or(defaults.checkpoint_interval_secs, Seconds::from_secs),
            automaton_config: HashMap::new(),
        })
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
    #[must_use]
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

// Custom validator functions

/// Sanitize a work directory path by making it absolute and removing traversal sequences.
///
/// This function:
/// 1. Converts relative paths to absolute by joining with `current_dir`
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
                if let Some(last) = components.last()
                    && !matches!(last, Component::RootDir | Component::Prefix(_))
                {
                    components.pop();
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

fn validate_seconds_nonzero(value: &Seconds) -> Result<(), validator::ValidationError> {
    if value.as_secs() == 0 {
        return Err(validator::ValidationError::new("min"));
    }
    Ok(())
}

fn env_var_optional(name: &str) -> Result<Option<String>, ConfigError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::Validation(format!(
            "Environment variable {name} is not valid UTF-8"
        ))),
    }
}

fn service_or_global_env_value(
    service_prefix: &str,
    suffix: &str,
) -> Result<Option<(String, String)>, ConfigError> {
    let service_key = format!("SINEX_{service_prefix}_{suffix}");
    if let Some(value) = env_var_optional(&service_key)? {
        return Ok(Some((service_key, value)));
    }

    let global_key = format!("SINEX_{suffix}");
    if let Some(value) = env_var_optional(&global_key)? {
        return Ok(Some((global_key, value)));
    }

    if suffix == "DATABASE_URL"
        && let Some(value) = env_var_optional("DATABASE_URL")?
    {
        return Ok(Some(("DATABASE_URL".to_string(), value)));
    }

    Ok(None)
}

fn service_or_global_env_string(
    service_prefix: &str,
    suffix: &str,
) -> Result<Option<String>, ConfigError> {
    Ok(service_or_global_env_value(service_prefix, suffix)?.map(|(_, value)| value))
}

fn service_or_global_env_parse<T>(service_prefix: &str, suffix: &str) -> Result<Option<T>, ConfigError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let Some((env_name, value)) = service_or_global_env_value(service_prefix, suffix)? else {
        return Ok(None);
    };

    value.parse().map(Some).map_err(|error| {
        ConfigError::Validation(format!(
            "Environment variable {env_name} has invalid value `{value}`: {error}"
        ))
    })
}

fn service_or_global_env_bool(
    service_prefix: &str,
    suffix: &str,
) -> Result<Option<bool>, ConfigError> {
    let Some((env_name, value)) = service_or_global_env_value(service_prefix, suffix)? else {
        return Ok(None);
    };

    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(ConfigError::Validation(format!(
            "Environment variable {env_name} has invalid boolean value `{value}`"
        ))),
    }
}

fn service_or_global_env_list(
    service_prefix: &str,
    suffix: &str,
) -> Result<Option<Vec<String>>, ConfigError> {
    Ok(service_or_global_env_string(service_prefix, suffix)?.map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }))
}

#[cfg(feature = "messaging")]
fn nats_config_from_env(
    service_prefix: &str,
) -> Result<sinex_primitives::nats::NatsConnectionConfig, ConfigError> {
    let mut config = sinex_primitives::nats::NatsConnectionConfig::from_env();

    if let Some(url) = service_or_global_env_string(service_prefix, "NATS_URL")? {
        config.url = url;
    }
    if let Some(name) = service_or_global_env_string(service_prefix, "NATS_NAME")? {
        config.name = Some(name);
    }
    if let Some(require_tls) = service_or_global_env_bool(service_prefix, "NATS_REQUIRE_TLS")? {
        config.require_tls = require_tls;
    }
    if let Some(path) = service_or_global_env_string(service_prefix, "NATS_CA_CERT")? {
        config.ca_cert = Some(path.into());
    }
    if let Some(path) = service_or_global_env_string(service_prefix, "NATS_CLIENT_CERT")? {
        config.client_cert = Some(path.into());
    }
    if let Some(path) = service_or_global_env_string(service_prefix, "NATS_CLIENT_KEY")? {
        config.client_key = Some(path.into());
    }
    if let Some(path) = service_or_global_env_string(service_prefix, "NATS_CREDS_FILE")? {
        config.creds_file = Some(path.into());
    }
    if let Some(path) = service_or_global_env_string(service_prefix, "NATS_NKEY_SEED_FILE")? {
        config.nkey_seed_file = Some(path.into());
    }
    if let Some(token) = service_or_global_env_string(service_prefix, "NATS_TOKEN")? {
        config.token = Some(token);
    }
    if let Some(path) = service_or_global_env_string(service_prefix, "NATS_TOKEN_FILE")? {
        config.token_file = Some(path.into());
    }

    Ok(config)
}
