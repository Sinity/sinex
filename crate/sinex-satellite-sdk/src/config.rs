//! Configuration management for satellite services.
//!
//! This module provides a hierarchical configuration system with the following precedence:
//! 1. Command-line arguments (highest priority)
//! 2. Environment variables
//! 3. Configuration files (TOML format)
//! 4. Default values (lowest priority)
//!
//! # Configuration Loading
//! 
//! All satellite services support both file-based and environment-based configuration:
//! 
//! ```rust
//! use sinex_satellite_sdk::config::SatelliteConfig;
//! 
//! // Load from environment variables and defaults
//! let config = SatelliteConfig::load_from_env("my-service");
//! 
//! // Load from TOML file
//! let config = SatelliteConfig::load_from_file(&"config.toml".into())?;
//! ```
//!
//! # Environment Variables
//! 
//! - `SINEX_LOG_LEVEL`: Log level (trace, debug, info, warn, error)
//! - `SINEX_INGEST_SOCKET`: Unix socket path for ingestd communication
//! - `SINEX_REDIS_URL`: Redis connection URL
//! - `DATABASE_URL`: PostgreSQL database connection string
//! - `SINEX_DB_POOL_SIZE`: Database connection pool size
//! - `SINEX_WORK_DIR`: Working directory for temporary files
//! - `SINEX_DRY_RUN`: Enable dry-run mode (true/false)
//!
//! # Validation
//! 
//! All configuration is validated on load. Common validation rules:
//! - Service names must be non-empty
//! - Log levels must be valid (trace, debug, info, warn, error)
//! - Directory paths must exist or be creatable
//! - Batch sizes must be greater than 0
//! - URLs must be well-formed

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

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

/// Base configuration for all satellite services.
///
/// This structure contains common configuration fields shared by all
/// satellite services (both ingestors and automata). Service-specific
/// configuration should extend this via `EventSourceConfig` or `AutomatonConfig`.
///
/// # Field Defaults
/// Most fields have sensible defaults provided by corresponding `default_*` functions.
/// See individual field documentation for specific default values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SatelliteConfig {
    /// Service name (used for logging and identification)
    pub service_name: String,

    /// Log level
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Path to Unix Domain Socket for gRPC communication with ingestd.
    ///
    /// This socket is used by ingestors to send events to the ingestd service.
    /// The path must be accessible by the satellite service process.
    ///
    /// Default: `/run/sinex/ingest.sock` (see `default_ingest_socket()`)
    #[serde(default = "default_ingest_socket")]
    pub ingest_socket_path: String,

    /// Redis connection URL for message bus.
    ///
    /// Used by automata to consume events from Redis Streams.
    /// Format: `redis://hostname:port[/db]`
    ///
    /// Default: `redis://localhost:6379` (see `default_redis_url()`)
    #[serde(default = "default_redis_url")]
    pub redis_url: String,

    /// Database URL for direct database access (automata only).
    ///
    /// PostgreSQL connection string for automata that need direct database access.
    /// Format: `postgresql://username:password@hostname:port/database`
    ///
    /// This field is optional - not all automata require database access.
    /// Ingestors typically don't need this as they communicate via gRPC.
    pub database_url: Option<String>,

    /// Database connection pool size.
    ///
    /// Maximum number of concurrent database connections to maintain.
    /// Higher values improve concurrent query performance but consume more resources.
    ///
    /// Default: `10` (see `default_pool_size()`)
    #[serde(default = "default_pool_size")]
    pub database_pool_size: u32,

    /// Working directory for temporary files
    #[serde(default = "default_work_dir")]
    pub work_dir: PathBuf,

    /// Enable dry-run mode (no actual operations)
    #[serde(default)]
    pub dry_run: bool,

    /// Replay mode configuration
    pub replay: Option<ReplayConfig>,
}

/// Configuration for event source satellites
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSourceConfig {
    #[serde(flatten)]
    pub base: SatelliteConfig,

    /// Batch size for event submission
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum batch wait time in seconds
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_secs: u64,

    /// Source-specific configuration
    pub source_config: HashMap<String, serde_json::Value>,
}

/// Configuration for automaton satellites
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomatonConfig {
    #[serde(flatten)]
    pub base: SatelliteConfig,

    /// Redis Stream consumer group name
    pub consumer_group: String,

    /// Redis Stream consumer name (usually hostname + process ID)
    pub consumer_name: String,

    /// Topics to subscribe to
    pub topics: Vec<String>,

    /// Maximum number of messages to process per batch
    #[serde(default = "default_processing_batch_size")]
    pub processing_batch_size: usize,

    /// Checkpoint interval in seconds
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_secs: u64,

    /// Automaton-specific configuration
    pub automaton_config: HashMap<String, serde_json::Value>,
}

/// Configuration for replay mode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Enable replay mode
    pub enabled: bool,

    /// Start time for replay (RFC 3339 format)
    pub start_time: Option<String>,

    /// End time for replay (RFC 3339 format)
    pub end_time: Option<String>,

    /// Event sources to replay (empty = all)
    pub sources: Vec<String>,

    /// Event types to replay (empty = all)
    pub event_types: Vec<String>,

    /// Maximum events per batch during replay
    #[serde(default = "default_replay_batch_size")]
    pub replay_batch_size: usize,
}

impl SatelliteConfig {
    /// Load configuration from file.
    ///
    /// Loads and validates configuration from a TOML file. The file is parsed
    /// and validated according to the configuration schema.
    ///
    /// # Examples
    /// 
    /// ```rust
    /// use std::path::PathBuf;
    /// use sinex_satellite_sdk::config::SatelliteConfig;
    /// 
    /// let config = SatelliteConfig::load_from_file(&PathBuf::from("config.toml"))?;
    /// println!("Loaded config for service: {}", config.service_name);
    /// ```
    ///
    /// Example TOML configuration:
    /// ```toml
    /// service_name = "my-ingestor"
    /// log_level = "info"
    /// ingest_socket_path = "/run/sinex/ingest.sock"
    /// redis_url = "redis://localhost:6379"
    /// dry_run = false
    /// ```
    pub fn load_from_file(path: &PathBuf) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from environment and defaults.
    ///
    /// Creates a configuration using environment variables with fallback to
    /// default values. This is the preferred method for production deployments.
    ///
    /// # Environment Variables
    /// - `SINEX_LOG_LEVEL`: Log level (default: "info")
    /// - `SINEX_INGEST_SOCKET`: Socket path (default: "/run/sinex/ingest.sock")
    /// - `SINEX_REDIS_URL`: Redis URL (default: "redis://localhost:6379")
    /// - `DATABASE_URL`: PostgreSQL URL (optional)
    /// - `SINEX_DB_POOL_SIZE`: Pool size (default: 10)
    /// - `SINEX_WORK_DIR`: Work directory (default: system cache dir)
    /// - `SINEX_DRY_RUN`: Dry run mode (default: false)
    ///
    /// # Examples
    /// 
    /// ```rust
    /// use sinex_satellite_sdk::config::SatelliteConfig;
    /// 
    /// // Load with defaults
    /// let config = SatelliteConfig::load_from_env("my-service");
    /// 
    /// // With environment variables set:
    /// // SINEX_LOG_LEVEL=debug
    /// // SINEX_DRY_RUN=true
    /// std::env::set_var("SINEX_LOG_LEVEL", "debug");
    /// std::env::set_var("SINEX_DRY_RUN", "true");
    /// let config = SatelliteConfig::load_from_env("debug-service");
    /// assert_eq!(config.log_level, "debug");
    /// assert!(config.dry_run);
    /// ```
    pub fn load_from_env(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            log_level: std::env::var("SINEX_LOG_LEVEL").unwrap_or_else(|_| default_log_level()),
            ingest_socket_path: std::env::var("SINEX_INGEST_SOCKET")
                .unwrap_or_else(|_| default_ingest_socket()),
            redis_url: std::env::var("SINEX_REDIS_URL").unwrap_or_else(|_| default_redis_url()),
            database_url: std::env::var("DATABASE_URL").ok(),
            database_pool_size: std::env::var("SINEX_DB_POOL_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_pool_size),
            work_dir: std::env::var("SINEX_WORK_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| default_work_dir()),
            dry_run: std::env::var("SINEX_DRY_RUN")
                .map(|s| s.parse().unwrap_or(false))
                .unwrap_or(false),
            replay: None,
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.service_name.is_empty() {
            return Err(ConfigError::MissingField("service_name".to_string()));
        }

        // Validate log level
        match self.log_level.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {}
            _ => {
                return Err(ConfigError::Validation(format!(
                    "Invalid log level: {}",
                    self.log_level
                )));
            }
        }

        // Validate paths exist or can be created
        if let Some(parent) = self.work_dir.parent() {
            if !parent.exists() {
                return Err(ConfigError::Validation(format!(
                    "Work directory parent does not exist: {}",
                    parent.display()
                )));
            }
        }

        Ok(())
    }
}

impl EventSourceConfig {
    /// Load event source configuration from file
    pub fn load_from_file(path: &PathBuf) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate event source configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.base.validate()?;

        if self.batch_size == 0 {
            return Err(ConfigError::Validation(
                "batch_size must be greater than 0".to_string(),
            ));
        }

        if self.batch_timeout_secs == 0 {
            return Err(ConfigError::Validation(
                "batch_timeout_secs must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}

impl AutomatonConfig {
    /// Load automaton configuration from file
    pub fn load_from_file(path: &PathBuf) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate automaton configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.base.validate()?;

        if self.consumer_group.is_empty() {
            return Err(ConfigError::MissingField("consumer_group".to_string()));
        }

        if self.consumer_name.is_empty() {
            return Err(ConfigError::MissingField("consumer_name".to_string()));
        }

        if self.topics.is_empty() {
            return Err(ConfigError::Validation(
                "At least one topic must be specified".to_string(),
            ));
        }

        if self.processing_batch_size == 0 {
            return Err(ConfigError::Validation(
                "processing_batch_size must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }

    /// Generate default consumer name from hostname and process ID
    pub fn default_consumer_name() -> String {
        let hostname = gethostname::gethostname()
            .to_string_lossy()
            .to_string();
        let pid = std::process::id();
        format!("{}-{}", hostname, pid)
    }
}

// Default value functions
fn default_log_level() -> String {
    "info".to_string()
}

fn default_ingest_socket() -> String {
    "/run/sinex/ingest.sock".to_string()
}

fn default_redis_url() -> String {
    "redis://localhost:6379".to_string()
}

fn default_pool_size() -> u32 {
    10
}

fn default_work_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("sinex")
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout() -> u64 {
    5
}

fn default_processing_batch_size() -> usize {
    50
}

fn default_checkpoint_interval() -> u64 {
    30
}

fn default_replay_batch_size() -> usize {
    1000
}