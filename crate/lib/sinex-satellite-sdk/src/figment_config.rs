//! Modern configuration for satellites using Figment
//!
//! This module provides configuration management for satellite services
//! using Figment for multi-source configuration loading.

use camino::{Utf8Path, Utf8PathBuf};
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Base satellite configuration using Figment
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct SatelliteFigmentConfig {
    /// Service name (used for logging and identification)
    #[validate(length(min = 1, message = "Service name cannot be empty"))]
    pub service_name: String,

    /// Log level
    #[serde(default = "default_log_level")]
    #[validate(custom(function = "validate_log_level", message = "Invalid log level"))]
    pub log_level: String,

    /// Path to Unix Domain Socket for gRPC communication with ingestd
    #[serde(default = "default_socket_path")]
    #[validate(custom(function = "validate_socket_path", message = "Invalid socket path"))]
    pub socket_path: String,

    /// Redis URL for message bus
    #[validate(url(message = "Invalid Redis URL"))]
    #[serde(default = "default_redis_url")]
    pub redis_url: String,

    /// Enable replay mode
    #[serde(default)]
    pub enable_replay: bool,

    /// Working directory for temporary files
    #[serde(default = "default_work_dir")]
    pub work_dir: Utf8PathBuf,

    /// Health check port (0 = disabled)
    #[serde(default)]
    pub health_port: u16,

    /// Checkpoint interval in seconds
    #[serde(default = "default_checkpoint_interval")]
    #[validate(range(min = 1, message = "Checkpoint interval must be at least 1 second"))]
    pub checkpoint_interval_secs: u64,

    /// Database URL for direct database access (optional)
    #[validate(url(message = "Invalid database URL"))]
    pub database_url: Option<String>,
}

/// Configuration for event source satellites
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct EventSourceFigmentConfig {
    #[serde(flatten)]
    #[validate(nested)]
    pub base: SatelliteFigmentConfig,

    /// Batch size for event submission
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, message = "Batch size must be greater than 0"))]
    pub batch_size: usize,

    /// Maximum batch wait time in seconds
    #[serde(default = "default_batch_wait")]
    #[validate(range(min = 1, message = "Batch wait time must be at least 1 second"))]
    pub batch_wait_secs: u64,

    /// Maximum number of retries for failed submissions
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Retry backoff multiplier
    #[serde(default = "default_retry_backoff")]
    pub retry_backoff_multiplier: f64,
}

/// Configuration for automaton satellites
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct AutomatonFigmentConfig {
    #[serde(flatten)]
    #[validate(nested)]
    pub base: SatelliteFigmentConfig,

    /// Redis Stream consumer group name
    #[validate(length(min = 1, message = "Consumer group cannot be empty"))]
    pub consumer_group: String,

    /// Redis Stream consumer name (usually hostname + process ID)
    #[validate(length(min = 1, message = "Consumer name cannot be empty"))]
    #[serde(default = "default_consumer_name")]
    pub consumer_name: String,

    /// List of event streams to consume
    #[validate(length(min = 1, message = "Must consume at least one stream"))]
    pub streams: Vec<String>,

    /// Batch size for stream reading
    #[serde(default = "default_stream_batch_size")]
    #[validate(range(min = 1, message = "Stream batch size must be greater than 0"))]
    pub stream_batch_size: usize,

    /// Stream read timeout in seconds
    #[serde(default = "default_stream_timeout")]
    pub stream_timeout_secs: u64,

    /// Max processing time per event in seconds
    #[serde(default = "default_max_processing_time")]
    pub max_processing_time_secs: u64,
}

// Default value functions
fn default_log_level() -> String {
    "info".to_string()
}
fn default_socket_path() -> String {
    "/tmp/sinex-ingestd.sock".to_string()
}
fn default_redis_url() -> String {
    "redis://localhost:6379".to_string()
}
fn default_work_dir() -> Utf8PathBuf {
    dirs::cache_dir()
        .map(|p| Utf8PathBuf::from_path_buf(p).unwrap_or_else(|_| Utf8PathBuf::from("/tmp")))
        .unwrap_or_else(|| Utf8PathBuf::from("/tmp"))
        .join("sinex")
}
fn default_checkpoint_interval() -> u64 {
    300
} // 5 minutes
fn default_batch_size() -> usize {
    100
}
fn default_batch_wait() -> u64 {
    5
}
fn default_max_retries() -> u32 {
    3
}
fn default_retry_backoff() -> f64 {
    2.0
}
fn default_consumer_name() -> String {
    format!(
        "{}:{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id()
    )
}
fn default_stream_batch_size() -> usize {
    10
}
fn default_stream_timeout() -> u64 {
    5
}
fn default_max_processing_time() -> u64 {
    60
}

impl SatelliteFigmentConfig {
    /// Load configuration for a specific satellite
    pub fn load(satellite_name: &str) -> Result<Self, figment::Error> {
        let env_prefix = satellite_name.to_uppercase().replace('-', "_");

        Figment::new()
            // Look for satellite-specific config file
            .merge(Toml::file(format!("{}.toml", satellite_name)).nested())
            .merge(Toml::file(format!("/etc/sinex/{}.toml", satellite_name)).nested())
            // Also check common satellite config
            .merge(Toml::file("satellite.toml").nested())
            .merge(Toml::file("/etc/sinex/satellite.toml").nested())
            // Override with environment variables
            .merge(Env::prefixed(&format!("{}_", env_prefix)).split("_"))
            // Common SINEX_ prefix for shared config
            .merge(Env::prefixed("SINEX_").split("_"))
            .extract()
    }
}

impl EventSourceFigmentConfig {
    /// Load configuration for an event source satellite
    pub fn load(satellite_name: &str) -> Result<Self, figment::Error> {
        let env_prefix = satellite_name.to_uppercase().replace('-', "_");

        Figment::new()
            // Look for satellite-specific config file
            .merge(Toml::file(format!("{}.toml", satellite_name)).nested())
            .merge(Toml::file(format!("/etc/sinex/{}.toml", satellite_name)).nested())
            // Also check common event source config
            .merge(Toml::file("event-source.toml").nested())
            .merge(Toml::file("/etc/sinex/event-source.toml").nested())
            // Override with environment variables
            .merge(Env::prefixed(&format!("{}_", env_prefix)).split("_"))
            // Common SINEX_ prefix for shared config
            .merge(Env::prefixed("SINEX_").split("_"))
            .extract()
    }
}

impl AutomatonFigmentConfig {
    /// Load configuration for an automaton satellite
    pub fn load(satellite_name: &str) -> Result<Self, figment::Error> {
        let env_prefix = satellite_name.to_uppercase().replace('-', "_");

        Figment::new()
            // Look for satellite-specific config file
            .merge(Toml::file(format!("{}.toml", satellite_name)).nested())
            .merge(Toml::file(format!("/etc/sinex/{}.toml", satellite_name)).nested())
            // Also check common automaton config
            .merge(Toml::file("automaton.toml").nested())
            .merge(Toml::file("/etc/sinex/automaton.toml").nested())
            // Override with environment variables
            .merge(Env::prefixed(&format!("{}_", env_prefix)).split("_"))
            // Common SINEX_ prefix for shared config
            .merge(Env::prefixed("SINEX_").split("_"))
            .extract()
    }
}

// Validation functions
fn validate_log_level(level: &str) -> Result<(), validator::ValidationError> {
    match level {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err(validator::ValidationError::new("invalid_log_level")),
    }
}

fn validate_socket_path(path: &str) -> Result<(), validator::ValidationError> {
    if path.is_empty() {
        return Err(validator::ValidationError::new("empty_socket_path"));
    }
    // Check parent directory exists
    if let Some(parent) = Utf8Path::new(path).parent() {
        if !parent.exists() && parent != Utf8Path::new("") {
            // Parent will be created, so this is ok
            return Ok(());
        }
    }
    Ok(())
}

/// Example configuration file generator
pub fn generate_example_configs() -> std::io::Result<()> {
    use std::fs;

    // Example event source config
    let event_source = EventSourceFigmentConfig {
        base: SatelliteFigmentConfig {
            service_name: "filesystem-watcher".to_string(),
            log_level: "info".to_string(),
            socket_path: "/tmp/sinex-ingestd.sock".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            enable_replay: false,
            work_dir: Utf8PathBuf::from("/tmp/sinex/filesystem"),
            health_port: 9090,
            checkpoint_interval_secs: 300,
            database_url: None,
        },
        batch_size: 100,
        batch_wait_secs: 5,
        max_retries: 3,
        retry_backoff_multiplier: 2.0,
    };

    let toml = toml::to_string_pretty(&event_source).map_err(std::io::Error::other)?;
    fs::write("event-source-example.toml", toml)?;

    // Example automaton config
    let automaton = AutomatonFigmentConfig {
        base: SatelliteFigmentConfig {
            service_name: "terminal-canonicalizer".to_string(),
            log_level: "info".to_string(),
            socket_path: "/tmp/sinex-ingestd.sock".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            enable_replay: false,
            work_dir: Utf8PathBuf::from("/tmp/sinex/canonicalizer"),
            health_port: 9091,
            checkpoint_interval_secs: 300,
            database_url: Some("postgresql:///sinex_dev?host=/run/postgresql".to_string()),
        },
        consumer_group: "canonicalizer".to_string(),
        consumer_name: default_consumer_name(),
        streams: vec!["sinex:events:terminal".to_string()],
        stream_batch_size: 10,
        stream_timeout_secs: 5,
        max_processing_time_secs: 60,
    };

    let toml = toml::to_string_pretty(&automaton).map_err(std::io::Error::other)?;
    fs::write("automaton-example.toml", toml)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn test_satellite_config_defaults() -> color_eyre::eyre::Result<()> {
        let config = SatelliteFigmentConfig {
            service_name: "test-satellite".to_string(),
            log_level: default_log_level(),
            socket_path: default_socket_path(),
            redis_url: default_redis_url(),
            enable_replay: false,
            work_dir: default_work_dir(),
            health_port: 0,
            checkpoint_interval_secs: default_checkpoint_interval(),
            database_url: None,
        };

        assert!(config.validate().is_ok());
        assert_eq!(config.log_level, "info");
        assert_eq!(config.checkpoint_interval_secs, 300);
        Ok(())
    }

    #[sinex_test]
    fn test_event_source_validation() -> color_eyre::eyre::Result<()> {
        let mut config = EventSourceFigmentConfig {
            base: SatelliteFigmentConfig {
                service_name: "".to_string(), // Invalid
                log_level: "info".to_string(),
                socket_path: "/tmp/test.sock".to_string(),
                redis_url: "redis://localhost".to_string(),
                enable_replay: false,
                work_dir: Utf8PathBuf::from("/tmp"),
                health_port: 0,
                checkpoint_interval_secs: 300,
                database_url: None,
            },
            batch_size: 100,
            batch_wait_secs: 5,
            max_retries: 3,
            retry_backoff_multiplier: 2.0,
        };

        assert!(config.validate().is_err());

        config.base.service_name = "valid-name".to_string();
        assert!(config.validate().is_ok());
        Ok(())
    }

    #[sinex_test]
    fn test_automaton_config_validation() -> color_eyre::eyre::Result<()> {
        let config = AutomatonFigmentConfig {
            base: SatelliteFigmentConfig {
                service_name: "test-automaton".to_string(),
                log_level: "debug".to_string(),
                socket_path: "/tmp/test.sock".to_string(),
                redis_url: "redis://localhost".to_string(),
                enable_replay: false,
                work_dir: Utf8PathBuf::from("/tmp"),
                health_port: 9090,
                checkpoint_interval_secs: 60,
                database_url: None,
            },
            consumer_group: "test-group".to_string(),
            consumer_name: "test-consumer".to_string(),
            streams: vec!["test:stream".to_string()],
            stream_batch_size: 10,
            stream_timeout_secs: 5,
            max_processing_time_secs: 30,
        };

        assert!(config.validate().is_ok());
        Ok(())
    }
}
