use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for the Kitty ingestor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Database configuration
    pub database: DatabaseConfig,
    
    /// Logging configuration
    pub logging: LoggingConfig,
    
    /// Kitty-specific configuration
    pub kitty: KittyConfig,
}

/// Database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database URL
    pub url: String,
    
    /// Maximum number of database connections
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    
    /// Connection timeout in seconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_secs: u64,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub level: String,
    
    /// Log format (json, pretty)
    #[serde(default = "default_log_format")]
    pub format: String,
    
    /// Whether to include source file/line information
    #[serde(default = "default_include_location")]
    pub include_location: bool,
}

/// Kitty-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyConfig {
    /// Path to Kitty socket (e.g., /tmp/kitty-*)
    #[serde(default = "default_socket_path")]
    pub socket_path: String,
    
    /// Polling interval for checking commands in seconds
    #[serde(default = "default_polling_interval")]
    pub polling_interval_secs: u64,
    
    /// Command execution timeout in seconds
    #[serde(default = "default_command_timeout")]
    pub command_timeout_secs: u64,
    
    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,
    
    /// Maximum retries for database operations
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    
    /// Retry delay in seconds
    #[serde(default = "default_retry_delay")]
    pub retry_delay_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            logging: LoggingConfig::default(),
            kitty: KittyConfig::default(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string()),
            max_connections: default_max_connections(),
            connection_timeout_secs: default_connection_timeout(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            include_location: default_include_location(),
        }
    }
}

impl Default for KittyConfig {
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
            polling_interval_secs: default_polling_interval(),
            command_timeout_secs: default_command_timeout(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            max_retries: default_max_retries(),
            retry_delay_secs: default_retry_delay(),
        }
    }
}

impl Config {
    /// Load configuration from environment variables and config files
    pub fn load() -> anyhow::Result<Self> {
        let mut cfg = config::Config::builder()
            // Start with default values
            .add_source(config::Config::try_from(&Config::default())?)
            // Add config file if it exists
            .add_source(
                config::File::with_name("kitty-ingestor.toml").required(false)
            )
            .add_source(
                config::File::with_name("~/.config/kitty-ingestor.toml").required(false)
            )
            // Override with environment variables (SINEX_DATABASE_URL, etc.)
            .add_source(
                config::Environment::with_prefix("SINEX")
                    .separator("_")
                    .try_parsing(true)
            );

        // Override with environment variables
        if let Ok(database_url) = std::env::var("DATABASE_URL") {
            cfg = cfg.set_override("database.url", database_url)?;
        }

        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            cfg = cfg.set_override("logging.level", rust_log)?;
        }

        let config: Config = cfg.build()?.try_deserialize()?;
        Ok(config)
    }

    /// Load configuration from a specific file
    pub fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        let mut cfg = config::Config::builder()
            .add_source(config::Config::try_from(&Config::default())?)
            .add_source(config::File::from(path.as_path()));

        // Still allow environment overrides
        if let Ok(database_url) = std::env::var("DATABASE_URL") {
            cfg = cfg.set_override("database.url", database_url)?;
        }

        let config: Config = cfg.build()?.try_deserialize()?;
        Ok(config)
    }
}

// Default value functions
fn default_max_connections() -> u32 { 5 }
fn default_connection_timeout() -> u64 { 30 }
fn default_log_level() -> String { "info".to_string() }
fn default_log_format() -> String { "pretty".to_string() }
fn default_include_location() -> bool { false }
fn default_socket_path() -> String { "/tmp/kitty-*".to_string() }
fn default_polling_interval() -> u64 { 5 }
fn default_command_timeout() -> u64 { 30 }
fn default_heartbeat_interval() -> u64 { 60 }
fn default_max_retries() -> u32 { 3 }
fn default_retry_delay() -> u64 { 5 }