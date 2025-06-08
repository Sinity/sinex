use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for the Filesystem ingestor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Database configuration
    pub database: DatabaseConfig,
    
    /// Logging configuration
    pub logging: LoggingConfig,
    
    /// Filesystem-specific configuration
    pub filesystem: FilesystemConfig,
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

/// Filesystem-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    /// Directories to watch
    #[serde(default = "default_watch_directories")]
    pub watch_directories: Vec<PathBuf>,
    
    /// Patterns to exclude (glob patterns)
    #[serde(default = "default_exclude_patterns")]
    pub exclude_patterns: Vec<String>,
    
    /// Patterns to include (glob patterns, overrides excludes)
    #[serde(default = "default_include_patterns")]
    pub include_patterns: Vec<String>,
    
    /// Debounce time in milliseconds
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    
    /// Batch size for events
    #[serde(default = "default_batch_size")]
    pub batch_size_events: usize,
    
    /// Batch timeout in milliseconds
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_ms: u64,
    
    /// Whether to calculate file hashes
    #[serde(default = "default_hash_files")]
    pub hash_files: bool,
    
    /// Maximum file size to hash (in bytes)
    #[serde(default = "default_max_hash_size")]
    pub max_hash_size_bytes: u64,
    
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
            filesystem: FilesystemConfig::default(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgresql://localhost/sinex".to_string(),
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

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            watch_directories: default_watch_directories(),
            exclude_patterns: default_exclude_patterns(),
            include_patterns: default_include_patterns(),
            debounce_ms: default_debounce_ms(),
            batch_size_events: default_batch_size(),
            batch_timeout_ms: default_batch_timeout(),
            hash_files: default_hash_files(),
            max_hash_size_bytes: default_max_hash_size(),
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
                config::File::with_name("/etc/sinex/filesystem-ingestor").required(false)
            )
            .add_source(
                config::File::with_name("~/.config/sinex/filesystem-ingestor").required(false)
            )
            .add_source(
                config::File::with_name("./config/filesystem-ingestor").required(false)
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
fn default_watch_directories() -> Vec<PathBuf> {
    vec![
        PathBuf::from("~/Documents"),
        PathBuf::from("~/Projects"),
    ]
}
fn default_exclude_patterns() -> Vec<String> {
    vec![
        "*.tmp".to_string(),
        "*.log".to_string(),
        "*.cache".to_string(),
        ".git/**".to_string(),
        "node_modules/**".to_string(),
        "__pycache__/**".to_string(),
    ]
}
fn default_include_patterns() -> Vec<String> { Vec::new() }
fn default_debounce_ms() -> u64 { 500 }
fn default_batch_size() -> usize { 50 }
fn default_batch_timeout() -> u64 { 5000 }
fn default_hash_files() -> bool { true }
fn default_max_hash_size() -> u64 { 10 * 1024 * 1024 } // 10MB
fn default_heartbeat_interval() -> u64 { 60 }
fn default_max_retries() -> u32 { 3 }
fn default_retry_delay() -> u64 { 5 }