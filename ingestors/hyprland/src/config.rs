use serde::{Deserialize, Serialize};
use crate::error::Result;

/// Configuration for the Hyprland ingestor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Database configuration
    pub database: DatabaseConfig,
    
    /// Logging configuration
    pub logging: LoggingConfig,
    
    /// Hyprland-specific configuration
    pub hyprland: HyprlandConfig,
    
    /// Application metadata
    pub app: AppConfig,
}

/// Database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database URL (e.g., postgresql://localhost/sinex)
    pub url: String,
    
    /// Maximum number of database connections in the pool
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    
    /// Connection timeout in seconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_secs: u64,
    
    /// Query timeout in seconds
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: u64,
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

/// Window augmentation level
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, PartialOrd)]
#[serde(rename_all = "lowercase")]
pub enum WindowAugmentation {
    /// No augmentation - just socket events as-is
    None,
    /// Basic - augment active window changes with window details
    Basic,
    /// Detailed - also capture context on window open/close
    Detailed,
    /// Full - augment all window events, capture neighbor windows
    Full,
}

/// Workspace tracking level
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceTracking {
    /// Just track workspace change events
    Events,
    /// Include window list when workspace changes
    WithWindows,
    /// Full workspace state including window geometries
    WithState,
}

/// Hyprland-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyprlandConfig {
    /// State snapshot interval in seconds
    #[serde(default = "default_state_snapshot_interval")]
    pub state_snapshot_interval_secs: u64,
    
    /// Descriptions capture interval in hours
    #[serde(default = "default_descriptions_interval")]
    pub descriptions_interval_hours: u64,
    
    /// Capture rolling log on config reload
    #[serde(default = "default_true")]
    pub rolling_log_on_reload: bool,
    
    /// Window augmentation level
    #[serde(default = "default_window_augmentation")]
    pub window_augmentation: WindowAugmentation,
    
    /// Workspace tracking level
    #[serde(default = "default_workspace_tracking")]
    pub workspace_tracking: WorkspaceTracking,
    
    /// Events to ignore (not capture)
    #[serde(default)]
    pub ignore_events: Vec<String>,
    
    /// Cache hyprctl results for this many milliseconds
    #[serde(default = "default_hyprctl_cache_ms")]
    pub hyprctl_cache_ms: u64,
    
    /// Augment events in parallel vs sequential
    #[serde(default = "default_true")]
    pub parallel_augmentation: bool,
    
    /// Track focus history
    #[serde(default = "default_true")]
    pub track_focus_history: bool,
    
    /// How many previous focus states to track
    #[serde(default = "default_focus_history_depth")]
    pub focus_history_depth: usize,
    
    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,
    
    /// Maximum retry attempts for failures
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    
    /// Retry delay in seconds
    #[serde(default = "default_retry_delay")]
    pub retry_delay_secs: u64,
}

/// Application metadata configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Application name
    #[serde(default = "default_app_name")]
    pub name: String,
    
    /// Application version
    #[serde(default = "default_app_version")]
    pub version: String,
    
    /// Graceful shutdown timeout in seconds
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            logging: LoggingConfig::default(),
            hyprland: HyprlandConfig::default(),
            app: AppConfig::default(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgresql://localhost/sinex".to_string(),
            max_connections: default_max_connections(),
            connection_timeout_secs: default_connection_timeout(),
            query_timeout_secs: default_query_timeout(),
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

impl Default for HyprlandConfig {
    fn default() -> Self {
        Self {
            state_snapshot_interval_secs: default_state_snapshot_interval(),
            descriptions_interval_hours: default_descriptions_interval(),
            rolling_log_on_reload: default_true(),
            window_augmentation: default_window_augmentation(),
            workspace_tracking: default_workspace_tracking(),
            ignore_events: Vec::new(),
            hyprctl_cache_ms: default_hyprctl_cache_ms(),
            parallel_augmentation: default_true(),
            track_focus_history: default_true(),
            focus_history_depth: default_focus_history_depth(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            max_retries: default_max_retries(),
            retry_delay_secs: default_retry_delay(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            name: default_app_name(),
            version: default_app_version(),
            shutdown_timeout_secs: default_shutdown_timeout(),
        }
    }
}

impl Config {
    /// Load configuration from environment variables and config files
    pub fn load() -> Result<Self> {
        let mut cfg = config::Config::builder()
            // Start with default values
            .add_source(config::Config::try_from(&Config::default())?)
            // Add config file if it exists
            .add_source(
                config::File::with_name("/etc/sinex/hyprland-ingestor").required(false)
            )
            .add_source(
                config::File::with_name("~/.config/sinex/hyprland-ingestor").required(false)
            )
            .add_source(
                config::File::with_name("./config/hyprland-ingestor").required(false)
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
    pub fn load_from_file(path: &std::path::Path) -> Result<Self> {
        let cfg = config::Config::builder()
            // Start with default values
            .add_source(config::Config::try_from(&Config::default())?)
            // Add the specified config file
            .add_source(config::File::from(path))
            // Override with environment variables
            .add_source(
                config::Environment::with_prefix("SINEX")
                    .separator("_")
                    .try_parsing(true)
            );

        let config: Config = cfg.build()?.try_deserialize()?;
        Ok(config)
    }

    /// Get the database URL with connection parameters
    pub fn database_url_with_params(&self) -> String {
        format!(
            "{}?connect_timeout={}",
            self.database.url,
            self.database.connection_timeout_secs
        )
    }
}

// Default value functions
fn default_max_connections() -> u32 { 10 }
fn default_connection_timeout() -> u64 { 30 }
fn default_query_timeout() -> u64 { 30 }
fn default_log_level() -> String { "info".to_string() }
fn default_log_format() -> String { "pretty".to_string() }
fn default_include_location() -> bool { false }
fn default_true() -> bool { true }
fn default_app_name() -> String { env!("CARGO_PKG_NAME").to_string() }
fn default_app_version() -> String { env!("CARGO_PKG_VERSION").to_string() }
fn default_shutdown_timeout() -> u64 { 30 }
fn default_state_snapshot_interval() -> u64 { 1800 } // 30 minutes
fn default_descriptions_interval() -> u64 { 4 } // 4 hours
fn default_window_augmentation() -> WindowAugmentation { WindowAugmentation::Basic }
fn default_workspace_tracking() -> WorkspaceTracking { WorkspaceTracking::Events }
fn default_hyprctl_cache_ms() -> u64 { 100 }
fn default_focus_history_depth() -> usize { 3 }
fn default_heartbeat_interval() -> u64 { 60 } // 1 minute
fn default_max_retries() -> u32 { 3 }
fn default_retry_delay() -> u64 { 5 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.database.url, "postgresql://localhost/sinex");
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.hyprland.window_augmentation, WindowAugmentation::Basic);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        let deserialized: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(config.database.url, deserialized.database.url);
    }
}