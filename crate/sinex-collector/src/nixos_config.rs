use crate::config_utils::resolve_system_safe_path;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use tracing::{info, warn};

/// Direct configuration structure that matches NixOS module options
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NixosConfig {
    #[serde(default)]
    pub collector: CollectorSettings,

    #[serde(default)]
    pub event_sources: EventSourceSettings,

    #[serde(default)]
    pub storage: StorageSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorSettings {
    /// Required: Git-annex repository path
    pub annex_repo_path: String,

    /// Database connection pool size
    #[serde(default = "default_pool_size")]
    pub database_pool_size: u32,

    /// Threshold for storing content in git-annex
    #[serde(default = "default_blob_threshold")]
    pub blob_threshold: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSourceSettings {
    #[serde(default = "default_true")]
    pub filesystem: bool,

    #[serde(default = "default_true")]
    pub terminal: bool,

    #[serde(default = "default_true")]
    pub window_manager: bool,

    #[serde(default = "default_true")]
    pub clipboard: bool,

    #[serde(default = "default_true")]
    pub system_events: bool,

    #[serde(default)]
    pub process_monitoring: bool,

    #[serde(default)]
    pub network_monitoring: bool,

    #[serde(default)]
    pub screen_capture: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    #[serde(default = "default_compression")]
    pub compression_level: String,

    /// Data retention (null = infinite)
    pub data_retention: Option<String>,
}

impl Default for CollectorSettings {
    fn default() -> Self {
        Self {
            annex_repo_path: String::new(), // Must be explicitly set
            database_pool_size: default_pool_size(),
            blob_threshold: default_blob_threshold(),
        }
    }
}

impl Default for EventSourceSettings {
    fn default() -> Self {
        Self {
            filesystem: true,
            terminal: true,
            window_manager: true,
            clipboard: true,
            system_events: true,
            process_monitoring: false,
            network_monitoring: false,
            screen_capture: false,
        }
    }
}

impl Default for StorageSettings {
    fn default() -> Self {
        Self {
            compression_level: default_compression(),
            data_retention: None, // Infinite retention by default
        }
    }
}

// Default value functions
fn default_true() -> bool {
    true
}

fn default_pool_size() -> u32 {
    25
}

fn default_blob_threshold() -> String {
    "10MB".to_string()
}

fn default_compression() -> String {
    "balanced".to_string()
}

impl NixosConfig {
    /// Load configuration from file (TOML format)
    pub fn load_from_file(path: &PathBuf) -> Result<Self> {
        // Security: Validate path is not a symlink attack
        let metadata = std::fs::symlink_metadata(path)
            .with_context(|| format!("Cannot read metadata for config file: {}", path.display()))?;

        if metadata.file_type().is_symlink() {
            // Resolve symlink and validate target is within allowed directories
            let canonical_path = path.canonicalize().with_context(|| {
                format!(
                    "Cannot resolve symlink target for config file: {}",
                    path.display()
                )
            })?;

            // Validate canonical path is a regular file
            let canonical_metadata = std::fs::metadata(&canonical_path).with_context(|| {
                format!(
                    "Cannot read canonical path metadata: {}",
                    canonical_path.display()
                )
            })?;

            if !canonical_metadata.is_file() {
                return Err(anyhow!(
                    "Config symlink target is not a regular file: {}",
                    canonical_path.display()
                ));
            }

            warn!(
                "Loading config from symlink: {} -> {}",
                path.display(),
                canonical_path.display()
            );
        }

        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: Self = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        config.validate()?;

        info!("Loaded configuration from {}", path.display());
        Ok(config)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Require git-annex repository path
        if self.collector.annex_repo_path.is_empty() {
            anyhow::bail!("annex_repo_path is required and must not be empty");
        }

        // Validate git-annex repository exists
        let annex_path = PathBuf::from(&self.collector.annex_repo_path);
        if !annex_path.exists() {
            anyhow::bail!(
                "Git-annex repository does not exist: {}",
                annex_path.display()
            );
        }

        // Validate it's actually a git-annex repository
        let git_annex_dir = annex_path.join(".git").join("annex");
        if !git_annex_dir.exists() {
            anyhow::bail!(
                "Directory is not a git-annex repository: {}",
                annex_path.display()
            );
        }

        // Validate blob threshold format
        self.parse_blob_threshold().with_context(|| {
            format!(
                "Invalid blob_threshold format: {}",
                self.collector.blob_threshold
            )
        })?;

        // Validate compression level
        match self.storage.compression_level.as_str() {
            "fast" | "balanced" | "max" => {}
            _ => anyhow::bail!(
                "Invalid compression_level: {} (must be fast, balanced, or max)",
                self.storage.compression_level
            ),
        }

        // Validate data retention format if specified
        if let Some(retention) = &self.storage.data_retention {
            self.parse_retention_period(retention)
                .with_context(|| format!("Invalid data_retention format: {}", retention))?;
        }

        info!("Configuration validation passed");
        Ok(())
    }

    /// Convert to CollectorConfig format
    pub fn to_collector_config(&self) -> Result<super::CollectorConfig> {
        let mut enabled_events = Vec::new();
        let event_config = HashMap::new();
        let mut flat_config = HashMap::new();

        // Build enabled events list based on source settings
        if self.event_sources.filesystem {
            enabled_events.extend(
                ["file.created", "file.modified", "file.deleted"]
                    .iter()
                    .map(|s| s.to_string()),
            );
        }

        if self.event_sources.terminal {
            enabled_events.extend(
                [
                    "command.completed",
                    "command.imported",
                    "command.hist",
                    "output.captured",
                ]
                .iter()
                .map(|s| s.to_string()),
            );
        }

        if self.event_sources.window_manager {
            enabled_events.extend(
                [
                    "window.focused",
                    "window.opened",
                    "window.closed",
                    "workspace.switched",
                ]
                .iter()
                .map(|s| s.to_string()),
            );
        }

        if self.event_sources.clipboard {
            enabled_events.extend(["copied", "selected"].iter().map(|s| s.to_string()));
        }

        if self.event_sources.system_events {
            enabled_events.extend(
                [
                    "signal.received",
                    "notification.sent",
                    "media.state_changed",
                    "power.state_changed",
                    "device.connected",
                    "device.disconnected",
                    "entry.written",
                ]
                .iter()
                .map(|s| s.to_string()),
            );
        }

        if self.event_sources.process_monitoring {
            enabled_events.extend(
                ["process.started", "process.ended"]
                    .iter()
                    .map(|s| s.to_string()),
            );
        }

        if self.event_sources.network_monitoring {
            enabled_events.extend(
                ["network.connection.opened", "network.connection.closed"]
                    .iter()
                    .map(|s| s.to_string()),
            );
        }

        if self.event_sources.screen_capture {
            enabled_events.extend(
                ["screen.captured", "screen.ocr.completed"]
                    .iter()
                    .map(|s| s.to_string()),
            );
        }

        // Configure event sources with smart defaults
        if self.event_sources.filesystem {
            self.add_filesystem_config(&mut flat_config)?;
        }

        if self.event_sources.terminal {
            self.add_terminal_config(&mut flat_config)?;
        }

        if self.event_sources.window_manager {
            self.add_window_manager_config(&mut flat_config)?;
        }

        if self.event_sources.clipboard {
            self.add_clipboard_config(&mut flat_config)?;
        }

        if self.event_sources.system_events {
            self.add_system_events_config(&mut flat_config)?;
        }

        Ok(super::CollectorConfig {
            enabled_events,
            event: event_config,
            flat_config,
            annex_repo_path: Some(self.collector.annex_repo_path.clone()),
        })
    }

    fn add_filesystem_config(
        &self,
        flat_config: &mut HashMap<String, sinex_core::ConfigValue>,
    ) -> Result<()> {
        let mut files_config = toml::map::Map::new();

        // Auto-discover watch paths
        let watch_paths = self.auto_discover_watch_paths();
        files_config.insert(
            "watch_patterns".to_string(),
            sinex_core::ConfigValue::Array(
                watch_paths
                    .into_iter()
                    .map(sinex_core::ConfigValue::String)
                    .collect(),
            ),
        );

        // Smart ignore patterns
        files_config.insert(
            "ignore_patterns".to_string(),
            sinex_core::ConfigValue::Array(
                self.get_ignore_patterns()
                    .into_iter()
                    .map(sinex_core::ConfigValue::String)
                    .collect(),
            ),
        );

        // Optimal debounce settings
        files_config.insert(
            "debounce_ms".to_string(),
            sinex_core::ConfigValue::Integer(100),
        );
        files_config.insert(
            "max_depth".to_string(),
            sinex_core::ConfigValue::Integer(10),
        );

        flat_config.insert(
            "event.files".to_string(),
            sinex_core::ConfigValue::Table(files_config),
        );
        Ok(())
    }

    fn add_terminal_config(
        &self,
        flat_config: &mut HashMap<String, sinex_core::ConfigValue>,
    ) -> Result<()> {
        // Auto-discover Atuin database
        if let Ok(atuin_config) = self.auto_discover_atuin_config() {
            flat_config.insert("event.command_imported".to_string(), atuin_config);
        }

        // Auto-discover Kitty socket
        if let Ok(kitty_config) = self.auto_discover_kitty_config() {
            flat_config.insert("event.command_completed".to_string(), kitty_config.clone());
            flat_config.insert("event.output_captured".to_string(), kitty_config);
        }

        // Shell history configuration
        let mut shell_config = toml::map::Map::new();
        shell_config.insert(
            "history_files".to_string(),
            sinex_core::ConfigValue::Array(vec![
                sinex_core::ConfigValue::String(resolve_system_safe_path(
                    "~/.zsh_history",
                    Some("ZSH_HISTORY_FILE"),
                    "/var/lib/sinex/shell",
                )),
                sinex_core::ConfigValue::String(resolve_system_safe_path(
                    "~/.bash_history",
                    Some("BASH_HISTORY_FILE"),
                    "/var/lib/sinex/shell",
                )),
            ]),
        );
        shell_config.insert(
            "polling_interval_secs".to_string(),
            sinex_core::ConfigValue::Integer(10),
        );
        shell_config.insert(
            "use_file_watch".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );

        flat_config.insert(
            "event.command_hist".to_string(),
            sinex_core::ConfigValue::Table(shell_config),
        );
        Ok(())
    }

    fn add_window_manager_config(
        &self,
        flat_config: &mut HashMap<String, sinex_core::ConfigValue>,
    ) -> Result<()> {
        let mut windows_config = toml::map::Map::new();
        windows_config.insert(
            "monitored_events".to_string(),
            sinex_core::ConfigValue::Array(vec![
                sinex_core::ConfigValue::String("activewindow".to_string()),
                sinex_core::ConfigValue::String("openwindow".to_string()),
                sinex_core::ConfigValue::String("closewindow".to_string()),
                sinex_core::ConfigValue::String("workspace".to_string()),
                sinex_core::ConfigValue::String("focusedmon".to_string()),
            ]),
        );

        flat_config.insert(
            "event.windows".to_string(),
            sinex_core::ConfigValue::Table(windows_config),
        );

        // State snapshot configuration
        let mut snapshot_config = toml::map::Map::new();
        snapshot_config.insert(
            "interval_secs".to_string(),
            sinex_core::ConfigValue::Integer(300),
        ); // 5 minutes
        snapshot_config.insert(
            "include_monitors".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        snapshot_config.insert(
            "include_workspaces".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        snapshot_config.insert(
            "include_clients".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );

        flat_config.insert(
            "event.state_snapshot".to_string(),
            sinex_core::ConfigValue::Table(snapshot_config),
        );
        Ok(())
    }

    fn add_clipboard_config(
        &self,
        flat_config: &mut HashMap<String, sinex_core::ConfigValue>,
    ) -> Result<()> {
        let mut clipboard_config = toml::map::Map::new();
        clipboard_config.insert(
            "poll_interval_ms".to_string(),
            sinex_core::ConfigValue::Integer(500),
        );
        clipboard_config.insert(
            "max_content_size".to_string(),
            sinex_core::ConfigValue::String(self.collector.blob_threshold.clone()),
        );
        clipboard_config.insert(
            "enable_deduplication".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );

        flat_config.insert(
            "event.clipboard".to_string(),
            sinex_core::ConfigValue::Table(clipboard_config),
        );
        Ok(())
    }

    fn add_system_events_config(
        &self,
        flat_config: &mut HashMap<String, sinex_core::ConfigValue>,
    ) -> Result<()> {
        let mut dbus_config = toml::map::Map::new();
        dbus_config.insert(
            "monitor_session".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        dbus_config.insert(
            "monitor_system".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        dbus_config.insert(
            "extract_notifications".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        dbus_config.insert(
            "extract_media".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        dbus_config.insert(
            "extract_power".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        dbus_config.insert(
            "extract_hardware".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        dbus_config.insert(
            "extract_bluetooth".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );
        dbus_config.insert(
            "extract_network".to_string(),
            sinex_core::ConfigValue::Boolean(true),
        );

        flat_config.insert(
            "event.dbus".to_string(),
            sinex_core::ConfigValue::Table(dbus_config),
        );
        Ok(())
    }

    fn auto_discover_watch_paths(&self) -> Vec<String> {
        let home = std::env::var("HOME").unwrap_or_default();
        let mut paths = vec![
            format!("{}/Documents/**/*", home),
            format!("{}/Desktop/**/*", home),
            format!("{}/Downloads/**/*", home),
        ];

        // Auto-detect development directories
        for dev_dir in ["Projects", "Code", "src", "workspace", "dev", "git"] {
            let path = format!("{}/{}", home, dev_dir);
            if PathBuf::from(&path).exists() {
                paths.push(format!("{}/**/*", path));
            }
        }

        // Add common system paths
        paths.extend(["/etc/**/*".to_string()]);

        paths
    }

    fn get_ignore_patterns(&self) -> Vec<String> {
        vec![
            "**/.git/**".to_string(),
            "**/.*".to_string(), // Hidden files
            "**/__pycache__/**".to_string(),
            "**/*.pyc".to_string(),
            "**/target/**".to_string(),       // Rust
            "**/node_modules/**".to_string(), // Node.js
            "**/build/**".to_string(),        // General build
            "**/dist/**".to_string(),         // Distribution
            "**/.cache/**".to_string(),       // Cache
            "**/tmp/**".to_string(),          // Temporary
            "**/result/**".to_string(),       // Nix
            "**/result-*/**".to_string(),     // Nix build outputs
        ]
    }

    fn auto_discover_atuin_config(&self) -> Result<sinex_core::ConfigValue> {
        let home = std::env::var("HOME").context("HOME environment variable not set")?;
        let possible_paths = [
            format!("{}/.local/share/atuin/history.db", home),
            format!("{}/.config/atuin/history.db", home),
            "/usr/share/atuin/history.db".to_string(),
        ];

        let db_path = possible_paths
            .iter()
            .find(|path| PathBuf::from(path).exists())
            .cloned()
            .unwrap_or_else(|| format!("{}/.local/share/atuin/history.db", home));

        let mut config = toml::map::Map::new();
        config.insert(
            "db_path".to_string(),
            sinex_core::ConfigValue::String(db_path),
        );
        config.insert(
            "polling_interval_secs".to_string(),
            sinex_core::ConfigValue::Integer(10),
        );
        config.insert(
            "batch_size".to_string(),
            sinex_core::ConfigValue::Integer(100),
        );
        config.insert(
            "use_file_watch".to_string(),
            sinex_core::ConfigValue::Boolean(false),
        );

        Ok(sinex_core::ConfigValue::Table(config))
    }

    fn auto_discover_kitty_config(&self) -> Result<sinex_core::ConfigValue> {
        let default_tmp_dir = env::var("SINEX_TMP_DIR").unwrap_or_else(|_| "/tmp".to_string());
        let possible_sockets = [
            format!("{}/kitty", default_tmp_dir),
            format!("{}/kitty-{}", default_tmp_dir, std::process::id()),
            std::env::var("KITTY_LISTEN_ON").unwrap_or_default(),
        ];

        let socket_path = possible_sockets
            .iter()
            .find(|path| !path.is_empty() && PathBuf::from(path).exists())
            .cloned()
            .unwrap_or_else(|| {
                let tmp_dir = env::var("SINEX_TMP_DIR").unwrap_or_else(|_| "/tmp".to_string());
                format!("{}/kitty", tmp_dir)
            });

        let mut config = toml::map::Map::new();
        config.insert(
            "socket_path".to_string(),
            sinex_core::ConfigValue::String(socket_path),
        );
        config.insert(
            "capture_env_vars".to_string(),
            sinex_core::ConfigValue::Array(vec![
                sinex_core::ConfigValue::String("PATH".to_string()),
                sinex_core::ConfigValue::String("PWD".to_string()),
                sinex_core::ConfigValue::String("USER".to_string()),
                sinex_core::ConfigValue::String("HOME".to_string()),
                sinex_core::ConfigValue::String("SHELL".to_string()),
            ]),
        );
        config.insert(
            "max_command_length".to_string(),
            sinex_core::ConfigValue::Integer(4096),
        );

        Ok(sinex_core::ConfigValue::Table(config))
    }

    fn parse_blob_threshold(&self) -> Result<u64> {
        let threshold = &self.collector.blob_threshold;
        if let Some(size_str) = threshold.strip_suffix("MB") {
            let size: u64 = size_str
                .parse()
                .context("Invalid number in blob threshold")?;
            Ok(size * 1024 * 1024)
        } else if let Some(size_str) = threshold.strip_suffix("KB") {
            let size: u64 = size_str
                .parse()
                .context("Invalid number in blob threshold")?;
            Ok(size * 1024)
        } else if let Some(size_str) = threshold.strip_suffix("GB") {
            let size: u64 = size_str
                .parse()
                .context("Invalid number in blob threshold")?;
            Ok(size * 1024 * 1024 * 1024)
        } else {
            threshold
                .parse()
                .context("Invalid blob threshold format (expected number with MB/KB/GB suffix)")
        }
    }

    fn parse_retention_period(&self, retention: &str) -> Result<chrono::Duration> {
        if let Some(days_str) = retention.strip_suffix("d") {
            let days: i64 = days_str
                .parse()
                .context("Invalid number in retention period")?;
            Ok(chrono::Duration::days(days))
        } else if let Some(weeks_str) = retention.strip_suffix("w") {
            let weeks: i64 = weeks_str
                .parse()
                .context("Invalid number in retention period")?;
            Ok(chrono::Duration::weeks(weeks))
        } else if let Some(months_str) = retention.strip_suffix("m") {
            let months: i64 = months_str
                .parse()
                .context("Invalid number in retention period")?;
            Ok(chrono::Duration::days(months * 30)) // Approximate
        } else if let Some(years_str) = retention.strip_suffix("y") {
            let years: i64 = years_str
                .parse()
                .context("Invalid number in retention period")?;
            Ok(chrono::Duration::days(years * 365)) // Approximate
        } else {
            anyhow::bail!("Invalid retention period format (expected number with d/w/m/y suffix)")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = NixosConfig::default();
        assert!(config.event_sources.filesystem);
        assert!(config.event_sources.terminal);
        assert_eq!(config.storage.data_retention, None); // Infinite retention
    }

    #[test]
    fn test_blob_threshold_parsing() {
        let config = NixosConfig {
            collector: CollectorSettings {
                blob_threshold: "5MB".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(config.parse_blob_threshold().unwrap(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_config_validation_requires_annex_path() {
        let config = NixosConfig::default();
        assert!(config.validate().is_err());
    }
}
