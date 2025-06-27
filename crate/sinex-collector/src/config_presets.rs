use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sinex_core::ConfigValue;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{info, warn};

/// Configuration presets for common use cases
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigPreset {
    #[serde(rename = "personal-desktop")]
    PersonalDesktop,
    #[serde(rename = "developer-focused")]
    DeveloperFocused,
    #[serde(rename = "researcher")]
    Researcher,
    #[serde(rename = "server-monitoring")]
    ServerMonitoring,
    #[serde(rename = "minimal")]
    Minimal,
    #[serde(rename = "comprehensive")]
    Comprehensive,
}

/// Simplified configuration structure with presets and smart defaults
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimplifiedConfig {
    /// Configuration preset (automatically configures everything)
    #[serde(default = "default_preset")]
    pub preset: ConfigPreset,

    /// Observability level
    #[serde(default = "default_observability")]
    pub observability: ObservabilityLevel,

    /// Privacy settings (opt-out approach)
    #[serde(default)]
    pub privacy: PrivacyConfig,

    /// Storage configuration (auto-configured)
    #[serde(default)]
    pub storage: StorageConfig,

    /// Event source frequency settings
    #[serde(default)]
    pub frequency: FrequencyConfig,

    /// Custom paths
    #[serde(default)]
    pub paths: PathsConfig,

    /// Advanced features (disabled by default)
    #[serde(default)]
    pub advanced: AdvancedConfig,

    /// Legacy configuration compatibility
    #[serde(default)]
    pub legacy_config: bool,

    /// Legacy detailed configuration (when legacy_config = true)
    #[serde(default)]
    pub legacy: LegacyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObservabilityLevel {
    #[serde(rename = "minimal")]
    Minimal,
    #[serde(rename = "standard")]
    Standard,
    #[serde(rename = "comprehensive")]
    Comprehensive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyConfig {
    /// Event sources to disable
    #[serde(default)]
    pub disable: Vec<String>,
    
    /// Hash sensitive content instead of storing plaintext
    #[serde(default = "default_true")]
    pub hash_sensitive: bool,
    
    /// Auto-delete events older than this (days)
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Database connection pool size
    #[serde(default = "default_pool_size")]
    pub database_pool: PoolSize,
    
    /// Git-annex repository location
    #[serde(default = "default_annex_repo")]
    pub annex_repo: String,
    
    /// Compression level
    #[serde(default = "default_compression")]
    pub compression: CompressionLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PoolSize {
    #[serde(rename = "small")]
    Small,
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "large")]
    Large,
    #[serde(untagged)]
    Number(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionLevel {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "fast")]
    Fast,
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "max")]
    Max,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrequencyConfig {
    /// Global frequency setting
    #[serde(default = "default_frequency")]
    pub global: FrequencyLevel,
    
    /// Per-source frequency overrides
    #[serde(default)]
    pub filesystem: Option<FrequencyLevel>,
    
    #[serde(default)]
    pub terminal: Option<FrequencyLevel>,
    
    #[serde(default)]
    pub clipboard: Option<FrequencyLevel>,
    
    #[serde(default)]
    pub dbus: Option<FrequencyLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrequencyLevel {
    #[serde(rename = "battery")]
    Battery,     // Battery-friendly: 30s+ intervals
    #[serde(rename = "normal")]
    Normal,      // Balanced: 5-10s intervals
    #[serde(rename = "responsive")]
    Responsive,  // Responsive: 1-3s intervals
    #[serde(rename = "realtime")]
    Realtime,    // Sub-second for critical events
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathsConfig {
    /// Additional filesystem paths to monitor
    #[serde(default)]
    pub watch: Vec<String>,
    
    /// Extra ignore patterns
    #[serde(default)]
    pub ignore: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedConfig {
    /// Enable screen capture with OCR
    #[serde(default)]
    pub enable_screen_capture: bool,
    
    /// Enable audio monitoring
    #[serde(default)]
    pub enable_audio_monitoring: bool,
    
    /// Enable network monitoring (requires root)
    #[serde(default)]
    pub enable_network_monitoring: bool,
    
    /// Enable process monitoring (requires root)
    #[serde(default)]
    pub enable_process_monitoring: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyConfig {
    /// Legacy enabled events list
    #[serde(default)]
    pub enabled_events: Vec<String>,
    
    /// Legacy event configuration
    #[serde(default)]
    pub event: HashMap<String, ConfigValue>,
}

// Default functions
fn default_preset() -> ConfigPreset {
    ConfigPreset::PersonalDesktop
}

fn default_observability() -> ObservabilityLevel {
    ObservabilityLevel::Standard
}

fn default_true() -> bool {
    true
}

fn default_retention_days() -> u32 {
    90
}

fn default_pool_size() -> PoolSize {
    PoolSize::Auto
}

fn default_annex_repo() -> String {
    "auto".to_string()
}

fn default_compression() -> CompressionLevel {
    CompressionLevel::Auto
}

fn default_frequency() -> FrequencyLevel {
    FrequencyLevel::Normal
}

impl Default for SimplifiedConfig {
    fn default() -> Self {
        Self {
            preset: default_preset(),
            observability: default_observability(),
            privacy: PrivacyConfig::default(),
            storage: StorageConfig::default(),
            frequency: FrequencyConfig::default(),
            paths: PathsConfig::default(),
            advanced: AdvancedConfig::default(),
            legacy_config: false,
            legacy: LegacyConfig::default(),
        }
    }
}

impl Default for PrivacyConfig {
    fn default() -> Self {
        Self {
            disable: vec![],
            hash_sensitive: default_true(),
            retention_days: default_retention_days(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_pool: default_pool_size(),
            annex_repo: default_annex_repo(),
            compression: default_compression(),
        }
    }
}

impl Default for FrequencyConfig {
    fn default() -> Self {
        Self {
            global: default_frequency(),
            filesystem: None,
            terminal: None,
            clipboard: None,
            dbus: None,
        }
    }
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            watch: vec![],
            ignore: vec![],
        }
    }
}

impl Default for AdvancedConfig {
    fn default() -> Self {
        Self {
            enable_screen_capture: false,
            enable_audio_monitoring: false,
            enable_network_monitoring: false,
            enable_process_monitoring: false,
        }
    }
}

impl Default for LegacyConfig {
    fn default() -> Self {
        Self {
            enabled_events: vec![],
            event: HashMap::new(),
        }
    }
}

impl SimplifiedConfig {
    /// Convert preset to detailed configuration
    pub fn to_detailed_config(&self) -> Result<super::CollectorConfig> {
        if self.legacy_config {
            // Use legacy configuration as-is
            return Ok(super::CollectorConfig {
                enabled_events: self.legacy.enabled_events.clone(),
                event: HashMap::new(),
                flat_config: self.legacy.event.clone(),
                annex_repo_path: self.resolve_annex_repo()?,
            });
        }

        let events = self.get_preset_events();
        let mut event_config = HashMap::new();
        let mut flat_config = HashMap::new();

        // Configure based on preset
        self.apply_preset_configuration(&mut event_config, &mut flat_config)?;
        
        // Apply frequency overrides
        self.apply_frequency_configuration(&mut event_config, &mut flat_config)?;
        
        // Apply path customizations
        self.apply_path_configuration(&mut event_config, &mut flat_config)?;
        
        // Apply privacy settings
        self.apply_privacy_configuration(&mut event_config, &mut flat_config)?;
        
        // Apply advanced features
        self.apply_advanced_configuration(&mut event_config, &mut flat_config)?;

        Ok(super::CollectorConfig {
            enabled_events: events,
            event: event_config,
            flat_config,
            annex_repo_path: self.resolve_annex_repo()?,
        })
    }

    fn get_preset_events(&self) -> Vec<String> {
        match self.preset {
            ConfigPreset::PersonalDesktop => vec![
                "file.created", "file.modified", "file.deleted",
                "command.executed", "shell.command.executed_atuin",
                "window.focused", "window.opened", "window.closed", "workspace.changed",
                "dbus.signal", "system.notification", "media.playback.changed",
                "clipboard.content.changed", "terminal.scrollback.captured",
            ],
            ConfigPreset::DeveloperFocused => vec![
                "file.created", "file.modified", "file.deleted",
                "command.executed", "shell.command.executed_atuin", "shell.history.command",
                "terminal.asciinema.session_started", "terminal.scrollback.captured",
                "window.focused", "dbus.signal",
                // TODO: Add git events, IDE events when implemented
            ],
            ConfigPreset::Researcher => vec![
                "file.created", "file.modified", "file.deleted",
                "window.focused", "window.opened", "window.closed",
                "dbus.signal", "system.notification",
                "clipboard.content.changed",
                // TODO: Add browser events, PDF events when implemented
            ],
            ConfigPreset::ServerMonitoring => vec![
                "dbus.signal", "system.power.event", "hardware.device.event",
                "network.connection.event", "storage.mount.event",
                "session.state.changed",
            ],
            ConfigPreset::Minimal => vec![
                "file.created", "file.modified", "file.deleted",
                "command.executed", "window.focused",
            ],
            ConfigPreset::Comprehensive => {
                // Enable everything available
                vec![
                    "file.created", "file.modified", "file.deleted",
                    "command.executed", "shell.command.executed_atuin", "shell.history.command",
                    "terminal.asciinema.session_started", "terminal.asciinema.session_ended",
                    "terminal.scrollback.captured", "terminal.command_output.captured",
                    "window.focused", "window.opened", "window.closed", "workspace.changed",
                    "dbus.signal", "dbus.method_call", "system.notification",
                    "media.playback.changed", "system.power.event", "hardware.device.event",
                    "session.state.changed", "bluetooth.device.event", "network.connection.event",
                    "screen.saver.event", "storage.mount.event", "security.policykit.authorization",
                    "clipboard.content.changed", "state.snapshot",
                ]
            }
        }.into_iter().map(String::from).collect()
    }

    fn apply_preset_configuration(&self, _event_config: &mut HashMap<String, ConfigValue>, flat_config: &mut HashMap<String, ConfigValue>) -> Result<()> {
        // Auto-discover common development paths
        let watch_paths = self.auto_discover_watch_paths();
        
        // Configure filesystem monitoring
        let mut files_config = toml::map::Map::new();
        files_config.insert(
            "watch_patterns".to_string(),
            ConfigValue::Array(watch_paths.into_iter().map(ConfigValue::String).collect()),
        );
        files_config.insert(
            "ignore_patterns".to_string(),
            ConfigValue::Array(self.get_smart_ignore_patterns().into_iter().map(ConfigValue::String).collect()),
        );
        files_config.insert(
            "debounce_ms".to_string(),
            ConfigValue::Integer(self.frequency_to_debounce_ms(self.frequency.filesystem.as_ref().unwrap_or(&self.frequency.global))),
        );
        
        flat_config.insert("event.files".to_string(), ConfigValue::Table(files_config));

        // Auto-configure Atuin if available
        if let Ok(atuin_config) = self.auto_discover_atuin_config() {
            flat_config.insert("event.shell_command_executed_atuin".to_string(), atuin_config);
        }

        // Auto-configure terminal sources
        if let Ok(kitty_config) = self.auto_discover_kitty_config() {
            flat_config.insert("event.command_executed".to_string(), kitty_config.clone());
            flat_config.insert("event.terminal_scrollback".to_string(), kitty_config);
        }

        // Configure D-Bus monitoring based on preset
        let mut dbus_config = toml::map::Map::new();
        match self.preset {
            ConfigPreset::ServerMonitoring => {
                dbus_config.insert("monitor_session".to_string(), ConfigValue::Boolean(false));
                dbus_config.insert("monitor_system".to_string(), ConfigValue::Boolean(true));
            }
            ConfigPreset::Minimal => {
                dbus_config.insert("extract_notifications".to_string(), ConfigValue::Boolean(true));
                dbus_config.insert("extract_media".to_string(), ConfigValue::Boolean(false));
                dbus_config.insert("extract_power".to_string(), ConfigValue::Boolean(false));
            }
            _ => {
                dbus_config.insert("monitor_session".to_string(), ConfigValue::Boolean(true));
                dbus_config.insert("monitor_system".to_string(), ConfigValue::Boolean(true));
                dbus_config.insert("extract_notifications".to_string(), ConfigValue::Boolean(true));
                dbus_config.insert("extract_media".to_string(), ConfigValue::Boolean(true));
                dbus_config.insert("extract_power".to_string(), ConfigValue::Boolean(true));
            }
        }
        flat_config.insert("event.dbus".to_string(), ConfigValue::Table(dbus_config));

        Ok(())
    }

    fn apply_frequency_configuration(&self, _event_config: &mut HashMap<String, ConfigValue>, flat_config: &mut HashMap<String, ConfigValue>) -> Result<()> {
        // Apply frequency overrides to existing configurations
        if let Some(fs_freq) = &self.frequency.filesystem {
            if let Some(ConfigValue::Table(ref mut files_config)) = flat_config.get_mut("event.files") {
                files_config.insert(
                    "debounce_ms".to_string(),
                    ConfigValue::Integer(self.frequency_to_debounce_ms(fs_freq)),
                );
            }
        }

        if let Some(clipboard_freq) = &self.frequency.clipboard {
            let mut clipboard_config = toml::map::Map::new();
            clipboard_config.insert(
                "poll_interval_ms".to_string(),
                ConfigValue::Integer(self.frequency_to_poll_ms(clipboard_freq)),
            );
            flat_config.insert("event.clipboard".to_string(), ConfigValue::Table(clipboard_config));
        }

        Ok(())
    }

    fn apply_path_configuration(&self, _event_config: &mut HashMap<String, ConfigValue>, flat_config: &mut HashMap<String, ConfigValue>) -> Result<()> {
        // Add custom watch paths
        if !self.paths.watch.is_empty() {
            if let Some(ConfigValue::Table(ref mut files_config)) = flat_config.get_mut("event.files") {
                if let Some(ConfigValue::Array(ref mut patterns)) = files_config.get_mut("watch_patterns") {
                    for path in &self.paths.watch {
                        patterns.push(ConfigValue::String(format!("{}/**/*", path)));
                    }
                }
            }
        }

        // Add custom ignore patterns
        if !self.paths.ignore.is_empty() {
            if let Some(ConfigValue::Table(ref mut files_config)) = flat_config.get_mut("event.files") {
                if let Some(ConfigValue::Array(ref mut patterns)) = files_config.get_mut("ignore_patterns") {
                    for ignore in &self.paths.ignore {
                        patterns.push(ConfigValue::String(ignore.clone()));
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_privacy_configuration(&self, _event_config: &mut HashMap<String, ConfigValue>, _flat_config: &mut HashMap<String, ConfigValue>) -> Result<()> {
        // Privacy settings are handled at the event filtering level
        // This would integrate with the event filtering system
        info!("Applied privacy configuration: disabled sources: {:?}", self.privacy.disable);
        Ok(())
    }

    fn apply_advanced_configuration(&self, _event_config: &mut HashMap<String, ConfigValue>, _flat_config: &mut HashMap<String, ConfigValue>) -> Result<()> {
        // Advanced features would add their configurations when enabled
        if self.advanced.enable_screen_capture {
            info!("Screen capture enabled - would add screenshot configuration");
        }
        if self.advanced.enable_network_monitoring {
            warn!("Network monitoring enabled - requires root privileges");
        }
        Ok(())
    }

    fn auto_discover_watch_paths(&self) -> Vec<String> {
        let home = std::env::var("HOME").unwrap_or_default();
        let mut paths = vec![
            format!("{}/Documents", home),
            format!("{}/Desktop", home),
            format!("{}/Downloads", home),
        ];

        // Auto-detect development directories
        for dev_dir in ["Projects", "Code", "src", "workspace", "dev", "git"] {
            let path = format!("{}/{}", home, dev_dir);
            if PathBuf::from(&path).exists() {
                paths.push(format!("{}/**/*", path));
            }
        }

        match self.preset {
            ConfigPreset::DeveloperFocused => {
                paths.push("/etc/**/*".to_string()); // System config files
            }
            ConfigPreset::ServerMonitoring => {
                paths.extend([
                    "/var/log/**/*".to_string(),
                    "/etc/**/*".to_string(),
                ]);
            }
            _ => {}
        }

        paths
    }

    fn get_smart_ignore_patterns(&self) -> Vec<String> {
        let mut patterns = vec![
            "**/.git/**".to_string(),
            "**/.*".to_string(), // Hidden files
            "**/__pycache__/**".to_string(),
            "**/*.pyc".to_string(),
        ];

        // Add development-specific ignores
        if matches!(self.preset, ConfigPreset::DeveloperFocused | ConfigPreset::PersonalDesktop) {
            patterns.extend([
                "**/target/**".to_string(),      // Rust
                "**/node_modules/**".to_string(), // Node.js
                "**/build/**".to_string(),       // General build dirs
                "**/dist/**".to_string(),        // Distribution dirs
                "**/.cache/**".to_string(),      // Cache dirs
                "**/tmp/**".to_string(),         // Temporary dirs
            ]);
        }

        patterns
    }

    fn auto_discover_atuin_config(&self) -> Result<ConfigValue> {
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
        config.insert("db_path".to_string(), ConfigValue::String(db_path));
        config.insert(
            "polling_interval_secs".to_string(),
            ConfigValue::Integer(self.frequency_to_poll_secs(&self.frequency.global)),
        );

        Ok(ConfigValue::Table(config))
    }

    fn auto_discover_kitty_config(&self) -> Result<ConfigValue> {
        let possible_sockets = [
            "/tmp/kitty".to_string(),
            format!("/tmp/kitty-{}", std::process::id()),
            std::env::var("KITTY_LISTEN_ON").unwrap_or_default(),
        ];

        let socket_path = possible_sockets
            .iter()
            .find(|path| !path.is_empty() && PathBuf::from(path).exists())
            .cloned()
            .unwrap_or_else(|| "/tmp/kitty".to_string());

        let mut config = toml::map::Map::new();
        config.insert("socket_path".to_string(), ConfigValue::String(socket_path));
        config.insert(
            "capture_env_vars".to_string(),
            ConfigValue::Array(vec![
                ConfigValue::String("PATH".to_string()),
                ConfigValue::String("PWD".to_string()),
                ConfigValue::String("USER".to_string()),
                ConfigValue::String("HOME".to_string()),
                ConfigValue::String("SHELL".to_string()),
            ]),
        );

        Ok(ConfigValue::Table(config))
    }

    fn resolve_annex_repo(&self) -> Result<Option<String>> {
        match self.storage.annex_repo.as_str() {
            "auto" => {
                // Auto-discover or suggest location
                let home = std::env::var("HOME").unwrap_or_default();
                let candidates = [
                    format!("{}/.local/share/sinex/annex", home),
                    "/var/lib/sinex/annex".to_string(),
                    format!("{}/sinex-annex", home),
                ];

                for candidate in &candidates {
                    if PathBuf::from(candidate).exists() {
                        return Ok(Some(candidate.clone()));
                    }
                }

                // Default to first candidate
                Ok(Some(candidates[0].clone()))
            }
            path => Ok(Some(path.to_string())),
        }
    }

    fn frequency_to_debounce_ms(&self, freq: &FrequencyLevel) -> i64 {
        match freq {
            FrequencyLevel::Battery => 1000,    // 1 second
            FrequencyLevel::Normal => 100,      // 100ms
            FrequencyLevel::Responsive => 50,   // 50ms
            FrequencyLevel::Realtime => 10,     // 10ms
        }
    }

    fn frequency_to_poll_ms(&self, freq: &FrequencyLevel) -> i64 {
        match freq {
            FrequencyLevel::Battery => 30000,   // 30 seconds
            FrequencyLevel::Normal => 5000,     // 5 seconds
            FrequencyLevel::Responsive => 1000, // 1 second
            FrequencyLevel::Realtime => 500,    // 500ms
        }
    }

    fn frequency_to_poll_secs(&self, freq: &FrequencyLevel) -> i64 {
        match freq {
            FrequencyLevel::Battery => 60,  // 1 minute
            FrequencyLevel::Normal => 10,   // 10 seconds
            FrequencyLevel::Responsive => 3, // 3 seconds
            FrequencyLevel::Realtime => 1,  // 1 second
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preset_configuration() {
        let config = SimplifiedConfig {
            preset: ConfigPreset::DeveloperFocused,
            ..Default::default()
        };

        let detailed = config.to_detailed_config().unwrap();
        assert!(detailed.enabled_events.contains(&"file.created".to_string()));
        assert!(detailed.enabled_events.contains(&"command.executed".to_string()));
    }

    #[test]
    fn test_frequency_conversion() {
        let config = SimplifiedConfig::default();
        assert_eq!(config.frequency_to_debounce_ms(&FrequencyLevel::Normal), 100);
        assert_eq!(config.frequency_to_poll_secs(&FrequencyLevel::Battery), 60);
    }

    #[test]
    fn test_auto_discovery() {
        let config = SimplifiedConfig::default();
        let paths = config.auto_discover_watch_paths();
        assert!(!paths.is_empty());
        // Should contain at least Documents directory
        assert!(paths.iter().any(|p| p.contains("Documents")));
    }
}