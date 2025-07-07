use anyhow::{anyhow, Context, Result};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use sinex_core::{ConfigValidator, ConfigValue};
use sinex_db::security::SecurityValidator;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::env;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorConfig {
    /// List of enabled event types
    pub enabled_events: Vec<String>,

    /// Event-specific configuration
    #[serde(default)]
    pub event: HashMap<String, ConfigValue>,

    /// Direct event config (e.g., event.file_created)
    #[serde(flatten)]
    pub flat_config: HashMap<String, ConfigValue>,

    /// Path to git-annex repository for large content storage
    #[serde(default)]
    pub annex_repo_path: Option<String>,
}

impl CollectorConfig {
    pub fn load() -> Result<Self> {
        Self::load_with_validation(true)
    }

    pub fn load_with_validation(validate: bool) -> Result<Self> {
        // Try environment variable first
        if let Ok(config_path) = env::var("SINEX_CONFIG_FILE") {
            let path = PathBuf::from(config_path);
            if path.exists() {
                let config = Self::load_from_file(&path)?;
                if validate {
                    config.validate()?;
                }
                return Ok(config);
            }
        }

        // Try standard locations with configurable system config directory
        let system_config_dir = env::var("SINEX_SYSTEM_CONFIG_DIR")
            .unwrap_or_else(|_| "/etc/sinex".to_string());
        
        let paths = vec![
            Some(PathBuf::from("sinex-collector.toml")),
            Some(PathBuf::from("unified-collector.toml")), // Legacy compatibility
            dirs::config_dir().map(|mut p| {
                p.push("sinex/collector.toml");
                p
            }),
            Some(PathBuf::from(format!("{}/collector.toml", system_config_dir))),
        ];

        for path in paths.into_iter().flatten() {
            if path.exists() {
                let config = Self::load_from_file(&path)?;
                if validate {
                    config.validate()?;
                }
                return Ok(config);
            }
        }

        // Default config
        let config = Self::default();
        if validate {
            config.validate()?;
        }
        Ok(config)
    }

    pub fn load_from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        
        // Validate configuration content for security issues
        SecurityValidator::validate_config_content(&content)
            .map_err(|e| anyhow!("Security validation failed: {}", e))?;

        // Try TOML first
        if let Ok(config) = toml::from_str::<Self>(&content) {
            return Ok(config);
        }

        // Fall back to JSON
        let config: Self =
            serde_json::from_str(&content).context("Failed to parse config as TOML or JSON")?;
        Ok(config)
    }

    /// Get configuration for a specific event, with hierarchical merging
    pub fn get_event_config(&self, event_name: &str) -> ConfigValue {
        let mut config = ConfigValue::Table(toml::map::Map::new());

        // First, check for category config (e.g., event.files for file.*)
        if let Some(category) = event_name.split('.').next() {
            let category_key = format!("event.{}s", category); // files, commands, etc.
            if let Some(cat_config) = self.flat_config.get(&category_key) {
                merge_toml(&mut config, cat_config.clone());
            }
        }

        // Then apply specific event config
        let event_key = format!("event.{}", event_name.replace('.', "_"));
        if let Some(event_config) = self.flat_config.get(&event_key) {
            merge_toml(&mut config, event_config.clone());
        }

        // Also check the event map
        if let Some(event_config) = self.event.get(event_name) {
            merge_toml(&mut config, event_config.clone());
        }

        config
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();

        // Validate event types
        for event_type in &self.enabled_events {
            if let Err(e) = self.validate_event_type(event_type) {
                errors.push(format!("Invalid event type '{}': {}", event_type, e));
            }
        }

        // Validate event configurations
        for (event_name, config) in &self.event {
            if let Err(e) = self.validate_event_config(event_name, config) {
                errors.push(format!("Invalid config for event '{}': {}", event_name, e));
            }
        }

        // Validate flat config
        for (config_key, config_value) in &self.flat_config {
            if let Err(e) = self.validate_flat_config(config_key, config_value) {
                errors.push(format!("Invalid config '{}': {}", config_key, e));
            }
        }

        if !errors.is_empty() {
            return Err(anyhow!(
                "Configuration validation failed:\n  - {}",
                errors.join("\n  - ")
            ));
        }

        Ok(())
    }

    /// Validate a single event type name
    fn validate_event_type(&self, event_type: &str) -> Result<()> {
        // Event types should follow pattern: category.subcategory[.specific]
        let parts: Vec<&str> = event_type.split('.').collect();

        if parts.len() < 2 {
            return Err(anyhow!(
                "Event type must have at least category.subcategory format"
            ));
        }

        for part in &parts {
            if part.is_empty() {
                return Err(anyhow!("Event type parts cannot be empty"));
            }

            if !part.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(anyhow!(
                    "Event type parts can only contain alphanumeric characters and underscores"
                ));
            }

            if !part.chars().next().unwrap().is_alphabetic() {
                return Err(anyhow!("Event type parts must start with a letter"));
            }
        }

        // Check against known event types (matching actual Rust EVENT_NAME constants)
        let known_events = [
            // Terminal/command events
            "command.executed",         // KittyCommandExecuted
            "command.completed",        // KittyCommandCompleted  
            "command.failed",           // KittyCommandFailed
            "command.imported",         // AtuinCommandImported, ShellHistoryCommandImported
            "session.started",          // ShellSessionStarted
            "session.ended",            // ShellSessionEnded
            
            // Terminal recording
            "recording.started",        // AsciinemaSessionStarted
            "recording.ended",          // AsciinemaSessionEnded
            "output.captured",          // ScrollbackCaptured
            
            // Filesystem events
            "file.created",             // FileCreated
            "file.modified",            // FileModified
            "file.deleted",             // FileDeleted
            "file.moved",               // FileMoved
            "dir.created",              // DirCreated
            "dir.deleted",              // DirDeleted
            
            // Window manager events
            "window.opened",            // WindowOpened
            "window.closed",            // WindowClosed
            "window.focused",           // WindowFocused
            "window.moved",             // WindowMoved
            "window.resized",           // WindowResized
            "workspace.switched",       // WorkspaceSwitched
            "workspace.created",        // WorkspaceCreated
            "workspace.destroyed",      // WorkspaceDestroyed
            "display.connected",        // DisplayConnected
            "display.disconnected",     // DisplayDisconnected
            "monitor.focused",          // MonitorFocused
            "state.captured",           // StateCapture
            
            // D-Bus events
            "signal.received",          // DbusSignalReceived
            "method.called",            // DbusMethodCalled
            "notification.sent",        // DbusNotificationSent
            "device.connected",         // DbusDeviceConnected
            "device.disconnected",      // DbusDeviceDisconnected
            "media.state_changed",      // DbusMediaStateChanged
            "power.state_changed",      // DbusPowerStateChanged
            "network.state_changed",    // DbusNetworkStateChanged
            "bluetooth.device_changed", // DbusBluetoothDeviceChanged
            "mount.changed",            // DbusMountChanged
            
            // Clipboard events
            "copied",                   // ClipboardCopied
            "selected",                 // ClipboardSelected
            
            // System journal
            "entry.written",            // JournaldEntryWritten
        ];

        if !known_events.contains(&event_type) {
            warn!(
                "Unknown event type '{}' - this may be a custom or experimental event",
                event_type
            );
        }

        Ok(())
    }

    /// Validate event-specific configuration
    fn validate_event_config(&self, event_name: &str, config: &ConfigValue) -> Result<()> {
        // Map the event key format to the actual event type
        let actual_event_name = match event_name {
            "shell_command_executed_atuin" => "command.imported",  // Legacy mapping
            "command_imported" => "command.imported",              // New mapping
            "terminal_scrollback_captured" => "output.captured",   // Updated mapping
            "terminal_command_output_captured" => "output.captured", // Updated mapping
            "command_executed" => "command.executed",              // New mapping
            "command_completed" => "command.completed",            // New mapping
            "file_created" => "file.created",
            "file_modified" => "file.modified",
            "file_deleted" => "file.deleted",
            "file_moved" => "file.moved",                          // New mapping
            "dir_created" => "dir.created",                        // New mapping
            "dir_deleted" => "dir.deleted",                        // New mapping
            "clipboard_content_changed" => "copied",               // Updated mapping
            "clipboard_selection_changed" => "selected",           // Updated mapping
            "copied" => "copied",                                   // New mapping
            "selected" => "selected",                               // New mapping
            "recording_started" => "recording.started",            // New mapping
            "recording_ended" => "recording.ended",                // New mapping
            // Allow the actual event names as well
            other if other.contains('.') => other,
            _ => event_name,
        };

        match actual_event_name {
            "command.imported" => self.validate_atuin_config(config),
            "command.executed" | "command.completed" | "output.captured" => {
                self.validate_kitty_config(config)
            }
            "file.created" | "file.modified" | "file.deleted" | "file.moved" | "dir.created" | "dir.deleted" => {
                self.validate_filesystem_config(config)
            }
            "copied" | "selected" => {
                self.validate_clipboard_config(config)
            }
            "recording.started" | "recording.ended" => {
                self.validate_asciinema_config(config)
            }
            _ => {
                // For unknown events, just validate that it's a valid TOML table
                if !config.is_table() {
                    return Err(anyhow!("Event configuration must be a TOML table"));
                }
                Ok(())
            }
        }
    }

    /// Validate flat configuration keys
    fn validate_flat_config(&self, config_key: &str, config_value: &ConfigValue) -> Result<()> {
        // Validate common configuration patterns
        if config_key.starts_with("event.") {
            // Event-specific configurations are handled separately
            return Ok(());
        }

        match config_key {
            "output.database" => {
                if !config_value.is_bool() {
                    return Err(anyhow!("output.database must be a boolean"));
                }
            }
            "output.logging" => {
                if !config_value.is_bool() {
                    return Err(anyhow!("output.logging must be a boolean"));
                }
            }
            "logging.level" => {
                if let Some(level) = config_value.as_str() {
                    if !["trace", "debug", "info", "warn", "error"].contains(&level) {
                        return Err(anyhow!(
                            "logging.level must be one of: trace, debug, info, warn, error"
                        ));
                    }
                } else {
                    return Err(anyhow!("logging.level must be a string"));
                }
            }
            _ => {
                // Unknown config keys are allowed but logged
                info!("Unknown configuration key: {}", config_key);
            }
        }

        Ok(())
    }

    /// Validate Atuin-specific configuration
    fn validate_atuin_config(&self, config: &ConfigValue) -> Result<()> {
        ConfigValidator::new()
            .validate_path_format("db_path")
            .validate_positive("polling_interval_secs")
            .build()(config)
        .map_err(|e| anyhow!("{}", e))
    }

    /// Validate Kitty terminal configuration
    fn validate_kitty_config(&self, config: &ConfigValue) -> Result<()> {
        ConfigValidator::new()
            .validate_absolute_path("kitty_socket_path")
            .validate_range("max_scrollback_lines", 100..=1_000_000)
            .build()(config)
        .map_err(|e| anyhow!("{}", e))
    }

    /// Validate filesystem monitoring configuration
    fn validate_filesystem_config(&self, config: &ConfigValue) -> Result<()> {
        ConfigValidator::new()
            .validate_path_array("watch_patterns")
            .build()(config)
        .map_err(|e| anyhow!("{}", e))
    }

    /// Validate clipboard monitoring configuration
    fn validate_clipboard_config(&self, config: &ConfigValue) -> Result<()> {
        ConfigValidator::new()
            .validate_range("poll_interval_ms", 1..=60_000)
            .validate_range("max_history_entries", 1..=100_000)
            .build()(config)
        .map_err(|e| anyhow!("{}", e))
    }

    /// Validate asciinema recording configuration
    fn validate_asciinema_config(&self, config: &ConfigValue) -> Result<()> {
        ConfigValidator::new()
            .validate_path_format("recordings_dir")
            .validate_range("polling_interval_secs", 1..=300)
            .build()(config)
        .map_err(|e| anyhow!("{}", e))
    }

    /// Perform cross-validation checks
    pub fn cross_validate(&self) -> Result<()> {
        let mut errors = Vec::new();

        // Check for event type and configuration consistency
        for event_type in &self.enabled_events {
            let event_config = self.get_event_config(event_type);

            // Check that required configurations are present for enabled events
            match event_type.as_str() {
                "command.imported" => {
                    if event_config.get("db_path").is_none() {
                        errors.push(format!(
                            "Event '{}' is enabled but missing required 'db_path' configuration",
                            event_type
                        ));
                    }
                }
                "command.executed" | "command.completed" => {
                    if event_config.get("socket_path").is_none() {
                        errors.push(format!("Event '{}' is enabled but missing required 'socket_path' configuration", event_type));
                    }
                }
                "file.created" | "file.modified" | "file.deleted" | "file.moved" | "dir.created" | "dir.deleted" => {
                    if event_config.get("watch_patterns").is_none() {
                        errors.push(format!("Event '{}' is enabled but missing required 'watch_patterns' configuration", event_type));
                    }
                }
                "recording.started" | "recording.ended" => {
                    if event_config.get("recordings_dir").is_none() {
                        errors.push(format!("Event '{}' is enabled but missing required 'recordings_dir' configuration", event_type));
                    }
                }
                "output.captured" => {
                    // Scrollback capture should have git-annex configured for large content
                    if event_config.get("git_annex_repo").is_none() {
                        errors.push(format!("Event '{}' is enabled but missing recommended 'git_annex_repo' configuration", event_type));
                    }
                }
                _ => {} // No specific requirements for other event types
            }
        }

        if !errors.is_empty() {
            return Err(anyhow!(
                "Cross-validation failed:\n  - {}",
                errors.join("\n  - ")
            ));
        }

        Ok(())
    }

    /// Get validation report
    pub fn get_validation_report(&self) -> ValidationReport {
        let mut report = ValidationReport::new();

        // Basic validation
        if let Err(e) = self.validate() {
            report
                .errors
                .push(format!("Basic validation failed: {}", e));
        }

        // Cross-validation
        if let Err(e) = self.cross_validate() {
            report
                .errors
                .push(format!("Cross-validation failed: {}", e));
        }

        // Performance warnings
        if self.enabled_events.len() > 20 {
            report
                .warnings
                .push("Large number of enabled events may impact performance".to_string());
        }

        // Recommendations
        if self.annex_repo_path.is_none()
            && self
                .enabled_events
                .iter()
                .any(|e| e.contains("asciinema") || e.contains("scrollback"))
        {
            report.recommendations.push(
                "Consider configuring git-annex for efficient storage of terminal recordings"
                    .to_string(),
            );
        }

        report.valid = report.errors.is_empty();
        report
    }
}

/// Configuration validation report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub recommendations: Vec<String>,
}

impl ValidationReport {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ValidationReport {
    fn default() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            recommendations: Vec::new(),
        }
    }
}

impl ValidationReport {
    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
        self.valid = false;
    }

    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    pub fn add_recommendation(&mut self, recommendation: String) {
        self.recommendations.push(recommendation);
    }

    pub fn merge(&mut self, other: ValidationReport) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
        self.recommendations.extend(other.recommendations);
        self.valid = self.valid && other.valid;
    }

    pub fn is_empty(&self) -> bool {
        self.errors.is_empty() && self.warnings.is_empty() && self.recommendations.is_empty()
    }
}

/// Resolve path without home directory assumptions for system services
fn resolve_system_safe_path(default_path: &str, env_var: Option<&str>, fallback_dir: &str) -> String {
    // First try environment variable if provided
    if let Some(var_name) = env_var {
        if let Ok(path) = env::var(var_name) {
            return path;
        }
    }
    
    // If path starts with ~, resolve to system-safe alternatives
    if let Some(relative_path) = default_path.strip_prefix("~/") {
        // Remove ~/
        
        // Try XDG directories first (most appropriate for system services)
        if let Ok(data_dir) = env::var("XDG_DATA_HOME") {
            return format!("{}/{}", data_dir, relative_path);
        }
        
        // Try HOME as last resort
        if let Ok(home) = env::var("HOME") {
            warn!("Using HOME directory for system service - consider setting XDG_DATA_HOME");
            return format!("{}/.local/share/{}", home, relative_path);
        }
        
        // Fall back to /var/lib or /tmp for system services
        warn!("No HOME or XDG_DATA_HOME available, using fallback: {}/{}", fallback_dir, relative_path);
        return format!("{}/{}", fallback_dir, relative_path);
    }
    
    // Return path as-is if not home directory based
    default_path.to_string()
}

impl Default for CollectorConfig {
    fn default() -> Self {
        // Create a minimal default configuration that is valid
        let mut flat_config = HashMap::new();

        // Add default configurations for enabled events
        flat_config.insert(
            "event.files".to_string(),
            ConfigValue::Table({
                let mut table = toml::map::Map::new();
                table.insert(
                    "watch_patterns".to_string(),
                    ConfigValue::Array(vec![
                        ConfigValue::String(resolve_system_safe_path("~/Documents/**/*", Some("SINEX_DOCUMENTS_DIR"), "/var/lib/sinex/documents")),
                        ConfigValue::String(resolve_system_safe_path("~/Code/**/*", Some("SINEX_CODE_DIR"), "/var/lib/sinex/code")),
                    ]),
                );
                table.insert(
                    "ignore_patterns".to_string(),
                    ConfigValue::Array(vec![
                        ConfigValue::String("**/.git/**".to_string()),
                        ConfigValue::String("**/target/**".to_string()),
                        ConfigValue::String("**/node_modules/**".to_string()),
                    ]),
                );
                table
            }),
        );

        flat_config.insert(
            "event.command_imported".to_string(),  // Updated event name
            ConfigValue::Table({
                let mut table = toml::map::Map::new();
                table.insert(
                    "db_path".to_string(),
                    ConfigValue::String(resolve_system_safe_path("~/.local/share/atuin/history.db", Some("ATUIN_DB_PATH"), "/var/lib/sinex/atuin")),
                );
                table.insert(
                    "polling_interval_secs".to_string(),
                    ConfigValue::Integer(10),
                );
                table
            }),
        );

        flat_config.insert(
            "event.command_executed".to_string(),
            ConfigValue::Table({
                let mut table = toml::map::Map::new();
                table.insert(
                    "socket_path".to_string(),
                    ConfigValue::String(
                        env::var("KITTY_SOCKET_PATH")
                            .unwrap_or_else(|_| "/tmp/kitty".to_string())
                    ),
                );
                table.insert("poll_interval_seconds".to_string(), ConfigValue::Integer(2));  // Use correct Rust field name
                table.insert("enabled".to_string(), ConfigValue::Boolean(true));  // Add Rust field
                table
            }),
        );

        Self {
            enabled_events: vec![
                "file.created".to_string(),
                "file.modified".to_string(),
                "file.deleted".to_string(),
                "command.executed".to_string(),
                "command.imported".to_string(),  // Updated from shell.command.executed_atuin
                "window.focused".to_string(),
                "workspace.switched".to_string(),  // Updated from workspace.changed
            ],
            event: HashMap::new(),
            flat_config,
            annex_repo_path: None,
        }
    }
}

/// Merge two TOML values, with `update` overriding values in `base`
fn merge_toml(base: &mut ConfigValue, update: ConfigValue) {
    match (base, update) {
        (ConfigValue::Table(base_map), ConfigValue::Table(update_map)) => {
            for (k, v) in update_map {
                match base_map.get_mut(&k) {
                    Some(base_val) => merge_toml(base_val, v),
                    None => {
                        base_map.insert(k, v);
                    }
                }
            }
        }
        (base, update) => *base = update,
    }
}

/// Configuration manager with hot-reload support
pub struct ConfigManager {
    config: Arc<RwLock<CollectorConfig>>,
    config_path: Option<PathBuf>,
    update_tx: Option<mpsc::Sender<CollectorConfig>>,
}

impl ConfigManager {
    /// Create a new config manager
    pub fn new(config: CollectorConfig, config_path: Option<PathBuf>) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            config_path,
            update_tx: None,
        }
    }

    /// Get the current configuration
    pub async fn get_config(&self) -> CollectorConfig {
        self.config.read().await.clone()
    }

    /// Start watching for configuration changes
    pub async fn start_watching(&mut self) -> Result<mpsc::Receiver<CollectorConfig>> {
        let (update_tx, update_rx) = mpsc::channel(10);

        if let Some(config_path) = &self.config_path {
            let path = config_path.clone();
            let config = Arc::clone(&self.config);
            let tx = update_tx.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::watch_config_file(path, config, tx).await {
                    error!("Config watching failed: {}", e);
                }
            });
        }

        self.update_tx = Some(update_tx);
        Ok(update_rx)
    }

    async fn watch_config_file(
        config_path: PathBuf,
        config: Arc<RwLock<CollectorConfig>>,
        update_tx: mpsc::Sender<CollectorConfig>,
    ) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(100);

        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                if matches!(event.kind, EventKind::Modify(_)) {
                    let _ = tx.blocking_send(event);
                }
            }
        })?;

        watcher.watch(&config_path, RecursiveMode::NonRecursive)?;
        info!("Watching config file: {:?}", config_path);

        while let Some(_event) = rx.recv().await {
            // Debounce rapid changes
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            match CollectorConfig::load_from_file(&config_path) {
                Ok(new_config) => {
                    info!("Configuration reloaded from file");

                    // Update the stored config
                    {
                        let mut config_guard = config.write().await;
                        *config_guard = new_config.clone();
                    }

                    // Notify listeners
                    if let Err(e) = update_tx.send(new_config).await {
                        warn!("Failed to send config update: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to reload configuration: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Manually update the configuration
    pub async fn update_config(&self, new_config: CollectorConfig) -> Result<()> {
        {
            let mut config_guard = self.config.write().await;
            *config_guard = new_config.clone();
        }

        if let Some(tx) = &self.update_tx {
            tx.send(new_config)
                .await
                .context("Failed to send config update")?;
        }

        Ok(())
    }

    /// Save current configuration to file
    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        let config = self.get_config().await;
        let content = toml::to_string_pretty(&config)?;
        tokio::fs::write(path, content).await?;
        info!("Configuration saved to: {:?}", path);
        Ok(())
    }
}
