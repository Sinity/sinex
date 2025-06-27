use anyhow::{anyhow, Context, Result};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use sinex_core::{ConfigValidator, ConfigValue};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
        // Try standard locations
        let paths = vec![
            Some(PathBuf::from("sinex-collector.toml")),
            Some(PathBuf::from("unified-collector.toml")), // Legacy compatibility
            dirs::config_dir().map(|mut p| {
                p.push("sinex/collector.toml");
                p
            }),
            Some(PathBuf::from("/etc/sinex/collector.toml")),
        ];

        for path_opt in paths {
            if let Some(path) = path_opt {
                if path.exists() {
                    let config = Self::load_from_file(&path)?;
                    if validate {
                        config.validate()?;
                    }
                    return Ok(config);
                }
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

        // Check against known event types
        let known_events = [
            "shell.command.executed_atuin",
            "shell.history.command",
            "terminal.asciinema.session_started",
            "terminal.asciinema.session_ended",
            "terminal.scrollback.captured",
            "terminal.command_output.captured",
            "file.created",
            "file.modified",
            "file.deleted",
            "dbus.signal",
            "dbus.method_call",
            "system.notification",
            "media.playback.changed",
            "system.power.event",
            "hardware.device.event",
            "session.state.changed",
            "security.policykit.authorization",
            "bluetooth.device.event",
            "network.connection.event",
            "screen.saver.event",
            "storage.mount.event",
            "clipboard.content.changed",
            "clipboard.selection.changed",
            "window.focused",
            "window.opened",
            "window.closed",
            "workspace.changed",
            "command.executed",
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
            "shell_command_executed_atuin" => "shell.command.executed_atuin",
            "terminal_scrollback_captured" => "terminal.scrollback.captured",
            "terminal_command_output_captured" => "terminal.command_output.captured",
            "file_created" => "file.created",
            "file_modified" => "file.modified",
            "file_deleted" => "file.deleted",
            "clipboard_content_changed" => "clipboard.content.changed",
            "clipboard_selection_changed" => "clipboard.selection.changed",
            // Allow the actual event names as well
            other if other.contains('.') => other,
            _ => event_name,
        };

        match actual_event_name {
            "shell.command.executed_atuin" => self.validate_atuin_config(config),
            "terminal.scrollback.captured" | "terminal.command_output.captured" => {
                self.validate_kitty_config(config)
            }
            "file.created" | "file.modified" | "file.deleted" => {
                self.validate_filesystem_config(config)
            }
            "clipboard.content.changed" | "clipboard.selection.changed" => {
                self.validate_clipboard_config(config)
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

    /// Perform cross-validation checks
    pub fn cross_validate(&self) -> Result<()> {
        let mut errors = Vec::new();

        // Check for event type and configuration consistency
        for event_type in &self.enabled_events {
            let event_config = self.get_event_config(event_type);

            // Check that required configurations are present for enabled events
            match event_type.as_str() {
                "shell.command.executed_atuin" => {
                    if event_config.get("db_path").is_none() {
                        errors.push(format!(
                            "Event '{}' is enabled but missing required 'db_path' configuration",
                            event_type
                        ));
                    }
                }
                "terminal.scrollback.captured" | "terminal.command_output.captured" => {
                    if event_config.get("kitty_socket_path").is_none() {
                        errors.push(format!("Event '{}' is enabled but missing required 'kitty_socket_path' configuration", event_type));
                    }
                }
                "file.created" | "file.modified" | "file.deleted" => {
                    if event_config.get("watch_patterns").is_none() {
                        errors.push(format!("Event '{}' is enabled but missing required 'watch_patterns' configuration", event_type));
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
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            recommendations: Vec::new(),
        }
    }

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
                        ConfigValue::String("~/Documents/**/*".to_string()),
                        ConfigValue::String("~/Code/**/*".to_string()),
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
            "event.shell_command_executed_atuin".to_string(),
            ConfigValue::Table({
                let mut table = toml::map::Map::new();
                table.insert(
                    "db_path".to_string(),
                    ConfigValue::String("~/.local/share/atuin/history.db".to_string()),
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
                    ConfigValue::String("/tmp/kitty".to_string()),
                );
                table.insert("polling_interval_secs".to_string(), ConfigValue::Integer(2));
                table
            }),
        );

        Self {
            enabled_events: vec![
                "file.created".to_string(),
                "file.modified".to_string(),
                "file.deleted".to_string(),
                "command.executed".to_string(),
                "shell.command.executed_atuin".to_string(),
                "window.focused".to_string(),
                "workspace.changed".to_string(),
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
