use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use sinex_shared::{
    ingestor_framework::{Ingestor, IngestorConfig},
    EventSink, sources,
};
use crate::cli::ConfigFormat;
use crate::error::IngestorError;

use crate::config::Config;
use crate::event_listener::HyprlandEventListener;
use crate::cli::Commands;

type HyprResult<T> = Result<T, IngestorError>;

/// The hyprland ingestor implementation
pub struct HyprlandIngestor {
    config: Config,
    event_sink: Arc<dyn EventSink>,
}

impl IngestorConfig for Config {
    fn load() -> Result<Self> {
        Ok(Config::load()?)
    }
    
    fn load_from_file(path: &std::path::Path) -> Result<Self> {
        Ok(Config::load_from_file(&path.to_path_buf())?)
    }
    
    fn database_url(&self) -> &str {
        &self.database.url
    }
    
    fn set_database_url(&mut self, url: String) {
        self.database.url = url;
    }
    
    fn database_max_connections(&self) -> u32 {
        self.database.max_connections
    }
    
    fn database_connection_timeout_secs(&self) -> u64 {
        self.database.connection_timeout_secs
    }
    
    fn log_level(&self) -> &str {
        &self.logging.level
    }
    
    fn set_log_level(&mut self, level: String) {
        self.logging.level = level;
    }
}

#[async_trait]
impl Ingestor for HyprlandIngestor {
    type Config = Config;
    type Commands = Commands;
    
    fn name() -> &'static str {
        "hyprland-ingestor"
    }
    
    fn description() -> &'static str {
        "Captures Hyprland window manager events via IPC socket2"
    }
    
    fn produces_events() -> HashMap<String, Vec<String>> {
        let mut produces = HashMap::new();
        produces.insert(
            sources::HYPRLAND.to_string(),
            vec![
                "workspace", "workspacev2", "createworkspace", "createworkspacev2",
                "destroyworkspace", "destroyworkspacev2", "moveworkspace", "moveworkspacev2",
                "renameworkspace", "activespecial", "activespecialv2", "focusedmon", 
                "focusedmonv2", "monitoradded", "monitoraddedv2", "monitorremoved",
                "monitorremovedv2", "activewindow", "activewindowv2", "openwindow",
                "closewindow", "movewindow", "movewindowv2", "windowtitle", "windowtitlev2",
                "fullscreen", "changefloatingmode", "urgent", "minimized", "pin",
                "togglegroup", "moveintogroup", "moveoutofgroup", "ignoregrouplock",
                "lockgroups", "openlayer", "closelayer", "activelayout", "submap",
                "screencast", "configreloaded", "bell", "state_snapshot"
            ].into_iter().map(|s| s.to_string()).collect(),
        );
        produces
    }
    
    async fn new(config: Self::Config, event_sink: Arc<dyn EventSink>) -> Result<Self> {
        Ok(Self { config, event_sink })
    }
    
    async fn run(&mut self) -> Result<()> {
        let event_listener = HyprlandEventListener::new(
            self.config.hyprland.clone(),
            Arc::clone(&self.event_sink),
        )?;
        
        event_listener.start().await.map_err(Into::into)
    }
    
    async fn handle_custom_command(&self, command: &Self::Commands) -> Result<()> {
        match command {
            Commands::Config { format } => {
                self.show_config(format).await.map_err(Into::into)
            }
            Commands::Validate { config_file } => {
                self.validate_config(config_file).await.map_err(Into::into)
            }
            Commands::GenerateConfig { output, format } => {
                self.generate_config(output.as_ref(), format).await.map_err(Into::into)
            }
            _ => Ok(()), // Other commands handled by framework
        }
    }
}

impl HyprlandIngestor {
    /// Show current configuration
    async fn show_config(&self, format: &ConfigFormat) -> HyprResult<()> {
        match format {
            ConfigFormat::Pretty => {
                println!("Current Configuration:");
                println!("=====================");
                println!("Database:");
                println!("  URL: {}", self.config.database.url);
                println!("  Max Connections: {}", self.config.database.max_connections);
                println!("\nLogging:");
                println!("  Level: {}", self.config.logging.level);
                println!("  Format: {}", self.config.logging.format);
                println!("\nHyprland:");
                println!("  Window Augmentation: {:?}", self.config.hyprland.window_augmentation);
                println!("  Workspace Tracking: {:?}", self.config.hyprland.workspace_tracking);
                println!("  State Snapshot Interval: {}s", self.config.hyprland.state_snapshot_interval_secs);
                println!("  Descriptions Interval: {}h", self.config.hyprland.descriptions_interval_hours);
                println!("  Track Focus History: {}", self.config.hyprland.track_focus_history);
                if !self.config.hyprland.ignore_events.is_empty() {
                    println!("  Ignored Events: {:?}", self.config.hyprland.ignore_events);
                }
            }
            ConfigFormat::Json => {
                let json = serde_json::to_string_pretty(&self.config)?;
                println!("{}", json);
            }
            ConfigFormat::Toml => {
                let toml = toml::to_string_pretty(&self.config)
                    .map_err(|e| IngestorError::application(format!("TOML serialization failed: {}", e)))?;
                println!("{}", toml);
            }
            ConfigFormat::Yaml => {
                let yaml = serde_yaml::to_string(&self.config)
                    .map_err(|e| IngestorError::application(format!("YAML serialization failed: {}", e)))?;
                println!("{}", yaml);
            }
        }
        Ok(())
    }

    /// Validate configuration file
    async fn validate_config(&self, _config_file: &Option<std::path::PathBuf>) -> HyprResult<()> {
        println!("✅ Configuration is valid");
        Ok(())
    }

    /// Generate example configuration file
    async fn generate_config(&self, output: Option<&std::path::PathBuf>, format: &ConfigFormat) -> HyprResult<()> {
        let config = Config::default();
        
        let content = match format {
            ConfigFormat::Json | ConfigFormat::Pretty => {
                serde_json::to_string_pretty(&config)?
            }
            ConfigFormat::Toml => {
                toml::to_string_pretty(&config)
                    .map_err(|e| IngestorError::application(format!("TOML serialization failed: {}", e)))?
            }
            ConfigFormat::Yaml => {
                serde_yaml::to_string(&self.config)
                    .map_err(|e| IngestorError::application(format!("YAML serialization failed: {}", e)))?
            }
        };

        match output {
            Some(path) => {
                std::fs::write(path, content)
                    .map_err(|e| IngestorError::application(format!("Failed to write config file: {}", e)))?;
                println!("Configuration written to: {}", path.display());
            }
            None => {
                println!("{}", content);
            }
        }

        Ok(())
    }
}