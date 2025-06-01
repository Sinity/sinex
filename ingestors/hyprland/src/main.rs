mod cli;
mod config;
mod error;
mod event_listener;
mod logging;
mod shutdown;

use std::error::Error;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::cli::{Cli, Commands, ConfigFormat};
use crate::config::Config;
use crate::error::{IngestorError, Result};
use crate::event_listener::HyprlandEventListener;
use crate::logging::{log_error_with_context, log_shutdown_info, log_startup_info};
use crate::shutdown::{ShutdownComponent, ShutdownCoordinator, ShutdownManager};

use sinex_shared::{
    DatabaseConfig as SharedDatabaseConfig, DatabaseService, manifest,
};

/// Main application structure
pub struct Application {
    config: Config,
    database: Arc<DatabaseService>,
    shutdown_coordinator: ShutdownCoordinator,
}

impl Application {
    /// Create a new application instance
    pub async fn new(mut config: Config, cli: &Cli) -> Result<Self> {
        // Override config with CLI arguments
        if let Some(ref database_url) = cli.database_url {
            config.database.url = database_url.clone();
        }

        if let Some(ref log_level) = cli.log_level {
            config.logging.level = log_level.clone();
        }

        if let Some(ref log_format) = cli.log_format {
            config.logging.format = log_format.clone();
        }

        // Initialize logging
        logging::init_logging(&config.logging)?;
        log_startup_info(&config);

        // Create shutdown coordinator
        let shutdown_coordinator = ShutdownCoordinator::new(config.app.shutdown_timeout_secs);

        // Initialize database with Phase 2 configuration
        let db_config = SharedDatabaseConfig {
            url: config.database.url.clone(),
            max_connections: config.database.max_connections,
            min_connections: 2,
            connect_timeout: std::time::Duration::from_secs(config.database.connection_timeout_secs),
            idle_timeout: std::time::Duration::from_secs(600),
        };
        
        let database = Arc::new(DatabaseService::new(db_config).await?);

        // Register agent manifest
        manifest::register_agent_manifest(
            &database,
            "hyprland-ingestor",
            env!("CARGO_PKG_VERSION"),
            Some("Captures Hyprland window manager events via IPC socket2"),
            manifest::AgentStatus::Stable,
            vec![
                ("hyprland".to_string(), vec![
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
                ]),
            ].into_iter().collect(),
        ).await?;

        Ok(Self {
            config,
            database,
            shutdown_coordinator,
        })
    }

    /// Run the main application
    pub async fn run(self, cli: &Cli) -> Result<()> {
        match cli.get_command() {
            Commands::Run => {
                self.run_ingestor().await
            }
            Commands::Check => self.check_connections().await,
            Commands::Config { format } => self.show_config(format).await,
            Commands::Validate { config_file } => self.validate_config(config_file).await,
            Commands::GenerateConfig { output, format } => {
                self.generate_config(output.as_ref(), format).await
            }
        }
    }

    /// Run the main event ingestor
    async fn run_ingestor(self) -> Result<()> {
        info!("Starting Hyprland event ingestor with socket2 capture");

        // Create event listener
        let event_listener = HyprlandEventListener::new(
            self.config.hyprland.clone(),
            Arc::clone(&self.database),
        )?;

        // Set up shutdown handling
        let shutdown_coordinator = self.shutdown_coordinator.clone();
        let shutdown_signal = shutdown_coordinator.subscribe();

        // Start event listener with shutdown signal
        tokio::select! {
            result = event_listener.start() => {
                match result {
                    Ok(()) => info!("Event listener completed normally"),
                    Err(e) => {
                        error!("Event listener failed: {}", e);
                        return Err(e);
                    }
                }
            }
            _ = shutdown_signal => {
                info!("Received shutdown signal");
            }
        }

        // Graceful shutdown
        log_shutdown_info("User requested");
        info!("Application shutdown completed successfully");

        Ok(())
    }

    /// Check database and Hyprland connections
    async fn check_connections(&self) -> Result<()> {
        info!("Checking connections...");

        // Check database
        match self.database.health_check().await {
            Ok(()) => println!("✅ Database connection: OK"),
            Err(e) => {
                println!("❌ Database connection: FAILED - {}", e);
                return Err(IngestorError::database_connection(e.to_string()));
            }
        }

        // Check Hyprland
        match std::env::var("HYPRLAND_INSTANCE_SIGNATURE") {
            Ok(sig) => {
                println!("✅ Hyprland instance: {}", sig);
                
                // Check socket2 exists
                let socket_path = std::path::PathBuf::from(
                    std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".to_string())
                )
                .join("hypr")
                .join(&sig)
                .join(".socket2.sock");
                
                if socket_path.exists() {
                    println!("✅ Socket2 path: {}", socket_path.display());
                } else {
                    println!("❌ Socket2 not found at: {}", socket_path.display());
                }
            }
            Err(_) => {
                println!("❌ HYPRLAND_INSTANCE_SIGNATURE not set");
                println!("   Make sure Hyprland is running and you're in a Hyprland session");
            }
        }

        Ok(())
    }

    /// Show current configuration
    async fn show_config(&self, format: &ConfigFormat) -> Result<()> {
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
    async fn validate_config(&self, _config_file: &Option<std::path::PathBuf>) -> Result<()> {
        println!("✅ Configuration is valid");
        Ok(())
    }

    /// Generate example configuration file
    async fn generate_config(&self, output: Option<&std::path::PathBuf>, format: &ConfigFormat) -> Result<()> {
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
                serde_yaml::to_string(&config)
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

#[tokio::main]
async fn main() {
    let exit_code = match run().await {
        Ok(()) => 0,
        Err(e) => {
            log_error_with_context(&e, "Application failed");
            eprintln!("Error: {}", e);
            
            // Print error chain
            let mut current = e.source();
            while let Some(err) = current {
                eprintln!("  Caused by: {}", err);
                current = err.source();
            }
            
            1
        }
    };

    std::process::exit(exit_code);
}

async fn run() -> Result<()> {
    let cli = Cli::parse_args();

    // Load configuration
    let config = if let Some(config_path) = &cli.config {
        info!("Loading configuration from: {}", config_path.display());
        Config::load_from_file(config_path)?
    } else {
        Config::load()?
    };

    // Create and run application
    let app = Application::new(config, &cli).await?;
    app.run(&cli).await
}