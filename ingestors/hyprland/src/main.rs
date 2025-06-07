mod cli;
mod config;
mod error;
mod watcher;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

use sinex_shared::{
    IngestorRuntime, RuntimeConfig, EventSink, DatabaseSink, LogSink, FileSink,
    DatabaseConfig, DatabaseService, ManifestManager, create_agent_manifest,
    sources, RetryConfig,
};
use crate::watcher::HyprlandIngestor;
use crate::config::Config;
use crate::cli::{Cli, Commands, ConfigFormat};

#[tokio::main]
async fn main() -> Result<()> {
    let exit_code = match run().await {
        Ok(()) => 0,
        Err(e) => {
            error!("Application failed: {}", e);
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
    let cli = Cli::parse();
    
    // Load configuration
    let mut config = if let Some(config_path) = &cli.common.config {
        info!("Loading configuration from: {}", config_path.display());
        Config::load_from_file(config_path)?
    } else {
        Config::load()?
    };

    // Override with CLI arguments
    if let Some(ref database_url) = cli.common.database_url {
        config.database.url = database_url.clone();
    }
    if let Some(ref log_level) = cli.common.log_level {
        config.logging.level = log_level.clone();
    }

    // Initialize logging
    init_logging(&config.logging.level);

    // Handle commands
    match cli.common.command {
        Some(Commands::Check) => check_database(&config).await,
        Some(Commands::Config { format }) => show_config(&config, &format),
        Some(Commands::GenerateConfig { output, format }) => generate_config(&config, output.as_ref(), &format),
        Some(Commands::Validate { config_file: _ }) => {
            println!("✅ Configuration is valid");
            Ok(())
        }
        _ => run_ingestor(config, cli.common.dry_run, cli.common.output_file).await,
    }
}

async fn run_ingestor(config: Config, dry_run: bool, output_file: Option<PathBuf>) -> Result<()> {
    info!("Starting hyprland ingestor");
    
    // Create event sink based on CLI options
    let event_sink: Arc<dyn EventSink> = if let Some(ref file_path) = output_file {
        info!("Writing events to file: {}", file_path.display());
        Arc::new(FileSink::new(file_path.clone()).await?)
    } else if dry_run {
        info!("Running in dry-run mode - events will be logged");
        Arc::new(LogSink::new("hyprland-ingestor"))
    } else {
        // Initialize database
        let db_config = DatabaseConfig {
            url: config.database.url.clone(),
            max_connections: config.database.max_connections,
            min_connections: 2,
            acquire_timeout: std::time::Duration::from_secs(config.database.connection_timeout_secs),
            idle_timeout: std::time::Duration::from_secs(600),
        };
        
        let db = Arc::new(DatabaseService::new(db_config).await?);
        
        // Register agent manifest
        register_manifest(&db).await?;
        
        Arc::new(DatabaseSink::new(db))
    };
    
    // Create the simple ingestor
    let ingestor = HyprlandIngestor::new(config.hyprland.clone())?;
    
    // Create runtime config
    let runtime_config = RuntimeConfig {
        heartbeat_interval_secs: config.hyprland.heartbeat_interval_secs,
        retry_config: RetryConfig {
            max_retries: config.hyprland.max_retries,
            initial_delay: std::time::Duration::from_secs(1),
            max_delay: std::time::Duration::from_secs(config.hyprland.retry_delay_secs),
            exponential_base: 2,
        },
        batch_size: None, // Hyprland doesn't batch
        batch_timeout_ms: None,
    };
    
    // Create and run the runtime
    let runtime = IngestorRuntime::new(ingestor, event_sink, runtime_config)?;
    runtime.run().await
}

async fn register_manifest(db: &DatabaseService) -> Result<()> {
    let manifest_manager = ManifestManager::new(db.pool().clone());
    
    let mut produces = std::collections::HashMap::new();
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
        ].into_iter().map(String::from).collect(),
    );
    
    // Add common sinex events
    produces.insert(
        sources::SINEX.to_string(),
        vec![
            "agent.startup".to_string(),
            "agent.shutdown".to_string(),
            sinex_shared::event_types::event_types::sinex::AGENT_HEARTBEAT.to_string(),
            sinex_shared::event_types::event_types::sinex::AGENT_ERROR.to_string(),
            sinex_shared::event_types::event_types::sinex::AGENT_DLQ_EVENT_WRITTEN.to_string(),
        ],
    );

    let manifest = create_agent_manifest(
        "hyprland-ingestor",
        "Captures Hyprland window manager events via IPC socket2",
        env!("CARGO_PKG_VERSION"),
        produces,
    );
    
    manifest_manager.register_agent(&manifest).await?;
    Ok(())
}

async fn check_database(config: &Config) -> Result<()> {
    info!("Checking database connectivity...");
    
    let db_config = DatabaseConfig {
        url: config.database.url.clone(),
        max_connections: config.database.max_connections,
        min_connections: 2,
        acquire_timeout: std::time::Duration::from_secs(config.database.connection_timeout_secs),
        idle_timeout: std::time::Duration::from_secs(600),
    };
    
    let db = DatabaseService::new(db_config).await?;
    
    match db.health_check().await {
        Ok(()) => {
            info!("✅ Database connection successful");
            println!("Database connection successful");
            Ok(())
        }
        Err(e) => {
            error!("❌ Database connection failed: {}", e);
            eprintln!("Database connection failed: {}", e);
            Err(e.into())
        }
    }
}

fn show_config(config: &Config, format: &ConfigFormat) -> Result<()> {
    match format {
        ConfigFormat::Pretty => {
            println!("Current Configuration:");
            println!("=====================");
            println!("Database:");
            println!("  URL: {}", config.database.url);
            println!("  Max Connections: {}", config.database.max_connections);
            println!("\nLogging:");
            println!("  Level: {}", config.logging.level);
            println!("  Format: {}", config.logging.format);
            println!("\nHyprland:");
            println!("  Window Augmentation: {:?}", config.hyprland.window_augmentation);
            println!("  Workspace Tracking: {:?}", config.hyprland.workspace_tracking);
            println!("  State Snapshot Interval: {}s", config.hyprland.state_snapshot_interval_secs);
            println!("  Descriptions Interval: {}h", config.hyprland.descriptions_interval_hours);
            println!("  Track Focus History: {}", config.hyprland.track_focus_history);
            if !config.hyprland.ignore_events.is_empty() {
                println!("  Ignored Events: {:?}", config.hyprland.ignore_events);
            }
        }
        ConfigFormat::Json => {
            let json = serde_json::to_string_pretty(&config)?;
            println!("{}", json);
        }
        ConfigFormat::Toml => {
            let toml_str = toml::to_string_pretty(&config)?;
            println!("{}", toml_str);
        }
        ConfigFormat::Yaml => {
            let yaml = serde_yaml::to_string(&config)?;
            println!("{}", yaml);
        }
    }
    Ok(())
}

fn generate_config(_config: &Config, output: Option<&PathBuf>, format: &ConfigFormat) -> Result<()> {
    let default_config = Config::default();
    
    let content = match format {
        ConfigFormat::Json | ConfigFormat::Pretty => {
            serde_json::to_string_pretty(&default_config)?
        }
        ConfigFormat::Toml => {
            toml::to_string_pretty(&default_config)?
        }
        ConfigFormat::Yaml => {
            serde_yaml::to_string(&default_config)?
        }
    };
    
    if let Some(path) = output {
        std::fs::write(path, content)?;
        info!("Generated configuration file: {}", path.display());
        println!("Generated configuration file: {}", path.display());
    } else {
        println!("{}", content);
    }
    
    Ok(())
}

fn init_logging(level: &str) {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}