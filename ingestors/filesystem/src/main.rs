mod cli;
mod config;
mod filesystem_watcher;

use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

use sinex_shared::{
    create_agent_manifest, DatabaseConfig as SharedDbConfig, DatabaseService, ManifestManager,
    sources, event_types,
};

use crate::cli::{Cli, Commands};
use crate::config::Config;
use crate::filesystem_watcher::FilesystemWatcher;

#[tokio::main]
async fn main() {
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

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load configuration
    let mut config = if let Some(config_path) = &cli.config {
        info!("Loading configuration from: {}", config_path.display());
        Config::load_from_file(config_path)?
    } else {
        Config::load()?
    };

    // Override config with CLI arguments
    if let Some(ref database_url) = cli.database_url {
        config.database.url = database_url.clone();
    }

    if let Some(ref log_level) = cli.log_level {
        config.logging.level = log_level.clone();
    }

    // Initialize logging
    init_logging(&config.logging)?;
    info!("Starting Filesystem Ingestor v{}", env!("CARGO_PKG_VERSION"));

    // Convert to shared database config
    let db_config = SharedDbConfig {
        url: config.database.url.clone(),
        max_connections: config.database.max_connections,
        min_connections: 2,
        acquire_timeout: std::time::Duration::from_secs(config.database.connection_timeout_secs),
        idle_timeout: std::time::Duration::from_secs(600),
    };

    // Initialize database
    let db_service = Arc::new(DatabaseService::new(db_config).await?);

    // Execute command
    match cli.command {
        Commands::Run => {
            run_ingestor(config, db_service).await
        }
        Commands::Check => check_database(&db_service).await,
        Commands::Config => show_config(&config),
        Commands::GenerateConfig { output } => {
            generate_config(output.as_ref())
        }
    }
}

async fn run_ingestor(
    config: Config,
    db: Arc<DatabaseService>,
) -> anyhow::Result<()> {

    // Register agent manifest
    let manifest_manager = ManifestManager::new(db.pool().clone());
    
    let mut produces = HashMap::new();
    produces.insert(
        sources::FILESYSTEM.to_string(),
        vec![
            event_types::filesystem::FILE_CREATED.to_string(),
            event_types::filesystem::FILE_MODIFIED.to_string(),
            event_types::filesystem::FILE_DELETED.to_string(),
            event_types::filesystem::FILE_RENAMED.to_string(),
        ],
    );
    produces.insert(
        sources::SINEX.to_string(),
        vec![
            event_types::sinex::AGENT_HEARTBEAT.to_string(),
            event_types::sinex::AGENT_ERROR.to_string(),
            event_types::sinex::AGENT_DLQ_EVENT_WRITTEN.to_string(),
        ],
    );

    let manifest = create_agent_manifest(
        "filesystem-ingestor",
        "Monitors filesystem activity and captures file operations",
        env!("CARGO_PKG_VERSION"),
        produces,
    );
    
    manifest_manager.register_agent(&manifest).await?;
    info!("Registered agent manifest");

    // Create and start filesystem watcher
    let watcher = FilesystemWatcher::new(config.filesystem, db)?;
    watcher.start().await?;

    Ok(())
}

async fn check_database(db: &DatabaseService) -> anyhow::Result<()> {
    info!("Checking database connection...");
    
    match db.health_check().await {
        Ok(()) => {
            println!("✅ Database connection: OK");
            Ok(())
        }
        Err(e) => {
            println!("❌ Database check failed: {}", e);
            Err(e.into())
        }
    }
}

fn show_config(config: &Config) -> anyhow::Result<()> {
    println!("Current Configuration:");
    println!("=====================");
    println!("Database URL: {}", config.database.url);
    println!("Max Connections: {}", config.database.max_connections);
    println!("Log Level: {}", config.logging.level);
    println!("Log Format: {}", config.logging.format);
    println!("Watch Directories:");
    for dir in &config.filesystem.watch_directories {
        println!("  - {}", dir.display());
    }
    println!("Exclude Patterns:");
    for pattern in &config.filesystem.exclude_patterns {
        println!("  - {}", pattern);
    }
    println!("Include Patterns:");
    for pattern in &config.filesystem.include_patterns {
        println!("  - {}", pattern);
    }
    println!("Debounce: {}ms", config.filesystem.debounce_ms);
    println!("Batch Size: {} events", config.filesystem.batch_size_events);
    println!("Batch Timeout: {}ms", config.filesystem.batch_timeout_ms);
    println!("Hash Files: {}", config.filesystem.hash_files);
    println!("Heartbeat Interval: {}s", config.filesystem.heartbeat_interval_secs);
    Ok(())
}

fn generate_config(output: Option<&std::path::PathBuf>) -> anyhow::Result<()> {
    let config = Config::default();
    let toml = toml::to_string_pretty(&config)?;

    match output {
        Some(path) => {
            std::fs::write(path, toml)?;
            println!("Configuration written to: {}", path.display());
        }
        None => {
            println!("{}", toml);
        }
    }

    Ok(())
}

// Helper function for logging initialization
fn init_logging(config: &crate::config::LoggingConfig) -> anyhow::Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.level));

    let fmt_layer = match config.format.as_str() {
        "json" => fmt::layer()
            .json()
            .with_target(true)
            .with_thread_ids(true)
            .with_file(config.include_location)
            .with_line_number(config.include_location)
            .boxed(),
        _ => fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .with_file(config.include_location)
            .with_line_number(config.include_location)
            .boxed(),
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();

    Ok(())
}