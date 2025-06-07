mod cli;
mod config;
mod watcher;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

use sinex_shared::{
    IngestorRuntime, RuntimeConfig, EventSink, DatabaseSink, LogSink, FileSink,
    DatabaseConfig, DatabaseService, ManifestManager, create_agent_manifest,
    sources, event_type_constants,
};
use crate::watcher::FilesystemIngestor;
use crate::config::Config;
use crate::cli::Cli;

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
        Some(cli::Commands::Check) => check_database(&config).await,
        Some(cli::Commands::Config) => show_config(&config),
        Some(cli::Commands::GenerateConfig { output }) => generate_config(output.as_ref()),
        _ => run_ingestor(config, cli.common.dry_run, cli.common.output_file).await,
    }
}

async fn run_ingestor(config: Config, dry_run: bool, output_file: Option<PathBuf>) -> Result<()> {
    info!("Starting filesystem ingestor");
    
    // Create event sink based on CLI options
    let event_sink: Arc<dyn EventSink> = if let Some(ref file_path) = output_file {
        info!("Writing events to file: {}", file_path.display());
        Arc::new(FileSink::new(file_path.clone()).await?)
    } else if dry_run {
        info!("Running in dry-run mode - events will be logged");
        Arc::new(LogSink::new("filesystem-ingestor"))
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
    let ingestor = FilesystemIngestor::new(config.filesystem.clone());
    
    // Create runtime config
    let runtime_config = RuntimeConfig {
        heartbeat_interval_secs: config.filesystem.heartbeat_interval_secs,
        batch_size: Some(config.filesystem.batch_size_events),
        batch_timeout_ms: Some(config.filesystem.batch_timeout_ms),
        retry_config: sinex_shared::RetryConfig {
            max_retries: config.filesystem.max_retries,
            initial_delay: std::time::Duration::from_secs(1),
            max_delay: std::time::Duration::from_secs(config.filesystem.retry_delay_secs),
            exponential_base: 2,
        },
    };
    
    // Create and run the runtime
    let runtime = IngestorRuntime::new(ingestor, event_sink, runtime_config)?;
    runtime.run().await
}

async fn register_manifest(db: &DatabaseService) -> Result<()> {
    let manifest_manager = ManifestManager::new(db.pool().clone());
    
    let mut produces = std::collections::HashMap::new();
    produces.insert(
        sources::FILESYSTEM.to_string(),
        vec![
            event_type_constants::filesystem::FILE_CREATED.to_string(),
            event_type_constants::filesystem::FILE_MODIFIED.to_string(),
            event_type_constants::filesystem::FILE_DELETED.to_string(),
            event_type_constants::filesystem::FILE_RENAMED.to_string(),
        ],
    );
    
    // Add common sinex events
    produces.insert(
        sources::SINEX.to_string(),
        vec![
            event_type_constants::sinex::AGENT_HEARTBEAT.to_string(),
            event_type_constants::sinex::AGENT_ERROR.to_string(),
            event_type_constants::sinex::AGENT_DLQ_EVENT_WRITTEN.to_string(),
        ],
    );

    let manifest = create_agent_manifest(
        "filesystem-ingestor",
        "Monitors filesystem changes and ingests file events",
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

fn show_config(config: &Config) -> Result<()> {
    let config_json = serde_json::to_string_pretty(&config)?;
    println!("{}", config_json);
    Ok(())
}

fn generate_config(output: Option<&PathBuf>) -> Result<()> {
    let default_config = Config::default();
    let config_str = toml::to_string_pretty(&default_config)?;
    
    if let Some(path) = output {
        std::fs::write(path, config_str)?;
        info!("Generated configuration file: {}", path.display());
        println!("Generated configuration file: {}", path.display());
    } else {
        println!("{}", config_str);
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