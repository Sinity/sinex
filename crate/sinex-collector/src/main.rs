use anyhow::Result;
use clap::Parser;
use sinex_collector::{CollectorConfig, OutputConfig, UnifiedCollector};
use sinex_db::{create_pool, validation::EventValidator};
use std::path::PathBuf;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about = "Sinex unified event collector")]
struct Args {
    /// Configuration file path
    #[arg(short, long, env = "SINEX_CONFIG")]
    config: Option<PathBuf>,
    
    /// Run in dry-run mode (no database writes, just log events)
    #[arg(long)]
    dry_run: bool,
    
    /// Skip database entirely (useful with --event-log for file-only mode)
    #[arg(long)]
    no_db: bool,
    
    /// Write events to file (one JSON object per line)
    #[arg(long)]
    event_log: Option<PathBuf>,
    
    /// Verbose logging (log all events to console)
    #[arg(short, long)]
    verbose: bool,
    
    /// Port for metrics server (default: 9090)
    #[arg(long, default_value = "9090")]
    metrics_port: u16,
    
    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
}



#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .init();
    
    info!("Starting Sinex Collector");
    
    // Load configuration
    let config = if let Some(path) = &args.config {
        CollectorConfig::load_from_file(path)?
    } else {
        CollectorConfig::load()?
    };
    
    info!(?config.enabled_events, "Configuration loaded");
    
    // Create output configuration
    let output_config = OutputConfig::new(
        !args.no_db,
        args.verbose,
        args.event_log.as_ref().map(|p| p.to_string_lossy().to_string()),
        args.dry_run,
    );
    
    // Initialize database if needed
    let (db_pool, validator) = if !args.no_db {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
        
        let pool = create_pool(&database_url).await?;
        let validator = if !args.dry_run {
            Some(EventValidator::load_from_db(&pool).await.unwrap_or_else(|e| {
                warn!("Failed to load schemas from database, using hardcoded rules: {}", e);
                EventValidator::new()
            }))
        } else {
            None
        };
        
        (Some(pool), validator)
    } else {
        info!("Database disabled, running in file/log-only mode");
        (None, None)
    };
    
    // Create and run the collector
    let mut collector = UnifiedCollector::new(config, output_config, db_pool, validator);
    
    // Run until shutdown signal
    tokio::select! {
        result = collector.run() => {
            if let Err(e) = result {
                tracing::error!("Collector failed: {}", e);
                return Err(e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }
    
    info!("Collector shutdown complete");
    Ok(())
}