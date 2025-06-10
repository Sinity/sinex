mod collector;
mod config;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use sinex_shared::{
    IngestorRuntime, RuntimeConfig, EventSink, DatabaseSink, LogSink,
    DatabaseConfig, DatabaseService
};
use tracing::info;

use crate::collector::UnifiedCollector;
use crate::config::UnifiedConfig;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, env = "UNIFIED_CONFIG")]
    config: Option<std::path::PathBuf>,
    
    /// Run in dry-run mode (no database writes)
    #[arg(long)]
    dry_run: bool,
    
    /// Log level
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .init();
    
    info!("Starting Unified Collector");
    
    // Load configuration
    let config = if let Some(path) = args.config {
        UnifiedConfig::load_from_file(&path)?
    } else {
        UnifiedConfig::load()?
    };
    
    // Create event sink based on mode
    let event_sink: Arc<dyn EventSink> = if args.dry_run {
        info!("Running in dry-run mode - events will be logged");
        Arc::new(LogSink::new("unified-collector"))
    } else {
        // Initialize database
        let db_config = DatabaseConfig {
            url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string()),
            max_connections: 10,
            min_connections: 2,
            acquire_timeout: std::time::Duration::from_secs(10),
            idle_timeout: std::time::Duration::from_secs(600),
        };
        
        let db = Arc::new(DatabaseService::new(db_config).await?);
        Arc::new(DatabaseSink::new(db))
    };
    
    // Create the collector
    let collector = UnifiedCollector::new(config.clone());
    
    // Create runtime config
    let runtime_config = RuntimeConfig {
        heartbeat_interval_secs: 60,
        ..Default::default()
    };
    
    // Create and run the runtime
    let runtime = IngestorRuntime::new(collector, event_sink, runtime_config)?;
    
    info!(
        enabled_events = ?config.enabled_events,
        "Unified Collector initialized"
    );
    
    runtime.run().await?;
    
    Ok(())
}