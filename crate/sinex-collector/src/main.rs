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
    
    /// Validate configuration and exit
    #[arg(long)]
    validate_config: bool,
}



#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Initialize logging - timestamps will be in local time by default
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
    
    // If validating config only, do that and exit
    if args.validate_config {
        info!("Validating configuration...");
        
        let report = config.get_validation_report();
        
        println!("=== Configuration Validation Report ===");
        println!("Configuration file: {}", args.config.as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "default locations".to_string()));
        println!("Valid: {}", if report.valid { "✓" } else { "✗" });
        println!("Enabled Events: {}", config.enabled_events.len());
        println!();
        
        if !report.errors.is_empty() {
            println!("❌ ERRORS:");
            for error in &report.errors {
                println!("  - {}", error);
            }
            println!();
        }
        
        if !report.warnings.is_empty() {
            println!("⚠️  WARNINGS:");
            for warning in &report.warnings {
                println!("  - {}", warning);
            }
            println!();
        }
        
        if !report.recommendations.is_empty() {
            println!("💡 RECOMMENDATIONS:");
            for rec in &report.recommendations {
                println!("  - {}", rec);
            }
            println!();
        }
        
        if report.valid {
            println!("✅ Configuration is valid");
            std::process::exit(0);
        } else {
            println!("❌ Configuration validation failed");
            std::process::exit(1);
        }
    }
    
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
    let mut collector = UnifiedCollector::new(config, output_config, db_pool.clone(), validator);
    
    // Spawn heartbeat emission task if database is available
    if let Some(ref pool) = db_pool {
        let heartbeat_pool = pool.clone();
        tokio::spawn(async move {
            use sinex_core::HeartbeatEmitter;
            let emitter = HeartbeatEmitter::new(heartbeat_pool, "unified-collector".to_string(), 30);
            emitter.run().await;
        });
        info!("Started heartbeat emission for unified-collector");
    }
    
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