use anyhow::Result;
use clap::Parser;
use sinex_collector::{CollectorConfig, OutputConfig, UnifiedCollector};
use sinex_db::{create_pool, validation::EventValidator};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, warn, error};

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
    
    // Set up graceful shutdown
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    
    // Spawn heartbeat emission task if database is available
    let heartbeat_handle = if let Some(ref pool) = db_pool {
        let heartbeat_pool = pool.clone();
        let heartbeat_shutdown = shutdown.clone();
        let handle = tokio::spawn(async move {
            use sinex_core::HeartbeatEmitter;
            let emitter = HeartbeatEmitter::new(heartbeat_pool, "unified-collector".to_string(), 30);
            
            // Create an interval for checking shutdown
            let mut shutdown_check = tokio::time::interval(tokio::time::Duration::from_secs(1));
            
            // Run heartbeat loop with shutdown check
            tokio::select! {
                _ = emitter.run() => {
                    warn!("Heartbeat emitter stopped unexpectedly");
                }
                _ = async {
                    loop {
                        shutdown_check.tick().await;
                        if heartbeat_shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                } => {
                    info!("Heartbeat emitter shutting down gracefully");
                }
            }
        });
        
        info!("Started heartbeat emission for unified-collector");
        Some(handle)
    } else {
        None
    };
    
    // Notify systemd that we're ready
    match sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
        Ok(_) => info!("Notified systemd: ready"),
        Err(e) => info!("Running without systemd integration: {}", e),
    }
    
    // Set up signal handlers
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    
    // Run until shutdown signal
    tokio::select! {
        result = collector.run() => {
            if let Err(e) = result {
                error!("Collector failed: {}", e);
                
                // Notify systemd of failure
                let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Status("Failed".into())]);
                
                // Signal shutdown to other tasks
                shutdown.store(true, Ordering::Relaxed);
                return Err(e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM, initiating graceful shutdown");
        }
    }
    
    // Notify systemd we're stopping
    let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Stopping]);
    
    // Signal shutdown to all tasks
    shutdown_clone.store(true, Ordering::Relaxed);
    
    // Give tasks time to complete gracefully
    info!("Waiting for tasks to complete...");
    
    // Wait for heartbeat task to complete
    if let Some(handle) = heartbeat_handle {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), handle).await {
            Ok(Ok(_)) => info!("Heartbeat task completed"),
            Ok(Err(e)) => warn!("Heartbeat task failed: {}", e),
            Err(_) => warn!("Heartbeat task timed out"),
        }
    }
    
    // Give collector time to flush any pending events
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    
    info!("Collector shutdown complete");
    Ok(())
}