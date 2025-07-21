use anyhow::Result;
use clap::Parser;
use sinex_ingestd::{IngestService, IngestdConfig};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Sinex ingestion daemon - central hub for event ingestion"
)]
struct Args {

    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Redis URL for message bus
    #[arg(
        long,
        env = "SINEX_REDIS_URL",
        default_value = "redis://localhost:6379"
    )]
    redis_url: String,

    /// Unix Domain Socket path for gRPC server
    #[arg(long, default_value = "/run/sinex/ingest.sock")]
    socket_path: String,

    /// Database connection pool size
    #[arg(long, default_value = "50")]
    pool_size: u32,

    /// Batch size for database writes
    #[arg(long, default_value = "1000")]
    batch_size: usize,

    /// Batch timeout in seconds
    #[arg(long, default_value = "5")]
    batch_timeout_secs: u64,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Enable dry-run mode (log events but don't persist)
    #[arg(long)]
    dry_run: bool,

    /// Validate configuration and exit
    #[arg(long)]
    validate_config: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("Starting Sinex Ingestion Daemon");

    // Load configuration from environment and command line arguments
    let config = IngestdConfig::from_args(
        args.database_url,
        args.redis_url,
        args.socket_path,
        args.pool_size,
        args.batch_size,
        args.batch_timeout_secs,
        args.dry_run,
    )?;

    if args.validate_config {
        info!("Validating configuration...");
        match config.validate().await {
            Ok(()) => {
                info!("✅ Configuration is valid");
                std::process::exit(0);
            }
            Err(e) => {
                error!("❌ Configuration validation failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    info!(?config, "Configuration loaded");

    // Create and run the service
    let mut service = IngestService::new(config).await?;

    // Set up graceful shutdown
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        info!("Received shutdown signal");
    };

    // Run the service
    tokio::select! {
        result = service.run() => {
            match result {
                Ok(()) => info!("Service completed successfully"),
                Err(e) => {
                    error!("Service failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
        _ = shutdown_signal => {
            info!("Shutting down gracefully...");
            if let Err(e) = service.shutdown().await {
                error!("Error during shutdown: {}", e);
            }
        }
    }

    info!("Sinex Ingestion Daemon stopped");
    Ok(())
}
