use clap::Parser;
use color_eyre::eyre::Result;
use sinex_core::types::domain::SanitizedPath;
use sinex_ingestd::{IngestService, IngestdConfig};
use std::str::FromStr;
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Validate and parse socket path for ingestd gRPC server
pub fn validate_socket_path(s: &str) -> Result<SanitizedPath, String> {
    if s.is_empty() {
        return Err("Socket path cannot be empty".to_string());
    }
    SanitizedPath::from_str(s)
}

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

    /// NATS URL for message bus
    #[arg(long, env = "SINEX_NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Unix Domain Socket path for gRPC server
    #[arg(long, default_value = "/run/sinex/ingest.sock", value_parser = validate_socket_path)]
    socket_path: SanitizedPath,

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
    color_eyre::install()?;
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
        args.nats_url,
        args.socket_path.into_string(),
        args.pool_size,
        args.batch_size,
        args.batch_timeout_secs,
        args.dry_run,
    )?;

    if args.validate_config {
        config.validate_and_exit().await;
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
