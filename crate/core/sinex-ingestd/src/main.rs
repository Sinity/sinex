mod build {
    include!(concat!(env!("OUT_DIR"), "/shadow.rs"));
}

use clap::Parser;
use color_eyre::eyre::Result;
use sinex_ingestd::{IngestService, IngestdConfig};
use std::io;
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(
    author,
    version = build::CLAP_LONG_VERSION,
    about = "Sinex ingestion daemon - central hub for event ingestion"
)]
struct Args {
    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// NATS URL for message bus
    #[arg(long, env = "SINEX_NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,
    /// Require TLS for NATS connections (enforces tls:// or wss://)
    #[arg(long, env = "SINEX_NATS_REQUIRE_TLS")]
    nats_require_tls: bool,

    /// Database connection pool size
    #[arg(long, env = "SINEX_INGESTD_POOL_SIZE", default_value = "50")]
    pool_size: u32,

    /// JetStream pull batch max messages
    #[arg(long, env = "SINEX_INGESTD_CONSUMER_FETCH_MAX_MESSAGES")]
    consumer_fetch_max_messages: Option<usize>,

    /// JetStream pull batch timeout in milliseconds
    #[arg(long, env = "SINEX_INGESTD_CONSUMER_FETCH_TIMEOUT_MS")]
    consumer_fetch_timeout_ms: Option<u64>,

    /// JetStream max_ack_pending for the main consumer
    #[arg(long, env = "SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING")]
    consumer_max_ack_pending: Option<i64>,

    /// JetStream max_ack_pending for the material slices consumer
    #[arg(long, env = "SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING")]
    material_slices_max_ack_pending: Option<i64>,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Enable dry-run mode (log events but don't persist)
    #[arg(long)]
    dry_run: bool,

    /// Validate configuration and exit
    #[arg(long)]
    validate_config: bool,

    /// Path to the git-annex repository for material storage
    #[arg(long, env = "SINEX_ANNEX_PATH")]
    annex_path: Option<String>,

    /// Directory used to persist assembler state between restarts
    #[arg(long, env = "SINEX_ASSEMBLER_STATE_DIR")]
    assembler_state_dir: Option<String>,

    /// NATS namespace for subject/stream isolation (used by test infrastructure)
    #[arg(long, env = "SINEX_NAMESPACE")]
    namespace: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    human_panic::setup_panic!();
    color_eyre::install()?;
    let args = Args::parse();

    // Initialize logging — explicitly write to stderr (fmt() defaults to stdout in 0.3.x).
    // RUST_LOG takes precedence over --log-level so operators can adjust verbosity at runtime
    // without restarting with a different flag (consistent with how the gateway behaves).
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level));
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("Starting Sinex Ingestion Daemon");

    // Load configuration from environment and command line arguments
    let config = IngestdConfig::from_args(
        args.database_url,
        args.nats_url,
        args.nats_require_tls,
        args.pool_size,
        args.consumer_fetch_max_messages,
        args.consumer_fetch_timeout_ms,
        args.consumer_max_ack_pending,
        args.material_slices_max_ack_pending,
        args.dry_run,
        args.annex_path,
        args.assembler_state_dir,
        args.namespace,
    );

    if args.validate_config {
        config.validate_and_exit().await;
    }

    info!(?config, "Configuration loaded");

    // Create and run the service
    let mut service = IngestService::new(config).await?;

    // Set up graceful shutdown
    let shutdown_signal = async {
        if let Err(err) = wait_for_shutdown_signal().await {
            error!("Failed to listen for shutdown signal: {}", err);
        } else {
            info!("Received shutdown signal");
        }
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
        () = shutdown_signal => {
            info!("Shutting down gracefully...");
            if let Err(e) = service.shutdown().await {
                error!("Error during shutdown: {}", e);
            }
        }
    }

    info!("Sinex Ingestion Daemon stopped");
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    tokio::select! {
        _ = sigterm.recv() => Ok(()),
        _ = sigint.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}
