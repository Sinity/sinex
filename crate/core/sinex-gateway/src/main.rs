//! Sinex Gateway - Unified API gateway for CLI and browser extension
//!
//! This binary provides two modes:
//! - RPC Server: JSON-RPC over TLS for CLI
//! - Native Messaging: stdin/stdout protocol for browser extensions

mod build {
    include!(concat!(env!("OUT_DIR"), "/shadow.rs"));
}

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;
use tracing::info;

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_gateway::rpc_server::DEFAULT_TCP_LISTEN;
use sinex_gateway::service_container::ServiceContainer;
use sinex_gateway::{native_messaging, rpc_server};

#[derive(Parser)]
#[command(name = "sinex-gateway")]
#[command(about = "Unified API gateway for Sinex")]
#[command(version = build::CLAP_LONG_VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start RPC server for CLI communication
    RpcServer {
        /// TCP listen address in host:port form (or via `SINEX_GATEWAY_TCP_LISTEN`)
        #[arg(long, env = "SINEX_GATEWAY_TCP_LISTEN", default_value = DEFAULT_TCP_LISTEN)]
        tcp_listen: String,

        /// Database URL
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,

        /// Allowed CORS origins (comma-separated). If not set, only localhost is allowed.
        #[arg(long, env = "SINEX_GATEWAY_CORS_ORIGINS")]
        cors_origins: Option<String>,
    },

    /// Start native messaging mode for browser extension
    NativeMessaging {
        /// Database URL
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
    },
}

/// Initialize tracing subscriber for the gateway
fn setup_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sinex_host=info".into()),
        )
        .try_init()
        .map_err(|e| color_eyre::eyre::eyre!("Failed to initialize tracing: {}", e))
}

#[tokio::main]
async fn main() -> Result<()> {
    human_panic::setup_panic!();
    color_eyre::install()?;
    setup_tracing()?;

    let cli = Cli::parse();

    // Issue 128: Set up graceful shutdown signal handling
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let shutdown_task = {
        let shutdown_tx = shutdown_tx.clone();
        #[allow(clippy::expect_used)] // Fatal: signal handlers must be installable
        tokio::spawn(async move {
            let ctrl_c = async {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install Ctrl+C handler");
            };

            #[cfg(unix)]
            let terminate = async {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler")
                    .recv()
                    .await;
            };

            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                () = ctrl_c => {
                    info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                },
                () = terminate => {
                    info!("Received SIGTERM, initiating graceful shutdown");
                },
            }

            let _ = shutdown_tx.send(true);
        })
    };

    match cli.command {
        Commands::RpcServer {
            tcp_listen,
            database_url,
            cors_origins,
        } => {
            info!("Starting RPC server on {}", tcp_listen);

            // Initialize service container
            let services = ServiceContainer::new(database_url).await.map_err(|e| {
                color_eyre::eyre::eyre!("Failed to initialize services").wrap_err(e)
            })?;

            // Parse CORS origins
            let origins: Vec<String> = cors_origins
                .map(|s| s.split(',').map(|o| o.trim().to_string()).collect())
                .unwrap_or_default();

            // Start RPC server with shutdown signal
            let result = rpc_server::run(Some(tcp_listen.as_str()), services, origins, shutdown_rx)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("RPC server failed").wrap_err(e));

            // Clean up shutdown task
            shutdown_task.abort();
            result?;
        }

        Commands::NativeMessaging { database_url } => {
            info!("Starting native messaging mode");

            // Initialize service container
            let services = ServiceContainer::new(database_url).await.map_err(|e| {
                color_eyre::eyre::eyre!("Failed to initialize services").wrap_err(e)
            })?;

            // Start native messaging loop with shutdown signal
            let result = native_messaging::run(services, shutdown_rx)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Native messaging failed").wrap_err(e));

            // Clean up shutdown task
            shutdown_task.abort();
            result?;
        }
    }

    Ok(())
}
