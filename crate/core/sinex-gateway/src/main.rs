//! Sinex Gateway - Unified API gateway for CLI and browser extension
//!
//! This binary provides two modes:
//! - RPC Server: JSON-RPC over TLS for CLI
//! - Native Messaging: stdin/stdout protocol for browser extensions

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;
use tracing::info;

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod handlers;
mod native_messaging;
mod replay_control;
mod replay_state_machine;
mod rpc_server;
mod service_container;

use crate::rpc_server::DEFAULT_TCP_LISTEN;
use service_container::ServiceContainer;

#[derive(Parser)]
#[command(name = "sinex-gateway")]
#[command(about = "Unified API gateway for Sinex")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start RPC server for CLI communication
    RpcServer {
        /// TCP listen address in host:port form (or via SINEX_GATEWAY_TCP_LISTEN)
        #[arg(long, env = "SINEX_GATEWAY_TCP_LISTEN", default_value = DEFAULT_TCP_LISTEN)]
        tcp_listen: String,

        /// Database URL
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
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
    color_eyre::install()?;
    setup_tracing()?;

    let cli = Cli::parse();

    match cli.command {
        Commands::RpcServer {
            tcp_listen,
            database_url,
        } => {
            info!("Starting RPC server on {}", tcp_listen);

            // Initialize service container
            let services = ServiceContainer::new(database_url).await.map_err(|e| {
                color_eyre::eyre::eyre!("Failed to initialize services").wrap_err(e)
            })?;

            // Start RPC server
            rpc_server::run(Some(tcp_listen.as_str()), services)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("RPC server failed").wrap_err(e))?;
        }

        Commands::NativeMessaging { database_url } => {
            info!("Starting native messaging mode");

            // Initialize service container
            let services = ServiceContainer::new(database_url).await.map_err(|e| {
                color_eyre::eyre::eyre!("Failed to initialize services").wrap_err(e)
            })?;

            // Start native messaging loop
            native_messaging::run(services)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Native messaging failed").wrap_err(e))?;
        }
    }

    Ok(())
}
