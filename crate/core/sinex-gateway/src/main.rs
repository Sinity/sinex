//! Sinex Gateway - Unified API gateway for CLI and browser extension
//!
//! This binary provides two modes:
//! - RPC Server: JSON-RPC over Unix socket for CLI
//! - Native Messaging: stdin/stdout protocol for browser extensions

use camino::Utf8PathBuf;
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
mod rpc_server;
mod service_container;

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
        /// Socket path (currently unused, binds to 127.0.0.1:9999)
        #[arg(long, default_value = "/tmp/sinex-host.sock")]
        socket: Utf8PathBuf,

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
            socket,
            database_url,
        } => {
            info!("Starting RPC server on {:?}", socket);

            // Initialize service container
            let services = ServiceContainer::new(database_url).await.map_err(|e| {
                color_eyre::eyre::eyre!("Failed to initialize services").wrap_err(e)
            })?;

            // Start RPC server
            rpc_server::run(socket, services)
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
