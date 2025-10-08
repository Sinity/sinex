//! Sinex Gateway - Unified API gateway for CLI and browser extension
//!
//! This binary provides two modes:
//! - RPC Server: JSON-RPC over Unix socket for CLI
//! - Native Messaging: stdin/stdout protocol for browser extensions

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;
use sinex_core::SanitizedPath;
use std::str::FromStr;
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

/// Validate and parse socket path for RPC server
pub fn validate_socket_path(s: &str) -> Result<SanitizedPath, String> {
    if s.is_empty() {
        return Err("Socket path cannot be empty".to_string());
    }
    SanitizedPath::from_str(s)
}

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
        /// Socket path (default; set SINEX_GATEWAY_HOST to bind TCP)
        #[arg(long, default_value = "/tmp/sinex-host.sock", value_parser = validate_socket_path)]
        socket: SanitizedPath,

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
