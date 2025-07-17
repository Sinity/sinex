//! Sinex Gateway - Unified API gateway for CLI and browser extension
//!
//! This binary provides two modes:
//! - RPC Server: JSON-RPC over Unix socket for CLI
//! - Native Messaging: stdin/stdout protocol for browser extensions

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;

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
        socket: PathBuf,

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

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sinex_host=info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::RpcServer {
            socket,
            database_url,
        } => {
            info!("Starting RPC server on {:?}", socket);

            // Initialize service container
            let services = ServiceContainer::new(database_url).await?;

            // Start RPC server
            rpc_server::run(socket, services).await?;
        }

        Commands::NativeMessaging { database_url } => {
            info!("Starting native messaging mode");

            // Initialize service container
            let services = ServiceContainer::new(database_url).await?;

            // Start native messaging loop
            native_messaging::run(services).await?;
        }
    }

    Ok(())
}
