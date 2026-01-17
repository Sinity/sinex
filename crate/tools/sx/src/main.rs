//! sx - Sinex development orchestrator
//!
//! Provides hot reload, state continuity, and prompt-to-node workflow
//! for developing SimpleProcessor nodes.

mod dev;
mod watcher;

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Sinex development orchestrator
#[derive(Parser)]
#[command(name = "sx", version, about)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a processor in development mode with hot reload
    Dev(dev::DevArgs),

    /// Build a processor
    Build {
        /// Path to the processor crate
        #[arg(default_value = ".")]
        path: String,

        /// Release mode
        #[arg(long)]
        release: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();

    // Set up logging
    let filter = if cli.verbose {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"))
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .init();

    match cli.command {
        Commands::Dev(args) => dev::run(args).await,
        Commands::Build { path, release } => {
            info!("Building processor at {} (release: {})", path, release);
            // TODO: Implement build command
            Ok(())
        }
    }
}
