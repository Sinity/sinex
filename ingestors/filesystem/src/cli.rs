use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Filesystem Activity Ingestor for Sinex
#[derive(Parser)]
#[command(
    name = "filesystem-ingestor",
    about = "Monitors filesystem activity and captures file operations",
    version = env!("CARGO_PKG_VERSION"),
    author = "Sinity"
)]
pub struct Cli {
    /// Configuration file path
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Database URL (overrides config file)
    #[arg(long, value_name = "URL")]
    pub database_url: Option<String>,

    /// Log level (overrides config file)
    #[arg(long, value_name = "LEVEL")]
    pub log_level: Option<String>,

    /// Subcommand
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the event ingestor (default)
    Run,

    /// Check database connection
    Check,

    /// Display current configuration
    Config,

    /// Generate example configuration file
    GenerateConfig {
        /// Output file path
        #[arg(short, long, value_name = "FILE")]
        output: Option<PathBuf>,
    },
}

impl Default for Commands {
    fn default() -> Self {
        Commands::Run
    }
}

impl Cli {
    /// Parse command line arguments
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}