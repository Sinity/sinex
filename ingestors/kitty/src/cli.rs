use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Kitty ingestor CLI
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub common: CommonArgs,
}

/// Common arguments for all ingestors
#[derive(Parser, Debug)]
pub struct CommonArgs {
    /// Path to configuration file
    #[arg(short, long, env = "SINEX_CONFIG")]
    pub config: Option<PathBuf>,

    /// Override database URL from config
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Override log level from config
    #[arg(long, env = "RUST_LOG")]
    pub log_level: Option<String>,
    
    /// Run in dry-run mode (log events instead of inserting to database)
    #[arg(long)]
    pub dry_run: bool,
    
    /// Write events to file (implies dry-run)
    #[arg(long)]
    pub output_file: Option<PathBuf>,

    /// Command to run
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Kitty-specific commands
#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Run the ingestor (default)
    Run,
    /// Check database connectivity
    Check,
    /// Show current configuration
    Config,
    /// Generate example configuration file
    GenerateConfig {
        /// Output file path (stdout if not specified)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}