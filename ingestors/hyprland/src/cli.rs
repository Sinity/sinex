use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Hyprland ingestor CLI
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

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Run the event ingestor (default)
    Run,

    /// Check database and Hyprland connections
    Check,

    /// Display current configuration
    Config {
        /// Output format
        #[arg(long, default_value = "pretty")]
        format: ConfigFormat,
    },

    /// Validate configuration file
    Validate {
        /// Configuration file to validate
        #[arg(value_name = "FILE")]
        config_file: Option<PathBuf>,
    },

    /// Generate example configuration file
    GenerateConfig {
        /// Output file path
        #[arg(short, long, value_name = "FILE")]
        output: Option<PathBuf>,

        /// Output format
        #[arg(long, default_value = "toml")]
        format: ConfigFormat,
    },
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum ConfigFormat {
    Pretty,
    Json,
    Toml,
    Yaml,
}