use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Hyprland Event Ingestor for Sinex
#[derive(Parser)]
#[command(
    name = "hyprland-ingestor",
    about = "Captures Hyprland window manager events via IPC socket2 and stores them in the Sinex database",
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

    /// Log format: pretty or json
    #[arg(long, value_name = "FORMAT")]
    pub log_format: Option<String>,

    /// Subcommand
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
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

impl Default for Commands {
    fn default() -> Self {
        Commands::Run
    }
}

impl Cli {
    /// Parse command line arguments
    pub fn parse_args() -> Self {
        Cli::parse()
    }

    /// Get the command to execute, with default
    pub fn get_command(&self) -> &Commands {
        self.command.as_ref().unwrap_or(&Commands::Run)
    }
}