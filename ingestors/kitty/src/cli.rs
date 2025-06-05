use clap::Subcommand;
use sinex_shared::ingestor_framework::CommonCommands;

/// Kitty-specific commands (we're just using the common ones)
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
        output: Option<std::path::PathBuf>,
    },
}

impl From<Commands> for CommonCommands {
    fn from(cmd: Commands) -> Self {
        match cmd {
            Commands::Run => CommonCommands::Run,
            Commands::Check => CommonCommands::Check,
            Commands::Config => CommonCommands::Config,
            Commands::GenerateConfig { output } => CommonCommands::GenerateConfig { output },
        }
    }
}