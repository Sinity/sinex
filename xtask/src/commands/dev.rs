//! Developer utilities (reduced).

use anyhow::Result;
use clap::Subcommand;
use std::path::PathBuf;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Developer utilities command variants
#[derive(Subcommand, Debug, Clone)]
pub enum DevSubcommand {
    /// Run a sinex binary with hot reload and lazy-start
    Run {
        binary: String,
        #[arg(long)]
        release: bool,
        #[arg(long)]
        no_watch: bool,
        #[arg(long)]
        tether: Option<String>,
        #[arg(long)]
        checkpoint: Option<PathBuf>,
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Build a processor crate
    Build {
        #[arg(default_value = ".")]
        path: String,
        #[arg(long)]
        release: bool,
    },
    /// Generate a SimpleProcessor from a natural language spec
    Generate {
        spec: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        dry_run: bool,
        #[arg(long, default_value = ".")]
        workspace: String,
    },
}

/// Developer utilities command
pub struct DevCommand {
    pub subcommand: DevSubcommand,
}

impl XtaskCommand for DevCommand {
    fn name(&self) -> &str {
        "dev"
    }

    fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        Ok(CommandResult::success()
            .with_message("Dev logic retained (placeholder for refactor step)"))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
