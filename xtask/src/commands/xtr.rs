//! xtr command - umbrella for rarely-used utilities
//!
//! Groups infrequently-used commands to reduce top-level clutter:
//! - patterns: AST-grep code pattern search
//! - ci: CI pipeline commands
//! - completions: Shell completion generation

use anyhow::Result;
use clap::Subcommand;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Rarely-used utilities (patterns, ci, completions)
#[derive(Debug, Clone, clap::Args)]
pub struct XtrCommand {
    #[command(subcommand)]
    pub subcommand: XtrSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum XtrSubcommand {
    /// Code pattern search using ast-grep
    Patterns(super::patterns::PatternsCommand),
    /// CI pipeline commands
    Ci(super::ci::CiCommand),
    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: super::completions::Shell,
    },
}

impl XtaskCommand for XtrCommand {
    fn name(&self) -> &str {
        "xtr"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            XtrSubcommand::Patterns(cmd) => cmd.execute(ctx),
            XtrSubcommand::Ci(cmd) => cmd.execute(ctx),
            XtrSubcommand::Completions { shell } => {
                use clap::CommandFactory;
                // Get the CLI command for completions
                let cmd = crate::Cli::command();
                super::completions::CompletionsCommand::generate_completions(*shell, cmd)?;
                Ok(CommandResult::success())
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::default()
    }
}
