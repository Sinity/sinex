//! Analyze command - codebase analysis tools.

use anyhow::Result;
use clap::Subcommand;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Analyze command - codebase insight.
pub struct AnalyzeCommand {
    pub subcommand: AnalyzeSubcommand,
}

#[derive(Subcommand)]
pub enum AnalyzeSubcommand {
    /// Check dependencies
    Deps {
        #[command(subcommand)]
        command: crate::deps::DepsCommand,
    },
    /// Visualize graph
    Graph {
        #[command(subcommand)]
        command: crate::graph::GraphCommand,
    },
    /// Build/Test history
    History(crate::commands::history::HistoryCommand),
}

impl XtaskCommand for AnalyzeCommand {
    fn name(&self) -> &str {
        "analyze"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            AnalyzeSubcommand::Deps { command } => {
                command.run(ctx)?;
                Ok(CommandResult::success().with_message("Dependency analysis complete"))
            }
            AnalyzeSubcommand::Graph { command } => {
                command.run(ctx)?;
                Ok(CommandResult::success().with_message("Graph visualization complete"))
            }
            AnalyzeSubcommand::History(cmd) => cmd.execute(ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
