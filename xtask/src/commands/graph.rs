//! Dependency graph visualization - promoted from analyze graph

use anyhow::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Graph visualization command (promoted from analyze graph)
#[derive(Debug, Clone, clap::Args)]
pub struct GraphCommand {
    #[command(subcommand)]
    pub subcommand: crate::graph::GraphCommand,
}

impl XtaskCommand for GraphCommand {
    fn name(&self) -> &str {
        "graph"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        self.subcommand.run(ctx)
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
