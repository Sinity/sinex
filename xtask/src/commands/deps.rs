//! Dependency analysis command - promoted from analyze deps

use anyhow::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Dependency analysis command (promoted from analyze deps)
#[derive(Debug, Clone, clap::Args)]
pub struct DepsCommand {
    #[command(subcommand)]
    pub subcommand: crate::deps::DepsCommand,
}

impl XtaskCommand for DepsCommand {
    fn name(&self) -> &str {
        "deps"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        self.subcommand.run(ctx)
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
