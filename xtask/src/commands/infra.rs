use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use anyhow::Result;

#[derive(Debug, Clone, clap::Args)]
pub struct InfraCommand {
    #[command(subcommand)]
    pub subcommand: InfraSubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum InfraSubcommand {
    /// Manage secrets
    Secrets,
    /// Apply terraform/tofu changes
    Apply,
}

impl XtaskCommand for InfraCommand {
    fn name(&self) -> &str {
        "infra"
    }

    fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        println!("Infra command not fully implemented yet.");
        Ok(CommandResult::success())
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::default()
    }
}
