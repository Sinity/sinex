use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
pub use crate::tls::TlsCommand;
use anyhow::Result;

impl XtaskCommand for TlsCommand {
    fn name(&self) -> &'static str {
        "tls"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        crate::tls::run(self.clone(), !ctx.is_human())
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::default()
    }
}
