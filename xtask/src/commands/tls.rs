use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
pub use crate::tls::TlsCommand;
use anyhow::Result;

#[async_trait::async_trait]
impl XtaskCommand for TlsCommand {
    fn name(&self) -> &'static str {
        "tls"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        crate::tls::run(self.clone(), !ctx.is_human())
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::default()
    }
}
