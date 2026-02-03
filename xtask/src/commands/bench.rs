pub use crate::bench::BenchConfig as BenchArgs;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use anyhow::Result;

#[async_trait::async_trait]
impl XtaskCommand for BenchArgs {
    fn name(&self) -> &'static str {
        "bench"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        crate::bench::run(self.clone())?;
        Ok(CommandResult::success())
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("test".to_string()),
            timeout: None,
            modifies_state: false,
            track_in_history: true,
        }
    }
}
