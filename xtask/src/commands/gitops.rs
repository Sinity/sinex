//! GitOps command - wrapper around sinexctl gitops
//!
//! Provides developer convenience for managing GitOps schema sources.

use color_eyre::eyre::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// GitOps schema source management
#[derive(Debug, Clone, clap::Args)]
pub struct GitOpsCommand {
    #[clap(allow_hyphen_values = true, trailing_var_arg = true)]
    /// Arguments passed to sinexctl gitops
    args: Vec<String>,
}

#[async_trait::async_trait]
impl XtaskCommand for GitOpsCommand {
    fn name(&self) -> &'static str {
        "gitops"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("========== sinexctl gitops ==========");
        }

        // Use the pre-built sinexctl binary directly instead of `cargo run --package sinexctl`
        // which would recompile the entire dependency graph (~30s) on every invocation.
        let stage = ctx.start_stage("gitops");
        let result = ProcessBuilder::new("sinexctl")
            .arg("gitops")
            .args(self.args.iter().map(String::as_str).collect::<Vec<_>>())
            .with_description("sinexctl gitops")
            .inherit_output()
            .run_ok();
        ctx.finish_stage(stage, result.is_ok());
        result?;

        Ok(CommandResult::success().with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
