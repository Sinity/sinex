//! GitOps command - wrapper around sinexctl gitops
//!
//! Provides developer convenience for managing GitOps schema sources.

use color_eyre::eyre::{bail, Result, WrapErr};
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

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
        let mut cmd = Command::new("cargo");
        cmd.arg("run")
            .arg("--quiet")
            .arg("--package")
            .arg("sinexctl")
            .arg("--")
            .arg("gitops")
            .args(&self.args);

        if ctx.is_human() {
            println!("========== sinexctl gitops ==========");
        }

        let status = cmd
            .status()
            .with_context(|| "failed to spawn sinexctl gitops")?;

        if !status.success() {
            bail!("gitops command failed with status {status}");
        }

        Ok(CommandResult::success().with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
