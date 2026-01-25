//! Lint command - clippy lint with -D warnings

use anyhow::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;
use crate::resources;

/// Lint command configuration
pub struct LintCommand;

impl XtaskCommand for LintCommand {
    fn name(&self) -> &str {
        "lint"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Resource warning before heavy operation
        if ctx.is_human() {
            if let Ok(status) = resources::ResourceStatus::capture() {
                if let Some(warning) = status.warning(resources::thresholds::CARGO_CHECK_GB) {
                    eprintln!("  ⚠ {}", warning);
                }
            }
        }

        ProcessBuilder::cargo()
            .args(&[
                "clippy",
                "--workspace",
                "--all-targets",
                "--all-features",
                "--",
                "-D",
                "warnings",
            ])
            .with_description("cargo clippy -D warnings")
            .inherit_output()
            .run_ok()?;

        Ok(CommandResult::success()
            .with_detail("clippy passed")
            .with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::{OutputFormat, OutputWriter};

    #[test]
    fn test_lint_command_name() {
        let cmd = LintCommand;
        assert_eq!(cmd.name(), "lint");
    }

    #[test]
    fn test_lint_command_metadata() {
        let cmd = LintCommand;
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check".to_string()));
    }
}
