//! Check command - fast correctness checks (fmt check + cargo check)

use anyhow::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;
use crate::resources;

/// Check command configuration
pub struct CheckCommand {
    pub skip_fmt: bool,
    pub skip_check: bool,
}

impl XtaskCommand for CheckCommand {
    fn name(&self) -> &str {
        "check"
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

        let mut result = CommandResult::success();

        if !self.skip_fmt {
            ProcessBuilder::cargo()
                .args(&["fmt", "--all", "--", "--check"])
                .with_description("cargo fmt --check")
                .inherit_output()
                .run_ok()?;
            result = result.with_detail("fmt check passed");
        }

        if !self.skip_check {
            ProcessBuilder::cargo()
                .args(&["check", "--workspace", "--all-features"])
                .with_description("cargo check")
                .inherit_output()
                .run_ok()?;
            result = result.with_detail("cargo check passed");
        }

        Ok(result.with_duration(ctx.elapsed()))
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
    fn test_check_command_metadata() {
        let cmd = CheckCommand {
            skip_fmt: false,
            skip_check: false,
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check".to_string()));
        assert!(metadata.timeout.is_some());
    }

    #[test]
    fn test_check_command_name() {
        let cmd = CheckCommand {
            skip_fmt: true,
            skip_check: true,
        };

        assert_eq!(cmd.name(), "check");
    }
}
