//! Check command - fast correctness checks (fmt check + cargo check)

use anyhow::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;
use crate::resources;

/// Check command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct CheckCommand {
    /// Skip formatting check
    #[arg(long)]
    pub skip_fmt: bool,
    /// Run clippy lints (default: true)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub lint: bool,
    /// Run forbidden pattern scan (default: true)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub forbidden: bool,
    /// Also run slow lints
    #[arg(short, long)]
    pub heavy: bool,
    /// Only check affected packages
    #[arg(short, long)]
    pub affected: bool,
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

        // 1. Formatting
        if !self.skip_fmt {
            if ctx.is_human() {
                println!("Checking formatting...");
            }
            ProcessBuilder::cargo()
                .args(&["fmt", "--all", "--", "--check"])
                .with_description("cargo fmt --check")
                .inherit_output()
                .run_ok()?;
            result = result.with_detail("fmt check passed");
        }

        // 2. Cargo Check
        if ctx.is_human() {
            println!("Checking compilation...");
        }
        let mut check = ProcessBuilder::cargo();
        check = check.arg("check").arg("--workspace").arg("--all-features");

        if self.affected {
            let affected_pkgs = crate::affected::affected_packages()?;
            if !affected_pkgs.is_empty() {
                check = ProcessBuilder::cargo().arg("check").arg("--all-features");
                for p in affected_pkgs {
                    check = check.arg("-p").arg(p);
                }
            }
        }

        check
            .with_description("cargo check")
            .inherit_output()
            .run_ok()?;
        result = result.with_detail("cargo check passed");

        // 3. Clippy
        if self.lint {
            if ctx.is_human() {
                println!("Running clippy...");
            }
            let mut clippy = ProcessBuilder::cargo();
            clippy = clippy.args(&[
                "clippy",
                "--workspace",
                "--all-targets",
                "--all-features",
                "--",
                "-D",
                "warnings",
            ]);
            clippy
                .with_description("cargo clippy -D warnings")
                .inherit_output()
                .run_ok()?;
            result = result.with_detail("clippy passed");
        }

        // 4. Forbidden patterns
        if self.forbidden {
            if ctx.is_human() {
                println!("Scanning for forbidden patterns...");
            }
            crate::commands::lint_forbidden::LintForbiddenCommand.execute(ctx)?;
            result = result.with_detail("forbidden pattern scan passed");
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

    #[test]
    fn test_check_command_metadata() {
        let cmd = CheckCommand {
            skip_fmt: false,
            lint: true,
            forbidden: true,
            heavy: false,
            affected: false,
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check".to_string()));
        assert!(metadata.timeout.is_some());
    }

    #[test]
    fn test_check_command_name() {
        let cmd = CheckCommand {
            skip_fmt: true,
            lint: false,
            forbidden: false,
            heavy: false,
            affected: false,
        };

        assert_eq!(cmd.name(), "check");
    }
}
