//! CI preflight command - full pre-merge validation suite

use anyhow::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::resources;

/// CI preflight command - runs complete pre-merge validation.
///
/// This is a comprehensive check that runs:
/// 1. Code formatting check
/// 2. Cargo check
/// 3. Clippy linting
/// 4. Forbidden pattern scanning
/// 5. SQLx query verification
/// 6. Schema generation and verification
/// 7. Default test suite
pub struct CiPreflightCommand;

impl XtaskCommand for CiPreflightCommand {
    fn name(&self) -> &str {
        "ci-preflight"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Resource warning before heavy operation
        if ctx.is_human() {
            if let Ok(status) = resources::ResourceStatus::capture() {
                if let Some(warning) = status.warning(resources::thresholds::FULL_CI_GB) {
                    eprintln!("  ⚠ {}", warning);
                }
                // Also show current resource status for ci-preflight (informational)
                eprintln!("  {}", status.summary());
            }
        }

        // Run fmt + cargo check first so contributors catch drift before heavier steps
        super::CheckCommand {
            skip_fmt: false,
            skip_check: false,
        }
        .execute(ctx)?;

        super::LintCommand {}.execute(ctx)?;

        super::LintForbiddenCommand {}.execute(ctx)?;

        // Verify SQLx query cache is up-to-date
        super::SqlxCommand {
            subcommand: super::SqlxSubcommand::Check,
        }
        .execute(ctx)?;

        // Regenerate schemas to ensure artifacts stay in sync with code
        super::ci::schema_generate("schemas/v1", false)?;
        super::ci::ensure_schemas_clean()?;

        // Run default test suite
        super::TestCommand {
            profile: "default".to_string(),
            prime: false,
            list: false,
            dry_run: false,
            preflight: false,
            affected: false,
            args: vec![],
        }
        .execute(ctx)?;

        Ok(CommandResult::success()
            .with_message("ci-preflight passed")
            .with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("ci".to_string()),
            timeout: Some(std::time::Duration::from_secs(1800)), // 30 minutes
            modifies_state: false,
            track_in_history: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::{OutputFormat, OutputWriter};

    #[test]
    fn test_ci_preflight_command_name() {
        let cmd = CiPreflightCommand;
        assert_eq!(cmd.name(), "ci-preflight");
    }

    #[test]
    fn test_ci_preflight_command_metadata() {
        let cmd = CiPreflightCommand;
        let metadata = cmd.metadata();

        assert_eq!(metadata.category, Some("ci".to_string()));
        assert!(metadata.timeout.is_some());
        assert!(!metadata.modifies_state);
        assert!(metadata.track_in_history);
    }
}
