//! SQLx compile-time query verification commands
//!
//! This module verifies that SQLx queries are correctly cached
//! and compatible with the database schema.

use anyhow::{bail, Result};
use std::env;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// SQLx query verification command variants
#[derive(Debug, Clone)]
pub enum SqlxSubcommand {
    /// Verify queries against cached metadata (.sqlx/)
    Check,
    /// Generate/update .sqlx query cache (requires DATABASE_URL)
    Prepare,
    /// Full verification: prepare then check (local dev workflow)
    Verify,
}

/// SQLx query verification command
pub struct SqlxCommand {
    pub subcommand: SqlxSubcommand,
}

impl XtaskCommand for SqlxCommand {
    fn name(&self) -> &str {
        "sqlx"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            SqlxSubcommand::Check => execute_check(ctx),
            SqlxSubcommand::Prepare => execute_prepare(ctx),
            SqlxSubcommand::Verify => {
                execute_prepare(ctx)?;
                execute_check(ctx)
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

fn execute_check(ctx: &CommandContext) -> Result<CommandResult> {
    if ctx.is_human() {
        println!("========== cargo sqlx prepare --check ==========");
    }

    let output = ProcessBuilder::cargo()
        .args(&["sqlx", "prepare", "--check", "--workspace"])
        .with_description("verifying SQLx query cache")
        .run()?;

    if !output.success() {
        bail!(
            "cargo sqlx prepare --check failed with status {}",
            output.exit_code
        );
    }

    Ok(CommandResult::success()
        .with_message("SQLx query cache verified")
        .with_duration(ctx.elapsed()))
}

fn execute_prepare(ctx: &CommandContext) -> Result<CommandResult> {
    // Check if DATABASE_URL is set
    if env::var("DATABASE_URL").is_err() {
        bail!("DATABASE_URL not set. SQLx prepare requires a live database connection.");
    }

    if ctx.is_human() {
        println!("========== cargo sqlx prepare ==========");
    }

    let output = ProcessBuilder::cargo()
        .args(&["sqlx", "prepare", "--workspace"])
        .with_description("generating SQLx query cache")
        .inherit_output()
        .run()?;

    if !output.success() {
        bail!("cargo sqlx prepare failed with status {}", output.exit_code);
    }

    Ok(CommandResult::success()
        .with_message("SQLx query cache generated")
        .with_duration(ctx.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputWriter;

    #[test]
    fn test_sqlx_command_name() {
        let cmd = SqlxCommand {
            subcommand: SqlxSubcommand::Check,
        };
        assert_eq!(cmd.name(), "sqlx");
    }

    #[test]
    fn test_sqlx_command_metadata() {
        let cmd = SqlxCommand {
            subcommand: SqlxSubcommand::Check,
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check".to_string()));
        assert!(metadata.timeout.is_some());
        assert!(!metadata.modifies_state);
    }

    #[test]
    fn test_prepare_requires_database_url() {
        let cmd = SqlxCommand {
            subcommand: SqlxSubcommand::Prepare,
        };

        let ctx = CommandContext::new(OutputWriter::new(crate::output::OutputFormat::Silent));

        // Temporarily unset DATABASE_URL if it exists
        let saved_env = env::var("DATABASE_URL").ok();
        env::remove_var("DATABASE_URL");

        let result = cmd.execute(&ctx);

        // Restore environment
        if let Some(url) = saved_env {
            env::set_var("DATABASE_URL", url);
        }

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("DATABASE_URL not set"));
    }

    #[test]
    fn test_verify_subcommand_exists() {
        let cmd = SqlxCommand {
            subcommand: SqlxSubcommand::Verify,
        };
        assert_eq!(cmd.name(), "sqlx");
        // Verify is the composite command: prepare + check
    }
}
