//! Database management commands - setup, migrate, reset

use anyhow::{bail, Context, Result};
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// Database command variants
#[derive(Debug, Clone)]
pub enum DbSubcommand {
    Status,
    Migrate,
    Setup,
    Reset { yes: bool },
}

/// Database management command
pub struct DbCommand {
    pub subcommand: DbSubcommand,
}

impl XtaskCommand for DbCommand {
    fn name(&self) -> &str {
        "db"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            DbSubcommand::Status => execute_status(ctx),
            DbSubcommand::Migrate => execute_migrate(ctx),
            DbSubcommand::Setup => execute_setup(ctx),
            DbSubcommand::Reset { yes } => execute_reset(*yes, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::database()
    }
}

fn execute_status(ctx: &CommandContext) -> Result<CommandResult> {
    if ctx.is_human() {
        println!("========== psql status ==========");
    }

    let output = ProcessBuilder::psql()
        .args(&["-c", "select current_database(), current_user"])
        .with_description("checking PostgreSQL connection")
        .run()?;

    if !output.success() {
        bail!("psql exited with status {}", output.exit_code);
    }

    if ctx.is_human() {
        println!("Postgres reachable");
    }

    Ok(CommandResult::success()
        .with_message("PostgreSQL connection successful")
        .with_duration(ctx.elapsed()))
}

fn execute_migrate(ctx: &CommandContext) -> Result<CommandResult> {
    run_db_migrate(ctx)?;
    Ok(CommandResult::success()
        .with_message("Database migrations applied")
        .with_duration(ctx.elapsed()))
}

fn execute_setup(ctx: &CommandContext) -> Result<CommandResult> {
    let db = std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string());

    // Try to create database (may already exist)
    let mut create = Command::new("createdb");
    create.arg(&db);
    if let Err(e) = create.status() {
        if ctx.is_human() {
            eprintln!("createdb failed or missing: {e}");
        }
    }

    run_db_migrate(ctx)?;

    Ok(CommandResult::success()
        .with_message(format!("Database '{}' setup complete", db))
        .with_duration(ctx.elapsed()))
}

fn execute_reset(yes: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if !yes {
        bail!("Refusing to drop DB without --yes");
    }

    let db = std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string());

    // Drop database
    if ctx.is_human() {
        println!("========== dropdb ==========");
    }

    ProcessBuilder::psql()
        .args(&["-c", &format!("DROP DATABASE IF EXISTS {db}")])
        .with_description("dropping database")
        .inherit_output()
        .run_ok()?;

    // Recreate database
    let mut create = Command::new("createdb");
    create.arg(&db);
    if let Err(e) = create.status() {
        if ctx.is_human() {
            eprintln!("createdb failed or missing: {e}");
        }
    }

    run_db_migrate(ctx)?;

    Ok(CommandResult::success()
        .with_message(format!("Database '{}' reset complete", db))
        .with_duration(ctx.elapsed()))
}

fn run_db_migrate(ctx: &CommandContext) -> Result<()> {
    if ctx.is_human() {
        println!("========== migrate ==========");
    }

    ProcessBuilder::cargo()
        .args(&[
            "run",
            "--package",
            "sinex-schema",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .with_description("cargo run -p sinex-schema --bin sinex-schema -- up")
        .inherit_output()
        .run_ok()
        .with_context(|| "database migration failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputWriter;

    #[test]
    fn test_db_command_metadata() {
        let cmd = DbCommand {
            subcommand: DbSubcommand::Status,
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("database".to_string()));
        assert!(metadata.timeout.is_some());
        assert!(metadata.modifies_state); // Database commands modify state
    }

    #[test]
    fn test_db_command_name() {
        let cmd = DbCommand {
            subcommand: DbSubcommand::Migrate,
        };

        assert_eq!(cmd.name(), "db");
    }

    #[test]
    fn test_reset_requires_yes() {
        let cmd = DbCommand {
            subcommand: DbSubcommand::Reset { yes: false },
        };

        let ctx = CommandContext::new(OutputWriter::new(crate::output::OutputFormat::Silent));
        let result = cmd.execute(&ctx);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Refusing to drop DB"));
    }
}
