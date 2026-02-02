//! Database management commands - setup, migrate, reset, schema

use anyhow::{bail, Context, Result};
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// Database command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum DbSubcommand {
    Status,
    Migrate,
    Setup,
    Reset {
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Schema management (alias for contracts)
    Schema {
        #[command(subcommand)]
        cmd: crate::commands::contracts::ContractsSubcommand,
    },
}

/// Database management command
#[derive(Debug, Clone, clap::Args)]
pub struct DbCommand {
    #[command(subcommand)]
    pub subcommand: DbSubcommand,
}

impl XtaskCommand for DbCommand {
    fn name(&self) -> &'static str {
        "db"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            DbSubcommand::Status => execute_status(ctx),
            DbSubcommand::Migrate => execute_migrate(ctx),
            DbSubcommand::Setup => execute_setup(ctx),
            DbSubcommand::Reset { yes } => execute_reset(*yes, ctx),
            DbSubcommand::Schema { cmd } => {
                let contracts_cmd = crate::commands::contracts::ContractsCommand {
                    subcommand: cmd.clone(),
                };
                contracts_cmd.execute(ctx)
            }
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

    let config = crate::infra::stack::StackConfig::for_current_checkout().ok();

    let mut cmd = ProcessBuilder::psql();
    cmd = cmd.args(["-c", "select current_database(), current_user"]);

    if let Some(cfg) = &config {
        cmd = cmd.env("PGHOST", cfg.run_dir().to_string_lossy());
        cmd = cmd.env("PGPORT", cfg.postgres.port.to_string());
        cmd = cmd.env("PGUSER", &cfg.postgres.user);
        cmd = cmd.env("PGDATABASE", &cfg.postgres.database);
    }

    let output = cmd
        .with_description("checking PostgreSQL connection")
        .run()?;

    if !output.success() {
        bail!("psql exited with status {}", output.exit_code);
    }

    if ctx.is_human() {
        if let Some(cfg) = config {
            // Using human_output helper if simpler, but print is fine
            println!("Postgres reachable (port {})", cfg.postgres.port);
        } else {
            println!("Postgres reachable");
        }
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
    let config = crate::infra::stack::StackConfig::for_current_checkout().ok();

    let db = if let Some(cfg) = &config {
        cfg.postgres.database.clone()
    } else {
        std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string())
    };

    // Try to create database (may already exist)
    let mut create = Command::new("createdb");
    if let Some(cfg) = &config {
        create.env("PGHOST", cfg.run_dir().to_string_lossy().to_string());
        create.env("PGPORT", cfg.postgres.port.to_string());
    }
    create.arg(&db);

    if let Err(e) = create.status() {
        if ctx.is_human() {
            eprintln!("createdb failed or missing: {e}");
        }
    }

    run_db_migrate(ctx)?;

    Ok(CommandResult::success()
        .with_message(format!("Database '{db}' setup complete"))
        .with_duration(ctx.elapsed()))
}

fn execute_reset(yes: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if !yes {
        bail!("Refusing to drop DB without --yes");
    }

    let config = crate::infra::stack::StackConfig::for_current_checkout().ok();
    let db = if let Some(cfg) = &config {
        cfg.postgres.database.clone()
    } else {
        std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string())
    };

    // Drop database
    if ctx.is_human() {
        println!("========== dropdb ==========");
    }

    let mut cmd = ProcessBuilder::psql();
    if let Some(cfg) = &config {
        cmd = cmd.env("PGHOST", cfg.run_dir().to_string_lossy());
        cmd = cmd.env("PGPORT", cfg.postgres.port.to_string());
        cmd = cmd.env("PGUSER", &cfg.postgres.superuser);
        cmd = cmd.env("PGDATABASE", "postgres");
    }

    cmd.args(["-c", &format!("DROP DATABASE IF EXISTS {db}")])
        .with_description("dropping database")
        .inherit_output()
        .run_ok()?;

    // Recreate database
    let mut create = Command::new("createdb");
    if let Some(cfg) = &config {
        create.env("PGHOST", cfg.run_dir());
        create.env("PGPORT", cfg.postgres.port.to_string());
    }
    create.arg(&db);

    if let Err(e) = create.status() {
        if ctx.is_human() {
            eprintln!("createdb failed or missing: {e}");
        }
    }

    run_db_migrate(ctx)?;

    Ok(CommandResult::success()
        .with_message(format!("Database '{db}' reset complete"))
        .with_duration(ctx.elapsed()))
}

fn run_db_migrate(ctx: &CommandContext) -> Result<()> {
    if ctx.is_human() {
        println!("========== migrate ==========");
    }

    let config = crate::infra::stack::StackConfig::for_current_checkout().ok();

    let mut cmd = ProcessBuilder::cargo();
    cmd = cmd.args([
        "run",
        "--package",
        "sinex-schema",
        "--bin",
        "sinex-schema",
        "--",
        "up",
    ]);

    if let Some(cfg) = &config {
        cmd = cmd.env("DATABASE_URL", cfg.database_url());
    }

    cmd.with_description("cargo run -p sinex-schema --bin sinex-schema -- up")
        .inherit_output()
        .run_ok()
        .with_context(|| "database migration failed")
}
