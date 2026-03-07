//! Database management commands - setup, apply, reset, schema

use color_eyre::eyre::{Result, WrapErr, bail};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// Database command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum DbSubcommand {
    Status,
    Apply,
    Setup,
    /// Logical database reset (drop and re-apply declarative schema)
    Reset {
        /// Confirm reset
        #[arg(short = 'y', long)]
        yes: bool,
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

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            DbSubcommand::Status => execute_status(ctx),
            DbSubcommand::Apply => execute_apply(ctx).await,
            DbSubcommand::Setup => execute_setup(ctx).await,
            DbSubcommand::Reset { yes } => execute_reset(*yes, ctx).await,
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

async fn execute_apply(ctx: &CommandContext) -> Result<CommandResult> {
    run_db_apply(ctx).await?;
    Ok(CommandResult::success()
        .with_message("Declarative schema applied")
        .with_duration(ctx.elapsed()))
}

async fn execute_setup(ctx: &CommandContext) -> Result<CommandResult> {
    let config = crate::infra::stack::StackConfig::for_current_checkout().ok();

    let db = if let Some(cfg) = &config {
        cfg.postgres.database.clone()
    } else {
        std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string())
    };

    // Try to create database (may already exist)
    let mut create = ProcessBuilder::new("createdb");
    if let Some(cfg) = &config {
        create = create.env("PGHOST", cfg.run_dir().to_string_lossy());
        create = create.env("PGPORT", cfg.postgres.port.to_string());
    }
    create = create.arg(&db);

    if let Err(e) = create.run_success()
        && ctx.is_human()
    {
        eprintln!("createdb failed or missing: {e}");
    }

    run_db_apply(ctx).await?;

    Ok(CommandResult::success()
        .with_message(format!("Database '{db}' setup complete"))
        .with_duration(ctx.elapsed()))
}

async fn execute_reset(yes: bool, ctx: &CommandContext) -> Result<CommandResult> {
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
    let mut create = ProcessBuilder::new("createdb");
    if let Some(cfg) = &config {
        create = create.env("PGHOST", cfg.run_dir().to_string_lossy());
        create = create.env("PGPORT", cfg.postgres.port.to_string());
    }
    create = create.arg(&db);

    if let Err(e) = create.run_success()
        && ctx.is_human()
    {
        eprintln!("createdb failed or missing: {e}");
    }

    run_db_apply(ctx).await?;

    // Invalidate the preflight result cache: the database was just reset,
    // so the next preflight must run in full (schema re-applied above,
    // but contracts may need re-deploying and infra status re-checked).
    crate::preflight::invalidate_cache();

    Ok(CommandResult::success()
        .with_message(format!("Database '{db}' reset complete"))
        .with_duration(ctx.elapsed()))
}

/// Apply declarative schema using sinex-db's in-process helper.
async fn run_db_apply(ctx: &CommandContext) -> Result<()> {
    if ctx.is_human() {
        println!("========== schema apply ==========");
    }

    let stage = ctx.start_stage("apply");
    let config = crate::infra::stack::StackConfig::for_current_checkout().ok();

    let db_url = config
        .map(|c| c.database_url())
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .ok_or_else(|| color_eyre::eyre::eyre!("DATABASE_URL is required for schema apply"))?;

    let result = sinex_db::apply_schema_for_url(&db_url).await;
    ctx.finish_stage(stage, result.is_ok());
    result
        .map_err(|e| color_eyre::eyre::eyre!("{e}"))
        .with_context(|| "declarative schema apply failed")
}
