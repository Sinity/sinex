//! CI infrastructure commands for running tests with ephemeral environments

use color_eyre::eyre::{Result, bail};
use std::env;
use std::path::PathBuf;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;
use crate::sandbox::postgres::{self, PostgresConfig};

/// CI command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct CiCommand {
    #[command(subcommand)]
    pub subcommand: CiSubcommand,
}

/// Parameters for the `ci postgres` ephemeral environment.
#[derive(Debug, Clone)]
pub struct EphemeralPostgresArgs {
    pub port: u16,
    pub data_dir: Option<PathBuf>,
    pub socket_dir: Option<PathBuf>,
    pub keep_data: bool,
    pub app_user: String,
    pub superuser: String,
    pub database: String,
    pub operation_id: String,
    pub command: Vec<String>,
}

/// CI subcommands
#[derive(Debug, Clone, clap::Subcommand)]
pub enum CiSubcommand {
    /// Start an ephemeral Postgres and run the given command with env vars set
    Postgres {
        #[arg(long, default_value = "5433")]
        port: u16,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        socket_dir: Option<PathBuf>,
        #[arg(long)]
        keep_data: bool,
        #[arg(long, default_value = "sinex_app")]
        app_user: String,
        #[arg(long, default_value = "sinex_superuser")]
        superuser: String,
        #[arg(long, default_value = "sinex_dev")]
        database: String,
        #[arg(long, default_value = "default-op")]
        operation_id: String,
        #[arg(last = true)]
        command: Vec<String>,
    },
    /// Full workspace validation (schema setup + lint + tests)
    Workspace {
        #[arg(long, default_value = "target/ci")]
        target_dir: String,
    },
    /// Schema-only pipeline (migrate, check-ready, regenerate)
    SchemaOnly {
        #[arg(long, default_value = "target/ci-schema")]
        target_dir: String,
        #[arg(long)]
        skip_clean: bool,
    },
    /// Schema validation pipeline (migrate, check-ready, seed registry, sync)
    SchemaSync {
        #[arg(long, default_value = "target/ci-sync")]
        target_dir: String,
    },
}

#[async_trait::async_trait]
impl XtaskCommand for CiCommand {
    fn name(&self) -> &'static str {
        "ci"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            CiSubcommand::Postgres {
                port,
                data_dir,
                socket_dir,
                keep_data,
                app_user,
                superuser,
                database,
                operation_id,
                command,
            } => {
                let args = EphemeralPostgresArgs {
                    port: *port,
                    data_dir: data_dir.clone(),
                    socket_dir: socket_dir.clone(),
                    keep_data: *keep_data,
                    app_user: app_user.clone(),
                    superuser: superuser.clone(),
                    database: database.clone(),
                    operation_id: operation_id.clone(),
                    command: command.clone(),
                };
                execute_postgres(&args, ctx)
            }
            CiSubcommand::Workspace { target_dir } => execute_workspace(target_dir, ctx).await,
            CiSubcommand::SchemaOnly {
                target_dir,
                skip_clean,
            } => execute_schema_only(target_dir, *skip_clean, ctx),
            CiSubcommand::SchemaSync { target_dir } => execute_schema_sync(target_dir, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::test() // CI commands are testing-related
    }
}

fn execute_postgres(args: &EphemeralPostgresArgs, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("ci postgres");

    let config = PostgresConfig {
        port: args.port,
        data_dir: args
            .data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(".sinex/ci-pgdata")),
        socket_dir: args
            .socket_dir
            .clone()
            .unwrap_or_else(|| env::current_dir().unwrap_or_default()),
        keep_data: args.keep_data,
        app_user: args.app_user.clone(),
        superuser: args.superuser.clone(),
        database: args.database.clone(),
        operation_id: args.operation_id.clone(),
    };

    let (pg_guard, pg_env) = postgres::setup_ephemeral(&config)?;

    let app_url = format!(
        "postgresql://{}@{}:{}/{}",
        args.app_user, pg_env.host, args.port, args.database
    );
    let super_url = format!(
        "postgresql://{}@{}:{}/{}",
        args.superuser, pg_env.host, args.port, args.database
    );

    let Some(program) = args.command.first() else {
        bail!("ci postgres requires a command to run");
    };

    if ctx.is_human() {
        println!("Running command: {:?}", args.command);
    }

    let status = ProcessBuilder::new(program)
        .args(&args.command[1..])
        .env("PGHOST", &pg_env.host)
        .env("PGPORT", args.port.to_string())
        .env("PGDATA", config.data_dir.to_string_lossy())
        .env("PGUSER", &args.app_user)
        .env("DATABASE_URL", &app_url)
        .env("DATABASE_URL_APP", &app_url)
        .env("DATABASE_URL_SUPERUSER", &super_url)
        .env("SUPERUSER", &args.superuser)
        .env("SINEX_OPERATION_ID", &args.operation_id)
        .run();

    drop(pg_guard);

    match status {
        Ok(_) => Ok(CommandResult::success()
            .with_message("Successfully ran command with ephemeral Postgres")
            .with_detail(format!("Port: {}", args.port))
            .with_detail(format!("Database: {}", args.database))
            .with_duration(ctx.elapsed())),
        Err(e) => Ok(CommandResult::failure(crate::output::StructuredError {
            code: "COMMAND_FAILED".to_string(),
            message: format!("Command {:?} failed", args.command),
            location: Some("ci::postgres".to_string()),
            suggestion: Some("Check DATABASE_URL and ensure Postgres is accessible".to_string()),
        })
        .with_detail(e.to_string())
        .with_duration(ctx.elapsed())),
    }
}

async fn execute_workspace(target_dir: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("ci workspace");

    // Run schema setup first
    let stage = ctx.start_stage("schema_setup");
    execute_schema_only(target_dir, false, ctx)?;
    ctx.finish_stage(stage, true);

    // Ensure formatting, compilation, and clippy all pass before we spend time on e2e suites.
    if ctx.is_human() {
        println!("Running check...");
    }
    let stage = ctx.start_stage("check");
    let check_result = crate::commands::check::CheckCommand {
        fmt: true,
        lint: true,
        forbidden: false, // LintForbiddenCommand runs separately below
        full: false,
        fix: false,
        fix_fmt: false,
        heavy: false,
        affected: false,
        all: true, // CI should check all packages
        packages: vec![],
        skip_tests: false,    // CI should always check tests
        lint_breakdown: true, // Show lint breakdown in CI
        by_file: false,
    }
    .execute(ctx)
    .await?;
    let check_ok = check_result.is_success();
    ctx.finish_stage(stage, check_ok);
    if !check_ok {
        return Ok(check_result);
    }

    if ctx.is_human() {
        println!("Running lint-forbidden...");
    }
    let stage = ctx.start_stage("forbidden");
    let forbidden_result = crate::commands::lint_forbidden::LintForbiddenCommand {}
        .execute(ctx)
        .await?;
    let forbidden_ok = forbidden_result.is_success();
    ctx.finish_stage(stage, forbidden_ok);
    if !forbidden_ok {
        return Ok(forbidden_result);
    }

    if ctx.is_human() {
        println!("Running E2E tests...");
    }
    let stage = ctx.start_stage("e2e_tests");
    ProcessBuilder::new("xtask")
        .args(["test", "--fail-fast", "-p", "sinex-e2e-tests"])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    if ctx.is_human() {
        println!("Running full test suite...");
    }
    let stage = ctx.start_stage("full_tests");
    ProcessBuilder::new("xtask")
        .args(["test", "--all", "--prime"])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    Ok(CommandResult::success()
        .with_message("Full workspace validation passed")
        .with_detail("Schema setup: ✓")
        .with_detail("Check: ✓")
        .with_detail("Lint: ✓")
        .with_detail("Forbidden patterns: ✓")
        .with_detail("E2E tests: ✓")
        .with_detail("Full test suite: ✓")
        .with_duration(ctx.elapsed()))
}

fn execute_schema_only(
    target_dir: &str,
    skip_clean: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("ci schema-only");

    unsafe { env::set_var("CARGO_TARGET_DIR", target_dir) };
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    if ctx.is_human() {
        println!("Running migrations...");
    }
    let stage = ctx.start_stage("migrate");
    ProcessBuilder::cargo()
        .args([
            "run",
            "--manifest-path",
            "crate/lib/sinex-schema/Cargo.toml",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .env("DATABASE_URL", &super_url)
        .run_ok()?;
    ctx.finish_stage(stage, true);

    if ctx.is_human() {
        println!("Checking schema readiness...");
    }
    let stage = ctx.start_stage("check_ready");
    ProcessBuilder::new("xtask")
        .args(["contracts", "check-ready"])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    if ctx.is_human() {
        println!("Generating schemas...");
    }
    let stage = ctx.start_stage("generate");
    ProcessBuilder::new("xtask")
        .args(["contracts", "generate"])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    if !skip_clean {
        if ctx.is_human() {
            println!("Verifying schema cleanliness...");
        }
        let stage = ctx.start_stage("verify_clean");
        let status = ProcessBuilder::new("git")
            .args(["status", "--porcelain", "crate/lib/sinex-schema/schemas"])
            .run_stdout()?;

        if !status.trim().is_empty() {
            ctx.finish_stage(stage, false);
            bail!("Schema generation resulted in dirty files:\n{status}");
        }
        ctx.finish_stage(stage, true);
    }

    Ok(CommandResult::success()
        .with_message("Schema validation passed")
        .with_duration(ctx.elapsed()))
}

fn execute_schema_sync(target_dir: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("ci schema-sync");

    unsafe { env::set_var("CARGO_TARGET_DIR", target_dir) };
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    let stage = ctx.start_stage("migrate");
    ProcessBuilder::cargo()
        .args([
            "run",
            "--manifest-path",
            "crate/lib/sinex-schema/Cargo.toml",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .env("DATABASE_URL", &super_url)
        .run_ok()?;
    ctx.finish_stage(stage, true);

    let stage = ctx.start_stage("check_ready");
    ProcessBuilder::new("xtask")
        .args(["contracts", "check-ready"])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    let db_url = env::var("DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    if ctx.is_human() {
        println!("Seeding test schema entries...");
    }

    let stage = ctx.start_stage("seed");
    let psql_run = |sql: &str| -> Result<()> {
        let status = Command::new("psql")
            .arg("-d")
            .arg(&db_url)
            .arg("-c")
            .arg(sql)
            .status()?;
        if !status.success() {
            bail!("psql failed");
        }
        Ok(())
    };

    psql_run(
        "INSERT INTO sinex_schemas.event_payload_schemas (source, event_type, schema_version, schema_content, content_hash) VALUES ('test.source', 'test.event', '1.0.0', '{}'::jsonb, md5(random()::text)) ON CONFLICT (source, event_type, schema_version) DO NOTHING;",
    )?;
    psql_run(
        "UPDATE sinex_schemas.event_payload_schemas SET is_active = true WHERE source = 'test.source' AND event_type = 'test.event';",
    )?;
    ctx.finish_stage(stage, true);

    let tmp_dir = tempfile::tempdir()?;
    if ctx.is_human() {
        println!("Running schema sync test...");
    }

    let stage = ctx.start_stage("sync_test");
    ProcessBuilder::new("xtask")
        .args([
            "contracts",
            "generate",
            "--output",
            tmp_dir
                .path()
                .to_str()
                .expect("temp dir must be valid UTF-8"),
        ])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    Ok(CommandResult::success()
        .with_message("Schema sync validation passed")
        .with_duration(ctx.elapsed()))
}
