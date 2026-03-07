//! CI infrastructure commands for running tests with ephemeral environments

use color_eyre::eyre::{Result, bail};
use std::env;
use std::path::PathBuf;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::infra::stack::StackConfig;
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
    /// Schema-only pipeline (apply, check-ready, regenerate)
    SchemaOnly {
        #[arg(long, default_value = "target/ci-schema")]
        target_dir: String,
        #[arg(long)]
        skip_clean: bool,
    },
}

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

/// Build the CheckCommand for CI (named constructor to avoid silent field default drift).
fn check_command_for_ci() -> crate::commands::check::CheckCommand {
    crate::commands::check::CheckCommand {
        fmt: true,
        lint: true,
        forbidden: false, // LintForbiddenCommand runs separately (can be parallelised)
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
}

async fn execute_workspace(target_dir: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("ci workspace");

    // Run schema setup first
    let stage = ctx.start_stage("schema_setup");
    run_schema_setup(target_dir, ctx)?;
    ctx.finish_stage(stage, true);

    // 3.2: Dependency audit — run before expensive test stages
    if ctx.is_human() {
        println!("Running cargo deny check...");
    }
    let stage = ctx.start_stage("deny_check");
    ProcessBuilder::new("cargo")
        .args(["deny", "check"])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    // 3.4: Check (clippy+fmt) and forbidden (ast-grep) are fully independent — run concurrently.
    if ctx.is_human() {
        println!("Running check and lint-forbidden in parallel...");
    }
    let check_stage = ctx.start_stage("check");
    let forbidden_stage = ctx.start_stage("forbidden");

    let check_cmd = check_command_for_ci();
    let (check_result, forbidden_result) = tokio::try_join!(
        check_cmd.execute(ctx),
        crate::commands::lint_forbidden::LintForbiddenCommand {}.execute(ctx),
    )?;

    let check_ok = check_result.is_success();
    let forbidden_ok = forbidden_result.is_success();
    ctx.finish_stage(check_stage, check_ok);
    ctx.finish_stage(forbidden_stage, forbidden_ok);

    if !check_ok {
        return Ok(check_result);
    }
    if !forbidden_ok {
        return Ok(forbidden_result);
    }

    // 3.3: Workspace git cleanliness gate — any tracked dirty file (beyond schema, already checked)
    // signals uncommitted generated code.
    let dirty = ProcessBuilder::new("git")
        .args(["status", "--porcelain"])
        .run_stdout()?;
    let workspace_dirty: Vec<&str> = dirty
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.contains("crate/lib/sinex-schema/schemas"))
        .collect();
    if !workspace_dirty.is_empty() {
        bail!(
            "Workspace has uncommitted changes after check stage. Commit or stash them:\n{}",
            workspace_dirty.join("\n")
        );
    }

    // E2E tests — run first with --fail-fast so we catch pipeline regressions quickly.
    if ctx.is_human() {
        println!("Running E2E tests...");
    }
    let stage = ctx.start_stage("e2e_tests");
    ProcessBuilder::new("xtask")
        .args(["test", "--fail-fast", "-p", "sinex-e2e-tests"])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    // Full test suite — excludes e2e (already run above) to avoid running them twice.
    if ctx.is_human() {
        println!("Running full test suite (excluding e2e)...");
    }
    let stage = ctx.start_stage("full_tests");
    ProcessBuilder::new("xtask")
        .args(["test", "--all", "--prime", "--exclude", "sinex-e2e-tests"])
        .run_ok()?;
    ctx.finish_stage(stage, true);

    Ok(CommandResult::success()
        .with_message("Full workspace validation passed")
        .with_detail("Schema setup: ✓")
        .with_detail("Dependency audit: ✓")
        .with_detail("Check: ✓")
        .with_detail("Forbidden patterns: ✓")
        .with_detail("Workspace clean: ✓")
        .with_detail("E2E tests: ✓")
        .with_detail("Full test suite: ✓")
        .with_duration(ctx.elapsed()))
}

/// Shared schema setup: declarative apply + check-ready.
/// Called from both workspace and schema-only pipelines.
fn run_schema_setup(target_dir: &str, ctx: &CommandContext) -> Result<()> {
    unsafe { env::set_var("CARGO_TARGET_DIR", target_dir) };
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| default_checkout_database_url());

    // 3.6: Guard against accidentally running CI schema apply against a non-local database.
    guard_local_database(&super_url)?;

    if ctx.is_human() {
        println!("Applying declarative schema...");
    }
    let stage = ctx.start_stage("schema_apply");
    ProcessBuilder::new("xtask")
        .args(["db", "apply"])
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

    Ok(())
}

/// Refuse to run CI schema apply against non-local databases to prevent accidental production writes.
fn guard_local_database(url: &str) -> Result<()> {
    // Allow if SINEX_CI_CONFIRMED=1 is set (explicit override)
    if env::var("SINEX_CI_CONFIRMED").as_deref() == Ok("1") {
        return Ok(());
    }
    // Parse the host from the URL. Socket connections (host=/path or no host) are always local.
    if let Ok(parsed) = url.parse::<url::Url>() {
        let host = parsed.host_str().unwrap_or("");
        let is_local = host.is_empty()
            || host == "localhost"
            || host == "127.0.0.1"
            || host == "::1"
            || host.starts_with('/'); // Unix socket in URL form
        if !is_local {
            bail!(
                "Refusing to run CI schema apply against non-local database host '{host}'. \
                 Set SINEX_CI_CONFIRMED=1 to override."
            );
        }
    }
    // If URL doesn't parse (e.g., socket-path form like postgresql:///db?host=/path), allow it.
    Ok(())
}

fn execute_schema_only(
    target_dir: &str,
    skip_clean: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("ci schema-only");

    run_schema_setup(target_dir, ctx)?;

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

fn default_checkout_database_url() -> String {
    StackConfig::for_current_checkout().map_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string(), |cfg| cfg.database_url())
}
