//! CI infrastructure commands for running tests with ephemeral environments

use color_eyre::eyre::{Result, WrapErr, bail};
use std::env;
use std::path::PathBuf;
use std::process::Command;

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
    /// Schema-only pipeline (apply, check-ready)
    SchemaOnly {
        #[arg(long, default_value = "target/ci-schema")]
        target_dir: String,
    },
    /// Verify required tables exist in database
    CheckReady {
        /// Database name
        #[arg(long)]
        database: Option<String>,
        /// Superuser for connection
        #[arg(long)]
        superuser: Option<String>,
    },
    /// Check schema contract regressions against a base branch
    Compat {
        /// Base branch/commit to compare against
        #[arg(long)]
        base: Option<String>,
        /// Glob pattern for schema files
        #[arg(long, default_value = "schemas/v1")]
        glob: String,
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
            CiSubcommand::SchemaOnly { target_dir } => execute_schema_only(target_dir, ctx).await,
            CiSubcommand::CheckReady {
                database,
                superuser,
            } => execute_check_ready(database.clone(), superuser.clone(), ctx),
            CiSubcommand::Compat { base, glob } => execute_compat(base.clone(), glob, ctx),
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
        socket_dir: resolve_socket_dir(args.socket_dir.clone(), env::current_dir())?,
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
        heavy: false,
        all: true, // CI should check all packages
        packages: vec![],
        skip_tests: false,    // CI should always check tests
        lint_breakdown: true, // Show lint breakdown in CI
        by_file: false,
        nix: false, // nix flake check runs in a dedicated CI stage if needed
    }
}

async fn execute_workspace(target_dir: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("ci workspace");

    // Run schema setup first
    let stage = ctx.start_stage("schema_setup");
    run_schema_setup(target_dir, ctx).await?;
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
async fn run_schema_setup(target_dir: &str, ctx: &CommandContext) -> Result<()> {
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
    sinex_db::apply_schema_for_url(&super_url)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("{e}"))
        .with_context(|| "declarative schema apply failed")?;
    ctx.finish_stage(stage, true);

    if ctx.is_human() {
        println!("Checking schema readiness...");
    }
    let stage = ctx.start_stage("check_ready");
    execute_check_ready(None, None, ctx)?;
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

async fn execute_schema_only(target_dir: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("ci schema-only");

    run_schema_setup(target_dir, ctx).await?;

    Ok(CommandResult::success()
        .with_message("Schema validation passed")
        .with_duration(ctx.elapsed()))
}

fn execute_check_ready(
    database: Option<String>,
    superuser: Option<String>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ensure_psql()?;

    let db = database
        .or_else(|| std::env::var("PGDATABASE").ok())
        .unwrap_or_else(|| "sinex_dev".to_string());

    let superuser = superuser
        .or_else(|| std::env::var("SUPERUSER").ok())
        .unwrap_or_else(|| "postgres".to_string());

    // Check core.events
    let mut cmd = pg_command("psql");
    cmd.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('core.events') AS reg")
        .env("PGUSER", &superuser);

    let output = cmd
        .output()
        .with_context(|| "psql core.events check failed")?;

    if !output.status.success() {
        bail!("core.events missing in database {db}");
    }

    if ctx.is_human() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }

    // Check sinex_schemas.event_payload_schemas
    let mut cmd2 = pg_command("psql");
    cmd2.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('sinex_schemas.event_payload_schemas') AS reg")
        .env("PGUSER", &superuser);

    let output2 = cmd2
        .output()
        .with_context(|| "psql contract registry check failed")?;

    if !output2.status.success() {
        bail!("sinex_schemas.event_payload_schemas missing in database {db}");
    }

    if ctx.is_human() {
        print!("{}", String::from_utf8_lossy(&output2.stdout));
        println!("✅ core.events and sinex_schemas.event_payload_schemas are present");
    }

    Ok(CommandResult::success()
        .with_message("Contract tables verified")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::json!({
            "database": db,
            "tables": {
                "core.events": true,
                "sinex_schemas.event_payload_schemas": true
            }
        })))
}

fn execute_compat(base: Option<String>, glob: &str, ctx: &CommandContext) -> Result<CommandResult> {
    // CI sometimes passes an empty base ref on branch pushes; treat that as "unspecified"
    let base_branch = base
        .or_else(|| std::env::var("CI_BASE_BRANCH").ok())
        .filter(|s| !s.trim().is_empty());

    let base = match base_branch {
        Some(b) => b,
        None => resolve_default_base_branch()?,
    };

    let diff_output = ProcessBuilder::git()
        .args(["diff", "--name-only", &format!("{base}...HEAD"), "--", glob])
        .with_description("git diff for contract regression check")
        .run()?;

    if diff_output.exit_code != 0 && diff_output.exit_code != 1 {
        bail!("git diff failed with status {}", diff_output.exit_code);
    }

    let changed = diff_output.stdout.trim();
    if changed.is_empty() {
        if ctx.is_human() {
            println!("✅ No contract edits detected");
        }
        return Ok(CommandResult::success()
            .with_message("No contract changes detected")
            .with_duration(ctx.elapsed()));
    }

    if ctx.is_human() {
        println!("🔍 Checking contract regressions for updated contracts against {base}:");
        println!("{changed}");
    }

    let mut errors = 0;
    let mut checked = Vec::new();

    for file in changed.lines().filter(|l| !l.trim().is_empty()) {
        let path = std::path::Path::new(file);
        if !path.exists() {
            if ctx.is_human() {
                println!("⚠️  Skipping deleted contract {file}");
            }
            continue;
        }

        let git_obj = format!("{base}:{file}");
        let cat_file = Command::new("git")
            .arg("cat-file")
            .arg("-e")
            .arg(&git_obj)
            .status()
            .unwrap_or_else(|_| Command::new("false").status().unwrap());
        if !cat_file.success() {
            if ctx.is_human() {
                println!("➕ New contract {file} (no base comparison required)");
            }
            continue;
        }

        let old_contents = ProcessBuilder::git()
            .args(["show", &git_obj])
            .with_description(format!("reading {git_obj}"))
            .run()?;

        let new_contents =
            std::fs::read_to_string(path).with_context(|| format!("failed to read {file}"))?;

        if ctx.is_human() {
            println!("Comparing {file} against {base}...");
        }

        let success = check_schema_contract_guard(&old_contents.stdout, &new_contents)
            .with_context(|| format!("failed to compare schema contract for {file}"))?;

        if success {
            if ctx.is_human() {
                println!("✅ {file} passes contract regression check");
            }
            checked.push(file.to_string());
        } else {
            errors += 1;
            if ctx.is_human() {
                eprintln!("❌ Contract regression detected in {file}");
            }
        }
    }

    if errors > 0 {
        bail!("Contract regression check failed ({errors} issue(s))");
    }

    if ctx.is_human() {
        println!("✅ Contract regression check passed");
    }

    Ok(CommandResult::success()
        .with_message("Contract regression check passed")
        .with_details(checked)
        .with_duration(ctx.elapsed()))
}

fn check_schema_contract_guard(old_json_str: &str, new_json_str: &str) -> Result<bool> {
    use color_eyre::eyre::Context;

    let old: serde_json::Value = serde_json::from_str(old_json_str)
        .context("failed to parse base schema JSON for contract guard")?;
    let new: serde_json::Value = serde_json::from_str(new_json_str)
        .context("failed to parse candidate schema JSON for contract guard")?;

    let old_required: Vec<&str> = old
        .get("required")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    let new_required: Vec<&str> = new
        .get("required")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    for req in &new_required {
        if !old_required.contains(req) {
            eprintln!("  Breaking: new required field '{req}' not in old schema");
            return Ok(false);
        }
    }

    Ok(true)
}

fn resolve_socket_dir(
    socket_dir: Option<PathBuf>,
    current_dir: std::io::Result<PathBuf>,
) -> Result<PathBuf> {
    match socket_dir {
        Some(path) => Ok(path),
        None => current_dir.wrap_err("failed to determine current directory for ephemeral Postgres socket dir"),
    }
}

fn resolve_default_base_branch() -> Result<String> {
    let output = ProcessBuilder::git()
        .args(["symbolic-ref", "refs/remotes/origin/HEAD"])
        .with_description("resolving origin/HEAD")
        .run()?;

    if output.success() {
        let text = output.stdout.trim();
        let branch = text.strip_prefix("refs/remotes/origin/").unwrap_or(text);
        if !branch.is_empty() {
            return Ok(branch.to_string());
        }
    }

    Ok("master".to_string())
}

fn ensure_psql() -> Result<()> {
    let output = pg_command("psql")
        .arg("--version")
        .output()
        .with_context(|| "failed to spawn psql")?;

    if !output.status.success() {
        bail!("psql not available on PATH");
    }
    Ok(())
}

fn pg_command(binary: &str) -> Command {
    if let Ok(prefix) = std::env::var("SINEX_PG_BIN") {
        let mut path = PathBuf::from(prefix);
        path.push(binary);
        Command::new(path)
    } else {
        Command::new(binary)
    }
}

fn default_checkout_database_url() -> String {
    StackConfig::for_current_checkout().map_or_else(
        |_| "postgresql:///sinex_dev?host=/run/postgresql".to_string(),
        |cfg| cfg.database_url(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_check_schema_contract_guard_reports_invalid_base_schema()
    -> ::xtask::sandbox::TestResult<()> {
        let error = check_schema_contract_guard("not json", r#"{"type":"object"}"#)
            .expect_err("invalid base schema should surface");
        assert!(error.to_string().contains("base schema JSON"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_schema_contract_guard_reports_invalid_candidate_schema()
    -> ::xtask::sandbox::TestResult<()> {
        let error = check_schema_contract_guard(r#"{"type":"object"}"#, "not json")
            .expect_err("invalid candidate schema should surface");
        assert!(error.to_string().contains("candidate schema JSON"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_schema_contract_guard_rejects_new_required_fields()
    -> ::xtask::sandbox::TestResult<()> {
        let success = check_schema_contract_guard(
            r#"{"type":"object","required":["a"]}"#,
            r#"{"type":"object","required":["a","b"]}"#,
        )?;
        assert!(!success, "new required fields should fail the contract guard");
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_socket_dir_reports_current_dir_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let error = resolve_socket_dir(None, Err(std::io::Error::other("cwd exploded")))
            .expect_err("current_dir failure should surface");
        let message = format!("{error:#}");
        assert!(message.contains("cwd exploded"));
        assert!(message.contains("socket dir"));
        Ok(())
    }
}
