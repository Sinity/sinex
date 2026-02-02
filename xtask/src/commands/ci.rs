//! CI infrastructure commands for running tests with ephemeral environments

use anyhow::{anyhow, bail, Context, Result};
use std::env;
use std::fs;
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// CI command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct CiCommand {
    #[command(subcommand)]
    pub subcommand: CiSubcommand,
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

impl XtaskCommand for CiCommand {
    fn name(&self) -> &'static str {
        "ci"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
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
            } => execute_postgres(
                *port,
                data_dir.clone(),
                socket_dir.clone(),
                *keep_data,
                app_user,
                superuser,
                database,
                operation_id,
                command,
                ctx,
            ),
            CiSubcommand::Workspace { target_dir } => execute_workspace(target_dir, ctx),
            CiSubcommand::SchemaOnly {
                target_dir,
                skip_clean,
            } => execute_schema_only(target_dir, *skip_clean, ctx),
            CiSubcommand::SchemaSync { target_dir } => execute_schema_sync(target_dir, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::test() // CI commands are testing-related, 10 min timeout
    }
}

// RAII guard for Postgres instance cleanup
struct PgInstance {
    data_dir: PathBuf,
}

impl Drop for PgInstance {
    fn drop(&mut self) {
        if let Some(data_dir) = self.data_dir.to_str() {
            let _ = pg_command("pg_ctl").args(["-D", data_dir, "stop"]).status();
        }
    }
}

#[derive(Clone)]
struct PgEnv {
    host: String,
    port: u16,
    superuser: String,
    app_user: String,
    database: String,
    operation_id: String,
}

#[allow(clippy::too_many_arguments)]
fn execute_postgres(
    port: u16,
    data_dir: Option<PathBuf>,
    socket_dir: Option<PathBuf>,
    keep_data: bool,
    app_user: &str,
    superuser: &str,
    database: &str,
    operation_id: &str,
    command: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("ci postgres");

    let data_dir = data_dir.unwrap_or_else(|| PathBuf::from("target/ci-pgdata"));
    let socket_dir = socket_dir.unwrap_or_else(|| env::current_dir().unwrap());
    let host = "127.0.0.1".to_string();

    if data_dir.exists() && !keep_data {
        fs::remove_dir_all(&data_dir)?;
    }
    fs::create_dir_all(&data_dir)?;

    let initdb_needed = !data_dir.join("PG_VERSION").exists();
    if initdb_needed {
        run_cmd("initdb", {
            let mut c = pg_command("initdb");
            c.args(["--auth=trust", "--no-locale", "--encoding=UTF8", "-D"])
                .arg(&data_dir);
            c
        })?;

        let mut conf = fs::OpenOptions::new()
            .append(true)
            .open(data_dir.join("postgresql.conf"))?;
        writeln!(conf, "unix_socket_directories = '{}'", socket_dir.display())?;
        writeln!(conf, "listen_addresses = '127.0.0.1'")?;
        writeln!(conf, "port = {port}")?;
        // Tests assume a relatively high connection ceiling (NixOS module uses >=800). Keep the
        // ephemeral CI cluster aligned so parallel nextest runs don't wedge on connection limits.
        writeln!(conf, "max_connections = 800")?;
        writeln!(conf, "shared_preload_libraries = 'timescaledb'")?;
    }

    let log_path = data_dir.join("postgres.log");
    run_cmd("pg_ctl start", {
        let mut c = pg_command("pg_ctl");
        c.args(["-D", data_dir.to_str().unwrap(), "start", "-w"])
            .arg("-l")
            .arg(&log_path)
            .arg("-o")
            .arg(format!("-k {} -p {}", socket_dir.display(), port));
        c
    })?;
    let pg_guard = PgInstance {
        data_dir: data_dir.clone(),
    };

    let env = PgEnv {
        host: host.clone(),
        port,
        superuser: superuser.to_string(),
        app_user: app_user.to_string(),
        database: database.to_string(),
        operation_id: operation_id.to_string(),
    };

    // `initdb` creates the bootstrap superuser role using the OS username, not `PGUSER`.
    // In CI, our devenv sets `PGUSER=sinity` by default, but that role doesn't exist yet
    // for a fresh ephemeral cluster, so prefer `USER`.
    let initial_user = env::var("USER").unwrap_or_else(|_| superuser.to_string());

    create_role_if_missing(&env, superuser, true, &initial_user)?;
    create_role_if_missing(&env, app_user, true, superuser)?;
    set_operation_id_default(&env)?;
    ensure_database(&env)?;
    ensure_extensions(&env)?;
    ensure_schema_grants(&env)?;

    let app_url = format!("postgresql://{app_user}@{host}:{port}/{database}");
    let super_url = format!("postgresql://{superuser}@{host}:{port}/{database}");

    let Some(program) = command.first() else {
        bail!("ci postgres requires a command to run");
    };

    if ctx.is_human() {
        println!("Running command: {command:?}");
    }

    let mut cmd = Command::new(program);
    cmd.args(&command[1..])
        .env("PGHOST", &host)
        .env("PGPORT", port.to_string())
        .env("PGDATA", &data_dir)
        .env("PGUSER", app_user)
        .env("DATABASE_URL", &app_url)
        .env("DATABASE_URL_APP", &app_url)
        .env("DATABASE_URL_SUPERUSER", &super_url)
        .env("SUPERUSER", superuser)
        .env("SINEX_OPERATION_ID", operation_id);

    let status = cmd
        .status()
        .with_context(|| format!("failed to run {command:?}"))?;

    drop(pg_guard);

    if !status.success() {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "COMMAND_FAILED".to_string(),
            message: format!("Command {command:?} failed with status {status}"),
            location: None,
            suggestion: Some("Check command output for details".to_string()),
        })
        .with_duration(ctx.elapsed()));
    }

    Ok(CommandResult::success()
        .with_message("Successfully ran command with ephemeral Postgres".to_string())
        .with_detail(format!("Port: {port}"))
        .with_detail(format!("Database: {database}"))
        .with_duration(ctx.elapsed()))
}

fn execute_workspace(target_dir: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("ci workspace");

    // Run schema setup first
    execute_schema_only(target_dir, false, ctx)?;

    // Ensure formatting, compilation, and clippy all pass before we spend time on e2e suites.
    if ctx.is_human() {
        println!("Running check...");
    }
    let check_result = crate::commands::check::CheckCommand {
        skip_fmt: false,
        lint: true,
        forbidden: true,
        heavy: false,
        affected: false,
        all: true, // CI should check all packages
        packages: vec![],
        skip_tests: false, // CI should always check tests
    }
    .execute(ctx)?;
    if !check_result.is_success() {
        return Ok(check_result);
    }

    // Lint is now part of check command, skip the separate lint step

    if ctx.is_human() {
        println!("Running lint-forbidden...");
    }
    let forbidden_result = crate::commands::lint_forbidden::LintForbiddenCommand {}.execute(ctx)?;
    if !forbidden_result.is_success() {
        return Ok(forbidden_result);
    }

    if ctx.is_human() {
        println!("Running E2E tests...");
    }
    run_cmd_ctx(
        "xtask test e2e fast",
        {
            let mut c = Command::new("cargo");
            c.args([
                "xtask",
                "test",
                "--profile",
                "fast",
                "--",
                "-p",
                "sinex-e2e-tests",
            ]);
            c
        },
        ctx,
    )?;

    if ctx.is_human() {
        println!("Running full test suite...");
    }
    run_cmd_ctx(
        "xtask test ci",
        {
            let mut c = Command::new("cargo");
            c.args(["xtask", "test", "--profile", "ci", "--prime"]);
            c
        },
        ctx,
    )?;

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

    env::set_var("CARGO_TARGET_DIR", target_dir);
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    if ctx.is_human() {
        println!("Running migrations...");
    }
    run_cmd("migrate", {
        let mut c = Command::new("cargo");
        c.args([
            "run",
            "--manifest-path",
            "crate/lib/sinex-schema/Cargo.toml",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .env("DATABASE_URL", &super_url);
        c
    })?;

    if ctx.is_human() {
        println!("Checking schema readiness...");
    }
    run_cmd("schema check-ready", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "check-ready"]);
        c
    })?;

    if ctx.is_human() {
        println!("Generating schemas...");
    }
    run_cmd("schema generate", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "generate"]);
        c
    })?;

    if !skip_clean {
        if ctx.is_human() {
            println!("Verifying schema cleanliness...");
        }
        ensure_schemas_clean()?;
    }

    Ok(CommandResult::success()
        .with_message("Schema validation passed")
        .with_detail("Migrations: ✓")
        .with_detail("Schema check-ready: ✓")
        .with_detail("Schema generate: ✓")
        .with_detail(if skip_clean {
            "Cleanliness check: skipped"
        } else {
            "Cleanliness check: ✓"
        })
        .with_duration(ctx.elapsed()))
}

fn execute_schema_sync(target_dir: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("ci schema-sync");

    env::set_var("CARGO_TARGET_DIR", target_dir);
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    if ctx.is_human() {
        println!("Running migrations...");
    }
    run_cmd("migrate", {
        let mut c = Command::new("cargo");
        c.args([
            "run",
            "--manifest-path",
            "crate/lib/sinex-schema/Cargo.toml",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .env("DATABASE_URL", &super_url);
        c
    })?;

    if ctx.is_human() {
        println!("Checking schema readiness...");
    }
    run_cmd("schema check-ready", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "check-ready"]);
        c
    })?;

    let db_url = env::var("DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    if ctx.is_human() {
        println!("Seeding test schema entries...");
    }
    psql_exec(
        &db_url,
        "INSERT INTO sinex_schemas.event_payload_schemas (source, event_type, schema_version, schema_content, content_hash)\n\
         VALUES ('test.source', 'test.event', '1.0.0', '{}'::jsonb, md5(random()::text))\n\
         ON CONFLICT (source, event_type, schema_version) DO NOTHING;",
    )?;
    psql_exec(
        &db_url,
        "UPDATE sinex_schemas.event_payload_schemas SET is_active = true\n\
         WHERE source = 'test.source' AND event_type = 'test.event';",
    )?;
    psql_exec(
        &db_url,
        "SELECT COUNT(*) FROM sinex_schemas.event_payload_schemas WHERE source = 'test.source';",
    )?;

    let tmp_dir = tempfile::tempdir()?;
    if ctx.is_human() {
        println!("Running schema sync test...");
    }
    schema_generate(
        tmp_dir
            .path()
            .to_str()
            .ok_or_else(|| anyhow!("temp dir path is not valid UTF-8"))?,
        true,
    )?;

    Ok(CommandResult::success()
        .with_message("Schema sync validation passed")
        .with_detail("Migrations: ✓")
        .with_detail("Schema check-ready: ✓")
        .with_detail("Test schema seeding: ✓")
        .with_detail("Schema sync: ✓")
        .with_duration(ctx.elapsed()))
}

// Helper functions

fn run_cmd(name: &str, mut cmd: Command) -> Result<()> {
    let status = cmd
        .status()
        .with_context(|| format!("{name} failed to spawn"))?;
    if !status.success() {
        return Err(anyhow!("{name} failed with status {status}"));
    }
    Ok(())
}

fn run_cmd_ctx(name: &str, mut cmd: Command, _ctx: &CommandContext) -> Result<()> {
    let status = cmd
        .status()
        .with_context(|| format!("{name} failed to spawn"))?;
    if !status.success() {
        return Err(anyhow!("{name} failed with status {status}"));
    }
    Ok(())
}

fn pg_command(binary: &str) -> Command {
    if let Ok(prefix) = env::var("SINEX_PG_BIN") {
        let mut path = PathBuf::from(prefix);
        path.push(binary);
        Command::new(path)
    } else {
        Command::new(binary)
    }
}

fn psql(env: &PgEnv, user: &str, database: &str, sql: &str) -> Result<String> {
    let output = pg_command("psql")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-h")
        .arg(&env.host)
        .arg("-p")
        .arg(env.port.to_string())
        .arg("-d")
        .arg(database)
        .arg("-tAc")
        .arg(sql)
        .env("PGUSER", user)
        .output()
        .with_context(|| format!("failed to run psql for query {sql}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "psql exited with status {} for query {sql}\n{}",
            output.status,
            stderr.trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn create_role_if_missing(env: &PgEnv, role: &str, superuser: bool, runner: &str) -> Result<()> {
    let exists = psql(
        env,
        runner,
        "postgres",
        &format!("SELECT 1 FROM pg_roles WHERE rolname = '{role}'"),
    )?;
    if exists.is_empty() {
        let mut stmt = format!("CREATE ROLE {role} LOGIN");
        if superuser {
            stmt.push_str(" SUPERUSER CREATEDB");
        }
        psql(env, runner, "postgres", &stmt)?;
    }
    Ok(())
}

fn set_operation_id_default(env: &PgEnv) -> Result<()> {
    let stmt = format!(
        "ALTER ROLE {} SET sinex.operation_id = '{}';",
        env.app_user, env.operation_id
    );
    psql(env, &env.superuser, "postgres", &stmt)?;
    Ok(())
}

fn ensure_database(env: &PgEnv) -> Result<()> {
    let exists = psql(
        env,
        &env.superuser,
        "postgres",
        &format!(
            "SELECT 1 FROM pg_database WHERE datname = '{}'",
            env.database
        ),
    )?;
    if exists.is_empty() {
        psql(
            env,
            &env.superuser,
            "postgres",
            &format!("CREATE DATABASE {} OWNER {};", env.database, env.app_user),
        )?;
    }
    Ok(())
}

fn ensure_extensions(env: &PgEnv) -> Result<()> {
    let candidates: &[(&[&str], bool)] = &[
        (&["pgx_ulid", "ulid"], true),
        (&["pg_jsonschema"], true),
        (&["timescaledb"], true),
        (&["vector"], true),
    ];
    for &(names, required) in candidates {
        let mut installed = false;
        for name in names {
            let available = psql(
                env,
                &env.superuser,
                &env.database,
                &format!("SELECT 1 FROM pg_available_extensions WHERE name = '{name}'"),
            )?;
            if available.is_empty() {
                continue;
            }
            psql(
                env,
                &env.superuser,
                &env.database,
                &format!("CREATE EXTENSION IF NOT EXISTS {name};"),
            )?;
            installed = true;
            break;
        }
        if !installed && required {
            bail!(
                "None of the requested extensions {names:?} are available in this PostgreSQL build"
            );
        }
    }
    Ok(())
}

fn ensure_schema_grants(env: &PgEnv) -> Result<()> {
    let schemas = schema_list()?;
    for schema in schemas {
        grant_schema(env, &schema)?;
    }
    Ok(())
}

fn schema_list() -> Result<Vec<String>> {
    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg("crate/lib/sinex-schema/Cargo.toml")
        .arg("--bin")
        .arg("schema-info")
        .arg("--")
        .arg("list-schemas")
        .output()
        .with_context(|| "failed to run schema-info list-schemas")?;
    if !output.status.success() {
        bail!(
            "schema-info list-schemas failed with status {}",
            output.status
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().map(str::to_string).collect())
}

fn grant_schema(env: &PgEnv, schema: &str) -> Result<()> {
    let stmts = [
        format!("CREATE SCHEMA IF NOT EXISTS {schema};"),
        format!("GRANT USAGE ON SCHEMA {schema} TO {};", env.app_user),
        format!(
            "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {};",
            env.app_user
        ),
        format!(
            "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {};",
            env.app_user
        ),
        format!(
            "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {};",
            env.app_user
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {schema} GRANT ALL PRIVILEGES ON TABLES TO {};",
            env.superuser, env.app_user
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {schema} GRANT ALL PRIVILEGES ON SEQUENCES TO {};",
            env.superuser, env.app_user
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {};",
            env.superuser, env.app_user
        ),
    ];
    for stmt in stmts {
        psql(env, &env.superuser, &env.database, &stmt)?;
    }
    Ok(())
}

fn psql_exec(db_url: &str, sql: &str) -> Result<()> {
    let output = pg_command("psql")
        .arg(db_url)
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-c")
        .arg(sql)
        .output()
        .with_context(|| "failed to run psql")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "psql exited with status {} for SQL:\n{}\n{}",
            output.status,
            sql,
            stderr.trim()
        );
    }
    Ok(())
}

pub fn ensure_schemas_clean() -> Result<()> {
    let status = Command::new("git")
        .args(["diff", "--quiet", "--", "schemas"])
        .status()
        .with_context(|| "git diff -- schemas failed")?;
    if status.success() {
        return Ok(());
    }
    let code = status.code().unwrap_or_default();
    if code == 1 {
        bail!("Schema artifacts are stale. Run 'cargo xtask schema generate'.");
    }
    bail!("git diff -- schemas failed with status {status}");
}

pub fn schema_generate(output: &str, sync: bool) -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg("crate/lib/sinex-schema/Cargo.toml")
        .arg("--bin")
        .arg("sinex-schema")
        .arg("--")
        .arg("generate")
        .arg("--output")
        .arg(output);
    if sync {
        cmd.arg("--sync");
    }
    run_cmd("schema generate", cmd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputWriter;

    #[test]
    fn test_command_name() {
        let cmd = CiCommand {
            subcommand: CiSubcommand::Workspace {
                target_dir: "target-ci".to_string(),
            },
        };
        assert_eq!(cmd.name(), "ci");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = CiCommand {
            subcommand: CiSubcommand::SchemaOnly {
                target_dir: "target-ci".to_string(),
                skip_clean: false,
            },
        };
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("test".to_string()));
        assert!(metadata.timeout.is_some());
    }

    #[test]
    fn test_clone() {
        let cmd = CiCommand {
            subcommand: CiSubcommand::SchemaSync {
                target_dir: "target-ci".to_string(),
            },
        };
        let cloned = cmd.subcommand;
        match cloned {
            CiSubcommand::SchemaSync { target_dir } => {
                assert_eq!(target_dir, "target-ci");
            }
            _ => panic!("Expected SchemaSync variant"),
        }
    }

    #[test]
    fn test_postgres_requires_command() {
        let cmd = CiCommand {
            subcommand: CiSubcommand::Postgres {
                port: 55432,
                data_dir: None,
                socket_dir: None,
                keep_data: false,
                app_user: "sinity".to_string(),
                superuser: "postgres".to_string(),
                database: "sinex_dev".to_string(),
                operation_id: "test".to_string(),
                command: vec![], // Empty command
            },
        };

        let ctx = CommandContext::new(OutputWriter::new(crate::output::OutputFormat::Silent));
        let result = cmd.execute(&ctx);
        // Should fail because command is empty
        assert!(result.is_err());
    }

    #[test]
    fn test_subcommand_variants() {
        // Test that all subcommands can be constructed
        let _postgres = CiSubcommand::Postgres {
            port: 5432,
            data_dir: None,
            socket_dir: None,
            keep_data: false,
            app_user: "user".to_string(),
            superuser: "postgres".to_string(),
            database: "db".to_string(),
            operation_id: "test".to_string(),
            command: vec!["echo".to_string()],
        };

        let _workspace = CiSubcommand::Workspace {
            target_dir: "target".to_string(),
        };

        let _schema_only = CiSubcommand::SchemaOnly {
            target_dir: "target".to_string(),
            skip_clean: true,
        };

        let _schema_sync = CiSubcommand::SchemaSync {
            target_dir: "target".to_string(),
        };
    }
}
