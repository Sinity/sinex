use anyhow::{anyhow, bail, Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, shells};
use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::NamedTempFile;

#[derive(Parser)]
#[command(author, version, about = "Developer tasks for the Sinex workspace")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fast correctness checks (sqlx check, fmt check, cargo check)
    Check {
        /// Skip sqlx metadata check
        #[arg(long)]
        skip_sqlx: bool,
        /// Skip fmt check
        #[arg(long)]
        skip_fmt: bool,
        /// Skip cargo check
        #[arg(long)]
        skip_check: bool,
    },
    /// Clippy lint with -D warnings
    Lint,
    /// Run nextest (reliable profile by default)
    Test {
        /// Disable SQLX_OFFLINE
        #[arg(long)]
        online_sqlx: bool,
        /// Nextest profile (default: reliable)
        #[arg(long, default_value = "reliable")]
        profile: String,
    },
    /// Regenerate .sqlx metadata (requires DB access)
    SqlxPrepare,
    /// Check .sqlx metadata without rewriting it
    SqlxCheck {
        /// Disable SQLX_OFFLINE
        #[arg(long)]
        online: bool,
    },
    /// Database utilities (setup/migrate/status/sqlx-prepare)
    Db {
        #[command(subcommand)]
        cmd: DbCommand,
    },
    /// Schema helpers (generate/deploy/compatibility)
    Schema {
        #[command(subcommand)]
        cmd: SchemaCommand,
    },
    /// Forbidden pattern guard (tokio::test, #[test], raw sqlx::query)
    LintForbidden,
    /// Quick CI preflight: sqlx-check, clippy, nextest reliable (offline)
    CiPreflight,
    /// Environment/health report (toolchain, sccache, Postgres, schema)
    Doctor,
    /// Generate shell completions for xtask
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
    /// CI helpers (Postgres bootstrap, workspace pipelines)
    Ci {
        #[command(subcommand)]
        cmd: CiCommand,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Shell {
    Bash,
    Zsh,
    Fish,
    PowerShell,
}

#[derive(Subcommand)]
enum CiCommand {
    /// Start an ephemeral Postgres and run the given command with env vars set
    Postgres {
        /// Port for Postgres
        #[arg(long, default_value_t = 55432)]
        port: u16,
        /// Data directory (defaults to target/ci-pgdata)
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Unix socket directory (defaults to repository root)
        #[arg(long)]
        socket_dir: Option<PathBuf>,
        /// Keep existing PGDATA if present
        #[arg(long, default_value_t = false)]
        keep_data: bool,
        /// Application user to create
        #[arg(long, default_value = "sinity")]
        app_user: String,
        /// Superuser role (created if missing)
        #[arg(long, default_value = "postgres")]
        superuser: String,
        /// Database name
        #[arg(long, default_value = "sinex_dev")]
        database: String,
        /// Default sinex.operation_id for the app user
        #[arg(long, default_value = "ci-tests")]
        operation_id: String,
        /// Command to run once Postgres is ready
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
    /// Full CI pipeline (migrate, schema check, lint-forbidden, tests)
    Workspace {
        /// Target directory for build artifacts
        #[arg(long, default_value = "target-ci")]
        target_dir: String,
    },
    /// Schema-only pipeline (migrate, check-ready, regenerate)
    SchemaOnly {
        /// Target directory for build artifacts
        #[arg(long, default_value = "target-ci")]
        target_dir: String,
        /// Skip schema cleanliness diff check
        #[arg(long, default_value_t = false)]
        skip_clean: bool,
    },
    /// Schema validation pipeline (migrate, check-ready, seed registry, sync)
    SchemaSync {
        /// Target directory for build artifacts
        #[arg(long, default_value = "target-ci")]
        target_dir: String,
    },
}

#[derive(Subcommand)]
enum SchemaCommand {
    /// Generate schemas from EventPayload types
    Generate {
        /// Output directory
        #[arg(long, default_value = "schemas/v1")]
        output: String,
        /// Also sync to database
        #[arg(long)]
        sync: bool,
    },
    /// Deploy schemas to the database (requires DATABASE_URL)
    Deploy {
        /// Input directory
        #[arg(long, default_value = "schemas/v1")]
        input: String,
    },
    /// Compatibility check against a base branch
    Compat {
        /// Base branch (defaults to CI_BASE_BRANCH or origin default)
        #[arg(long)]
        base: Option<String>,
        /// Glob of schema files to check
        #[arg(long, default_value = "schemas/**/*.json")]
        glob: String,
    },
    /// Sanity check that core schema tables exist
    CheckReady {
        /// Database name
        #[arg(long)]
        database: Option<String>,
        /// Superuser (defaults to SUPERUSER or postgres)
        #[arg(long)]
        superuser: Option<String>,
    },
}

#[derive(Subcommand)]
enum DbCommand {
    /// Check Postgres reachability and report current database
    Status,
    /// Apply migrations using sinex-schema migrator
    Migrate,
    /// Create database if missing, then migrate
    Setup,
    /// Drop and recreate database, then migrate (dangerous; requires --yes)
    Reset {
        #[arg(long)]
        yes: bool,
    },
    /// Regenerate .sqlx metadata
    PrepareSqlx,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Check {
            skip_sqlx,
            skip_fmt,
            skip_check,
        } => check(skip_sqlx, skip_fmt, skip_check),
        Commands::Lint => lint(),
        Commands::Test {
            online_sqlx,
            profile,
        } => test(online_sqlx, &profile),
        Commands::SqlxPrepare => sqlx_prepare(),
        Commands::SqlxCheck { online } => sqlx_check(online),
        Commands::Db { cmd } => db(cmd),
        Commands::Schema { cmd } => schema(cmd),
        Commands::LintForbidden => lint_forbidden(),
        Commands::CiPreflight => ci_preflight(),
        Commands::Doctor => doctor(),
        Commands::Completions { shell } => completions(shell),
        Commands::Ci { cmd } => ci(cmd),
    }
}

fn heading(title: &str) {
    println!("========== {title} ==========");
}

fn run_cmd(name: &str, mut cmd: Command) -> Result<()> {
    heading(name);
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

fn sqlx_check(online: bool) -> Result<()> {
    let mut c = Command::new("cargo");
    if !online {
        c.env("SQLX_OFFLINE", "1");
    }
    c.arg("sqlx")
        .arg("prepare")
        .arg("--workspace")
        .arg("--check")
        .arg("--")
        .arg("--all-targets")
        .arg("--all-features");
    run_cmd("sqlx prepare --check", c)
}

fn check(skip_sqlx: bool, skip_fmt: bool, skip_check: bool) -> Result<()> {
    if !skip_sqlx {
        sqlx_check(false)?;
    }

    if !skip_fmt {
        let mut fmt = Command::new("cargo");
        fmt.arg("fmt").arg("--all").arg("--").arg("--check");
        run_cmd("cargo fmt --check", fmt)?;
    }

    if !skip_check {
        let mut chk = Command::new("cargo");
        chk.arg("check").arg("--workspace").arg("--all-features");
        run_cmd("cargo check", chk)?;
    }

    Ok(())
}

fn lint() -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("clippy")
        .arg("--workspace")
        .arg("--all-targets")
        .arg("--all-features")
        .arg("--")
        .arg("-D")
        .arg("warnings");
    run_cmd("cargo clippy -D warnings", cmd)
}

fn test(online_sqlx: bool, profile: &str) -> Result<()> {
    let mut cmd = Command::new("cargo");
    if !online_sqlx {
        cmd.env("SQLX_OFFLINE", "1");
    }
    cmd.arg("nextest")
        .arg("run")
        .arg("--workspace")
        .arg("--profile")
        .arg(profile);
    run_cmd("nextest", cmd)
}

fn sqlx_prepare() -> Result<()> {
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    // Ensure the DB schema is up-to-date before validating queries.
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

    let mut c = Command::new("cargo");
    c.arg("sqlx")
        .arg("prepare")
        .arg("--workspace")
        .arg("--")
        .arg("--all-targets")
        .arg("--all-features");
    // `cargo sqlx prepare` should validate queries against a live DB, regardless of any global CI flag.
    c.env_remove("SQLX_OFFLINE");
    run_cmd("cargo sqlx prepare", c)
}

fn ci_preflight() -> Result<()> {
    sqlx_check(false)?;
    lint()?;
    test(false, "reliable")
}

fn doctor() -> Result<()> {
    heading("toolchain");
    run_cmd("rustc --version", {
        let mut c = Command::new("rustc");
        c.arg("--version");
        c
    })
    .ok();
    run_cmd("cargo --version", {
        let mut c = Command::new("cargo");
        c.arg("--version");
        c
    })
    .ok();

    heading("sccache");
    if let Err(err) = run_cmd("sccache --show-stats", {
        let mut c = Command::new("sccache");
        c.arg("--show-stats");
        c
    }) {
        eprintln!("sccache not available: {err}");
    }

    heading("postgres reachability");
    let pg_ok = pg_command("psql")
        .args(["-c", "select 1"])
        .status()
        .ok()
        .map(|s| s.success())
        .unwrap_or(false);
    println!("Postgres reachable: {}", if pg_ok { "yes" } else { "no" });

    Ok(())
}

fn completions(shell: Shell) -> Result<()> {
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    match shell {
        Shell::Bash => generate(shells::Bash, &mut cmd, name, &mut std::io::stdout()),
        Shell::Zsh => generate(shells::Zsh, &mut cmd, name, &mut std::io::stdout()),
        Shell::Fish => generate(shells::Fish, &mut cmd, name, &mut std::io::stdout()),
        Shell::PowerShell => generate(shells::PowerShell, &mut cmd, name, &mut std::io::stdout()),
    }
    Ok(())
}

fn ci(cmd: CiCommand) -> Result<()> {
    match cmd {
        CiCommand::Postgres {
            port,
            data_dir,
            socket_dir,
            keep_data,
            app_user,
            superuser,
            database,
            operation_id,
            command,
        } => ci_postgres(
            port,
            data_dir,
            socket_dir,
            keep_data,
            app_user,
            superuser,
            database,
            operation_id,
            command,
        ),
        CiCommand::Workspace { target_dir } => ci_workspace(&target_dir),
        CiCommand::SchemaOnly {
            target_dir,
            skip_clean,
        } => ci_schema_only(&target_dir, skip_clean),
        CiCommand::SchemaSync { target_dir } => ci_schema_sync(&target_dir),
    }
}

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

fn ci_postgres(
    port: u16,
    data_dir: Option<PathBuf>,
    socket_dir: Option<PathBuf>,
    keep_data: bool,
    app_user: String,
    superuser: String,
    database: String,
    operation_id: String,
    command: Vec<String>,
) -> Result<()> {
    let data_dir = data_dir.unwrap_or_else(|| PathBuf::from("target/ci-pgdata"));
    let socket_dir = socket_dir.unwrap_or(env::current_dir()?);
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
        writeln!(conf, "port = {}", port)?;
        // Tests assume a relatively high connection ceiling (NixOS module uses >=500). Keep the
        // ephemeral CI cluster aligned so parallel nextest runs don't wedge on connection limits.
        writeln!(conf, "max_connections = 500")?;
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
        superuser: superuser.clone(),
        app_user: app_user.clone(),
        database: database.clone(),
        operation_id: operation_id.clone(),
    };

    // `initdb` creates the bootstrap superuser role using the OS username, not `PGUSER`.
    // In CI, our devenv sets `PGUSER=sinity` by default, but that role doesn't exist yet
    // for a fresh ephemeral cluster, so prefer `USER`.
    let initial_user = env::var("USER").unwrap_or_else(|_| superuser.clone());

    create_role_if_missing(&env, &superuser, true, &initial_user)?;
    create_role_if_missing(&env, &app_user, true, &superuser)?;
    set_operation_id_default(&env)?;
    ensure_database(&env)?;
    ensure_extensions(&env)?;
    ensure_schema_grants(&env)?;

    let app_url = format!("postgresql://{app_user}@{host}:{port}/{database}");
    let super_url = format!("postgresql://{superuser}@{host}:{port}/{database}");

    let Some(program) = command.first() else {
        bail!("ci postgres requires a command to run");
    };
    heading("ci command");
    let mut cmd = Command::new(program);
    cmd.args(&command[1..])
        .env("PGHOST", &host)
        .env("PGPORT", port.to_string())
        .env("PGDATA", &data_dir)
        .env("PGUSER", &app_user)
        .env("DATABASE_URL", &app_url)
        .env("DATABASE_URL_APP", &app_url)
        .env("DATABASE_URL_SUPERUSER", &super_url)
        .env("SUPERUSER", &superuser)
        .env("SINEX_OPERATION_ID", &operation_id)
        // CI often exports `SQLX_OFFLINE=1` globally, but anything run under `ci postgres`
        // should validate queries against the ephemeral DB.
        .env_remove("SQLX_OFFLINE");

    let status = cmd
        .status()
        .with_context(|| format!("failed to run {:?}", command))?;
    if !status.success() {
        bail!("command {:?} failed with status {status}", command);
    }
    drop(pg_guard);
    Ok(())
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
                &format!(
                    "SELECT 1 FROM pg_available_extensions WHERE name = '{}'",
                    name
                ),
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
                "None of the requested extensions {:?} are available in this PostgreSQL build",
                names
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

fn ci_schema_only(target_dir: &str, skip_clean: bool) -> Result<()> {
    env::set_var("CARGO_TARGET_DIR", target_dir);
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

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

    run_cmd("schema check-ready", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "check-ready"]);
        c
    })?;

    run_cmd("schema generate", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "generate"]);
        c
    })?;

    if !skip_clean {
        ensure_schemas_clean()?;
    }
    Ok(())
}

fn ci_schema_sync(target_dir: &str) -> Result<()> {
    env::set_var("CARGO_TARGET_DIR", target_dir);
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

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

    run_cmd("schema check-ready", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "check-ready"]);
        c
    })?;

    let db_url = env::var("DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

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
    schema_generate(
        tmp_dir
            .path()
            .to_str()
            .ok_or_else(|| anyhow!("temp dir path is not valid UTF-8"))?,
        true,
    )?;

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

fn ensure_schemas_clean() -> Result<()> {
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

fn ci_workspace(target_dir: &str) -> Result<()> {
    ci_schema_only(target_dir, false)?;

    run_cmd("lint forbidden patterns", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "lint-forbidden"]);
        c
    })?;

    run_cmd("nextest e2e fast", {
        let mut c = Command::new("cargo");
        c.args([
            "nextest",
            "run",
            "-p",
            "sinex-e2e-tests",
            "--profile",
            "fast",
        ]);
        // Use a smaller DB pool for fast e2e runs to keep first-time
        // template + pool initialization well under the per-test timeout.
        c.env("SINEX_TESTUTILS_POOL_SIZE", "8");
        c
    })?;

    run_cmd("xtask test reliable", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "test", "--online-sqlx", "--profile", "reliable"]);
        c
    })?;

    Ok(())
}

fn db(cmd: DbCommand) -> Result<()> {
    match cmd {
        DbCommand::Status => {
            heading("psql status");
            let status = Command::new("psql")
                .args(["-c", "select current_database(), current_user"])
                .status();
            match status {
                Ok(s) if s.success() => println!("Postgres reachable"),
                Ok(s) => anyhow::bail!("psql exited with status {s}"),
                Err(e) => anyhow::bail!("psql not available: {e}"),
            }
        }
        DbCommand::Migrate => run_db_migrate()?,
        DbCommand::Setup => {
            // Create DB if missing, then migrate.
            let db = std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string());
            let mut create = Command::new("createdb");
            create.arg(&db);
            if let Err(e) = create.status() {
                eprintln!("createdb failed or missing: {e}");
            }
            run_db_migrate()?;
        }
        DbCommand::Reset { yes } => {
            if !yes {
                anyhow::bail!("Refusing to drop DB without --yes");
            }
            let db = std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string());
            let mut drop = Command::new("psql");
            drop.args(["-c", &format!("DROP DATABASE IF EXISTS {db}")]);
            run_cmd("dropdb", drop)?;
            let mut create = Command::new("createdb");
            create.arg(&db);
            if let Err(e) = create.status() {
                eprintln!("createdb failed or missing: {e}");
            }
            run_db_migrate()?;
        }
        DbCommand::PrepareSqlx => {
            sqlx_prepare()?;
        }
    }
    Ok(())
}

fn run_db_migrate() -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.args(["run", "--package", "sinex-schema", "--", "up"]);
    run_cmd("cargo run -p sinex-schema -- up", cmd)
}

fn schema(cmd: SchemaCommand) -> Result<()> {
    match cmd {
        SchemaCommand::Generate { output, sync } => schema_generate(&output, sync),
        SchemaCommand::Deploy { input } => schema_deploy(&input),
        SchemaCommand::Compat { base, glob } => schema_compat(base, &glob),
        SchemaCommand::CheckReady {
            database,
            superuser,
        } => schema_check_ready(database, superuser),
    }
}

fn schema_generate(output: &str, sync: bool) -> Result<()> {
    let mut cmd = sinex_schema_cmd();
    cmd.arg("generate").arg("--output").arg(output);
    if sync {
        cmd.arg("--sync");
    }
    run_cmd("schema generate", cmd)
}

fn schema_deploy(input: &str) -> Result<()> {
    let db_url = env::var("DATABASE_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    ensure_psql()?;
    ensure_db_connection(&db_url)?;

    let required_exts = ["pg_jsonschema", "pgx_ulid", "timescaledb", "vector"];
    let mut missing = Vec::new();
    for ext in required_exts {
        if !psql_query_bool(
            &db_url,
            &format!("SELECT 1 FROM pg_extension WHERE extname='{ext}'"),
        )? {
            missing.push(ext);
        }
    }
    if !missing.is_empty() {
        bail!(
            "Missing extensions in target database: {}",
            missing.join(", ")
        );
    }

    let mut cmd = sinex_schema_cmd();
    cmd.arg("sync").arg("--input").arg(input);
    run_cmd("schema deploy", cmd)
}

fn schema_compat(base: Option<String>, glob: &str) -> Result<()> {
    // CI sometimes passes an empty base ref on branch pushes; treat that as "unspecified"
    let base_branch = base
        .or_else(|| env::var("CI_BASE_BRANCH").ok())
        .filter(|s| !s.trim().is_empty());

    let base = match base_branch {
        Some(b) => b,
        None => resolve_default_base_branch()?,
    };

    let diff_output = Command::new("git")
        .arg("diff")
        .arg("--name-only")
        .arg(format!("{base}...HEAD"))
        .arg("--")
        .arg(glob)
        .output()
        .with_context(|| "failed to run git diff for schema compat")?;

    let code = diff_output.status.code().unwrap_or_default();
    if code != 0 && code != 1 {
        bail!("git diff failed with status {}", diff_output.status);
    }

    let changed = String::from_utf8_lossy(&diff_output.stdout);
    if changed.trim().is_empty() {
        println!("✅ No schema edits detected");
        return Ok(());
    }

    println!("🔍 Checking compatibility for updated schemas against {base}:");
    println!("{changed}");

    let mut errors = 0;
    for file in changed.lines().filter(|l| !l.trim().is_empty()) {
        let path = Path::new(file);
        if !path.exists() {
            println!("⚠️  Skipping deleted schema {file}");
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
            println!("➕ New schema {file} (no backward check required)");
            continue;
        }

        let tmp = NamedTempFile::new()?;
        let old_contents = Command::new("git")
            .arg("show")
            .arg(&git_obj)
            .output()
            .with_context(|| format!("failed to read {git_obj}"))?;
        fs::write(tmp.path(), &old_contents.stdout)?;

        println!("Comparing {file} against {base}...");
        let mut cmd = sinex_schema_cmd();
        cmd.arg("validate").arg(tmp.path()).arg(path.as_os_str());
        let status = cmd
            .status()
            .with_context(|| format!("failed to spawn schema validate for {file}"))?;
        if !status.success() {
            errors += 1;
            eprintln!("❌ Compatibility regression detected in {file}");
        } else {
            println!("✅ {file} remains backward compatible");
        }
    }

    if errors > 0 {
        bail!("Schema compatibility check failed ({errors} issue(s))");
    }

    println!("✅ Schema compatibility check passed");
    Ok(())
}

fn schema_check_ready(database: Option<String>, superuser: Option<String>) -> Result<()> {
    ensure_psql()?;
    let db = database
        .or_else(|| env::var("DATABASE_NAME").ok())
        .or_else(|| env::var("PGDATABASE").ok())
        .unwrap_or_else(|| "sinex_dev".to_string());
    let superuser = superuser
        .or_else(|| env::var("SUPERUSER").ok())
        .unwrap_or_else(|| "postgres".to_string());

    let mut cmd = pg_command("psql");
    cmd.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('core.events') AS reg")
        .env("PGUSER", &superuser);
    let status = cmd
        .status()
        .with_context(|| "psql core.events check failed")?;
    if !status.success() {
        bail!("core.events missing in database {db}");
    }

    let mut cmd2 = pg_command("psql");
    cmd2.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('sinex_schemas.event_payload_schemas') AS reg")
        .env("PGUSER", &superuser);
    let status2 = cmd2
        .status()
        .with_context(|| "psql schema registry check failed")?;
    if !status2.success() {
        bail!("sinex_schemas.event_payload_schemas missing in database {db}");
    }

    println!("✅ core.events and sinex_schemas.event_payload_schemas are present");
    Ok(())
}

fn resolve_default_base_branch() -> Result<String> {
    let output = Command::new("git")
        .args(["symbolic-ref", "refs/remotes/origin/HEAD"])
        .output()
        .with_context(|| "failed to resolve origin/HEAD")?;
    if output.status.success() {
        let text = String::from_utf8_lossy(&output.stdout);
        let branch = text
            .trim()
            .strip_prefix("refs/remotes/origin/")
            .unwrap_or(text.trim());
        if !branch.is_empty() {
            return Ok(branch.to_string());
        }
    }
    Ok("master".to_string())
}

fn ensure_psql() -> Result<()> {
    let status = pg_command("psql")
        .arg("--version")
        .status()
        .with_context(|| "failed to spawn psql")?;
    if !status.success() {
        bail!("psql not available on PATH");
    }
    Ok(())
}

fn ensure_db_connection(db_url: &str) -> Result<()> {
    let status = pg_command("psql")
        .arg(db_url)
        .arg("-c")
        .arg("SELECT 1")
        .status()
        .with_context(|| format!("failed to connect to {db_url}"))?;
    if !status.success() {
        bail!("Unable to connect to {db_url}");
    }
    Ok(())
}

fn psql_query_bool(db_url: &str, query: &str) -> Result<bool> {
    let output = pg_command("psql")
        .arg(db_url)
        .args(["-Atqc", query])
        .output()
        .with_context(|| format!("failed to run psql query: {query}"))?;
    if !output.status.success() {
        bail!("psql exited with status {}", output.status);
    }
    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn sinex_schema_cmd() -> Command {
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("--quiet")
        .arg("--package")
        .arg("sinex-core")
        .arg("--bin")
        .arg("sinex-schema")
        .arg("--features")
        .arg("schema-manager")
        .arg("--");
    cmd
}

fn lint_forbidden() -> Result<()> {
    heading("forbidden pattern scan");
    let tokio_test_allow = [
        "crate/lib/sinex-test-utils/macros/src/lib.rs",
        "crate/lib/sinex-test-utils/tests/rstest_integration_example.rs",
        "crate/lib/sinex-test-utils/tests/database_pool_tests.rs",
        "crate/lib/sinex-test-utils/tests/channel_backpressure_test.rs",
        "crate/lib/sinex-test-utils/tests/select_cancellation_test.rs",
        "crate/core/sinex-ingestd/src/service.rs",
        "crate/lib/sinex-satellite-sdk/src/lifecycle.rs",
        "xtask/src/main.rs",
    ];
    let rust_test_allow = [
        "crate/lib/sinex-test-utils/macros/src/lib.rs",
        "crate/satellites/sinex-desktop-satellite/src/window_manager.rs",
        "crate/lib/sinex-core/src/db/sanitization.rs",
        "crate/core/sinex-ingestd/src/material_assembler.rs",
        "crate/core/sinex-gateway/src/native_messaging.rs",
        "crate/core/sinex-gateway/src/rpc_server.rs",
        "crate/lib/sinex-schema/src/schema_registry.rs",
        "crate/lib/sinex-test-utils/src/cleanup_config.rs",
        "crate/lib/sinex-test-utils/src/permissions.rs",
        "xtask/src/main.rs",
    ];
    let sqlx_query_allow = [
        "crate/core/sinex-gateway/src/cascade_analyzer.rs",
        "crate/lib/sinex-core/src/db/repositories/events.rs",
        "crate/lib/sinex-core/src/db/replay/state_machine.rs",
        "crate/lib/sinex-satellite-sdk/src/preflight/database.rs",
        "crate/lib/sinex-satellite-sdk/src/preflight/verification.rs",
        "crate/lib/sinex-test-utils/src/database_pool.rs",
        "crate/lib/sinex-test-utils/src/db_common.rs",
        "crate/lib/sinex-test-utils/src/fixture_generator.rs",
        "crate/lib/sinex-test-utils/src/fixtures.rs",
        "crate/lib/sinex-test-utils/src/session_guards.rs",
        "crate/lib/sinex-test-utils/src/permissions.rs",
        "xtask/src/main.rs",
    ];
    let sqlx_query_as_allow = [
        "crate/lib/sinex-core/src/db/repositories/common.rs",
        "crate/lib/sinex-satellite-sdk/src/preflight/database.rs",
        "xtask/src/main.rs",
    ];

    let mut violations: Vec<String> = Vec::new();
    violations.extend(check_pattern_strict(
        "#[tokio::test]",
        r"#\[tokio::test",
        &tokio_test_allow,
    )?);
    violations.extend(check_pattern_allow_tests(
        "#[test]",
        r"#\[test\]",
        &rust_test_allow,
    )?);
    violations.extend(check_pattern_allow_tests(
        "sqlx::query(",
        r"sqlx::query\(",
        &sqlx_query_allow,
    )?);
    violations.extend(check_pattern_allow_tests(
        "sqlx::query_as(",
        r"sqlx::query_as\(",
        &sqlx_query_as_allow,
    )?);

    if violations.is_empty() {
        println!("✅ No forbidden patterns found");
        return Ok(());
    }

    eprintln!("Forbidden pattern detected:");
    for v in &violations {
        eprintln!("  {v}");
    }
    bail!("forbidden pattern scan failed");
}

fn check_pattern_strict(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .map(|matches| filter_allowlist(matches, allow, |_| false))
        .with_context(|| format!("failed to scan for {label}"))
}

fn check_pattern_allow_tests(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .map(|matches| filter_allowlist(matches, allow, is_tests_path))
        .with_context(|| format!("failed to scan for {label}"))
}

fn run_rg(pattern: &str) -> Result<Vec<String>> {
    let output = Command::new("rg")
        .args([
            "--color=never",
            "--no-heading",
            "--with-filename",
            "--line-number",
            pattern,
            "--glob",
            "*.rs",
            "--glob",
            "!docs/agent/**",
        ])
        .output()
        .with_context(|| "failed to invoke ripgrep")?;
    let code = output.status.code().unwrap_or_default();
    if code != 0 && code != 1 {
        bail!("ripgrep failed with status {}", output.status);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().map(str::to_string).collect::<Vec<String>>())
}

fn filter_allowlist<F>(matches: Vec<String>, allow: &[&str], mut skip: F) -> Vec<String>
where
    F: FnMut(&str) -> bool,
{
    matches
        .into_iter()
        .filter(|line| {
            let file = line.split(':').next().unwrap_or_default();
            !allow.contains(&file) && !skip(file)
        })
        .collect()
}

fn is_tests_path(path: &str) -> bool {
    path.contains("/tests/") || path.starts_with("tests/")
}
