use anyhow::{anyhow, bail, Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, shells};
use std::{env, fs, path::Path, process::Command};
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
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Shell {
    Bash,
    Zsh,
    Fish,
    PowerShell,
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
    let mut c = Command::new("cargo");
    c.arg("sqlx")
        .arg("prepare")
        .arg("--workspace")
        .arg("--")
        .arg("--all-targets")
        .arg("--all-features");
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
    let pg_ok = Command::new("psql")
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
    let base_branch = base.or_else(|| env::var("CI_BASE_BRANCH").ok());
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

    let mut cmd = Command::new("psql");
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

    let mut cmd2 = Command::new("psql");
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
    let status = Command::new("psql")
        .arg("--version")
        .status()
        .with_context(|| "failed to spawn psql")?;
    if !status.success() {
        bail!("psql not available on PATH");
    }
    Ok(())
}

fn ensure_db_connection(db_url: &str) -> Result<()> {
    let status = Command::new("psql")
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
    let output = Command::new("psql")
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
    ];
    let rust_test_allow = [
        "crate/lib/sinex-test-utils/macros/src/lib.rs",
        "crate/satellites/sinex-desktop-satellite/src/window_manager.rs",
        "crate/lib/sinex-core/src/db/sanitization.rs",
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
    ];
    let sqlx_query_as_allow = [
        "crate/lib/sinex-core/src/db/repositories/common.rs",
        "crate/lib/sinex-satellite-sdk/src/preflight/database.rs",
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
