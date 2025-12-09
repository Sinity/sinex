use anyhow::{anyhow, Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, shells};
use std::process::Command;

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
