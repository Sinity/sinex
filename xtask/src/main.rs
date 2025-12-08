use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::process::Command;

#[derive(Parser)]
#[command(author, version, about = "Developer tasks for the Sinex workspace")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fast correctness checks (sqlx metadata check, fmt, cargo check)
    Check {
        /// Skip sqlx metadata check
        #[arg(long)]
        skip_sqlx: bool,
    },
    /// Run full test suite with nextest (reliable profile, SQLX offline by default)
    Test {
        /// Disable SQLX_OFFLINE for the run
        #[arg(long)]
        online_sqlx: bool,
    },
    /// Regenerate .sqlx metadata (requires DB access)
    SqlxPrepare,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Check { skip_sqlx } => check(skip_sqlx),
        Commands::Test { online_sqlx } => test(online_sqlx),
        Commands::SqlxPrepare => sqlx_prepare(),
    }
}

fn run_cmd(name: &str, mut cmd: Command) -> Result<()> {
    println!("→ {name}: {:?}", cmd);
    let status = cmd
        .status()
        .with_context(|| format!("{name} failed to spawn"))?;
    if !status.success() {
        anyhow::bail!("{name} failed with status {status}");
    }
    Ok(())
}

fn check(skip_sqlx: bool) -> Result<()> {
    if !skip_sqlx {
        run_cmd("sqlx prepare --check", {
            let mut c = Command::new("cargo");
            c.env("SQLX_OFFLINE", "1")
                .arg("sqlx")
                .arg("prepare")
                .arg("--workspace")
                .arg("--check")
                .arg("--")
                .arg("--all-targets")
                .arg("--all-features");
            c
        })?;
    }

    run_cmd("cargo fmt --check", {
        let mut c = Command::new("cargo");
        c.arg("fmt").arg("--all").arg("--").arg("--check");
        c
    })?;

    run_cmd("cargo check", {
        let mut c = Command::new("cargo");
        c.arg("check").arg("--workspace").arg("--all-features");
        c
    })?;
    Ok(())
}

fn test(online_sqlx: bool) -> Result<()> {
    let mut cmd = Command::new("cargo");
    if !online_sqlx {
        cmd.env("SQLX_OFFLINE", "1");
    }
    cmd.arg("nextest")
        .arg("run")
        .arg("--workspace")
        .arg("--profile")
        .arg("reliable");
    run_cmd("nextest", cmd)
}

fn sqlx_prepare() -> Result<()> {
    run_cmd("cargo sqlx prepare", {
        let mut c = Command::new("cargo");
        c.arg("sqlx")
            .arg("prepare")
            .arg("--workspace")
            .arg("--")
            .arg("--all-targets")
            .arg("--all-features");
        c
    })
}
