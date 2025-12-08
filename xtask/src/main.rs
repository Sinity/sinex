use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use walkdir::WalkDir;

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
    }
}

fn heading(title: &str) {
    println!("========== {title} ==========");
}

fn schema_paths() -> Vec<PathBuf> {
    let candidates = [
        PathBuf::from("crate/lib/sinex-schema/migrations"),
        PathBuf::from("schemas"),
    ];
    let mut files = Vec::new();
    for dir in candidates {
        if dir.exists() {
            for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
                if entry.file_type().is_file() {
                    files.push(entry.path().to_path_buf());
                }
            }
        }
    }
    files.sort();
    files
}

fn compute_schema_fingerprint() -> Result<String> {
    let mut hasher = Sha256::new();
    for path in schema_paths() {
        let content =
            fs::read(&path).with_context(|| format!("reading schema file {}", path.display()))?;
        hasher.update(path.to_string_lossy().as_bytes());
        hasher.update(&content);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn stamp_path() -> PathBuf {
    PathBuf::from(".sqlx/stamp")
}

fn read_stamp() -> Option<String> {
    fs::read_to_string(stamp_path()).ok()
}

fn write_stamp(fingerprint: &str) -> Result<()> {
    if let Some(parent) = stamp_path().parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(stamp_path(), fingerprint)?;
    Ok(())
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
    let fingerprint = compute_schema_fingerprint()?;
    if let Some(existing) = read_stamp() {
        if existing != fingerprint {
            return Err(anyhow!(
                "schema fingerprint changed; run `cargo xtask sqlx-prepare` to refresh .sqlx (or ensure DB is reachable)"
            ));
        }
    }

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
    let fingerprint = compute_schema_fingerprint()?;
    let mut c = Command::new("cargo");
    c.arg("sqlx")
        .arg("prepare")
        .arg("--workspace")
        .arg("--")
        .arg("--all-targets")
        .arg("--all-features");
    run_cmd("cargo sqlx prepare", c)?;
    write_stamp(&fingerprint)?;
    Ok(())
}
