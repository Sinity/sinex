//! Developer utilities - isolated stack management, hot reload, LLM generation.
//!
//! This module provides the unified development stack for sinex:
//! - Per-checkout isolated infrastructure (Postgres, NATS, git-annex)
//! - Hot reload with file watching
//! - Production event tethering
//! - LLM-based node generation

use anyhow::{bail, Context, Result};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config;
use crate::devtools::state::CheckoutState;
use crate::output::StructuredError;

// ─────────────────────────────────────────────────────────────────────────────
// Command Definitions
// ─────────────────────────────────────────────────────────────────────────────

/// Developer utilities command variants
#[derive(Debug, Clone)]
pub enum DevSubcommand {
    /// Manage the isolated development stack (Postgres, NATS, git-annex)
    Stack { cmd: StackSubcommand },
    /// Run a sinex binary with hot reload and lazy-start
    Run {
        binary: String,
        release: bool,
        no_watch: bool,
        tether: Option<String>,
        checkpoint: Option<PathBuf>,
        args: Vec<String>,
    },
    /// Build a processor crate
    Build { path: String, release: bool },
    /// Generate a SimpleProcessor from a natural language spec
    Generate {
        spec: String,
        name: Option<String>,
        dry_run: bool,
        workspace: String,
    },
    /// Snapshot management
    Snapshot { cmd: SnapshotSubcommand },
    /// Generate TLS fixtures for secure NATS tests
    TlsFixtures { output: String },
}

/// Stack management subcommands
#[derive(Debug, Clone)]
pub enum StackSubcommand {
    /// Start the isolated stack (Postgres, NATS, git-annex)
    Start,
    /// Stop the isolated stack
    Stop,
    /// Show stack status
    Status,
    /// Reset the stack (wipe all data)
    Reset { yes: bool },
    /// Create a named snapshot
    Snapshot { name: String },
    /// Restore from a named snapshot
    Restore { name: String },
    /// List available snapshots
    Snapshots,
}

/// Snapshot subcommands
#[derive(Debug, Clone)]
pub enum SnapshotSubcommand {
    /// Import from production
    PullProd {
        host: Option<String>,
        database: Option<String>,
    },
}

/// Developer utilities command
pub struct DevCommand {
    pub subcommand: DevSubcommand,
}

impl XtaskCommand for DevCommand {
    fn name(&self) -> &str {
        "dev"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            DevSubcommand::Stack { cmd } => execute_stack(cmd, ctx),
            DevSubcommand::Run {
                binary,
                release,
                no_watch,
                tether,
                checkpoint,
                args,
            } => execute_run(binary, *release, *no_watch, tether.clone(), checkpoint.clone(), args, ctx),
            DevSubcommand::Build { path, release } => execute_build(path, *release, ctx),
            DevSubcommand::Generate {
                spec,
                name,
                dry_run,
                workspace,
            } => execute_generate(spec, name.clone(), *dry_run, workspace, ctx),
            DevSubcommand::Snapshot { cmd } => execute_snapshot(cmd, ctx),
            DevSubcommand::TlsFixtures { output } => execute_tls_fixtures(output),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        match &self.subcommand {
            DevSubcommand::Stack { cmd } => match cmd {
                StackSubcommand::Start | StackSubcommand::Stop | StackSubcommand::Reset { .. } => {
                    CommandMetadata::build()
                }
                _ => CommandMetadata::utility(),
            },
            DevSubcommand::Run { .. } | DevSubcommand::Build { .. } => CommandMetadata::build(),
            DevSubcommand::Generate { .. } => CommandMetadata::build(),
            DevSubcommand::Snapshot { .. } | DevSubcommand::TlsFixtures { .. } => {
                CommandMetadata::utility()
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stack Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Stack configuration, now uses per-checkout state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackConfig {
    pub state_dir: PathBuf,
    pub postgres: PostgresConfig,
    pub nats: NatsConfig,
    pub annex: AnnexConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub port: u16,
    pub database: String,
    pub user: String,
    pub superuser: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    pub port: u16,
    pub jetstream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnexConfig {
    pub enable: bool,
    pub backend: String,
}

impl StackConfig {
    /// Create config for the current checkout with per-checkout state
    pub fn for_current_checkout() -> Result<Self> {
        let checkout_state = CheckoutState::for_current_checkout()?;
        Ok(Self::from_checkout_state(&checkout_state))
    }

    /// Create config from a CheckoutState
    pub fn from_checkout_state(state: &CheckoutState) -> Self {
        let port_offset = Self::port_offset_for_checkout(state.checkout_root());

        Self {
            state_dir: state.state_dir().to_path_buf(),
            postgres: PostgresConfig {
                port: 5433 + port_offset,
                database: "sinex_dev".to_string(),
                user: std::env::var("USER").unwrap_or_else(|_| "sinity".to_string()),
                superuser: "postgres".to_string(),
            },
            nats: NatsConfig {
                port: 4223 + port_offset,
                jetstream: true,
            },
            annex: AnnexConfig {
                enable: true,
                backend: "SHA256E".to_string(),
            },
        }
    }

    /// Generate a port offset based on checkout path hash (0-99)
    /// This helps avoid port conflicts between different worktrees
    fn port_offset_for_checkout(checkout_root: &Path) -> u16 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        checkout_root.hash(&mut hasher);
        (hasher.finish() % 100) as u16
    }

    /// Derived paths
    pub fn data_dir(&self) -> PathBuf {
        self.state_dir.join("data")
    }
    pub fn run_dir(&self) -> PathBuf {
        self.state_dir.join("run")
    }
    pub fn logs_dir(&self) -> PathBuf {
        self.run_dir().join("logs")
    }
    pub fn snapshots_dir(&self) -> PathBuf {
        self.state_dir.join("snapshots")
    }
    pub fn config_dir(&self) -> PathBuf {
        self.state_dir.join("config")
    }
    pub fn pg_data(&self) -> PathBuf {
        self.data_dir().join("postgres")
    }
    pub fn nats_data(&self) -> PathBuf {
        self.data_dir().join("nats")
    }
    pub fn annex_data(&self) -> PathBuf {
        self.data_dir().join("annex")
    }
    pub fn pg_pid_file(&self) -> PathBuf {
        self.run_dir().join("postgres.pid")
    }
    pub fn nats_pid_file(&self) -> PathBuf {
        self.run_dir().join("nats.pid")
    }
    pub fn nats_config(&self) -> PathBuf {
        self.config_dir().join("nats").join("nats.conf")
    }

    pub fn database_url(&self) -> String {
        format!(
            "postgresql:///{}?host={}&port={}",
            self.postgres.database,
            self.run_dir().display(),
            self.postgres.port
        )
    }

    pub fn nats_url(&self) -> String {
        format!("nats://localhost:{}", self.nats.port)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stack Status
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct StackStatus {
    pub initialized: bool,
    pub postgres: ServiceStatus,
    pub nats: ServiceStatus,
    pub annex: AnnexStatus,
    pub data_sizes: DataSizes,
    pub snapshots: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ServiceStatus {
    pub running: bool,
    pub pid: Option<u32>,
    pub port: u16,
}

#[derive(Debug, Serialize)]
pub struct AnnexStatus {
    pub initialized: bool,
    pub path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct DataSizes {
    pub postgres_bytes: u64,
    pub nats_bytes: u64,
    pub annex_bytes: u64,
}

impl StackStatus {
    pub fn gather(config: &StackConfig) -> Self {
        let initialized = config.state_dir.exists()
            && (config.pg_data().exists() || config.nats_data().exists());

        let postgres = ServiceStatus {
            running: is_process_running(&config.pg_pid_file()),
            pid: read_pid(&config.pg_pid_file()),
            port: config.postgres.port,
        };

        let nats = ServiceStatus {
            running: is_process_running(&config.nats_pid_file()),
            pid: read_pid(&config.nats_pid_file()),
            port: config.nats.port,
        };

        let annex = AnnexStatus {
            initialized: config.annex_data().join(".git").exists(),
            path: config.annex_data(),
        };

        let data_sizes = DataSizes {
            postgres_bytes: dir_size(&config.pg_data()),
            nats_bytes: dir_size(&config.nats_data()),
            annex_bytes: dir_size(&config.annex_data()),
        };

        let snapshots = list_snapshots(&config.snapshots_dir());

        Self {
            initialized,
            postgres,
            nats,
            annex,
            data_sizes,
            snapshots,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stack Operations
// ─────────────────────────────────────────────────────────────────────────────

fn execute_stack(cmd: &StackSubcommand, ctx: &CommandContext) -> Result<CommandResult> {
    let config = StackConfig::for_current_checkout()?;

    match cmd {
        StackSubcommand::Start => stack_start(&config, ctx),
        StackSubcommand::Stop => stack_stop(&config, ctx),
        StackSubcommand::Status => stack_status(&config, ctx),
        StackSubcommand::Reset { yes } => stack_reset(&config, *yes, ctx),
        StackSubcommand::Snapshot { name } => stack_snapshot(&config, name, ctx),
        StackSubcommand::Restore { name } => stack_restore(&config, name, ctx),
        StackSubcommand::Snapshots => stack_list_snapshots(&config, ctx),
    }
}

fn stack_start(config: &StackConfig, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("dev stack start");

    // Check for existing lock (multi-stack blocking)
    let checkout_state = CheckoutState::for_current_checkout()?;
    if let Some(lock_info) = checkout_state.is_locked_by_other()? {
        return Ok(CommandResult::failure(StructuredError {
            code: "STACK_LOCKED".to_string(),
            message: format!(
                "Another dev stack is already running:\n\n\
                 Checkout: {}\n\
                 PID: {}\n\
                 Started: {}\n\n\
                 Stop it first: cd {} && cargo xtask dev stack stop",
                lock_info.checkout_path.display(),
                lock_info.pid,
                lock_info.acquired_at.format("%Y-%m-%d %H:%M:%S UTC"),
                lock_info.checkout_path.display()
            ),
            location: None,
            suggestion: Some("Stop the other stack before starting a new one".to_string()),
        }));
    }

    // Acquire lock (writes lock file, guard is intentionally dropped)
    // Lock persists in file system until stack_stop calls release_lock()
    let _lock_guard = checkout_state.acquire_lock(Some("dev stack".to_string()))?;
    std::mem::forget(_lock_guard); // Don't auto-remove lock on drop

    // Ensure directories
    ensure_directories(config)?;

    // Initialize and start git-annex
    if config.annex.enable {
        annex_init(config, ctx)?;
    }

    // Initialize and start PostgreSQL
    pg_init(config, ctx)?;
    pg_start(config, ctx)?;
    pg_setup_database(config, ctx)?;
    pg_run_migrations(config, ctx)?;

    // Initialize and start NATS
    nats_generate_config(config, ctx)?;
    nats_start(config, ctx)?;

    // Note: lock is intentionally NOT dropped here - it persists while stack runs
    // The lock file will be cleaned up by stack_stop or if the process dies

    Ok(CommandResult::success()
        .with_message("Development stack started")
        .with_detail(format!("PostgreSQL: localhost:{}", config.postgres.port))
        .with_detail(format!("NATS: localhost:{}", config.nats.port))
        .with_detail(format!("Git-annex: {}", config.annex_data().display()))
        .with_detail(format!("DATABASE_URL={}", config.database_url())))
}

fn stack_stop(config: &StackConfig, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("dev stack stop");

    nats_stop(config, ctx)?;
    pg_stop(config, ctx)?;

    // Release the lock
    let checkout_state = CheckoutState::for_current_checkout()?;
    checkout_state.release_lock()?;

    Ok(CommandResult::success().with_message("Development stack stopped"))
}

fn stack_status(config: &StackConfig, ctx: &CommandContext) -> Result<CommandResult> {
    let status = StackStatus::gather(config);

    if !status.initialized {
        return Ok(CommandResult::success()
            .with_message("Stack not initialized")
            .with_detail("Run 'cargo xtask dev stack start' to initialize"));
    }

    let pg_status = if status.postgres.running {
        format!("running (pid {})", status.postgres.pid.unwrap_or(0))
    } else {
        "stopped".to_string()
    };

    let nats_status = if status.nats.running {
        format!("running (pid {})", status.nats.pid.unwrap_or(0))
    } else {
        "stopped".to_string()
    };

    let annex_status = if status.annex.initialized {
        "initialized"
    } else {
        "not initialized"
    };

    if ctx.is_human() {
        println!();
        println!("sinex-dev stack status (per-checkout)");
        println!("────────────────────────────────────────");
        println!("State dir:   {}", config.state_dir.display());
        println!(
            "PostgreSQL:  {}  (port: {})",
            pg_status, status.postgres.port
        );
        println!("NATS:        {}  (port: {})", nats_status, status.nats.port);
        println!("Git-annex:   {}", annex_status);
        println!("────────────────────────────────────────");
        println!("Data sizes:");
        println!(
            "  PostgreSQL: {}",
            human_size(status.data_sizes.postgres_bytes)
        );
        println!("  NATS:       {}", human_size(status.data_sizes.nats_bytes));
        println!(
            "  Git-annex:  {}",
            human_size(status.data_sizes.annex_bytes)
        );
        println!("  Snapshots:  {} saved", status.snapshots.len());
        println!();
    }

    Ok(CommandResult::success().with_message("Stack status retrieved"))
}

fn stack_reset(config: &StackConfig, yes: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if !yes {
        bail!("Reset requires --yes flag to confirm data deletion");
    }

    ctx.heading("dev stack reset");

    // Stop services first
    let _ = stack_stop(config, ctx);

    if ctx.is_human() {
        println!("Removing data directories...");
    }

    // Remove data
    let _ = fs::remove_dir_all(config.pg_data());
    let _ = fs::remove_dir_all(config.nats_data());
    let _ = fs::remove_dir_all(config.annex_data());
    let _ = fs::remove_file(config.nats_config());

    // Reinitialize
    stack_start(config, ctx)
}

fn stack_snapshot(config: &StackConfig, name: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("dev stack snapshot");

    // Sanitize name
    let safe_name: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let snapshot_path = config
        .snapshots_dir()
        .join(format!("{}.tar.zst", safe_name));

    if snapshot_path.exists() {
        bail!("Snapshot '{}' already exists", safe_name);
    }

    fs::create_dir_all(config.snapshots_dir())?;

    // Stop services for consistent snapshot
    let status = StackStatus::gather(config);
    let pg_was_running = status.postgres.running;
    let nats_was_running = status.nats.running;

    if pg_was_running {
        pg_stop(config, ctx)?;
    }
    if nats_was_running {
        nats_stop(config, ctx)?;
    }

    if ctx.is_human() {
        println!("Creating snapshot: {}", safe_name);
    }

    // Create tarball with zstd compression
    let tar_status = Command::new("tar")
        .args([
            "-C",
            config.state_dir.to_str().unwrap(),
            "-cf",
            "-",
            "config",
            "data",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .context("Failed to start tar")?;

    let zstd_status = Command::new("zstd")
        .args(["-T0", "-3", "-o", snapshot_path.to_str().unwrap()])
        .stdin(tar_status.stdout.unwrap())
        .status()
        .context("Failed to run zstd")?;

    if !zstd_status.success() {
        bail!("Snapshot compression failed");
    }

    let size = fs::metadata(&snapshot_path).map(|m| m.len()).unwrap_or(0);

    // Restart services if they were running
    if pg_was_running {
        pg_start(config, ctx)?;
    }
    if nats_was_running {
        nats_start(config, ctx)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Snapshot '{}' created", safe_name))
        .with_detail(format!("Size: {}", human_size(size)))
        .with_detail(format!("Path: {}", snapshot_path.display())))
}

fn stack_restore(config: &StackConfig, name: &str, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("dev stack restore");

    let safe_name: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let snapshot_path = config
        .snapshots_dir()
        .join(format!("{}.tar.zst", safe_name));

    if !snapshot_path.exists() {
        let available = list_snapshots(&config.snapshots_dir());
        return Ok(CommandResult::failure(StructuredError {
            code: "SNAPSHOT_NOT_FOUND".to_string(),
            message: format!("Snapshot '{}' not found", safe_name),
            location: None,
            suggestion: Some(format!("Available snapshots: {}", available.join(", "))),
        }));
    }

    // Stop services
    let _ = stack_stop(config, ctx);

    if ctx.is_human() {
        println!("Restoring from snapshot: {}", safe_name);
    }

    // Remove current data
    let _ = fs::remove_dir_all(config.pg_data());
    let _ = fs::remove_dir_all(config.nats_data());
    let _ = fs::remove_dir_all(config.annex_data());
    let _ = fs::remove_dir_all(config.config_dir());

    // Extract snapshot
    let zstd = Command::new("zstd")
        .args(["-d", "-c", snapshot_path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .spawn()
        .context("Failed to decompress snapshot")?;

    let tar_status = Command::new("tar")
        .args(["-C", config.state_dir.to_str().unwrap(), "-xf", "-"])
        .stdin(zstd.stdout.unwrap())
        .status()
        .context("Failed to extract snapshot")?;

    if !tar_status.success() {
        bail!("Snapshot extraction failed");
    }

    // Restart
    stack_start(config, ctx)
}

fn stack_list_snapshots(config: &StackConfig, ctx: &CommandContext) -> Result<CommandResult> {
    let snapshots = list_snapshots(&config.snapshots_dir());

    if ctx.is_human() {
        if snapshots.is_empty() {
            println!("No snapshots found");
        } else {
            println!("Available snapshots:");
            for name in &snapshots {
                let path = config.snapshots_dir().join(format!("{}.tar.zst", name));
                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                println!("  {} ({})", name, human_size(size));
            }
        }
    }

    Ok(CommandResult::success().with_message(format!("{} snapshots available", snapshots.len())))
}

// ─────────────────────────────────────────────────────────────────────────────
// Run Binary (with hot reload and lazy-start)
// ─────────────────────────────────────────────────────────────────────────────

fn execute_run(
    binary: &str,
    release: bool,
    no_watch: bool,
    tether: Option<String>,
    checkpoint: Option<PathBuf>,
    args: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let config = StackConfig::for_current_checkout()?;

    // Map short names to full binary names
    let full_binary = match binary {
        "ingestd" => "sinex-ingestd",
        "gateway" => "sinex-gateway",
        "fs" | "fs-ingestor" => "sinex-fs-ingestor",
        "terminal" | "terminal-ingestor" => "sinex-terminal-ingestor",
        "desktop" | "desktop-ingestor" => "sinex-desktop-ingestor",
        "system" | "system-ingestor" => "sinex-system-ingestor",
        "document" | "document-ingestor" => "sinex-document-ingestor",
        "canonicalizer" => "sinex-terminal-command-canonicalizer",
        "health" | "health-aggregator" => "sinex-health-aggregator",
        other => other,
    };

    // Lazy-start: Check if infrastructure is running, auto-start if not
    let status = StackStatus::gather(&config);
    if !status.postgres.running || !status.nats.running {
        if ctx.is_human() {
            println!("Infrastructure not running, auto-starting...");
        }
        stack_start(&config, ctx)?;
    }

    // If hot reload is enabled (default), use the orchestrator
    if !no_watch {
        // Use the orchestrator from dev module
        let workspace_root = Utf8PathBuf::try_from(config::workspace_root())
            .context("Workspace path is not valid UTF-8")?;

        // Prepare environment variables for the isolated stack
        let env_vars = vec![
            ("DATABASE_URL".to_string(), config.database_url()),
            ("PGHOST".to_string(), config.run_dir().to_string_lossy().to_string()),
            ("PGPORT".to_string(), config.postgres.port.to_string()),
            ("SINEX_NATS_URL".to_string(), config.nats_url()),
            ("SINEX_ANNEX_PATH".to_string(), config.annex_data().to_string_lossy().to_string()),
        ];

        let run_args = crate::devtools::orchestrator::RunArgs {
            binary: full_binary.to_string(),
            release,
            no_watch,
            tether,
            checkpoint,
            args: args.to_vec(),
            env_vars,
        };

        // Run with tokio runtime
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            crate::devtools::orchestrator::run_binary(run_args, workspace_root).await
        })?;

        return Ok(CommandResult::success().with_message(format!("{} exited", full_binary)));
    }

    // Simple run without hot reload
    let mut cmd = Command::new("cargo");
    cmd.arg("run");

    if release {
        cmd.arg("--release");
    }

    cmd.arg("-p").arg(full_binary).arg("--bin").arg(full_binary);

    if !args.is_empty() {
        cmd.arg("--");
        cmd.args(args);
    }

    // Set environment variables for isolated stack
    cmd.env("DATABASE_URL", config.database_url());
    cmd.env("PGHOST", config.run_dir());
    cmd.env("PGPORT", config.postgres.port.to_string());
    cmd.env("SINEX_NATS_URL", config.nats_url());
    cmd.env("SINEX_ANNEX_PATH", config.annex_data());

    if let Some(ref checkpoint_path) = checkpoint {
        cmd.env("SINEX_CHECKPOINT_FILE", checkpoint_path);
    }

    if let Some(ref tether_target) = tether {
        cmd.env("SINEX_TETHER_TARGET", tether_target);
    }

    if ctx.is_human() {
        println!("Running {} with isolated stack environment", full_binary);
        println!("  DATABASE_URL={}", config.database_url());
        println!("  SINEX_NATS_URL={}", config.nats_url());
        println!();
    }

    let cmd_status = cmd.status().context("Failed to run cargo")?;

    if cmd_status.success() {
        Ok(CommandResult::success().with_message(format!("{} exited successfully", full_binary)))
    } else {
        Ok(CommandResult::failure(StructuredError {
            code: "BINARY_FAILED".to_string(),
            message: format!("{} exited with status {}", full_binary, cmd_status),
            location: None,
            suggestion: None,
        }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Build Command
// ─────────────────────────────────────────────────────────────────────────────

fn execute_build(path: &str, release: bool, ctx: &CommandContext) -> Result<CommandResult> {
    let crate_path = Path::new(path);

    // Verify Cargo.toml exists
    let cargo_toml = crate_path.join("Cargo.toml");
    if !cargo_toml.exists() {
        bail!(
            "No Cargo.toml found at {}. Is this a Rust crate directory?",
            crate_path.display()
        );
    }

    if ctx.is_human() {
        println!(
            "Building {} ({})",
            crate_path.display(),
            if release { "release" } else { "debug" }
        );
    }

    let mut cmd = Command::new("cargo");
    cmd.arg("build");

    if release {
        cmd.arg("--release");
    }

    cmd.current_dir(crate_path);
    cmd.stdout(Stdio::inherit());
    cmd.stderr(Stdio::inherit());

    let status = cmd.status().context("Failed to execute cargo build")?;

    if status.success() {
        Ok(CommandResult::success().with_message("Build completed successfully"))
    } else {
        Ok(CommandResult::failure(StructuredError {
            code: "BUILD_FAILED".to_string(),
            message: format!("cargo build failed with exit code: {:?}", status.code()),
            location: None,
            suggestion: None,
        }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Generate Command
// ─────────────────────────────────────────────────────────────────────────────

fn execute_generate(
    spec: &str,
    name: Option<String>,
    dry_run: bool,
    workspace: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace_root = Utf8PathBuf::from(workspace);
    let args = crate::devtools::generate::GenerateArgs {
        spec: spec.to_string(),
        name,
        dry_run,
    };

    // Run with tokio runtime
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { crate::devtools::generate::run_generate(args, workspace_root).await })?;

    Ok(CommandResult::success().with_message("Node generation completed"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot Command (Pull from Production)
// ─────────────────────────────────────────────────────────────────────────────

fn execute_snapshot(cmd: &SnapshotSubcommand, ctx: &CommandContext) -> Result<CommandResult> {
    match cmd {
        SnapshotSubcommand::PullProd { host, database } => {
            execute_pull_prod(host.as_deref(), database.as_deref(), ctx)
        }
    }
}

fn execute_pull_prod(
    host: Option<&str>,
    database: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let config = StackConfig::for_current_checkout()?;
    let prod_host = host.unwrap_or("prod.sinex.io");
    let prod_db = database.unwrap_or("sinex");

    ctx.heading("dev snapshot pull-prod");

    if ctx.is_human() {
        println!("Pulling snapshot from production...");
        println!("  Host: {}", prod_host);
        println!("  Database: {}", prod_db);
        println!();
    }

    // Stop local stack
    let status = StackStatus::gather(&config);
    if status.postgres.running {
        pg_stop(&config, ctx)?;
    }

    // Create snapshot via SSH + pg_dump | zstd
    let snapshot_path = config.snapshots_dir().join("prod-latest.sql.zst");
    fs::create_dir_all(config.snapshots_dir())?;

    if ctx.is_human() {
        println!("Running: ssh {} pg_dump {} | zstd > {}", prod_host, prod_db, snapshot_path.display());
    }

    let ssh = Command::new("ssh")
        .args([prod_host, "pg_dump", "-Fc", prod_db])
        .stdout(Stdio::piped())
        .spawn()
        .context("Failed to connect to production host")?;

    let zstd_status = Command::new("zstd")
        .args(["-T0", "-3", "-o", snapshot_path.to_str().unwrap()])
        .stdin(ssh.stdout.unwrap())
        .status()
        .context("Failed to compress dump")?;

    if !zstd_status.success() {
        bail!("Failed to pull production snapshot");
    }

    let size = fs::metadata(&snapshot_path).map(|m| m.len()).unwrap_or(0);

    if ctx.is_human() {
        println!();
        println!("Snapshot pulled: {}", snapshot_path.display());
        println!("Size: {}", human_size(size));
        println!();
        println!("To restore: cargo xtask dev stack restore prod-latest");
    }

    Ok(CommandResult::success()
        .with_message("Production snapshot pulled")
        .with_detail(format!("Size: {}", human_size(size))))
}

// ─────────────────────────────────────────────────────────────────────────────
// TLS Fixtures
// ─────────────────────────────────────────────────────────────────────────────

fn execute_tls_fixtures(output: &str) -> Result<CommandResult> {
    let script = Path::new("scripts").join("generate_tls_fixtures.sh");
    if !script.exists() {
        bail!("TLS fixture script missing at {}", script.to_string_lossy());
    }

    let status = Command::new(&script)
        .arg(output)
        .status()
        .with_context(|| format!("failed to run {}", script.display()))?;

    if !status.success() {
        bail!("{} exited with {}", script.display(), status);
    }

    Ok(CommandResult::success().with_detail(format!("TLS fixtures generated in {output}")))
}

// ─────────────────────────────────────────────────────────────────────────────
// PostgreSQL Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn pg_bin(binary: &str) -> PathBuf {
    if let Ok(prefix) = std::env::var("SINEX_PG_BIN") {
        PathBuf::from(prefix).join(binary)
    } else {
        PathBuf::from(binary)
    }
}

fn pg_init(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    if config.pg_data().join("PG_VERSION").exists() {
        if ctx.is_human() {
            println!("PostgreSQL data directory already initialized");
        }
        return Ok(());
    }

    if ctx.is_human() {
        println!("Initializing PostgreSQL data directory...");
    }

    let status = Command::new(pg_bin("initdb"))
        .args(["--auth=trust", "--no-locale", "--encoding=UTF8", "-D"])
        .arg(&config.pg_data())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("Failed to run initdb")?;

    if !status.success() {
        bail!("initdb failed with status {}", status);
    }

    // Configure postgresql.conf
    let conf_path = config.pg_data().join("postgresql.conf");
    let mut conf = fs::OpenOptions::new()
        .append(true)
        .open(&conf_path)
        .context("Failed to open postgresql.conf")?;

    writeln!(conf, "\n# sinex-dev isolated configuration (per-checkout)")?;
    writeln!(
        conf,
        "unix_socket_directories = '{}'",
        config.run_dir().display()
    )?;
    writeln!(conf, "listen_addresses = '127.0.0.1'")?;
    writeln!(conf, "port = {}", config.postgres.port)?;
    writeln!(conf, "max_connections = 200")?;
    writeln!(conf, "shared_preload_libraries = 'timescaledb'")?;
    writeln!(conf, "log_destination = 'stderr'")?;
    writeln!(conf, "logging_collector = on")?;
    writeln!(conf, "log_directory = '{}'", config.logs_dir().display())?;
    writeln!(conf, "log_filename = 'postgres.log'")?;

    if ctx.is_human() {
        println!("PostgreSQL initialized");
    }

    Ok(())
}

fn pg_start(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    if is_process_running(&config.pg_pid_file()) {
        if ctx.is_human() {
            println!("PostgreSQL already running");
        }
        return Ok(());
    }

    if ctx.is_human() {
        println!("Starting PostgreSQL on port {}...", config.postgres.port);
    }

    let log_path = config.logs_dir().join("postgres.log");

    let status = Command::new(pg_bin("pg_ctl"))
        .args(["-D", config.pg_data().to_str().unwrap(), "start", "-w"])
        .arg("-l")
        .arg(&log_path)
        .arg("-o")
        .arg(format!(
            "-k {} -p {}",
            config.run_dir().display(),
            config.postgres.port
        ))
        .status()
        .context("Failed to start PostgreSQL")?;

    if !status.success() {
        bail!("pg_ctl start failed with status {}", status);
    }

    // Copy PID from postmaster.pid
    if let Ok(content) = fs::read_to_string(config.pg_data().join("postmaster.pid")) {
        if let Some(first_line) = content.lines().next() {
            fs::write(&config.pg_pid_file(), first_line)?;
        }
    }

    // Wait for ready
    for _ in 0..60 {
        let check = Command::new(pg_bin("pg_isready"))
            .args(["-h", config.run_dir().to_str().unwrap()])
            .args(["-p", &config.postgres.port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        if check.map(|s| s.success()).unwrap_or(false) {
            if ctx.is_human() {
                println!("PostgreSQL started");
            }
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    bail!("PostgreSQL failed to start within 30 seconds")
}

fn pg_stop(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    if !is_process_running(&config.pg_pid_file()) {
        if ctx.is_human() {
            println!("PostgreSQL not running");
        }
        let _ = fs::remove_file(&config.pg_pid_file());
        return Ok(());
    }

    if ctx.is_human() {
        println!("Stopping PostgreSQL...");
    }

    let _ = Command::new(pg_bin("pg_ctl"))
        .args([
            "-D",
            config.pg_data().to_str().unwrap(),
            "stop",
            "-m",
            "fast",
        ])
        .status();

    let _ = fs::remove_file(&config.pg_pid_file());

    if ctx.is_human() {
        println!("PostgreSQL stopped");
    }

    Ok(())
}

fn pg_setup_database(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    let initial_user =
        std::env::var("USER").unwrap_or_else(|_| config.postgres.superuser.clone());

    // Helper to run psql
    let psql = |user: &str, db: &str, sql: &str| -> Result<String> {
        let output = Command::new(pg_bin("psql"))
            .args(["-h", config.run_dir().to_str().unwrap()])
            .args(["-p", &config.postgres.port.to_string()])
            .args(["-U", user])
            .args(["-d", db])
            .args(["-tAc", sql])
            .output()
            .context("Failed to run psql")?;

        if !output.status.success() {
            bail!("psql failed: {}", String::from_utf8_lossy(&output.stderr));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    };

    // Create superuser if needed
    let exists = psql(
        &initial_user,
        "postgres",
        &format!(
            "SELECT 1 FROM pg_roles WHERE rolname = '{}'",
            config.postgres.superuser
        ),
    )?;
    if exists.is_empty() {
        if ctx.is_human() {
            println!("Creating superuser role: {}", config.postgres.superuser);
        }
        psql(
            &initial_user,
            "postgres",
            &format!(
                "CREATE ROLE {} LOGIN SUPERUSER CREATEDB",
                config.postgres.superuser
            ),
        )?;
    }

    // Create app user if needed
    let exists = psql(
        &config.postgres.superuser,
        "postgres",
        &format!(
            "SELECT 1 FROM pg_roles WHERE rolname = '{}'",
            config.postgres.user
        ),
    )?;
    if exists.is_empty() {
        if ctx.is_human() {
            println!("Creating application role: {}", config.postgres.user);
        }
        psql(
            &config.postgres.superuser,
            "postgres",
            &format!(
                "CREATE ROLE {} LOGIN SUPERUSER CREATEDB",
                config.postgres.user
            ),
        )?;
    }

    // Create database if needed
    let exists = psql(
        &config.postgres.superuser,
        "postgres",
        &format!(
            "SELECT 1 FROM pg_database WHERE datname = '{}'",
            config.postgres.database
        ),
    )?;
    if exists.is_empty() {
        if ctx.is_human() {
            println!("Creating database: {}", config.postgres.database);
        }
        psql(
            &config.postgres.superuser,
            "postgres",
            &format!(
                "CREATE DATABASE {} OWNER {}",
                config.postgres.database, config.postgres.user
            ),
        )?;
    }

    // Enable extensions
    if ctx.is_human() {
        println!("Enabling PostgreSQL extensions...");
    }
    for ext in &["timescaledb", "vector", "pg_jsonschema"] {
        let _ = psql(
            &config.postgres.superuser,
            &config.postgres.database,
            &format!("CREATE EXTENSION IF NOT EXISTS {}", ext),
        );
    }
    // Try pgx_ulid first, fall back to ulid
    let _ = psql(
        &config.postgres.superuser,
        &config.postgres.database,
        "CREATE EXTENSION IF NOT EXISTS pgx_ulid",
    )
    .or_else(|_| {
        psql(
            &config.postgres.superuser,
            &config.postgres.database,
            "CREATE EXTENSION IF NOT EXISTS ulid",
        )
    });

    if ctx.is_human() {
        println!("Database setup complete");
    }

    Ok(())
}

fn pg_run_migrations(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    if ctx.is_human() {
        println!("Running database migrations...");
    }

    let status = Command::new("cargo")
        .args([
            "run",
            "--manifest-path",
            "crate/lib/sinex-schema/Cargo.toml",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .env("DATABASE_URL", config.database_url())
        .status()
        .context("Failed to run migrations")?;

    if !status.success() {
        bail!("Migrations failed with status {}", status);
    }

    if ctx.is_human() {
        println!("Migrations complete");
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// NATS Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn nats_bin() -> PathBuf {
    if let Ok(path) = std::env::var("NATS_SERVER_BIN") {
        PathBuf::from(path)
    } else {
        PathBuf::from("nats-server")
    }
}

fn nats_generate_config(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    if config.nats_config().exists() {
        if ctx.is_human() {
            println!("NATS config already exists");
        }
        return Ok(());
    }

    if ctx.is_human() {
        println!("Generating NATS configuration...");
    }

    fs::create_dir_all(config.nats_config().parent().unwrap())?;

    let nats_conf = format!(
        r#"# sinex-dev isolated NATS configuration (per-checkout)
port = {}
jetstream {{
    store_dir = "{}"
    max_mem = 256MB
    max_file = 1GB
}}
"#,
        config.nats.port,
        config.nats_data().join("jetstream").display()
    );

    fs::write(&config.nats_config(), nats_conf)?;

    if ctx.is_human() {
        println!("NATS configuration generated");
    }

    Ok(())
}

fn nats_start(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    if is_process_running(&config.nats_pid_file()) {
        if ctx.is_human() {
            println!("NATS already running");
        }
        return Ok(());
    }

    if ctx.is_human() {
        println!("Starting NATS on port {}...", config.nats.port);
    }

    let log_path = config.logs_dir().join("nats.log");
    let log_file = fs::File::create(&log_path)?;

    let child = Command::new(nats_bin())
        .args(["-js", "-c", config.nats_config().to_str().unwrap()])
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        .spawn()
        .context("Failed to start NATS")?;

    fs::write(&config.nats_pid_file(), child.id().to_string())?;

    // Wait for ready
    for _ in 0..30 {
        let check = std::net::TcpStream::connect(format!("127.0.0.1:{}", config.nats.port));
        if check.is_ok() {
            if ctx.is_human() {
                println!("NATS started");
            }
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    bail!("NATS failed to start within 15 seconds")
}

fn nats_stop(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    if !is_process_running(&config.nats_pid_file()) {
        if ctx.is_human() {
            println!("NATS not running");
        }
        let _ = fs::remove_file(&config.nats_pid_file());
        return Ok(());
    }

    if ctx.is_human() {
        println!("Stopping NATS...");
    }

    if let Some(pid) = read_pid(&config.nats_pid_file()) {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
        // Wait for clean shutdown
        for _ in 0..40 {
            if !is_process_running(&config.nats_pid_file()) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }

    let _ = fs::remove_file(&config.nats_pid_file());

    if ctx.is_human() {
        println!("NATS stopped");
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Git-Annex Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn annex_init(config: &StackConfig, ctx: &CommandContext) -> Result<()> {
    if config.annex_data().join(".git").exists() {
        if ctx.is_human() {
            println!("Git-annex repository already initialized");
        }
        return Ok(());
    }

    // Check if git-annex is available
    if Command::new("git-annex").arg("version").output().is_err() {
        if ctx.is_human() {
            println!("git-annex not found, skipping annex initialization");
        }
        return Ok(());
    }

    if ctx.is_human() {
        println!("Initializing git-annex repository...");
    }

    fs::create_dir_all(&config.annex_data())?;

    let _ = Command::new("git")
        .args(["init"])
        .current_dir(&config.annex_data())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    let _ = Command::new("git-annex")
        .args(["init", "sinex-dev-isolated"])
        .current_dir(&config.annex_data())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    let _ = Command::new("git")
        .args(["config", "annex.thin", "true"])
        .current_dir(&config.annex_data())
        .status();

    let _ = Command::new("git")
        .args(["config", "annex.backend", &config.annex.backend])
        .current_dir(&config.annex_data())
        .status();

    if ctx.is_human() {
        println!("Git-annex initialized");
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Utility Functions
// ─────────────────────────────────────────────────────────────────────────────

fn ensure_directories(config: &StackConfig) -> Result<()> {
    fs::create_dir_all(config.config_dir().join("nats"))?;
    fs::create_dir_all(&config.pg_data())?;
    fs::create_dir_all(&config.nats_data())?;
    fs::create_dir_all(config.nats_data().join("jetstream"))?;
    fs::create_dir_all(&config.annex_data())?;
    fs::create_dir_all(&config.run_dir())?;
    fs::create_dir_all(&config.logs_dir())?;
    fs::create_dir_all(&config.snapshots_dir())?;
    Ok(())
}

fn is_process_running(pid_file: &Path) -> bool {
    read_pid(pid_file).map_or(false, |pid| unsafe { libc::kill(pid as i32, 0) == 0 })
}

fn read_pid(pid_file: &Path) -> Option<u32> {
    fs::read_to_string(pid_file)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn dir_size(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    walkdir::WalkDir::new(path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

fn human_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{}KB", bytes / KB)
    } else {
        format!("{}B", bytes)
    }
}

fn list_snapshots(dir: &Path) -> Vec<String> {
    if !dir.exists() {
        return vec![];
    }
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().to_string();
                    if name.ends_with(".tar.zst") {
                        Some(name.trim_end_matches(".tar.zst").to_string())
                    } else if name.ends_with(".sql.zst") {
                        Some(name.trim_end_matches(".sql.zst").to_string())
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_human_size() {
        assert_eq!(human_size(500), "500B");
        assert_eq!(human_size(1500), "1KB");
        assert_eq!(human_size(1_500_000), "1.4MB");
        assert_eq!(human_size(1_500_000_000), "1.4GB");
    }

    #[test]
    fn test_binary_name_mapping() {
        // Test that short names are mapped correctly
        assert_eq!(
            match "ingestd" {
                "ingestd" => "sinex-ingestd",
                _ => "unknown",
            },
            "sinex-ingestd"
        );
    }
}
