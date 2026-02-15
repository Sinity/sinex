//! Infra command - infrastructure management.

use anyhow::{bail, Context, Result};
use clap::Subcommand;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::infra::stack::{self, StackConfig, StackStatus};
use crate::infra::state::CheckoutState;

/// Infra command - manages the isolated development environment.
pub struct InfraCommand {
    pub subcommand: InfraSubcommand,
}

#[derive(Subcommand)]
pub enum InfraSubcommand {
    /// Start the infrastructure
    Start {
        /// Start all processes
        #[arg(long)]
        all: bool,
        /// Specific processes to start
        processes: Vec<String>,
    },
    /// Stop the infrastructure
    Stop,
    /// Show infrastructure status
    Status {
        /// Watch mode
        #[arg(long, short)]
        watch: bool,
    },
    /// Stop services and wipe infrastructure state (wipes data-dir!)
    Reset {
        /// Automatically confirm reset
        #[arg(long)]
        yes: bool,
    },
    /// View logs
    Logs {
        /// Process name
        #[arg(value_name = "PROCESS", default_value = "all")]
        process: String,
        /// Lines to show
        #[arg(long, short, default_value_t = 50)]
        lines: usize,
        /// Follow output
        #[arg(long, short)]
        follow: bool,
    },
    /// Create/Manage snapshots
    Snapshot {
        #[command(subcommand)]
        cmd: SnapshotSubcommand,
    },
    /// Manage VM integration
    Vm {
        #[command(subcommand)]
        cmd: crate::commands::vm::VmSubcommand,
    },
    /// Print infrastructure environment variables
    Env {
        /// Shell format (export NAME=VALUE)
        #[arg(long, default_value_t = true)]
        export: bool,
    },
}

#[derive(Subcommand)]
pub enum SnapshotSubcommand {
    /// Create a named snapshot
    Create { name: String },
    /// Restore from a named snapshot
    Restore { name: String },
    /// List available snapshots
    List,
}

#[async_trait::async_trait]
impl XtaskCommand for InfraCommand {
    fn name(&self) -> &'static str {
        "infra"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let config = StackConfig::for_current_checkout()?;

        match &self.subcommand {
            InfraSubcommand::Start { all, processes } => {
                execute_start(&config, *all, processes, ctx).await
            }
            InfraSubcommand::Stop => execute_stop(&config, ctx).await,
            InfraSubcommand::Status { watch } => execute_status(&config, *watch, ctx).await,
            InfraSubcommand::Reset { yes } => execute_reset(&config, *yes, ctx).await,
            InfraSubcommand::Logs {
                process,
                lines,
                follow,
            } => execute_logs(&config, process, *lines, *follow, ctx).await,
            InfraSubcommand::Snapshot { cmd } => execute_snapshot(&config, cmd, ctx).await,
            InfraSubcommand::Vm { cmd } => {
                let vm_cmd = crate::commands::vm::VmCommand {
                    subcommand: cmd.clone(),
                };
                vm_cmd.execute(ctx).await
            }
            InfraSubcommand::Env { export } => execute_env(&config, *export),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Implementations
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_start(
    config: &StackConfig,
    _all: bool,
    _processes: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra start");

    // Check lock
    let checkout_state = CheckoutState::for_current_checkout()?;
    if let Some(lock_info) = checkout_state.is_locked_by_other()? {
        let pid = lock_info.pid;
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "INFRA_LOCKED".to_string(),
            message: format!("Infra locked by {pid}"),
            location: None,
            suggestion: Some("Stop other infra stack".into()),
        }));
    }

    let _lock = checkout_state.acquire_lock(Some("infra".into()))?;
    std::mem::forget(_lock);

    stack::ensure_directories(config)?;

    if config.annex.enable {
        stack::annex_init(config, ctx.is_human())?;
    }

    stack::pg_init(config, ctx.is_human())?;
    stack::pg_start(config, ctx.is_human())?;
    stack::pg_setup_database(config, ctx.is_human())?;
    stack::pg_run_migrations(config, ctx.is_human())?;

    stack::nats_generate_config(config, ctx.is_human())?;
    stack::nats_start(config, ctx.is_human())?;

    let pg_port = config.postgres.port;
    let nats_port = config.nats.port;
    Ok(CommandResult::success()
        .with_message("Infra started")
        .with_detail(format!("Postgres on port {pg_port}"))
        .with_detail(format!("NATS on port {nats_port}")))
}

async fn execute_stop(config: &StackConfig, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("infra stop");

    stack::nats_stop(config, ctx.is_human())?;
    stack::pg_stop(config, ctx.is_human())?;

    let checkout_state = CheckoutState::for_current_checkout()?;
    checkout_state.release_lock()?;
    Ok(CommandResult::success().with_message("Infra stopped"))
}

async fn execute_status(
    config: &StackConfig,
    watch: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    loop {
        if watch {
            print!("\x1B[2J\x1B[H");
        }

        let status = StackStatus::gather(config);

        if ctx.is_human() {
            println!("sinex-dev infra status");
            println!("────────────────────────────────────────");
            println!(
                "PostgreSQL:  {} (unix socket, port: {})",
                if status.postgres.running {
                    "running"
                } else {
                    "stopped"
                },
                status.postgres.port
            );
            println!(
                "NATS:        {} (port: {})",
                if status.nats.running {
                    "running"
                } else {
                    "stopped"
                },
                status.nats.port
            );
            println!(
                "Git-annex:   {}",
                if status.annex.initialized {
                    "initialized"
                } else {
                    "not initialized"
                }
            );
        }

        if !watch {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    Ok(CommandResult::success())
}

async fn execute_reset(
    config: &StackConfig,
    yes: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if !yes {
        bail!("Reset requires --yes");
    }
    execute_stop(config, ctx).await?;
    fs::remove_dir_all(config.data_dir())?;
    execute_start(config, false, &[], ctx).await
}

async fn execute_logs(
    config: &StackConfig,
    process: &str,
    lines: usize,
    follow: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let log_file = match process {
        "postgres" => "postgres.log",
        "nats" | "nats-server" => "nats.log",
        _ => {
            // Try generic process log location if orchestrator uses it
            if Path::new(".sinex/state")
                .join(process)
                .join("process.log")
                .exists()
            {
                "process.log"
            } else {
                bail!("Unknown process: {process}");
            }
        }
    };

    let log_path = if log_file == "postgres.log" || log_file == "nats.log" {
        config.logs_dir().join(log_file)
    } else {
        // Fallback logic
        PathBuf::from(format!(".sinex/state/{process}/process.log"))
    };

    if !log_path.exists() {
        bail!("Log file not found: {}", log_path.display());
    }

    ctx.heading(&format!("logs: {process}"));

    let mut cmd = Command::new("tail");
    cmd.arg("-n").arg(lines.to_string());
    if follow {
        cmd.arg("-f");
    }
    cmd.arg(&log_path);

    let status = cmd.status().context("tail failed")?;
    if !status.success() {
        bail!("tail failed");
    }

    Ok(CommandResult::success())
}

async fn execute_snapshot(
    config: &StackConfig,
    cmd: &SnapshotSubcommand,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    match cmd {
        SnapshotSubcommand::Create { name } => {
            ctx.heading("infra snapshot create");
            let safe_name = name.replace(|c: char| !c.is_alphanumeric() && c != '-', "_");
            let snapshot_path = config.snapshots_dir().join(format!("{safe_name}.tar.zst"));

            fs::create_dir_all(config.snapshots_dir())?;

            let tar = Command::new("tar")
                .args([
                    "-C",
                    config
                        .state_dir
                        .to_str()
                        .expect("state dir must be valid UTF-8"),
                    "-cf",
                    "-",
                    "config",
                    "data",
                ])
                .stdout(Stdio::piped())
                .spawn()?;

            let zstd = Command::new("zstd")
                .args([
                    "-T0",
                    "-3",
                    "-o",
                    snapshot_path
                        .to_str()
                        .expect("snapshot path must be valid UTF-8"),
                ])
                .stdin(tar.stdout.expect("tar stdout pipe should be available"))
                .status()?;

            if !zstd.success() {
                bail!("Snapshot failed");
            }
            Ok(CommandResult::success().with_message(format!("Snapshot {safe_name} created")))
        }
        SnapshotSubcommand::Restore { name } => {
            let safe_name = name.replace(|c: char| !c.is_alphanumeric() && c != '-', "_");
            let snapshot_path = config.snapshots_dir().join(format!("{safe_name}.tar.zst"));
            if !snapshot_path.exists() {
                bail!("Snapshot not found");
            }

            execute_stop(config, ctx).await?;
            fs::remove_dir_all(config.data_dir()).ok();
            fs::remove_dir_all(config.pg_data()).ok();
            fs::remove_dir_all(config.nats_data()).ok();

            let zstd = Command::new("zstd")
                .args([
                    "-d",
                    "-c",
                    snapshot_path
                        .to_str()
                        .expect("snapshot path must be valid UTF-8"),
                ])
                .stdout(Stdio::piped())
                .spawn()?;

            let tar = Command::new("tar")
                .args([
                    "-C",
                    config
                        .state_dir
                        .to_str()
                        .expect("state dir must be valid UTF-8"),
                    "-xf",
                    "-",
                ])
                .stdin(zstd.stdout.expect("zstd stdout pipe should be available"))
                .status()?;

            if !tar.success() {
                bail!("Restore failed");
            }
            execute_start(config, false, &[], ctx).await
        }
        SnapshotSubcommand::List => {
            let snaps = stack::list_snapshots(&config.snapshots_dir());
            println!("Snapshots: {snaps:?}");
            Ok(CommandResult::success())
        }
    }
}

fn execute_env(config: &StackConfig, export: bool) -> Result<CommandResult> {
    let prefix = if export { "export " } else { "" };
    println!("{}DATABASE_URL=\"{}\"", prefix, config.database_url());
    println!("{}SINEX_NATS_URL=\"{}\"", prefix, config.nats_url());
    println!("{}PGPORT=\"{}\"", prefix, config.postgres.port);
    println!("{}SINEX_DEV_PG_PORT=\"{}\"", prefix, config.postgres.port);
    println!("{}SINEX_DEV_NATS_PORT=\"{}\"", prefix, config.nats.port);
    Ok(CommandResult::success())
}
