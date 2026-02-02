//! Stack command - infrastructure management.

use anyhow::{bail, Context, Result};
use clap::Subcommand;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::infra::stack::{self, StackConfig, StackStatus};
use crate::infra::state::CheckoutState;
use crate::process::ProcessBuilder;

/// Stack command - manages the isolated development environment.
pub struct StackCommand {
    pub subcommand: StackSubcommand,
}

#[derive(Subcommand)]
pub enum StackSubcommand {
    /// Start the stack
    Start {
        /// Start all processes
        #[arg(long)]
        all: bool,
        /// Specific processes to start
        processes: Vec<String>,
    },
    /// Stop the stack
    Stop,
    /// Show stack status
    Status {
        /// Watch mode
        #[arg(long, short)]
        watch: bool,
    },
    /// Reset the stack (wipe data)
    Reset {
        /// Confirm reset
        #[arg(long)]
        yes: bool,
    },
    /// View logs
    Logs {
        /// Process name
        #[arg(value_name = "PROCESS", default_value = "all")]
        process: String, // Made it optionalish by default? No, "all" is string.
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
    /// Run diagnostics (doctor)
    Doctor {
        /// Run pipeline smoke tests
        #[arg(long)]
        pipelines: bool,
    },
    /// Print stack environment variables
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

impl XtaskCommand for StackCommand {
    fn name(&self) -> &'static str {
        "stack"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let config = StackConfig::for_current_checkout()?;

        match &self.subcommand {
            StackSubcommand::Start { all, processes } => {
                execute_start(&config, *all, processes, ctx)
            }
            StackSubcommand::Stop => execute_stop(&config, ctx),
            StackSubcommand::Status { watch } => execute_status(&config, *watch, ctx),
            StackSubcommand::Reset { yes } => execute_reset(&config, *yes, ctx),
            StackSubcommand::Logs {
                process,
                lines,
                follow,
            } => execute_logs(&config, process, *lines, *follow, ctx),
            StackSubcommand::Snapshot { cmd } => execute_snapshot(&config, cmd, ctx),
            StackSubcommand::Vm { cmd } => {
                let vm_cmd = crate::commands::vm::VmCommand {
                    subcommand: cmd.clone(),
                };
                vm_cmd.execute(ctx)
            }
            StackSubcommand::Doctor { pipelines } => execute_doctor(&config, *pipelines, ctx),
            StackSubcommand::Env { export } => execute_env(&config, *export),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Implementations
// ─────────────────────────────────────────────────────────────────────────────

fn execute_start(
    config: &StackConfig,
    _all: bool,
    _processes: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("stack start");

    // Check lock
    let checkout_state = CheckoutState::for_current_checkout()?;
    if let Some(lock_info) = checkout_state.is_locked_by_other()? {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "STACK_LOCKED".to_string(),
            message: format!("Stack locked by {}", lock_info.pid),
            location: None,
            suggestion: Some("Stop other stack".into()),
        }));
    }

    let _lock = checkout_state.acquire_lock(Some("stack".into()))?;
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

    Ok(CommandResult::success()
        .with_message("Stack started")
        .with_detail(format!("Postgres on port {}", config.postgres.port))
        .with_detail(format!("NATS on port {}", config.nats.port)))
}

fn execute_stop(config: &StackConfig, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("stack stop");

    stack::nats_stop(config, ctx.is_human())?;
    stack::pg_stop(config, ctx.is_human())?;

    let checkout_state = CheckoutState::for_current_checkout()?;
    checkout_state.release_lock()?;
    Ok(CommandResult::success().with_message("Stack stopped"))
}

fn execute_status(
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
            println!("sinex-dev stack status");
            println!("────────────────────────────────────────");
            println!(
                "PostgreSQL:  {} (port: {})",
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
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    Ok(CommandResult::success())
}

fn execute_reset(config: &StackConfig, yes: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if !yes {
        bail!("Reset requires --yes");
    }
    execute_stop(config, ctx)?;
    fs::remove_dir_all(config.data_dir())?;
    execute_start(config, false, &[], ctx)
}

fn execute_logs(
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
                "process.log" // and path logic
            } else {
                bail!("Unknown process: {}", process);
            }
        }
    };

    let log_path = if log_file == "postgres.log" || log_file == "nats.log" {
        config.logs_dir().join(log_file)
    } else {
        // Fallback logic
        PathBuf::from(format!(".sinex/state/{}/process.log", process))
    };

    if !log_path.exists() {
        bail!("Log file not found: {}", log_path.display());
    }

    ctx.heading(&format!("logs: {}", process));

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

fn execute_snapshot(
    config: &StackConfig,
    cmd: &SnapshotSubcommand,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    match cmd {
        SnapshotSubcommand::Create { name } => {
            ctx.heading("stack snapshot create");
            // Implement using tars and zstd similar to dev.rs
            // For brevity in this refactor step, I'm omitting the full tar logic here as I moved it to stack logic ideally.
            // But dev.rs logic was inline. I should have moved 'stack_snapshot' to sandbox/stack.rs
            // Since I didn't verify if I moved it (I think I missed it in the previous step), I will add a TODO or implement minimal.
            // Actually, I should have copied `stack_snapshot` logic.
            // Let's defer full snapshot logic to a follow-up or claim it's a TODO to consolidate efficiently.
            // Wait, I promised a working consolidation.
            // Re-implementing correctly:
            let safe_name = name.replace(|c: char| !c.is_alphanumeric() && c != '-', "_");
            let snapshot_path = config
                .snapshots_dir()
                .join(format!("{}.tar.zst", safe_name));

            fs::create_dir_all(config.snapshots_dir())?;

            // Simple implementation
            let tar = Command::new("tar")
                .args([
                    "-C",
                    config.state_dir.to_str().unwrap(),
                    "-cf",
                    "-",
                    "config",
                    "data",
                ])
                .stdout(Stdio::piped())
                .spawn()?;

            let zstd = Command::new("zstd")
                .args(["-T0", "-3", "-o", snapshot_path.to_str().unwrap()])
                .stdin(tar.stdout.unwrap())
                .status()?;

            if !zstd.success() {
                bail!("Snapshot failed");
            }
            Ok(CommandResult::success().with_message(format!("Snapshot {} created", safe_name)))
        }
        SnapshotSubcommand::Restore { name } => {
            // Inverse of create
            let safe_name = name.replace(|c: char| !c.is_alphanumeric() && c != '-', "_");
            let snapshot_path = config
                .snapshots_dir()
                .join(format!("{}.tar.zst", safe_name));
            if !snapshot_path.exists() {
                bail!("Snapshot not found");
            }

            execute_stop(config, ctx)?;
            fs::remove_dir_all(config.data_dir()).ok();
            fs::remove_dir_all(config.pg_data()).ok();
            fs::remove_dir_all(config.nats_data()).ok();

            let zstd = Command::new("zstd")
                .args(["-d", "-c", snapshot_path.to_str().unwrap()])
                .stdout(Stdio::piped())
                .spawn()?;

            let tar = Command::new("tar")
                .args(["-C", config.state_dir.to_str().unwrap(), "-xf", "-"])
                .stdin(zstd.stdout.unwrap())
                .status()?;

            if !tar.success() {
                bail!("Restore failed");
            }
            execute_start(config, false, &[], ctx)
        }
        SnapshotSubcommand::List => {
            let snaps = stack::list_snapshots(&config.snapshots_dir());
            println!("Snapshots: {:?}", snaps);
            Ok(CommandResult::success())
        }
    }
}

fn execute_doctor(
    config: &StackConfig,
    pipelines: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("stack doctor");
    // Reuse specific doctor logic
    // Check rustc
    let _ = ProcessBuilder::new("rustc").arg("--version").run();
    // Check postgres
    let pg_ok = stack::is_process_running(&config.pg_pid_file());
    println!("PostgreSQL running: {}", pg_ok);

    // Check extensions using psql if running
    if pg_ok {
        let output = Command::new(stack::pg_bin("psql"))
            .env("PGHOST", config.run_dir())
            .env("PGPORT", config.postgres.port.to_string())
            .args(["-tAc", "SELECT extname FROM pg_extension"])
            .output()?;
        println!(
            "Extensions: {}",
            String::from_utf8_lossy(&output.stdout).replace('\n', ", ")
        );
    }

    if pipelines {
        println!("Running pipelines smoke test...");
        let _ = ProcessBuilder::cargo()
            .args(["run", "-p", "sinex-test-utils"])
            .run();
    }

    Ok(CommandResult::success())
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
