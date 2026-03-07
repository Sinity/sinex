//! Infra command - infrastructure management.

use clap::Subcommand;
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use std::path::{Path, PathBuf};
use std::process::Command;

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
    /// Manage VM integration
    Vm {
        #[command(subcommand)]
        cmd: crate::commands::vm::VmSubcommand,
    },
}

impl XtaskCommand for InfraCommand {
    fn name(&self) -> &'static str {
        "infra"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let config = StackConfig::for_current_checkout()?;

        match &self.subcommand {
            InfraSubcommand::Start { all, processes } => {
                execute_start(&config, *all, processes, ctx)
            }
            InfraSubcommand::Stop => execute_stop(&config, ctx),
            InfraSubcommand::Status { watch } => execute_status(&config, *watch, ctx).await,
            InfraSubcommand::Logs {
                process,
                lines,
                follow,
            } => execute_logs(&config, process, *lines, *follow, ctx),
            InfraSubcommand::Vm { cmd } => {
                let vm_cmd = crate::commands::vm::VmCommand {
                    subcommand: cmd.clone(),
                };
                vm_cmd.execute(ctx).await
            }
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
    ctx.heading("infra start");

    // Check lock
    let checkout_state = CheckoutState::for_current_checkout()?;
    if let Some(lock_info) = checkout_state.is_locked_by_other()? {
        let pid = lock_info.pid;
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "INFRA_LOCKED".to_string(),
            message: format!("Infra locked by {pid}"),
            location: Some("infra::start".to_string()),
            suggestion: Some(format!("Stop running instance: kill {pid}")),
        }));
    }

    let _lock = checkout_state.acquire_lock(Some("infra".into()))?;
    std::mem::forget(_lock);

    stack::ensure_directories(config)?;

    let verbose = ctx.is_human();

    // Parallelize independent Postgres and NATS startup.
    // NATS has zero dependency on Postgres — run them concurrently.
    std::thread::scope(|s| -> Result<()> {
        // Spawn NATS startup in background thread
        let nats_handle = s.spawn(|| -> Result<()> {
            stack::nats_generate_config(config, verbose)?;
            stack::nats_start(config, verbose)
        });

        // Postgres chain runs in the foreground (critical path)
        let pg_result = (|| -> Result<()> {
            if config.annex.enable {
                stack::annex_init(config, verbose)?;
            }

            stack::pg_init(config, verbose)?;
            stack::pg_start(config, verbose)?;
            stack::pg_setup_database(config, verbose)?;

            // Skip schema apply when declarative sources haven't changed since last apply
            if crate::preflight::schema_changed_since_last_apply() {
                stack::pg_apply_schema(config, verbose)?;
                crate::preflight::record_schema_applied();
            }

            Ok(())
        })();

        // Collect NATS result
        let nats_result = nats_handle
            .join()
            .map_err(|_| eyre!("NATS startup thread panicked"))?;

        // Report errors from both paths
        pg_result?;
        nats_result?;
        Ok(())
    })?;

    let pg_port = config.postgres.port;
    let nats_port = config.nats.port;
    Ok(CommandResult::success()
        .with_message("Infra started")
        .with_detail(format!("Postgres on port {pg_port}"))
        .with_detail(format!("NATS on port {nats_port}")))
}

fn execute_stop(config: &StackConfig, ctx: &CommandContext) -> Result<CommandResult> {
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
