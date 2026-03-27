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
    /// Apply the declarative schema to a database
    SchemaApply {
        /// Target database URL. Falls back to DATABASE_URL, then the current checkout stack.
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
    },
    /// Generate gateway TLS certificates using rcgen
    TlsInitGateway {
        /// Output directory for generated files
        #[arg(long, default_value = "/var/lib/sinex/tls")]
        output_dir: PathBuf,
        /// Subject alternative name to include. Repeat for multiple SANs.
        #[arg(long = "san", value_name = "SAN")]
        san: Vec<String>,
        /// Common name for the generated certificate authority
        #[arg(long, default_value = "Sinex Gateway CA")]
        ca_name: String,
        /// Certificate validity in days
        #[arg(long, default_value_t = crate::tls::DEFAULT_DEV_CERT_VALIDITY_DAYS)]
        validity_days: u32,
        /// Overwrite an existing certificate set
        #[arg(long)]
        force: bool,
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
        match &self.subcommand {
            InfraSubcommand::Start { all, processes } => {
                let config = StackConfig::for_current_checkout()?;
                execute_start(&config, *all, processes, ctx)
            }
            InfraSubcommand::Stop => {
                let config = StackConfig::for_current_checkout()?;
                execute_stop(&config, ctx)
            }
            InfraSubcommand::Status { watch } => {
                let config = StackConfig::for_current_checkout()?;
                execute_status(&config, *watch, ctx).await
            }
            InfraSubcommand::Logs {
                process,
                lines,
                follow,
            } => {
                let config = StackConfig::for_current_checkout()?;
                execute_logs(&config, process, *lines, *follow, ctx)
            }
            InfraSubcommand::SchemaApply { database_url } => {
                execute_schema_apply(database_url.as_deref(), ctx)
            }
            InfraSubcommand::TlsInitGateway {
                output_dir,
                san,
                ca_name,
                validity_days,
                force,
            } => execute_tls_init_gateway(output_dir, san, ca_name, *validity_days, *force, ctx),
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

fn resolve_database_url(database_url: Option<&str>) -> Result<String> {
    if let Some(database_url) = database_url {
        return Ok(database_url.to_owned());
    }

    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        return Ok(database_url);
    }

    Ok(StackConfig::for_current_checkout()?.database_url())
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

fn execute_schema_apply(database_url: Option<&str>, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("infra schema-apply");

    let database_url = resolve_database_url(database_url)?;
    stack::apply_schema_for_database_url(&database_url, ctx.is_human())?;

    Ok(CommandResult::success().with_message("Schema applied"))
}

fn execute_tls_init_gateway(
    output_dir: &Path,
    san: &[String],
    ca_name: &str,
    validity_days: u32,
    force: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra tls-init-gateway");

    let san = if san.is_empty() {
        vec!["localhost".to_string(), "127.0.0.1".to_string()]
    } else {
        san.to_vec()
    };

    let data = crate::tls::generate_dev_certs(&crate::tls::CertConfig {
        output_dir: output_dir.to_path_buf(),
        san: san.clone(),
        ca_name: ca_name.to_string(),
        validity_days,
        force,
    })?;

    let mut result = CommandResult::success()
        .with_message("Gateway TLS initialized")
        .with_data(data)
        .with_detail(format!("Output directory: {}", output_dir.display()));
    for san_entry in san {
        result = result.with_detail(format!("SAN: {san_entry}"));
    }
    Ok(result)
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
            println!(
                "Data sizes:  pg={} nats={} annex={}",
                format_bytes(status.data_sizes.postgres_bytes),
                format_bytes(status.data_sizes.nats_bytes),
                format_bytes(status.data_sizes.annex_bytes),
            );
            for issue in &status.data_size_issues {
                println!("             {issue}");
            }
            if let Some(issue) = &status.snapshot_issue {
                println!("Snapshots:   unavailable");
                println!("             {issue}");
            } else {
                println!("Snapshots:   {}", status.snapshots.len());
            }
        }

        if !watch {
            let mut result = CommandResult::success().with_data(serde_json::to_value(&status)?);
            for issue in &status.data_size_issues {
                result = result.with_warning(issue.clone());
            }
            if let Some(issue) = &status.snapshot_issue {
                result = result.with_warning(issue.clone());
            }
            return Ok(result);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = UNITS[0];
    for next_unit in &UNITS[1..] {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next_unit;
    }
    if unit == "B" {
        format!("{bytes}B")
    } else {
        format!("{value:.1}{unit}")
    }
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
