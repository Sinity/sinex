//! Infra command - infrastructure management.

use clap::Subcommand;
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config as xtask_config;
use crate::history::JobLifecycleStatus;
use crate::infra::flake_stage::stage_checkout_for_flake;
use crate::infra::stack::{self, AllCheckoutsStatus, StackConfig, StackStatus};
use crate::infra::state::CheckoutState;
use crate::jobs::JobManager;

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
    Stop {
        /// Stop/clean every checkout-local dev-state root under /var/cache/sinex/$USER
        #[arg(long)]
        all_checkouts: bool,
        /// Only remove stale/malformed lock and PID files; do not stop live processes
        #[arg(long)]
        stale_only: bool,
        /// Print planned actions without stopping processes or removing files
        #[arg(long)]
        dry_run: bool,
        /// Specific processes to stop
        processes: Vec<String>,
    },
    /// Run the explicit local devshell/runtime lifecycle smoke
    Smoke {
        /// Print the smoke plan and current coordinates without starting or stopping services
        #[arg(long)]
        dry_run: bool,
        /// Stop current-checkout infra before the smoke if it is already running
        #[arg(long)]
        reset_first: bool,
        /// Skip the explicit infra start/stop phase and only verify read-only probes
        #[arg(long)]
        skip_start: bool,
        /// Start a managed local sinexd job, observe it in infra status, and cancel it
        #[arg(long)]
        run_core: bool,
    },
    /// Show infrastructure status
    Status {
        /// Watch mode
        #[arg(long, short)]
        watch: bool,
        /// Show every checkout-local dev-state root under /var/cache/sinex/$USER
        #[arg(long)]
        all_checkouts: bool,
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
    /// Stage a flake-safe checkout copy for local Nix builds and deploys
    FlakeStage {
        /// Output directory for the staged checkout. Defaults to a unique /tmp path.
        #[arg(long)]
        output_dir: Option<PathBuf>,
        /// Replace an existing output directory instead of failing.
        #[arg(long)]
        force: bool,
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
            InfraSubcommand::Stop {
                all_checkouts,
                stale_only,
                dry_run,
                processes,
            } => {
                let config = StackConfig::for_current_checkout()?;
                execute_stop(
                    config,
                    *all_checkouts,
                    *stale_only,
                    *dry_run,
                    processes,
                    ctx,
                )
            }
            InfraSubcommand::Smoke {
                dry_run,
                reset_first,
                skip_start,
                run_core,
            } => {
                let config = StackConfig::for_current_checkout()?;
                execute_smoke(&config, *dry_run, *reset_first, *skip_start, *run_core, ctx)
            }
            InfraSubcommand::Status {
                watch,
                all_checkouts,
            } => {
                let config = StackConfig::for_current_checkout()?;
                execute_status(&config, *watch, *all_checkouts, ctx).await
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
            InfraSubcommand::FlakeStage { output_dir, force } => {
                execute_flake_stage(output_dir.as_deref(), *force, ctx)
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
    all: bool,
    processes: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra start");

    // Validate process names before starting anything
    for p in processes {
        if p != "postgres" && p != "nats" {
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "UNKNOWN_PROCESS".to_string(),
                message: format!("unknown process: {p}"),
                location: Some("infra::start".to_string()),
                suggestion: Some("valid processes: postgres, nats".to_string()),
            }));
        }
    }

    let start_pg = all || processes.is_empty() || processes.iter().any(|p| p == "postgres");
    let start_nats = all || processes.is_empty() || processes.iter().any(|p| p == "nats");

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
        let nats_handle = if start_nats {
            Some(s.spawn(|| -> Result<()> {
                stack::nats_generate_config(config, verbose)?;
                stack::nats_start(config, verbose)
            }))
        } else {
            None
        };

        // Postgres chain runs in the foreground (critical path)
        if start_pg {
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
        }

        // Collect NATS result
        if let Some(handle) = nats_handle {
            let nats_result = handle
                .join()
                .map_err(|_| eyre!("NATS startup thread panicked"))?;
            nats_result?;
        }
        Ok(())
    })?;

    let pg_port = config.postgres.port;
    let nats_port = config.nats.port;
    let mut result = CommandResult::success().with_message("Infra started");
    if start_pg {
        result = result.with_detail(format!("Postgres on port {pg_port}"));
    }
    if start_nats {
        result = result.with_detail(format!("NATS on port {nats_port}"));
    }
    Ok(result)
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

#[derive(Debug, Clone, Serialize)]
struct FlakeStageResult {
    staged_root: String,
    flake_uri: String,
    copied_dirs: usize,
    copied_files: usize,
    copied_symlinks: usize,
    excluded_count: usize,
    unsupported_count: usize,
    excluded_paths: Vec<String>,
    unsupported_paths: Vec<String>,
}

fn execute_flake_stage(
    output_dir: Option<&Path>,
    force: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra flake-stage");

    let report = stage_checkout_for_flake(&crate::config::workspace_root(), output_dir, force)?;
    let result = FlakeStageResult {
        staged_root: report.staged_root.clone(),
        flake_uri: report.flake_uri.clone(),
        copied_dirs: report.copied_dirs,
        copied_files: report.copied_files,
        copied_symlinks: report.copied_symlinks,
        excluded_count: report.excluded_paths.len(),
        unsupported_count: report.unsupported_paths.len(),
        excluded_paths: report.excluded_paths.clone(),
        unsupported_paths: report.unsupported_paths.clone(),
    };

    let mut command_result = CommandResult::success()
        .with_message("Flake-safe checkout staged")
        .with_detail(format!("Stage root: {}", report.staged_root))
        .with_detail(format!("Flake URI: {}", report.flake_uri))
        .with_detail(format!(
            "Copied {} directories, {} files, {} symlinks",
            report.copied_dirs, report.copied_files, report.copied_symlinks
        ))
        .with_detail(format!(
            "Excluded {} paths and skipped {} unsupported entries",
            report.excluded_paths.len(),
            report.unsupported_paths.len()
        ))
        .with_data(serde_json::to_value(result)?)
        .with_duration(ctx.elapsed());

    if !report.unsupported_paths.is_empty() {
        command_result = command_result.with_warning(format!(
            "Skipped unsupported filesystem entries: {}",
            report.unsupported_paths.join(", ")
        ));
    }

    Ok(command_result)
}

fn execute_stop(
    config: StackConfig,
    all_checkouts: bool,
    stale_only: bool,
    dry_run: bool,
    processes: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra stop");

    for process in processes {
        if process != "postgres" && process != "nats" {
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "UNKNOWN_PROCESS".to_string(),
                message: format!("unknown process: {process}"),
                location: Some("infra::stop".to_string()),
                suggestion: Some("valid processes: postgres, nats".to_string()),
            }));
        }
    }

    if all_checkouts {
        if !processes.is_empty() {
            bail!("infra stop --all-checkouts does not accept process names");
        }
        return execute_all_checkouts_stop(stale_only, dry_run, ctx);
    }
    if stale_only {
        bail!("infra stop --stale-only requires --all-checkouts");
    }
    if dry_run {
        bail!("infra stop --dry-run requires --all-checkouts");
    }

    let stop_pg = processes.is_empty() || processes.iter().any(|p| p == "postgres");
    let stop_nats = processes.is_empty() || processes.iter().any(|p| p == "nats");

    if stop_nats {
        stack::nats_stop(&config, ctx.is_human())?;
    }
    if stop_pg {
        stack::pg_stop(&config, ctx.is_human())?;
    }

    let status = StackStatus::gather(&config);
    if !status.postgres.running && !status.nats.running {
        let checkout_state = CheckoutState::for_current_checkout()?;
        checkout_state.release_lock()?;
    }

    let message = if processes.is_empty() {
        "Infra stopped".to_string()
    } else {
        format!("Infra stopped: {}", processes.join(", "))
    };
    Ok(CommandResult::success().with_message(message))
}

fn execute_all_checkouts_stop(
    stale_only: bool,
    dry_run: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let base_dir = CheckoutState::default_inventory_base_dir();
    let roots = CheckoutState::inventory_roots_under(&base_dir)?;
    let cleanup = stack::AllCheckoutsCleanup::run(base_dir, roots, dry_run, stale_only)?;

    if ctx.is_human() {
        println!("sinex-dev infra cleanup: all checkouts");
        println!("────────────────────────────────────────");
        println!(
            "Checkouts: {}  actions: {}  skipped: {}",
            cleanup.totals.checkouts, cleanup.totals.actions, cleanup.totals.skipped
        );
        println!(
            "Stopped:   postgres={} nats={} sinexd={}",
            cleanup.totals.stopped_postgres,
            cleanup.totals.stopped_nats,
            cleanup.totals.stopped_sinexd
        );
        println!(
            "Removed:   files={}{}{}",
            cleanup.totals.removed_files,
            if cleanup.stale_only {
                " (stale-only)"
            } else {
                ""
            },
            if cleanup.dry_run { " (dry-run)" } else { "" }
        );
        for checkout in &cleanup.checkouts {
            if checkout.actions.is_empty() && checkout.skipped.is_empty() {
                continue;
            }
            println!();
            println!("{}", checkout.cache_root.display());
            if let Some(path) = &checkout.checkout_path {
                println!("  checkout: {}", path.display());
            }
            for action in &checkout.actions {
                println!(
                    "  action:   {:?} {}{}",
                    action.action,
                    action.target.display(),
                    if action.dry_run { " (dry-run)" } else { "" }
                );
            }
            for skipped in &checkout.skipped {
                println!("  skipped:  {skipped}");
            }
        }
    }

    let mut result = CommandResult::success()
        .with_message(if dry_run {
            "All-checkout infra cleanup dry-run complete"
        } else {
            "All-checkout infra cleanup complete"
        })
        .with_data(serde_json::to_value(&cleanup)?);
    for warning in &cleanup.warnings {
        result = result.with_warning(warning.clone());
    }
    Ok(result)
}

#[derive(Debug, Serialize)]
struct InfraSmokeReport {
    checkout_root: String,
    dev_state_dir: String,
    database_url: String,
    nats_url: String,
    dry_run: bool,
    reset_first: bool,
    skip_start: bool,
    run_core: bool,
    steps: Vec<InfraSmokeStep>,
    baseline: InfraSmokeSnapshot,
    final_state: InfraSmokeSnapshot,
    all_checkouts: AllCheckoutsStatus,
    service_mode_decision: InfraServiceModeDecision,
}

#[derive(Debug, Clone, Serialize)]
struct InfraSmokeStep {
    name: String,
    command: Vec<String>,
    status: String,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
struct InfraSmokeSnapshot {
    postgres: String,
    nats: String,
    sinexd: String,
    rss_bytes: u64,
    state_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
struct InfraServiceModeDecision {
    selected_default: &'static str,
    reason: &'static str,
    shared_service_status: &'static str,
    hybrid_status: &'static str,
    correctness_notes: Vec<&'static str>,
}

fn execute_smoke(
    config: &StackConfig,
    dry_run: bool,
    reset_first: bool,
    skip_start: bool,
    run_core: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra smoke");

    let mut steps = Vec::new();
    let baseline = smoke_snapshot(config);
    let mut current = StackStatus::gather(config);

    if reset_first {
        steps.push(InfraSmokeStep {
            name: "reset current checkout infra".to_string(),
            command: vec!["xtask".into(), "infra".into(), "stop".into()],
            status: if dry_run { "planned" } else { "ran" }.to_string(),
            detail: "current-checkout Postgres/NATS are stopped before the smoke".to_string(),
        });
        if !dry_run {
            stack::nats_stop(config, ctx.is_human())?;
            stack::pg_stop(config, ctx.is_human())?;
            CheckoutState::for_current_checkout()?.release_lock()?;
            current = StackStatus::gather(config);
        }
    }

    if services_running(&current) {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "INFRA_SMOKE_NOT_STOPPED".to_string(),
            message: "current checkout infra is already running".to_string(),
            location: Some("infra::smoke".to_string()),
            suggestion: Some(
                "rerun with --reset-first, or stop current-checkout infra before the smoke"
                    .to_string(),
            ),
        })
        .with_data(serde_json::to_value(InfraSmokeReport {
            checkout_root: crate::config::workspace_root().display().to_string(),
            dev_state_dir: config.state_dir.display().to_string(),
            database_url: config.database_url(),
            nats_url: config.nats_url(),
            dry_run,
            reset_first,
            skip_start,
            run_core,
            steps,
            baseline,
            final_state: smoke_snapshot(config),
            all_checkouts: all_checkouts_status()?,
            service_mode_decision: service_mode_decision(),
        })?));
    }

    for (name, args) in read_only_smoke_commands() {
        let mut command = vec!["xtask".to_string()];
        command.extend(args.iter().map(ToString::to_string));
        steps.push(if dry_run {
            InfraSmokeStep {
                name: name.to_string(),
                command,
                status: "planned".to_string(),
                detail: "read-only probe should not start Postgres, NATS, or sinexd".to_string(),
            }
        } else {
            match run_xtask_probe(name, &args) {
                Ok(step) => step,
                Err(err) => {
                    stop_current_checkout_infra(config, ctx.is_human())?;
                    return Err(err);
                }
            }
        });

        let after_probe = StackStatus::gather(config);
        if services_running(&after_probe) {
            if !dry_run {
                stop_current_checkout_infra(config, ctx.is_human())?;
            }
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "INFRA_SMOKE_READ_ONLY_STARTED_SERVICE".to_string(),
                message: format!("read-only probe {name} started local infra"),
                location: Some("infra::smoke".to_string()),
                suggestion: Some(
                    "inspect the wrapper/command classification for this probe".to_string(),
                ),
            })
            .with_data(serde_json::to_value(InfraSmokeReport {
                checkout_root: crate::config::workspace_root().display().to_string(),
                dev_state_dir: config.state_dir.display().to_string(),
                database_url: config.database_url(),
                nats_url: config.nats_url(),
                dry_run,
                reset_first,
                skip_start,
                run_core,
                steps,
                baseline,
                final_state: smoke_snapshot(config),
                all_checkouts: all_checkouts_status()?,
                service_mode_decision: service_mode_decision(),
            })?));
        }
    }

    if !skip_start {
        steps.push(InfraSmokeStep {
            name: "explicit infra start".to_string(),
            command: vec!["xtask".into(), "infra".into(), "start".into()],
            status: if dry_run { "planned" } else { "ran" }.to_string(),
            detail: "Postgres/NATS may start only at this explicit phase".to_string(),
        });
        if !dry_run {
            if let Err(err) = execute_start(config, true, &[], ctx) {
                stop_current_checkout_infra(config, ctx.is_human())?;
                return Err(err);
            }
            let running = StackStatus::gather(config);
            if !running.postgres.running || !running.nats.running {
                stop_current_checkout_infra(config, ctx.is_human())?;
                return Ok(CommandResult::failure(crate::output::StructuredError {
                    code: "INFRA_SMOKE_START_FAILED".to_string(),
                    message: "explicit infra start did not bring up Postgres and NATS".to_string(),
                    location: Some("infra::smoke".to_string()),
                    suggestion: Some(
                        "inspect xtask infra status/logs for the current checkout".to_string(),
                    ),
                }));
            }
        }

        let run_dry_step = if dry_run {
            InfraSmokeStep {
                name: "local sinexd dry-run".to_string(),
                command: vec![
                    "xtask".into(),
                    "run".into(),
                    "core".into(),
                    "--dry-run".into(),
                ],
                status: "planned".to_string(),
                detail: "prints checkout-local DB/NATS/API coordinates without starting sinexd"
                    .to_string(),
            }
        } else {
            match run_xtask_probe("local sinexd dry-run", &["run", "core", "--dry-run"]) {
                Ok(step) => step,
                Err(err) => {
                    stop_current_checkout_infra(config, ctx.is_human())?;
                    return Err(err);
                }
            }
        };
        steps.push(run_dry_step);

        let after_dry_run = StackStatus::gather(config);
        if after_dry_run.sinexd.running {
            stop_current_checkout_infra(config, ctx.is_human())?;
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "INFRA_SMOKE_DRY_RUN_STARTED_SINEXD".to_string(),
                message: "xtask run core --dry-run started a local sinexd process".to_string(),
                location: Some("infra::smoke".to_string()),
                suggestion: Some("dry-run must remain an explicit non-runtime probe".to_string()),
            }));
        }

        if run_core {
            let runtime_step = if dry_run {
                InfraSmokeStep {
                    name: "managed local sinexd runtime".to_string(),
                    command: vec!["xtask".into(), "--bg".into(), "run".into(), "core".into()],
                    status: "planned".to_string(),
                    detail:
                        "would start a managed background sinexd job, observe it, and cancel it"
                            .to_string(),
                }
            } else {
                match run_managed_core_smoke(config) {
                    Ok(step) => step,
                    Err(err) => {
                        stop_current_checkout_infra(config, ctx.is_human())?;
                        return Err(err);
                    }
                }
            };
            steps.push(runtime_step);
        }

        steps.push(InfraSmokeStep {
            name: "explicit infra stop".to_string(),
            command: vec!["xtask".into(), "infra".into(), "stop".into()],
            status: if dry_run { "planned" } else { "ran" }.to_string(),
            detail: "current-checkout Postgres/NATS are stopped at the end of the smoke"
                .to_string(),
        });
        if !dry_run {
            stop_current_checkout_infra(config, ctx.is_human())?;
        }
    }

    let all_checkouts = all_checkouts_status()?;
    let final_state = smoke_snapshot(config);
    let report = InfraSmokeReport {
        checkout_root: crate::config::workspace_root().display().to_string(),
        dev_state_dir: config.state_dir.display().to_string(),
        database_url: config.database_url(),
        nats_url: config.nats_url(),
        dry_run,
        reset_first,
        skip_start,
        run_core,
        steps,
        baseline,
        final_state,
        all_checkouts,
        service_mode_decision: service_mode_decision(),
    };

    if ctx.is_human() {
        println!("Checkout:  {}", report.checkout_root);
        println!("Dev-state: {}", report.dev_state_dir);
        println!("Database:  {}", report.database_url);
        println!("NATS:      {}", report.nats_url);
        println!();
        println!("Smoke steps:");
        for step in &report.steps {
            println!("  {:<34} {:<8} {}", step.name, step.status, step.detail);
        }
        println!();
        println!(
            "Final: pg={} nats={} sinexd={} rss={}",
            report.final_state.postgres,
            report.final_state.nats,
            report.final_state.sinexd,
            format_bytes(report.final_state.rss_bytes)
        );
        println!(
            "All checkouts: {} roots, {} RSS, {} state",
            report.all_checkouts.totals.checkout_count,
            format_bytes(report.all_checkouts.totals.rss_bytes),
            format_bytes(report.all_checkouts.totals.state_bytes)
        );
    }

    let mut result = CommandResult::success()
        .with_message(if dry_run {
            "Infra smoke dry-run complete"
        } else {
            "Infra smoke complete"
        })
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed());
    if dry_run {
        result = result.with_warning("dry-run did not start or stop services".to_string());
    }
    Ok(result)
}

fn read_only_smoke_commands() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        ("current infra status", vec!["infra", "status"]),
        (
            "all-checkout infra status",
            vec!["infra", "status", "--all-checkouts"],
        ),
        ("run target list", vec!["run", "list"]),
        ("run core dry-run", vec!["run", "core", "--dry-run"]),
    ]
}

fn run_managed_core_smoke(config: &StackConfig) -> Result<InfraSmokeStep> {
    let manager = JobManager::new(xtask_config().jobs_dir())?;
    let current_exe = std::env::current_exe().wrap_err("failed to resolve current xtask binary")?;
    let args = vec![
        "--fg".to_string(),
        "--format".to_string(),
        "json".to_string(),
        "run".to_string(),
        "core".to_string(),
    ];
    let env_vars = crate::preflight::local_runtime_env_overrides();
    let job = manager.spawn_with_env(&current_exe.to_string_lossy(), &args, &env_vars)?;
    let job_id = job.id;

    match wait_for_sinexd_observed(config, Duration::from_secs(300)) {
        Ok(status) => {
            let cancel_result = manager.cancel(job_id);
            let stopped = wait_for_sinexd_stopped(config, Duration::from_secs(10));
            match (cancel_result, stopped) {
                (Ok(true), Ok(())) => Ok(InfraSmokeStep {
                    name: "managed local sinexd runtime".to_string(),
                    command: vec!["xtask".into(), "--bg".into(), "run".into(), "core".into()],
                    status: "passed".to_string(),
                    detail: format!(
                        "job {job_id} observed pids {:?} rss={}, then cancelled cleanly",
                        status.sinexd.pids,
                        format_bytes(status.sinexd.rss_bytes)
                    ),
                }),
                (Ok(false), _) => bail!(
                    "managed local sinexd smoke observed job {job_id}, but jobs cancel reported it was not running"
                ),
                (Err(error), _) => Err(error)
                    .with_context(|| format!("failed to cancel managed sinexd smoke job {job_id}")),
                (_, Err(error)) => Err(error).with_context(|| {
                    format!("managed sinexd smoke job {job_id} did not disappear after cancel")
                }),
            }
        }
        Err(error) => {
            let _ = manager.cancel(job_id);
            let status = manager
                .get(job_id)
                .ok()
                .flatten()
                .map(|job| job.job_status)
                .unwrap_or(JobLifecycleStatus::Orphaned);
            Err(error).with_context(|| {
                format!("managed sinexd smoke job {job_id} was not observable; last job status was {status:?}")
            })
        }
    }
}

fn wait_for_sinexd_observed(config: &StackConfig, timeout: Duration) -> Result<StackStatus> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let status = StackStatus::gather(config);
        if status.sinexd.running {
            return Ok(status);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    bail!(
        "timed out after {}s waiting for dev-local sinexd to appear in infra status",
        timeout.as_secs()
    )
}

fn wait_for_sinexd_stopped(config: &StackConfig, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let status = StackStatus::gather(config);
        if !status.sinexd.running {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    bail!(
        "timed out after {}s waiting for dev-local sinexd to stop",
        timeout.as_secs()
    )
}

fn run_xtask_probe(name: &str, args: &[&str]) -> Result<InfraSmokeStep> {
    let current_exe = std::env::current_exe().wrap_err("failed to resolve current xtask binary")?;
    let output = Command::new(&current_exe)
        .arg("--format")
        .arg("json")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .wrap_err_with(|| format!("failed to run smoke probe {name}"))?;
    let command = std::iter::once("xtask".to_string())
        .chain(args.iter().map(|arg| (*arg).to_string()))
        .collect();
    let status = if output.status.success() {
        "passed"
    } else {
        "failed"
    }
    .to_string();
    let detail = if output.status.success() {
        "probe completed without starting unexpected services".to_string()
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        format!(
            "exit status {}; stderr={}; stdout={}",
            output.status,
            stderr.trim(),
            stdout.trim()
        )
    };
    if !output.status.success() {
        bail!("infra smoke probe {name} failed: {detail}");
    }
    Ok(InfraSmokeStep {
        name: name.to_string(),
        command,
        status,
        detail,
    })
}

fn stop_current_checkout_infra(config: &StackConfig, verbose: bool) -> Result<()> {
    stack::nats_stop(config, verbose)?;
    stack::pg_stop(config, verbose)?;
    CheckoutState::for_current_checkout()?.release_lock()?;
    Ok(())
}

fn all_checkouts_status() -> Result<AllCheckoutsStatus> {
    let base_dir = CheckoutState::default_inventory_base_dir();
    let roots = CheckoutState::inventory_roots_under(&base_dir)?;
    Ok(AllCheckoutsStatus::gather(base_dir, roots))
}

fn smoke_snapshot(config: &StackConfig) -> InfraSmokeSnapshot {
    let status = StackStatus::gather(config);
    let state_bytes = status.data_sizes.postgres_bytes
        + status.data_sizes.nats_bytes
        + status.data_sizes.annex_bytes;
    InfraSmokeSnapshot {
        postgres: format_service_state(&status.postgres).to_string(),
        nats: format_service_state(&status.nats).to_string(),
        sinexd: if status.sinexd.running {
            "running".to_string()
        } else {
            "stopped".to_string()
        },
        rss_bytes: status.postgres.rss_bytes.unwrap_or(0)
            + status.nats.rss_bytes.unwrap_or(0)
            + status.sinexd.rss_bytes,
        state_bytes,
    }
}

fn services_running(status: &StackStatus) -> bool {
    status.postgres.running || status.nats.running || status.sinexd.running
}

fn service_mode_decision() -> InfraServiceModeDecision {
    InfraServiceModeDecision {
        selected_default: "per-checkout isolated",
        reason: "SQLx validation, schema drift, destructive tests, and JetStream state remain branch-sensitive; current cleanup makes the cost visible and stoppable instead of sharing mutable state by default.",
        shared_service_status: "not default; requires a proven namespace/database cleanup design before use",
        hybrid_status: "allowed as future optimization only when compile-only and destructive/runtime paths are separated by command metadata",
        correctness_notes: vec![
            "Postgres uses checkout-local Unix sockets and databases so schema apply and test cleanup cannot cross branches.",
            "NATS uses checkout-derived ports and JetStream storage so subjects, streams, durable consumers, and DLQ state stay isolated.",
            "All-checkout status/stop expose the RAM/process cost and stale state without touching system-managed services.",
        ],
    }
}

async fn execute_status(
    config: &StackConfig,
    watch: bool,
    all_checkouts: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if all_checkouts {
        if watch {
            bail!("infra status --all-checkouts does not support --watch");
        }
        return execute_all_checkouts_status(ctx);
    }

    loop {
        if watch {
            print!("\x1B[2J\x1B[H");
        }

        let status = StackStatus::gather(config);

        if ctx.is_human() {
            println!("sinex-dev infra status");
            println!("────────────────────────────────────────");
            println!("Checkout:    {}", status.checkout_root.display());
            println!("Dev-state:   {}", status.dev_state_dir.display());
            println!("Logs:        {}", status.logs_dir.display());
            println!(
                "PostgreSQL:  {}{} (unix socket, port: {}, rss: {})",
                format_service_state(&status.postgres),
                format_pid(status.postgres.pid),
                status.postgres.port,
                format_optional_bytes(status.postgres.rss_bytes),
            );
            println!(
                "NATS:        {}{} (port: {}, rss: {})",
                format_service_state(&status.nats),
                format_pid(status.nats.pid),
                status.nats.port,
                format_optional_bytes(status.nats.rss_bytes),
            );
            println!(
                "sinexd:      {} (rss: {})",
                format_runtime_state(&status.sinexd),
                format_bytes(status.sinexd.rss_bytes)
            );
            if let Some(issue) = &status.sinexd.issue {
                println!("             {issue}");
            }
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
            if let Some(issue) = &status.sinexd.issue {
                result = result.with_warning(issue.clone());
            }
            return Ok(result);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn execute_all_checkouts_status(ctx: &CommandContext) -> Result<CommandResult> {
    let base_dir = CheckoutState::default_inventory_base_dir();
    let roots = CheckoutState::inventory_roots_under(&base_dir)?;
    let status = AllCheckoutsStatus::gather(base_dir, roots);

    if ctx.is_human() {
        println!("sinex-dev infra status: all checkouts");
        println!("────────────────────────────────────────");
        println!(
            "Checkouts: {}  RSS: {}  state: {}",
            status.totals.checkout_count,
            format_bytes(status.totals.rss_bytes),
            format_bytes(status.totals.state_bytes)
        );
        println!(
            "Running:   postgres={} nats={} sinexd={}",
            status.totals.running_postgres,
            status.totals.running_nats,
            status.totals.running_sinexd
        );
        println!(
            "Stale PIDs: postgres={} nats={}",
            status.totals.stale_postgres_pid_files, status.totals.stale_nats_pid_files
        );
        for checkout in &status.checkouts {
            println!();
            println!("{}", checkout.cache_root.display());
            println!("  dev-state: {}", checkout.dev_state_dir.display());
            match &checkout.checkout_path {
                Some(path) => println!(
                    "  checkout:  {} ({})",
                    path.display(),
                    if checkout.checkout_path_exists == Some(true) {
                        "exists"
                    } else {
                        "missing"
                    }
                ),
                None => println!("  checkout:  unknown"),
            }
            println!(
                "  lock:      {}{}",
                format_lock_state(checkout.lock.state),
                checkout
                    .lock
                    .pid
                    .map(|pid| format!(" pid={pid}"))
                    .unwrap_or_default()
            );
            if let Some(issue) = &checkout.lock.issue {
                println!("             {issue}");
            }
            println!(
                "  postgres:  {}{} rss={}",
                format_service_state(&checkout.postgres),
                format_pid(checkout.postgres.pid),
                format_optional_bytes(checkout.postgres.rss_bytes)
            );
            println!(
                "  nats:      {}{} port={} rss={}",
                format_service_state(&checkout.nats),
                format_pid(checkout.nats.pid),
                checkout.nats.port,
                format_optional_bytes(checkout.nats.rss_bytes)
            );
            println!(
                "  sinexd:    {} rss={}",
                if checkout.sinexd.running {
                    format!("running pids={:?}", checkout.sinexd.pids)
                } else {
                    "stopped".to_string()
                },
                format_bytes(checkout.sinexd.rss_bytes)
            );
            if let Some(issue) = &checkout.sinexd.issue {
                println!("             {issue}");
            }
            println!(
                "  sizes:     pg={} nats={} annex={} logs={} total={}",
                format_bytes(checkout.data_sizes.postgres_bytes),
                format_bytes(checkout.data_sizes.nats_bytes),
                format_bytes(checkout.data_sizes.annex_bytes),
                format_bytes(checkout.logs_bytes),
                format_bytes(checkout.total_state_bytes)
            );
            for command in &checkout.remediation {
                println!("  remedy:    {command}");
            }
            for issue in &checkout.data_size_issues {
                println!("  issue:     {issue}");
            }
        }
    }

    let mut result = CommandResult::success().with_data(serde_json::to_value(&status)?);
    for issue in &status.issues {
        result = result.with_warning(issue.clone());
    }
    Ok(result)
}

fn format_service_state(status: &stack::ServiceStatus) -> &'static str {
    match status.pid_state {
        stack::ServicePidState::Missing => "stopped",
        stack::ServicePidState::Running => "running",
        stack::ServicePidState::Stale => "stale-pid",
        stack::ServicePidState::Malformed => "malformed-pid",
    }
}

fn format_lock_state(state: stack::LockState) -> &'static str {
    match state {
        stack::LockState::Missing => "missing",
        stack::LockState::Live => "live",
        stack::LockState::Stale => "stale",
        stack::LockState::Malformed => "malformed",
    }
}

fn format_pid(pid: Option<u32>) -> String {
    pid.map(|pid| format!(" pid={pid}")).unwrap_or_default()
}

fn format_runtime_state(status: &stack::RuntimeProcessStatus) -> String {
    if status.running {
        format!("running pids={:?}", status.pids)
    } else {
        "stopped".to_string()
    }
}

fn format_optional_bytes(bytes: Option<u64>) -> String {
    bytes.map(format_bytes).unwrap_or_else(|| "-".to_string())
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

    let status = crate::process::run_managed_foreground_std_command(&mut cmd, "infra logs")
        .context("tail failed")?;
    if !crate::process::status_indicates_clean_interactive_shutdown(&status) {
        bail!("tail failed");
    }

    Ok(CommandResult::success())
}
