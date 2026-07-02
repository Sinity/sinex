// xtask is build tooling, not library code — allow infrastructure patterns
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::missing_errors_doc)] // Internal build tooling, not a public library API
#![allow(clippy::doc_markdown)] // Internal docs, not published
#![allow(async_fn_in_trait)] // XtaskCommand uses async fn execute() without async_trait
#![feature(impl_trait_in_assoc_type)] // Used in IntoFuture implementations for sandbox builders

// Allow xtask to reference itself as ::xtask for macro-generated code
extern crate self as xtask;

use clap::{FromArgMatches, Parser, Subcommand};
use color_eyre::eyre::{Result, bail, eyre};

// Build-time metadata from shadow-rs
shadow_rs::shadow!(build_info);

mod affected;
pub mod bench;
pub mod cache_hygiene;
pub mod cargo_diagnostics;
pub mod cargo_runner;
pub mod command;
pub mod command_catalog;
pub mod command_docs;
pub mod commands;
mod config;
pub mod coordinator;
pub mod deps;
pub mod git_stack;
pub mod graph;
pub mod history;
pub mod impact;
pub mod infra;
pub mod jobs;
pub mod orchestrator;
pub mod output;
pub mod planner;
pub mod preflight;
pub mod process;
pub mod resources;
pub mod runtime_metrics;
pub mod runtime_target;
pub mod sandbox;
pub mod strict_changed;
pub use sandbox::context::Sandbox;
pub use sandbox::events::EventPublisher;
pub use sandbox::nats::EventOverrides;
pub use sandbox::prelude::{TestContext, TestResult};
pub mod nextest;
pub mod session;
pub mod tls;
mod tools;
pub mod watcher;

use command::{CommandContext, HistoryAccessMode, XtaskCommand};
use commands::{
    AnalyticsCommand, BuildCommand, CheckCommand, DoctorCommand, FixCommand, FreshnessCommand,
    GitStackCommand, ImpactCommand, JobsCommand, PrivacyCommand, RaDiagnoseCommand,
    RecordDriftBypassCommand, ReleaseReadinessCommand, ResetCommand, SchemaCommand, StatusCommand,
    TestCommand, ci::CiCommand, completions::CompletionsCommand, verify::VerifyCommand,
};
use config::config;
pub use config::workspace_target_dir_for;
use history::HistoryDb;
use output::{OutputFormat, OutputWriter};

/// Global options shared across all commands.
#[derive(Parser, Clone)]
struct GlobalOpts {
    /// Output format (human, json, compact, silent). When omitted, auto-detects:
    /// non-TTY stdout → json, TTY → human. Explicit --format human forces human
    /// output even when stdout is redirected.
    #[arg(long, global = true)]
    format: Option<OutputFormat>,

    /// Shorthand for --format json
    #[arg(long, global = true)]
    json: bool,

    /// List all available commands and exit
    #[arg(long, global = true)]
    list_commands: bool,

    /// Run command in background (returns immediately with job ID).
    /// Output is captured to files accessible via `xtask jobs`.
    #[arg(long, global = true)]
    bg: bool,

    /// Run command in foreground (default). Explicit flag for scripts.
    #[arg(long, global = true, conflicts_with = "bg")]
    fg: bool,

    /// Increase log verbosity. Use -v for INFO, -vv for DEBUG, -vvv for TRACE.
    /// Overridden by SINEX_LOG env var.
    #[arg(short = 'v', action = clap::ArgAction::Count, global = true)]
    verbosity: u8,
}

impl GlobalOpts {
    /// Get the effective output format.
    ///
    /// Precedence: `--json` > explicit `--format` > TTY detection > Human default.
    /// When stdout is not a TTY and no format was explicitly requested,
    /// JSON is selected automatically and a note is printed to stderr.
    /// Passing `--format human` explicitly forces human output even in non-TTY.
    pub(crate) fn output_format(&self) -> OutputFormat {
        if self.json {
            return OutputFormat::Json;
        }
        // If --format was explicitly set, honour it (overrides TTY detection).
        if let Some(explicit) = self.format {
            return explicit;
        }
        // Auto-detect: non-TTY stdout with no explicit format → JSON.
        if !output::is_tty() {
            eprintln!("Non-interactive output active (non-TTY).");
            return OutputFormat::Json;
        }
        OutputFormat::Human
    }

    /// Check if background execution is requested.
    pub(crate) fn is_background(&self) -> bool {
        self.bg && !self.fg
    }
}

pub(crate) fn parse_positive_u64_env_or_default(
    var_name: &str,
    default: u64,
    purpose: &str,
) -> u64 {
    match std::env::var(var_name) {
        Ok(raw) => match raw.parse::<u64>() {
            Ok(value) if value > 0 => value,
            Ok(_) => {
                tracing::warn!(
                    env_var = var_name,
                    value = %raw,
                    purpose,
                    default,
                    "ignoring non-positive environment override"
                );
                default
            }
            Err(error) => {
                tracing::warn!(
                    env_var = var_name,
                    value = %raw,
                    purpose,
                    default,
                    error = %error,
                    "ignoring invalid environment override"
                );
                default
            }
        },
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            tracing::warn!(
                env_var = var_name,
                purpose,
                default,
                "ignoring non-unicode environment override"
            );
            default
        }
    }
}

pub(crate) fn parse_one_shot_i64_env(var_name: &str, purpose: &str) -> Option<i64> {
    let raw = match std::env::var(var_name) {
        Ok(raw) => Some(raw),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            tracing::warn!(
                env_var = var_name,
                purpose,
                "ignoring non-unicode one-shot environment value"
            );
            None
        }
    };

    unsafe {
        std::env::remove_var(var_name);
    }

    raw.and_then(|raw| match raw.parse::<i64>() {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::warn!(
                env_var = var_name,
                purpose,
                value = %raw,
                error = %error,
                "ignoring invalid one-shot environment value"
            );
            None
        }
    })
}

/// Categorized command help text, rendered by the custom help_template.
///
/// Subcommands are hidden from clap's auto-generated list and presented here
/// grouped by category instead. Each subcommand's own `--help` still works.
fn commands_help() -> String {
    let commands = crate::command_catalog::collect_command_catalog();
    crate::command_docs::render_commands_help(&commands)
}

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Developer tasks for the Sinex workspace",
    long_version = long_version(),
    // Custom template: categorized commands before options.
    // {usage} omits [COMMAND] when all subcommands are hidden, so we add it explicitly.
    help_template = "{about-with-newline}\
        \nUsage: xtask [COMMAND] [OPTIONS]\n{after-help}\
        \nOptions:\n{options}",
)]
pub struct Cli {
    #[command(flatten)]
    global: GlobalOpts,

    /// The command to run
    #[command(subcommand)]
    command: Option<Commands>,
}

impl Cli {
    /// Build the CLI command with dynamic categorized help (TTY-aware coloring).
    fn command_with_help() -> clap::Command {
        use clap::CommandFactory;
        Self::command().after_help(commands_help())
    }
}

/// Generate a detailed version string with build info
fn long_version() -> &'static str {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    VERSION.get_or_init(|| {
        format!(
            "{}\ncommit: {} ({})\nbuild: {}",
            build_info::PKG_VERSION,
            build_info::SHORT_COMMIT,
            build_info::BRANCH,
            build_info::BUILD_TIME
        )
    })
}

#[derive(Subcommand)]
enum Commands {
    // All subcommands hidden from auto-help; categorized listing via COMMANDS_HELP.

    // ─── Development ───────────────────────────────────────────────
    #[command(hide = true)]
    Fix(FixCommand),
    #[command(hide = true)]
    Check(CheckCommand),
    #[command(hide = true)]
    Test(TestCommand),
    #[command(hide = true)]
    Build(BuildCommand),

    // ─── Runtime ───────────────────────────────────────────────────
    #[command(hide = true)]
    Run(commands::RunCommand),
    #[command(
        hide = true,
        about = "Manage local infrastructure (Postgres, NATS, VMs)"
    )]
    Infra {
        #[command(subcommand)]
        cmd: commands::infra::InfraSubcommand,
    },
    #[command(hide = true)]
    Jobs(JobsCommand),
    #[command(hide = true)]
    Status(StatusCommand),

    // ─── Analysis ──────────────────────────────────────────────────
    #[command(hide = true)]
    Deps(commands::DepsCommand),
    #[command(hide = true)]
    History(commands::history::HistoryCommand),
    #[command(hide = true)]
    Analytics(AnalyticsCommand),
    #[command(hide = true)]
    Freshness(FreshnessCommand),
    #[command(hide = true)]
    Impact(ImpactCommand),
    #[command(hide = true)]
    GitStack(GitStackCommand),

    // ─── Diagnostics ───────────────────────────────────────────────
    #[command(hide = true)]
    Doctor(DoctorCommand),
    #[command(hide = true)]
    RaDiagnose(RaDiagnoseCommand),
    #[command(hide = true)]
    Ra(commands::RaCommand),
    #[command(hide = true)]
    Privacy(PrivacyCommand),
    #[command(hide = true)]
    Schema(SchemaCommand),
    #[command(hide = true)]
    Verify(VerifyCommand),
    #[command(hide = true)]
    ReleaseReadiness(ReleaseReadinessCommand),

    // ─── Generation ────────────────────────────────────────────────
    #[command(hide = true)]
    Docs(commands::DocsCommand),

    // ─── Maintenance ───────────────────────────────────────────────
    #[command(hide = true)]
    Exercise(commands::ExerciseCommand),
    #[command(hide = true)]
    Reset(ResetCommand),

    // ─── Hidden (not listed even in categorized help) ──────────────
    #[command(hide = true)]
    Ci(CiCommand),
    #[command(hide = true)]
    Completions(CompletionsCommand),
    /// Internal detached process watchdog — not for human use.
    #[command(hide = true, name = "__reap")]
    Reap(commands::reap::ReapCommand),
    /// Internal: record drift guard bypass events from the pre-push hook (#1565).
    #[command(hide = true, name = "record-drift-bypass")]
    RecordDriftBypass(RecordDriftBypassCommand),
}

/// Parse CLI matches, emitting a JSON error if `--json` is present and clap fails.
fn try_get_matches_or_exit() -> Result<clap::ArgMatches> {
    let clap_cmd = Cli::command_with_help();
    match clap_cmd.try_get_matches() {
        Ok(m) => Ok(m),
        Err(e) => {
            let want_json =
                std::env::args().any(|a| a == "--json" || a == "--format=json" || a == "-f=json");
            if want_json {
                let json = serde_json::json!({
                    "command": "xtask",
                    "status": "error",
                    "errors": [{"code": "INVALID_ARGUMENTS", "message": e.to_string().lines().next().unwrap_or("invalid arguments")}]
                });
                println!("{}", serde_json::to_string_pretty(&json)?);
                std::process::exit(2);
            }
            e.exit();
        }
    }
}

fn test_subcommand_name(cmd: &commands::TestCommand) -> Option<&'static str> {
    use commands::test::TestSubcommand;
    match cmd.subcommand.as_ref()? {
        TestSubcommand::Bench(_) => Some("bench"),
        TestSubcommand::Fuzz(_) => Some("fuzz"),
        TestSubcommand::Coverage(_) => Some("coverage"),
        TestSubcommand::Mutants(_) => Some("mutants"),
        TestSubcommand::Vm(_) => Some("vm"),
    }
}

/// Map a parsed command to its `(name, subcommand, profile, metadata)` tuple.
///
/// Extracted from `run_cli` so the top-level driver stays under the cognitive
/// complexity budget; this is a pure per-variant lookup with no side effects.
fn command_dispatch_metadata(
    command: &Commands,
) -> (
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
    command::CommandMetadata,
) {
    match command {
        Commands::Fix(cmd) => ("fix", None, None, cmd.metadata()),
        Commands::Check(cmd) => ("check", None, None, cmd.metadata()),
        Commands::Test(cmd) => ("test", test_subcommand_name(cmd), None, cmd.metadata()),
        Commands::Build(cmd) => ("build", None, None, cmd.metadata()),
        Commands::Run(cmd) => ("run", None, None, cmd.metadata()),
        Commands::Infra { .. } => ("infra", None, None, command::CommandMetadata::default()),
        Commands::Jobs(cmd) => ("jobs", None, None, cmd.metadata()),
        Commands::Status(cmd) => ("status", None, None, cmd.metadata()),
        Commands::Deps(cmd) => ("deps", None, None, cmd.metadata()),
        Commands::History(cmd) => ("history", None, None, cmd.metadata()),
        Commands::Analytics(cmd) => ("analytics", None, None, cmd.metadata()),
        Commands::Freshness(cmd) => ("freshness", None, None, cmd.metadata()),
        Commands::Impact(cmd) => ("impact", None, None, cmd.metadata()),
        Commands::GitStack(cmd) => ("git-stack", None, None, cmd.metadata()),
        Commands::Docs(cmd) => ("docs", None, None, cmd.metadata()),
        Commands::Doctor(cmd) => ("doctor", None, None, cmd.metadata()),
        Commands::RaDiagnose(cmd) => ("ra-diagnose", None, None, cmd.metadata()),
        Commands::Ra(cmd) => ("ra", None, None, cmd.metadata()),
        Commands::Privacy(cmd) => ("privacy", None, None, cmd.metadata()),
        Commands::Schema(cmd) => ("schema", None, None, cmd.metadata()),
        Commands::Verify(cmd) => ("verify", None, None, cmd.metadata()),
        Commands::ReleaseReadiness(cmd) => ("release-readiness", None, None, cmd.metadata()),
        Commands::Exercise(cmd) => ("exercise", None, None, cmd.metadata()),
        Commands::Reset(cmd) => ("reset", None, None, cmd.metadata()),
        Commands::Ci(cmd) => ("ci", None, None, cmd.metadata()),
        Commands::Completions(cmd) => ("completions", None, None, cmd.metadata()),
        Commands::Reap(cmd) => ("__reap", None, None, cmd.metadata()),
        Commands::RecordDriftBypass(cmd) => ("record-drift-bypass", None, None, cmd.metadata()),
    }
}

pub async fn run_cli() -> Result<()> {
    // Use try_get_matches so we can intercept parse errors and route them
    // through the JSON formatter when --json is present, instead of letting
    // clap print human text to stderr and exit. This prevents JSON consumers
    // from receiving clap's error prose on invalid args.
    let matches = try_get_matches_or_exit()?;
    let cli = Cli::from_arg_matches(&matches).map_err(|e| eyre!(e.to_string()))?;
    let output_format = cli.global.output_format();

    let bg_job_dir = std::env::var("XTASK_JOB_DIR").ok();
    if bg_job_dir.is_some() {
        // One-shot handoff: avoid leaking job control env vars to nested child
        // xtask processes spawned by tests.
        unsafe {
            std::env::remove_var("XTASK_JOB_DIR");
        }
    } else if let Err(error) = process::arm_current_process_parent_death_signal() {
        eprintln!("⚠️  Failed to arm xtask parent-death signal: {error}");
    }
    let claimed_bg_job = parse_one_shot_i64_env("XTASK_BG_JOB_ID", "background job claim");

    // Handle --list-commands before normal dispatch
    if cli.global.list_commands {
        return list_commands(output_format);
    }

    // Require a command if not using --list-commands
    let command = cli.command.ok_or_else(|| {
        eyre!("No command provided. Use --help to see available commands, or --list-commands for a summary.")
    });
    let command = match command {
        Ok(c) => c,
        Err(e) => {
            if matches!(output_format, OutputFormat::Json) {
                let json = serde_json::json!({
                    "command": "xtask",
                    "status": "error",
                    "errors": [{"code": "NO_COMMAND", "message": e.to_string()}]
                });
                println!("{}", serde_json::to_string_pretty(&json)?);
                std::process::exit(1);
            }
            return Err(e);
        }
    };

    // Dispatch — extract metadata (including timeout/history behavior) before consuming the command
    let (command_name, subcommand, profile, command_metadata) = command_dispatch_metadata(&command);

    let command_timeout = command_metadata.timeout;
    let tracks_invocation = command_metadata.track_in_history;
    let history_access = command_metadata.history_access;
    let (mut history_db_write, mut history_db_query, history_db_open_error) =
        if history_access.needs_history_db() {
            match open_history_db(history_access) {
                Ok(db) => {
                    if history_access.uses_writer() {
                        // Emit synthetic warning before start_invocation() clears the marker (T3).
                        // This must happen here — start_invocation() removes the metadata row, so any
                        // subsequent HistoryDb::open() would see is_synthetic=false.
                        db.warn_if_synthetic(&config().history_db_path());
                    }
                    match history_access {
                        HistoryAccessMode::None => (None, None, None),
                        HistoryAccessMode::Query => (None, Some(db), None),
                        HistoryAccessMode::ReadWrite => (Some(db), None, None),
                    }
                }
                Err(error) => (None, None, Some(error.to_string())),
            }
        } else {
            (None, None, None)
        };
    let claimed_bg_invocation =
        parse_one_shot_i64_env("XTASK_BG_INVOCATION_ID", "background invocation claim");
    let launcher_only_background_request =
        cli.global.is_background() && claimed_bg_invocation.is_none() && claimed_bg_job.is_none();
    let tracks_invocation = tracks_invocation && !launcher_only_background_request;

    let invocation_id = start_or_claim_invocation(
        tracks_invocation,
        claimed_bg_invocation,
        history_db_write.as_ref(),
        history_db_open_error.as_deref(),
        command_name,
        subcommand,
        profile,
    );

    // Update the tracing layer's invocation ID so all subsequent events are tagged.
    if let Some(id) = invocation_id {
        history::CURRENT_INVOCATION_ID.store(id, std::sync::atomic::Ordering::SeqCst);
    }

    // Initialize tracing only after the primary history DB connection and
    // invocation row exist. Observational commands skip history tracing entirely
    // so they do not open a competing writer connection through the trace layer.
    init_tracing(cli.global.verbosity, tracks_invocation);

    // Create context with invocation ID
    let ctx = CommandContext::new(
        OutputWriter::new(output_format),
        cli.global.is_background(),
        invocation_id,
        command_name,
    )
    .with_preopened_history_db_write(history_db_write.take())
    .with_preopened_history_db_query(history_db_query.take());

    // Fingerprint+scope recording moved to each command's execute() method
    // where it has access to the actual command args. See record_coordination_fingerprint().

    let execute_fut = async {
        match command {
            Commands::Fix(cmd) => cmd.execute(&ctx).await,
            Commands::Check(cmd) => cmd.execute(&ctx).await,
            Commands::Test(cmd) => cmd.execute(&ctx).await,
            Commands::Build(cmd) => cmd.execute(&ctx).await,
            Commands::Run(cmd) => cmd.execute(&ctx).await,
            Commands::Infra { cmd } => {
                commands::InfraCommand { subcommand: cmd }
                    .execute(&ctx)
                    .await
            }
            Commands::Jobs(cmd) => cmd.execute(&ctx).await,
            Commands::Status(cmd) => cmd.execute(&ctx).await,
            Commands::Deps(cmd) => cmd.execute(&ctx).await,
            Commands::History(cmd) => cmd.execute(&ctx).await,
            Commands::Analytics(cmd) => cmd.execute(&ctx).await,
            Commands::Freshness(cmd) => cmd.execute(&ctx).await,
            Commands::Impact(cmd) => cmd.execute(&ctx).await,
            Commands::GitStack(cmd) => cmd.execute(&ctx).await,
            Commands::Docs(cmd) => cmd.execute(&ctx).await,
            Commands::Doctor(cmd) => cmd.execute(&ctx).await,
            Commands::RaDiagnose(cmd) => cmd.execute(&ctx).await,
            Commands::Ra(cmd) => cmd.execute(&ctx).await,
            Commands::Privacy(cmd) => cmd.execute(&ctx).await,
            Commands::Schema(cmd) => cmd.execute(&ctx).await,
            Commands::Verify(cmd) => cmd.execute(&ctx).await,
            Commands::ReleaseReadiness(cmd) => cmd.execute(&ctx).await,
            Commands::Exercise(cmd) => cmd.execute(&ctx).await,
            Commands::Reset(cmd) => cmd.execute(&ctx).await,
            Commands::Ci(cmd) => cmd.execute(&ctx).await,
            Commands::Completions(cmd) => cmd.execute(&ctx).await,
            Commands::Reap(cmd) => cmd.execute(&ctx).await,
            Commands::RecordDriftBypass(cmd) => cmd.execute(&ctx).await,
        }
    };

    let mut process_monitor =
        tracks_invocation.then(process::InvocationResourceMonitor::start_for_current_process);
    let mut timed_out = false;
    let mut result = Box::pin(execute_with_optional_timeout(
        execute_fut,
        command_timeout,
        command_name,
        &mut timed_out,
    ))
    .await;

    let lingering_process_groups = if timed_out {
        0
    } else {
        match process::terminate_registered_process_groups("command completion") {
            Ok(count) => count,
            Err(error) => {
                eprintln!(
                    "⚠️  Failed to reap lingering child process groups after {command_name} completed: {error:#}"
                );
                0
            }
        }
    };

    if lingering_process_groups > 0 {
        let warning = format!(
            "Reaped {lingering_process_groups} lingering child process group(s) after {command_name} completed"
        );
        eprintln!("⚠️  {warning}");
        if let Ok(command_result) = &mut result {
            command_result.warnings.push(warning);
        }
    }

    let process_metrics = process_monitor
        .as_mut()
        .map(process::InvocationResourceMonitor::stop);

    let invocation_exit_code = match &result {
        Ok(res)
            if res.status == crate::output::Status::Failed
                || res.status == crate::output::Status::Partial =>
        {
            1
        }
        Ok(_) => 0,
        Err(_) => 1,
    };

    // Update history
    if let Some(id) = invocation_id {
        finish_invocation_history(
            &ctx,
            id,
            &result,
            invocation_exit_code,
            process_metrics.as_ref(),
            history_db_open_error.as_deref(),
        );
    }

    // Handle coordinator completion: clear state, spawn queued work (FIFO).
    if let Some(true) =
        claimed_bg_job.map(|_| matches!(command_name, "check" | "test" | "build" | "fix" | "vm"))
    {
        handle_coordinator_completion(command_name);
    }

    // Write exit_code file and record background job completion.
    if let Some(job_dir) = bg_job_dir {
        record_bg_job_completion(
            &job_dir,
            claimed_bg_job,
            invocation_exit_code,
            command_name,
            &ctx,
            history_db_open_error.as_deref(),
        );
    }

    match result {
        Ok(res) => {
            res.print(ctx.writer(), command_name);
            if res.status == crate::output::Status::Failed
                || res.status == crate::output::Status::Partial
            {
                bail!("Command failed with status: {:?}", res.status);
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Record invocation completion in history and mark context as finished.
fn finish_invocation_history(
    ctx: &command::CommandContext,
    id: i64,
    result: &Result<command::CommandResult>,
    invocation_exit_code: i32,
    process_metrics: Option<&process::InvocationResourceMetrics>,
    history_db_open_error: Option<&str>,
) {
    let status = match result {
        Ok(res)
            if res.status == crate::output::Status::Failed
                || res.status == crate::output::Status::Partial =>
        {
            crate::history::InvocationStatus::Failed
        }
        Ok(_) => crate::history::InvocationStatus::Success,
        Err(_) => crate::history::InvocationStatus::Failed,
    };
    let duration = match result {
        Ok(res) => res.duration_secs.unwrap_or(ctx.elapsed().as_secs_f64()),
        Err(_) => ctx.elapsed().as_secs_f64(),
    };
    match ctx.try_with_history_db(|db| {
        if let Some(metrics) = process_metrics {
            db.record_resource_metrics(id, metrics)?;
        }
        db.finish_invocation(id, status, Some(invocation_exit_code), duration)
    }) {
        Some(Ok(())) => {}
        Some(Err(error)) => {
            eprintln!("⚠️  Failed to record invocation result: {error}");
        }
        None => {
            let error = history_db_open_error.unwrap_or("history DB unavailable");
            eprintln!("⚠️  Failed to open history DB to finish invocation {id}: {error}");
        }
    }
    ctx.mark_finished();
}

/// Start a new invocation row or claim a pre-reserved background invocation.
/// Returns the invocation id, or None if tracking is disabled or the DB is unavailable.
fn start_or_claim_invocation(
    tracks: bool,
    claimed_bg_invocation: Option<i64>,
    history_db: Option<&HistoryDb>,
    history_db_open_error: Option<&str>,
    command_name: &str,
    subcommand: Option<&str>,
    profile: Option<&str>,
) -> Option<i64> {
    if !tracks {
        return None;
    }
    let db_err = || history_db_open_error.unwrap_or("unknown history DB open failure");
    if let Some(bg_id) = claimed_bg_invocation {
        if let Some(db) = history_db {
            if let Err(error) =
                db.claim_background_invocation(bg_id, command_name, subcommand, profile, None)
            {
                eprintln!("⚠️  Failed to claim background invocation {bg_id}: {error}");
            }
        } else {
            eprintln!(
                "⚠️  Failed to open history DB for background invocation {bg_id}: {}",
                db_err()
            );
        }
        return Some(bg_id);
    }
    if let Some(db) = history_db {
        match db.start_invocation(command_name, subcommand, profile, None) {
            Ok(id) => return Some(id),
            Err(error) => {
                eprintln!("⚠️  Failed to start invocation history row: {error}");
                return None;
            }
        }
    }
    eprintln!(
        "⚠️  Failed to open history DB to start invocation: {}",
        db_err()
    );
    None
}

/// Attempt to pop and spawn the next queued work item after a coordinated job completes.
fn handle_coordinator_completion(command_name: &str) {
    let Ok(coord) = coordinator::JobCoordinator::new() else {
        return;
    };
    let Ok(Some(queued)) = coord.handle_completion(command_name) else {
        return;
    };
    let cfg = config();
    let manager = match jobs::JobManager::new(cfg.jobs_dir()) {
        Ok(m) => m,
        Err(error) => {
            eprintln!(
                "Warning: failed to open jobs directory for queued {command_name} work: {error}"
            );
            return;
        }
    };
    let queued_command = if queued.command.is_empty() {
        command_name.to_string()
    } else {
        queued.command.clone()
    };
    match manager.spawn_xtask(&queued_command, &queued.args, queued.output_format) {
        Ok(job) => {
            if let Some(pid) = job.pid {
                let start_ticks =
                    crate::process::read_proc_sample(pid).map_or(0, |s| s.start_ticks);
                if let Err(error) = coord.update_state(&queued_command, job.id, pid, start_ticks) {
                    eprintln!(
                        "⚠️  Failed to update queued {queued_command} coordinator state for job {}: {error}",
                        job.id
                    );
                }
            } else {
                eprintln!(
                    "⚠️  Failed to update queued {queued_command} coordinator state for job {}: spawned job did not expose a PID",
                    job.id
                );
            }
        }
        Err(error) => {
            eprintln!("Warning: failed to spawn queued {queued_command} work: {error}");
        }
    }
}

/// Write exit_code to the job dir, record completion in history, and optionally
/// send a desktop notification. Called only when `XTASK_JOB_DIR` is set.
fn record_bg_job_completion(
    job_dir: &str,
    claimed_bg_job: Option<i64>,
    invocation_exit_code: i32,
    command_name: &str,
    ctx: &command::CommandContext,
    history_db_open_error: Option<&str>,
) {
    let exit_code_path = std::path::Path::new(job_dir).join("exit_code");
    if let Err(error) = std::fs::write(&exit_code_path, format!("{invocation_exit_code}\n")) {
        eprintln!(
            "⚠️  Failed to write background exit code file {}: {error}",
            exit_code_path.display()
        );
    }

    if let Some(job_id) = claimed_bg_job {
        let stdout_path = std::path::Path::new(job_dir).join("stdout.log");
        let stderr_path = std::path::Path::new(job_dir).join("stderr.log");
        let job_status = if invocation_exit_code == 0 {
            crate::history::JobLifecycleStatus::Completed
        } else if invocation_exit_code == 124 {
            crate::history::JobLifecycleStatus::Killed
        } else {
            crate::history::JobLifecycleStatus::Failed
        };
        match ctx.try_with_history_db(|db| {
            db.finish_background_job(
                job_id,
                job_status,
                Some(invocation_exit_code),
                ctx.elapsed().as_secs_f64(),
                stdout_path.exists().then_some(stdout_path.as_path()),
                stderr_path.exists().then_some(stderr_path.as_path()),
            )
        }) {
            Some(Ok(())) => {}
            Some(Err(error)) => {
                eprintln!("⚠️  Failed to record background job completion for {job_id}: {error}");
            }
            None => {
                let error = history_db_open_error.unwrap_or("history DB unavailable");
                eprintln!(
                    "⚠️  Failed to open history DB to record background job completion for {job_id}: {error}"
                );
            }
        }
    }

    if config().prefs.notify_on_completion {
        let status_str = if invocation_exit_code == 0 {
            "success"
        } else {
            "failed"
        };
        let duration = ctx.elapsed().as_secs_f64();
        let summary = format!("xtask {command_name}");
        let body = format!("{command_name}: {status_str} ({duration:.1}s)");
        let _ = std::process::Command::new("notify-send")
            .arg("--app-name=xtask")
            .arg(&summary)
            .arg(&body)
            .status();
    }
}

/// Run `fut` with an optional deadline. Sets `timed_out` when the deadline fires
/// and kills lingering child processes before returning the timeout error.
async fn execute_with_optional_timeout<F>(
    fut: F,
    timeout: Option<std::time::Duration>,
    command_name: &str,
    timed_out: &mut bool,
) -> Result<crate::command::CommandResult>
where
    F: std::future::Future<Output = Result<crate::command::CommandResult>>,
{
    let Some(timeout) = timeout else {
        return fut.await;
    };
    if let Ok(result) = tokio::time::timeout(timeout, fut).await {
        return result;
    }
    *timed_out = true;
    match process::terminate_registered_process_groups("command timeout") {
        Ok(terminated) if terminated > 0 => {
            eprintln!(
                "⚠️  Terminated {terminated} lingering child process group(s) after {command_name} timed out"
            );
        }
        Ok(_) => {}
        Err(error) => {
            eprintln!(
                "⚠️  Failed to terminate child process groups after {command_name} timed out: {error:#}"
            );
        }
    }
    match process::terminate_current_process_descendants("command timeout") {
        Ok(terminated) if terminated > 0 => {
            eprintln!(
                "⚠️  Terminated {terminated} remaining descendant process(es) after {command_name} timed out"
            );
        }
        Ok(_) => {}
        Err(error) => {
            eprintln!(
                "⚠️  Failed to terminate descendant processes after {command_name} timed out: {error:#}"
            );
        }
    }
    Err(eyre!(
        "Command '{command_name}' timed out after {timeout:?}"
    ))
}

fn open_history_db(history_access: HistoryAccessMode) -> Result<HistoryDb> {
    let cfg = config();
    cfg.ensure_state_dir()
        .map_err(|e| eyre!("Failed to create state directory: {e}"))?;
    match history_access {
        HistoryAccessMode::None => {
            bail!("history DB requested for command that declared no history access")
        }
        HistoryAccessMode::Query => HistoryDb::open_query(&cfg.history_db_path()),
        HistoryAccessMode::ReadWrite => HistoryDb::open(&cfg.history_db_path()),
    }
}

fn init_tracing(verbosity: u8, enable_history_layer: bool) {
    use tracing::Level;
    use tracing_subscriber::filter::filter_fn;
    use tracing_subscriber::prelude::*;

    let level_filter = match verbosity {
        0 => tracing_subscriber::filter::LevelFilter::OFF,
        1 => tracing_subscriber::filter::LevelFilter::INFO,
        2 => tracing_subscriber::filter::LevelFilter::DEBUG,
        _ => tracing_subscriber::filter::LevelFilter::TRACE,
    };
    // The console fmt layer gets the global EnvFilter (level_filter drives it).
    // When verbosity=0 (default), LevelFilter::OFF suppresses all stderr output —
    // intentional: xtask is quiet by default unless -v is passed.
    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level_filter.into())
                .with_env_var("SINEX_LOG")
                .from_env_lossy(),
        );

    let init_result = if enable_history_layer {
        // The history layer MUST have its own per-layer filter so it receives
        // events regardless of the global EnvFilter level set above.
        //
        // Without `.with_filter(...)` here the global LevelFilter::OFF (verbosity=0)
        // drops all events before `HistoryTracingLayer::on_event()` is called,
        // resulting in 0 rows ever written to the trace_events table despite the
        // layer and schema being fully implemented.
        //
        // The per-layer filter mirrors `should_persist()` in tracing_layer.rs:
        // WARN/ERROR always; INFO only from coordinator, preflight, cargo targets.
        let history_filter = filter_fn(|metadata| match *metadata.level() {
            Level::ERROR | Level::WARN => true,
            Level::INFO => {
                metadata.target().starts_with("xtask::coordinator")
                    || metadata.target().starts_with("xtask::preflight")
                    || metadata.target().starts_with("xtask::cargo")
            }
            Level::DEBUG | Level::TRACE => false,
        });
        registry
            .with(
                history::HistoryTracingLayer::new(config::config().history_db_path())
                    .with_filter(history_filter),
            )
            .try_init()
    } else {
        registry.try_init()
    };

    let _ = init_result;
}

/// List all available commands using clap introspection.
fn list_commands(format: OutputFormat) -> Result<()> {
    let commands = crate::command_catalog::collect_command_catalog();

    if matches!(format, OutputFormat::Json) {
        let output = serde_json::json!({
            "commands": commands,
            "version": build_info::PKG_VERSION,
            "git_hash": build_info::SHORT_COMMIT,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("Available commands:\n");
        for cmd in &commands {
            let about = cmd.about.as_deref().unwrap_or("");
            println!("  {:16} {}", cmd.name, about);

            if !cmd.args.is_empty() {
                // Filter out global args for cleaner output in the main listing
                let local_args: Vec<&crate::command_catalog::ArgInfo> =
                    cmd.args.iter().filter(|a| !a.global).collect();

                if !local_args.is_empty() {
                    println!();
                    for arg in local_args {
                        let mut flags = String::new();
                        if let Some(short) = arg.short {
                            flags.push('-');
                            flags.push(short);
                        }
                        if let Some(long) = &arg.long {
                            if !flags.is_empty() {
                                flags.push_str(", ");
                            }
                            flags.push_str("--");
                            flags.push_str(long);
                        }
                        // Add required indicator
                        if arg.required {
                            flags.push_str(" <REQUIRED>");
                        }

                        let help = arg.help.as_deref().unwrap_or("");
                        // Simple padding
                        println!("    {flags:<24} {help}");
                    }
                    println!();
                }
            }

            for sub in &cmd.subcommands {
                let sub_about = sub.about.as_deref().unwrap_or("");
                println!("    {:14} {}", sub.name, sub_about);

                if !sub.args.is_empty() {
                    for arg in &sub.args {
                        let mut flags = String::new();
                        if let Some(short) = arg.short {
                            flags.push('-');
                            flags.push(short);
                        }
                        if let Some(long) = &arg.long {
                            if !flags.is_empty() {
                                flags.push_str(", ");
                            }
                            flags.push_str("--");
                            flags.push_str(long);
                        }
                        if arg.required {
                            flags.push_str(" <REQUIRED>");
                        }

                        let help = arg.help.as_deref().unwrap_or("");
                        println!("      {flags:<22} {help}");
                    }
                    // Add a small spacer after args if there were any
                    println!();
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "lib_test.rs"]
mod tests;
