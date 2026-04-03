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
pub mod cargo_diagnostics;
pub mod cargo_runner;
pub mod command;
pub mod commands;
mod config;
pub mod coordinator;
pub mod deps;
pub mod graph;
pub mod history;
pub mod infra;
pub mod jobs;
pub mod orchestrator;
pub mod output;
pub mod preflight;
pub mod process;
pub mod resources;
pub mod runtime_metrics;
pub mod sandbox;
pub use sandbox::context::Sandbox;
pub use sandbox::events::EventPublisher;
pub use sandbox::nats::EventOverrides;
pub use sandbox::prelude::{TestContext, TestResult};
pub mod nextest;
pub mod session;
pub mod tls;
mod tools;
pub mod watcher;

use command::{CommandContext, XtaskCommand};
use commands::{
    AnalyticsCommand, BuildCommand, CheckCommand, DoctorCommand, FixCommand, JobsCommand,
    PrivacyCommand, ResetCommand, StatusCommand, TestCommand, WorkCommand, ci::CiCommand,
    completions::CompletionsCommand,
};
use config::config;
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
    use std::io::IsTerminal;

    let use_color = std::io::stdout().is_terminal();

    let mut out = String::from("Commands:\n");
    let categories: &[(&str, &[(&str, &str)])] = &[
        (
            "Development",
            &[
                ("fix", "Apply automatic fixes (fmt, clippy, fix)"),
                ("check", "Fast compile check, clippy, lint-forbidden"),
                (
                    "test",
                    "Run tests (subcommands: bench, fuzz, coverage, mutants, vm)",
                ),
                ("build", "Build workspace packages"),
                ("work", "Workflow shortcut (check → test pipeline)"),
            ],
        ),
        (
            "Runtime",
            &[
                ("run", "Run sinex binaries (ingestd, gateway, nodes)"),
                ("infra", "Manage local infrastructure (Postgres, NATS, VMs)"),
                ("jobs", "Background job management"),
                ("status", "Workspace status and service health"),
            ],
        ),
        (
            "Analysis",
            &[
                (
                    "deps",
                    "Dependency analysis (tree, duplicates, unused, timings, impact)",
                ),
                ("history", "Build/test execution history and trends"),
                (
                    "analytics",
                    "Developer intelligence (health, hotspots, reliability, velocity)",
                ),
            ],
        ),
        (
            "Diagnostics",
            &[
                ("doctor", "Health check and auto-remediation"),
                ("privacy", "Privacy engine utilities"),
            ],
        ),
        (
            "Generation",
            &[(
                "docs",
                "Documentation (rustdoc, AGENTS.md, AI snapshot) — subcommands: build, serve, agents, snapshot",
            )],
        ),
        (
            "Maintenance",
            &[
                ("exercise", "xtask self-validation suite"),
                ("reset", "Wipe developer state for a clean slate"),
            ],
        ),
    ];

    for (i, (category, cmds)) in categories.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        if use_color {
            // Bold + underline
            out.push_str(&format!("  \x1b[1;4m{category}\x1b[0m\n"));
        } else {
            out.push_str(&format!("  {category}:\n"));
        }
        for (name, desc) in *cmds {
            out.push_str(&format!("    {name:<12}{desc}\n"));
        }
    }
    out
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
    #[command(hide = true)]
    Work(WorkCommand),

    // ─── Runtime ───────────────────────────────────────────────────
    #[command(hide = true)]
    Run(commands::RunCommand),
    #[command(hide = true)]
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

    // ─── Diagnostics ───────────────────────────────────────────────
    #[command(hide = true)]
    Doctor(DoctorCommand),
    #[command(hide = true)]
    Privacy(PrivacyCommand),

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
}

pub async fn run_cli() -> Result<()> {
    // Use try_get_matches so we can intercept parse errors and route them
    // through the JSON formatter when --json is present, instead of letting
    // clap print human text to stderr and exit. This prevents JSON consumers
    // from receiving clap's error prose on invalid args.
    let clap_cmd = Cli::command_with_help();
    let matches = match clap_cmd.try_get_matches() {
        Ok(m) => m,
        Err(e) => {
            // If --json appears anywhere in raw args, emit a structured error.
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
            // Otherwise let clap print its formatted error and exit normally.
            e.exit();
        }
    };
    let cli = Cli::from_arg_matches(&matches).map_err(|e| eyre!(e.to_string()))?;

    let bg_job_dir = std::env::var("XTASK_JOB_DIR").ok();
    if bg_job_dir.is_some() {
        // One-shot handoff: avoid leaking job control env vars to nested child
        // xtask processes spawned by tests.
        unsafe {
            std::env::remove_var("XTASK_JOB_DIR");
        }
    }
    let claimed_bg_job = parse_one_shot_i64_env("XTASK_BG_JOB_ID", "background job claim");

    // Handle --list-commands before normal dispatch
    if cli.global.list_commands {
        return list_commands(cli.global.output_format());
    }

    // Require a command if not using --list-commands
    let command = cli.command.ok_or_else(|| {
        eyre!("No command provided. Use --help to see available commands, or --list-commands for a summary.")
    });
    let command = match command {
        Ok(c) => c,
        Err(e) => {
            if matches!(cli.global.output_format(), OutputFormat::Json) {
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

    // Dispatch — extract metadata (including timeout) before consuming the command
    let (command_name, subcommand, profile, command_timeout) = match &command {
        Commands::Fix(cmd) => ("fix", None, None, cmd.metadata().timeout),
        Commands::Check(cmd) => ("check", None, None, cmd.metadata().timeout),
        Commands::Test(cmd) => ("test", None, None, cmd.metadata().timeout),
        Commands::Build(cmd) => ("build", None, None, cmd.metadata().timeout),
        Commands::Run(cmd) => ("run", None, None, cmd.metadata().timeout),
        Commands::Infra { .. } => ("infra", None, None, None),
        Commands::Jobs(cmd) => ("jobs", None, None, cmd.metadata().timeout),
        Commands::Status(cmd) => ("status", None, None, cmd.metadata().timeout),
        Commands::Deps(cmd) => ("deps", None, None, cmd.metadata().timeout),
        Commands::History(cmd) => ("history", None, None, cmd.metadata().timeout),
        Commands::Analytics(cmd) => ("analytics", None, None, cmd.metadata().timeout),
        Commands::Docs(cmd) => ("docs", None, None, cmd.metadata().timeout),
        Commands::Doctor(cmd) => ("doctor", None, None, cmd.metadata().timeout),
        Commands::Privacy(cmd) => ("privacy", None, None, cmd.metadata().timeout),
        Commands::Exercise(cmd) => ("exercise", None, None, cmd.metadata().timeout),
        Commands::Reset(cmd) => ("reset", None, None, cmd.metadata().timeout),
        Commands::Work(cmd) => ("work", None, None, cmd.metadata().timeout),
        Commands::Ci(cmd) => ("ci", None, None, cmd.metadata().timeout),
        Commands::Completions(cmd) => ("completions", None, None, cmd.metadata().timeout),
    };

    // Track invocation in history
    let history_db = open_history_db();
    // Emit synthetic warning before start_invocation() clears the marker (T3).
    // This must happen here — start_invocation() removes the metadata row, so any
    // subsequent HistoryDb::open() would see is_synthetic=false.
    if let Ok(db) = history_db.as_ref() {
        db.warn_if_synthetic(&config().history_db_path());
    }
    let claimed_bg_invocation =
        parse_one_shot_i64_env("XTASK_BG_INVOCATION_ID", "background invocation claim");
    let invocation_id = if command_name != "completions" && command_name != "status" {
        if let Some(bg_id) = claimed_bg_invocation {
            match history_db.as_ref() {
                Ok(db) => {
                    if let Err(error) = db.claim_background_invocation(
                        bg_id,
                        command_name,
                        subcommand,
                        profile,
                        None,
                    ) {
                        eprintln!("⚠️  Failed to claim background invocation {bg_id}: {error}");
                    }
                }
                Err(error) => {
                    eprintln!(
                        "⚠️  Failed to open history DB for background invocation {bg_id}: {error}"
                    );
                }
            }
            Some(bg_id)
        } else {
            match history_db.as_ref() {
                Ok(db) => match db.start_invocation(command_name, subcommand, profile, None) {
                    Ok(id) => Some(id),
                    Err(error) => {
                        eprintln!("⚠️  Failed to start invocation history row: {error}");
                        None
                    }
                },
                Err(error) => {
                    eprintln!("⚠️  Failed to open history DB to start invocation: {error}");
                    None
                }
            }
        }
    } else {
        None
    };

    // Update the tracing layer's invocation ID so all subsequent events are tagged.
    if let Some(id) = invocation_id {
        history::CURRENT_INVOCATION_ID.store(id, std::sync::atomic::Ordering::SeqCst);
    }

    // Initialize tracing only after the primary history DB connection and
    // invocation row exist. This prevents the background trace writer from
    // racing `open_history_db()` on startup and spuriously reporting
    // `database is locked` against the same SQLite file.
    init_tracing(cli.global.verbosity);

    // Create context with invocation ID
    let ctx = CommandContext::new(
        OutputWriter::new(cli.global.output_format()),
        cli.global.is_background(),
        invocation_id,
        command_name,
    );

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
            Commands::Docs(cmd) => cmd.execute(&ctx).await,
            Commands::Doctor(cmd) => cmd.execute(&ctx).await,
            Commands::Privacy(cmd) => cmd.execute(&ctx).await,
            Commands::Exercise(cmd) => cmd.execute(&ctx).await,
            Commands::Reset(cmd) => cmd.execute(&ctx).await,
            Commands::Work(cmd) => cmd.execute(&ctx).await,
            Commands::Ci(cmd) => cmd.execute(&ctx).await,
            Commands::Completions(cmd) => cmd.execute(&ctx).await,
        }
    };

    let result = if let Some(timeout) = command_timeout {
        match tokio::time::timeout(timeout, execute_fut).await {
            Ok(result) => result,
            Err(_) => Err(eyre!(
                "Command '{command_name}' timed out after {timeout:?}"
            )),
        }
    } else {
        execute_fut.await
    };

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
        let status = match &result {
            Ok(res)
                if res.status == crate::output::Status::Failed
                    || res.status == crate::output::Status::Partial =>
            {
                crate::history::InvocationStatus::Failed
            }
            Ok(_) => crate::history::InvocationStatus::Success,
            Err(_) => crate::history::InvocationStatus::Failed,
        };
        let duration = match &result {
            Ok(res) => res.duration_secs.unwrap_or(ctx.elapsed().as_secs_f64()),
            Err(_) => ctx.elapsed().as_secs_f64(),
        };
        match history_db.as_ref() {
            Ok(db) => {
                if let Err(error) =
                    db.finish_invocation(id, status, Some(invocation_exit_code), duration)
                {
                    eprintln!("⚠️  Failed to record invocation result: {error}");
                }
            }
            Err(error) => {
                eprintln!("⚠️  Failed to open history DB to finish invocation {id}: {error}");
            }
        }
        ctx.mark_finished();
    }

    // Handle coordinator completion: clear state, spawn queued work (FIFO).
    // Uses block_in_place to ensure the spawn completes before process exits
    // (fire-and-forget tokio::spawn could lose work if runtime shuts down first).
    if matches!(command_name, "check" | "test" | "build" | "fix")
        && let Ok(coord) = coordinator::JobCoordinator::new()
        && let Ok(Some(queued)) = coord.handle_completion(command_name)
    {
        let cfg = config();
        match jobs::JobManager::new(cfg.jobs_dir()) {
            Ok(manager) => {
                match manager.spawn_xtask(command_name, &queued.args, queued.output_format) {
                    Ok(job) => {
                        // Update coordinator state with real job_id + pid.
                        // Critical for FIFO queue: handle_completion may have
                        // left remaining items in the state file with sentinel values.
                        if let Some(pid) = job.pid {
                            if let Err(error) = coord.update_state(command_name, job.id, pid) {
                                eprintln!(
                                    "⚠️  Failed to update queued {command_name} coordinator state for job {}: {error}",
                                    job.id
                                );
                            }
                        } else {
                            eprintln!(
                                "⚠️  Failed to update queued {command_name} coordinator state for job {}: spawned job did not expose a PID",
                                job.id
                            );
                        }
                    }
                    Err(error) => {
                        eprintln!("Warning: failed to spawn queued {command_name} work: {error}");
                    }
                }
            }
            Err(error) => {
                eprintln!(
                    "Warning: failed to open jobs directory for queued {command_name} work: {error}"
                );
            }
        }
    }

    // Write exit_code file for background job tracking.
    // XTASK_JOB_DIR is set by the bg job spawner so the zombie reaper can
    // determine success vs failure after the process exits.
    if let Some(job_dir) = bg_job_dir {
        let exit_code_path = std::path::Path::new(&job_dir).join("exit_code");
        if let Err(error) = std::fs::write(&exit_code_path, format!("{invocation_exit_code}\n")) {
            eprintln!(
                "⚠️  Failed to write background exit code file {}: {error}",
                exit_code_path.display()
            );
        }

        if let Some(job_id) = claimed_bg_job
            && let Ok(db) = history_db.as_ref()
        {
            let stdout_path = std::path::Path::new(&job_dir).join("stdout.log");
            let stderr_path = std::path::Path::new(&job_dir).join("stderr.log");
            let job_status = if invocation_exit_code == 0 {
                crate::history::JobLifecycleStatus::Completed
            } else if invocation_exit_code == 124 {
                crate::history::JobLifecycleStatus::Killed
            } else {
                crate::history::JobLifecycleStatus::Failed
            };
            if let Err(error) = db.finish_background_job(
                job_id,
                job_status,
                Some(invocation_exit_code),
                ctx.elapsed().as_secs_f64(),
                stdout_path.exists().then_some(stdout_path.as_path()),
                stderr_path.exists().then_some(stderr_path.as_path()),
            ) {
                eprintln!("⚠️  Failed to record background job completion for {job_id}: {error}");
            }
        }

        // W3: Desktop notification when running as a background subprocess.
        // Only fires when XTASK_JOB_DIR is set (we ARE the bg subprocess) and
        // the user has notify_on_completion = true in their preferences file.
        if config().prefs.notify_on_completion {
            let status_str = if invocation_exit_code == 0 {
                "success"
            } else {
                "failed"
            };
            let duration = ctx.elapsed().as_secs_f64();
            let summary = format!("xtask {command_name}");
            let body = format!("{command_name}: {status_str} ({duration:.1}s)");
            // notify-send is fire-and-forget; ignore failures (not installed, no DE, etc.)
            let _ = std::process::Command::new("notify-send")
                .arg("--app-name=xtask")
                .arg(&summary)
                .arg(&body)
                .status();
        }
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

fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    cfg.ensure_state_dir()
        .map_err(|e| eyre!("Failed to create state directory: {e}"))?;
    HistoryDb::open(&cfg.history_db_path())
}

fn init_tracing(verbosity: u8) {
    use tracing_subscriber::prelude::*;

    let level_filter = match verbosity {
        0 => tracing_subscriber::filter::LevelFilter::OFF,
        1 => tracing_subscriber::filter::LevelFilter::INFO,
        2 => tracing_subscriber::filter::LevelFilter::DEBUG,
        _ => tracing_subscriber::filter::LevelFilter::TRACE,
    };
    let history_layer = history::HistoryTracingLayer::new(config::config().history_db_path());
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level_filter.into())
                .with_env_var("SINEX_LOG")
                .from_env_lossy(),
        )
        .with(history_layer)
        .try_init();
}

/// List all available commands using clap introspection.
fn list_commands(format: OutputFormat) -> Result<()> {
    use clap::CommandFactory;
    use serde::Serialize;

    #[derive(Serialize)]
    struct ArgInfo {
        name: String,
        short: Option<char>,
        long: Option<String>,
        help: Option<String>,
        required: bool,
        global: bool,
        possible_values: Vec<String>,
        takes_value: bool,
    }

    #[derive(Serialize)]
    struct CommandInfo {
        name: String,
        about: Option<String>,
        subcommands: Vec<CommandInfo>,
        args: Vec<ArgInfo>,
    }

    // Internal commands excluded from --list-commands output
    const INTERNAL_COMMANDS: &[&str] = &["ci", "completions", "help"];

    fn extract_commands(cmd: &clap::Command) -> Vec<CommandInfo> {
        cmd.get_subcommands()
            // All user-facing commands are hidden from clap's auto-help (we render
            // categorized help ourselves), so we can't use is_hide_set() as filter.
            // Instead, exclude only truly internal commands by name.
            .filter(|sub| !INTERNAL_COMMANDS.contains(&sub.get_name()))
            .map(|sub| {
                let args = sub
                    .get_arguments()
                    .map(|arg| ArgInfo {
                        name: arg.get_id().to_string(),
                        short: arg.get_short(),
                        long: arg.get_long().map(String::from),
                        help: arg.get_help().map(ToString::to_string),
                        required: arg.is_required_set(),
                        global: arg.is_global_set(),
                        possible_values: arg
                            .get_possible_values()
                            .iter()
                            .map(|v| v.get_name().to_string())
                            .collect(),
                        takes_value: matches!(
                            arg.get_action(),
                            clap::ArgAction::Set | clap::ArgAction::Append
                        ),
                    })
                    .collect();

                CommandInfo {
                    name: sub.get_name().to_string(),
                    about: sub.get_about().map(std::string::ToString::to_string),
                    subcommands: extract_commands(sub),
                    args,
                }
            })
            .collect()
    }

    let cli = Cli::command();
    let commands = extract_commands(&cli);

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
                let local_args: Vec<&ArgInfo> = cmd.args.iter().filter(|a| !a.global).collect();

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
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    use crate::sandbox::EnvGuard;

    fn env_set(key: &str, value: Option<std::ffi::OsString>) -> EnvGuard {
        let mut guard = EnvGuard::new();
        match value {
            Some(v) => guard.set(key, v),
            None => guard.clear(key),
        }
        guard
    }

    #[sinex_test]
    async fn parse_positive_u64_env_or_default_rejects_invalid_values() -> TestResult<()> {
        let _guard = env_set("SINEX_TEST_TIMEOUT", Some("not-a-number".into()));

        assert_eq!(
            parse_positive_u64_env_or_default("SINEX_TEST_TIMEOUT", 42, "test timeout"),
            42
        );
        Ok(())
    }

    #[sinex_test]
    async fn parse_positive_u64_env_or_default_rejects_zero() -> TestResult<()> {
        let _guard = env_set("SINEX_TEST_TIMEOUT", Some("0".into()));

        assert_eq!(
            parse_positive_u64_env_or_default("SINEX_TEST_TIMEOUT", 42, "test timeout"),
            42
        );
        Ok(())
    }

    #[sinex_test]
    async fn parse_one_shot_i64_env_returns_value_and_clears_env() -> TestResult<()> {
        let _guard = env_set("SINEX_TEST_CLAIM", Some("123".into()));

        assert_eq!(
            parse_one_shot_i64_env("SINEX_TEST_CLAIM", "test claim"),
            Some(123)
        );
        assert!(
            std::env::var_os("SINEX_TEST_CLAIM").is_none(),
            "one-shot env var must be removed after claim"
        );
        Ok(())
    }

    #[sinex_test]
    async fn parse_one_shot_i64_env_rejects_invalid_values_and_clears_env() -> TestResult<()> {
        let _guard = env_set("SINEX_TEST_CLAIM", Some("abc".into()));

        assert_eq!(
            parse_one_shot_i64_env("SINEX_TEST_CLAIM", "test claim"),
            None
        );
        assert!(
            std::env::var_os("SINEX_TEST_CLAIM").is_none(),
            "invalid one-shot env var must still be removed"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn parse_one_shot_i64_env_rejects_non_unicode_and_clears_env() -> TestResult<()> {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let _guard = env_set("SINEX_TEST_CLAIM", Some(OsString::from_vec(vec![0xff])));

        assert_eq!(
            parse_one_shot_i64_env("SINEX_TEST_CLAIM", "test claim"),
            None
        );
        assert!(
            std::env::var_os("SINEX_TEST_CLAIM").is_none(),
            "non-unicode one-shot env var must still be removed"
        );
        Ok(())
    }
}
