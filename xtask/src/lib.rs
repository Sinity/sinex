// xtask is build tooling, not library code — allow infrastructure patterns
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::missing_errors_doc)] // Internal build tooling, not a public library API
#![allow(clippy::doc_markdown)] // Internal docs, not published
#![allow(async_fn_in_trait)] // XtaskCommand uses async fn execute() without async_trait
#![feature(impl_trait_in_assoc_type)] // Used in IntoFuture implementations for sandbox builders

// Allow xtask to reference itself as ::xtask for macro-generated code
extern crate self as xtask;

use clap::{Parser, Subcommand};
use color_eyre::eyre::{Result, bail, eyre};

// Build-time metadata from shadow-rs
shadow_rs::shadow!(build_info);

mod affected;
pub mod bench;
pub mod cargo_diagnostics;
pub mod command;
pub mod commands;
mod config;
pub mod coordinator;
pub mod deps;
pub mod graph;
pub mod history;
pub mod infra;
pub mod jobs;
pub mod output;
pub mod preflight;
pub mod process;
pub mod resources;
pub mod sandbox;
pub use sandbox::context::Sandbox;
pub use sandbox::events::EventPublisher;
pub use sandbox::nats::EventOverrides;
pub use sandbox::prelude::{TestContext, TestResult};
pub mod nextest;
pub mod tls;
mod tools;
pub mod watcher;

use command::{CommandContext, XtaskCommand};
use commands::{
    BuildCommand, CheckCommand, FixCommand, JobsCommand, PrivacyCommand, StatusCommand,
    TestCommand, VerifyCommand,
};
use config::config;
use history::HistoryDb;
use output::{OutputFormat, OutputWriter};

/// Global options shared across all commands.
#[derive(Parser, Clone)]
struct GlobalOpts {
    /// Output format (human, json, compact, silent)
    #[arg(long, global = true, default_value = "human")]
    format: OutputFormat,

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
}

impl GlobalOpts {
    /// Get the effective output format.
    pub(crate) fn output_format(&self) -> OutputFormat {
        if self.json {
            OutputFormat::Json
        } else {
            self.format
        }
    }

    /// Check if background execution is requested.
    pub(crate) fn is_background(&self) -> bool {
        self.bg && !self.fg
    }
}

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Developer tasks for the Sinex workspace",
    long_version = long_version()
)]
pub struct Cli {
    #[command(flatten)]
    global: GlobalOpts,

    /// The command to run
    #[command(subcommand)]
    command: Option<Commands>,
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
    // === Core (daily use) ===
    /// Apply automatic fixes (fmt, clippy, fix)
    Fix(FixCommand),
    /// Fast validation (check, clippy, lint-forbidden)
    Check(CheckCommand),
    /// Run test suite
    Test(TestCommand),
    /// Build packages
    Build(BuildCommand),

    // === Runtime ===
    /// Run binaries with hot-reload support
    Run(commands::RunCommand),
    /// Manage local infrastructure (database, NATS)
    Infra {
        #[command(subcommand)]
        cmd: commands::infra::InfraSubcommand,
    },
    /// Database operations (apply, reset, setup, status)
    Db {
        #[command(subcommand)]
        cmd: commands::db::DbSubcommand,
    },
    /// Background job management
    Jobs(JobsCommand),
    /// Workspace status and service health
    Status(StatusCommand),

    // === Analysis ===
    /// Dependency analysis (list, tree, duplicates, unused, timings, impact, graph)
    Deps(commands::DepsCommand),
    /// Build/test history and trends
    History(commands::history::HistoryCommand),

    // === Generation ===
    /// Codebase snapshot for AI context (repomix)
    Snapshot(commands::SnapshotCommand),
    /// Event payload schema/contract management
    Contracts(commands::ContractsCommand),
    /// Documentation generation
    Docs(commands::DocsCommand),

    // === Diagnostics ===
    /// Privacy engine utilities (catalog, test, decrypt, key, config)
    Privacy(PrivacyCommand),

    // === Validation ===
    /// Full surface area validation of xtask commands
    Exercise(commands::ExerciseCommand),
    /// Unified verification entrypoint (conformance/replay/perf)
    Verify(VerifyCommand),

    // === Less frequent (xtr umbrella) ===
    /// Rarely-used utilities (patterns, ci, completions)
    Xtr(commands::XtrCommand),
}

pub async fn run_cli() -> Result<()> {
    let cli = Cli::parse();
    let bg_job_dir = std::env::var("XTASK_JOB_DIR").ok();
    if bg_job_dir.is_some() {
        // One-shot handoff: avoid leaking job control env vars to nested child
        // xtask processes spawned by tests.
        unsafe {
            std::env::remove_var("XTASK_JOB_DIR");
        }
    }

    // Handle --list-commands before normal dispatch
    if cli.global.list_commands {
        return list_commands(cli.global.output_format());
    }

    // Require a command if not using --list-commands
    let command = cli.command.ok_or_else(|| {
        eyre!("No command provided. Use --help to see available commands, or --list-commands for a summary.")
    })?;

    // Dispatch — extract metadata (including timeout) before consuming the command
    let (command_name, subcommand, profile, command_timeout) = match &command {
        Commands::Fix(cmd) => ("fix", None, None, cmd.metadata().timeout),
        Commands::Check(cmd) => ("check", None, None, cmd.metadata().timeout),
        Commands::Test(cmd) => ("test", None, None, cmd.metadata().timeout),
        Commands::Build(cmd) => ("build", None, None, cmd.metadata().timeout),
        Commands::Run(cmd) => ("run", None, None, cmd.metadata().timeout),
        Commands::Infra { .. } => ("infra", None, None, None),
        Commands::Db { .. } => ("db", None, None, None),
        Commands::Jobs(cmd) => ("jobs", None, None, cmd.metadata().timeout),
        Commands::Status(cmd) => ("status", None, None, cmd.metadata().timeout),
        Commands::Deps(cmd) => ("deps", None, None, cmd.metadata().timeout),
        Commands::History(cmd) => ("history", None, None, cmd.metadata().timeout),
        Commands::Snapshot(cmd) => ("snapshot", None, None, cmd.metadata().timeout),
        Commands::Contracts(cmd) => ("contracts", None, None, cmd.metadata().timeout),
        Commands::Docs(cmd) => ("docs", None, None, cmd.metadata().timeout),
        Commands::Privacy(cmd) => ("privacy", None, None, cmd.metadata().timeout),
        Commands::Exercise(cmd) => ("exercise", None, None, cmd.metadata().timeout),
        Commands::Verify(cmd) => ("verify", None, None, cmd.metadata().timeout),
        Commands::Xtr(cmd) => ("xtr", None, None, cmd.metadata().timeout),
    };

    // Track invocation in history
    let history_db = open_history_db();
    let claimed_bg_invocation = std::env::var("XTASK_BG_INVOCATION_ID")
        .ok()
        .and_then(|v| v.parse::<i64>().ok());
    if claimed_bg_invocation.is_some() {
        // One-shot handoff: prevent nested xtask subprocesses (spawned by tests)
        // from inheriting and accidentally claiming the same invocation row.
        unsafe {
            std::env::remove_var("XTASK_BG_INVOCATION_ID");
        }
    }
    let invocation_id = if command_name != "completions" && command_name != "status" {
        if let Some(bg_id) = claimed_bg_invocation {
            if let Ok(db) = history_db.as_ref() {
                let _ =
                    db.claim_background_invocation(bg_id, command_name, subcommand, profile, None);
            }
            Some(bg_id)
        } else {
            history_db.as_ref().ok().and_then(|db| {
                db.start_invocation(command_name, subcommand, profile, None)
                    .ok()
            })
        }
    } else {
        None
    };

    // Create context with invocation ID
    let ctx = CommandContext::new(
        OutputWriter::new(cli.global.output_format()),
        cli.global.json,
        cli.global.is_background(),
        invocation_id,
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
            Commands::Db { cmd } => commands::DbCommand { subcommand: cmd }.execute(&ctx).await,
            Commands::Jobs(cmd) => cmd.execute(&ctx).await,
            Commands::Status(cmd) => cmd.execute(&ctx).await,
            Commands::Deps(cmd) => cmd.execute(&ctx).await,
            Commands::History(cmd) => cmd.execute(&ctx).await,
            Commands::Snapshot(cmd) => cmd.execute(&ctx).await,
            Commands::Contracts(cmd) => cmd.execute(&ctx).await,
            Commands::Docs(cmd) => cmd.execute(&ctx).await,
            Commands::Privacy(cmd) => cmd.execute(&ctx).await,
            Commands::Exercise(cmd) => cmd.execute(&ctx).await,
            Commands::Verify(cmd) => cmd.execute(&ctx).await,
            Commands::Xtr(cmd) => cmd.execute(&ctx).await,
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
    if let Some(id) = invocation_id
        && let Ok(db) = history_db
    {
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
        if let Err(e) = db.finish_invocation(id, status, Some(invocation_exit_code), duration) {
            eprintln!("⚠️  Failed to record invocation result: {e}");
        }
        ctx.mark_finished();
    }

    // Handle coordinator completion: clear state, spawn queued work (FIFO).
    // Uses block_in_place to ensure the spawn completes before process exits
    // (fire-and-forget tokio::spawn could lose work if runtime shuts down first).
    if matches!(command_name, "check" | "test" | "build")
        && let Ok(coord) = coordinator::JobCoordinator::new()
        && let Ok(Some(queued)) = coord.handle_completion(command_name)
    {
        let cfg = config();
        if let Ok(manager) = jobs::JobManager::new(cfg.jobs_dir()) {
            match manager.spawn_xtask(command_name, &queued.args, queued.output_format) {
                Ok(job) => {
                    // Update coordinator state with real job_id + pid.
                    // Critical for FIFO queue: handle_completion may have
                    // left remaining items in the state file with sentinel values.
                    let _ = coord.update_state(command_name, job.id, job.pid);
                }
                Err(e) => {
                    eprintln!("Warning: failed to spawn queued {command_name} work: {e}");
                }
            }
        }
    }

    // Write exit_code file for background job tracking.
    // XTASK_JOB_DIR is set by the bg job spawner so the zombie reaper can
    // determine success vs failure after the process exits.
    if let Some(job_dir) = bg_job_dir {
        let _ = std::fs::write(
            std::path::Path::new(&job_dir).join("exit_code"),
            format!("{invocation_exit_code}\n"),
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

fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    cfg.ensure_state_dir()
        .map_err(|e| eyre!("Failed to create state directory: {e}"))?;
    HistoryDb::open(&cfg.history_db_path())
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

    fn extract_commands(cmd: &clap::Command) -> Vec<CommandInfo> {
        cmd.get_subcommands()
            .filter(|sub| !sub.is_hide_set())
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
