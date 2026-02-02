// Allow xtask to reference itself as ::xtask for macro-generated code
extern crate self as xtask;

use anyhow::Result;
pub const DEFAULT_TEST_MATERIAL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
use clap::{Parser, Subcommand};

// Build-time metadata from shadow-rs
shadow_rs::shadow!(build_info);

mod affected;
pub mod bench;
pub mod cargo_diagnostics;
pub mod command;
pub mod commands;
mod config;
pub mod deps;
pub mod graph;
pub mod history;
pub mod infra;
pub mod jobs;
pub mod output;
pub mod preflight;
pub mod process;
pub mod resources;
#[cfg(feature = "sandbox")]
pub mod sandbox;
#[cfg(feature = "sandbox")]
pub use sandbox::{EventOverrides, Sandbox, TestContext, TestResult};
pub mod tls;
mod tools;

use command::{CommandContext, XtaskCommand};
use commands::{
    BenchArgs, BuildCommand, CheckCommand, FixCommand, JobsCommand, StatusCommand, TestCommand,
    VmCommand,
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
    /// Run benchmarks
    Bench(BenchArgs),
    /// Build packages
    Build(BuildCommand),

    // === Runtime ===
    /// Run binaries with hot-reload support
    Run(commands::RunCommand),
    /// Manage local stack (database, NATS)
    Stack {
        #[command(subcommand)]
        cmd: commands::stack::StackSubcommand,
    },
    /// Database operations (migrate, seed, setup)
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

    // === Code coverage ===
    /// Code coverage reporting
    Coverage(commands::CoverageCommand),

    // === Less frequent (xtr umbrella) ===
    /// Rarely-used utilities (patterns, ci, completions)
    Xtr(commands::XtrCommand),

    // === Infrastructure (less common) ===
    /// VM and NixOS operations
    Vm(VmCommand),
}

pub fn run_cli() -> Result<()> {
    let cli = Cli::parse();

    // Handle --list-commands before normal dispatch
    if cli.global.list_commands {
        return list_commands(cli.global.output_format());
    }

    // Require a command if not using --list-commands
    let command = cli.command.ok_or_else(|| {
        anyhow::anyhow!("No command provided. Use --help to see available commands, or --list-commands for a summary.")
    })?;

    // Dispatch
    let (command_name, subcommand, profile) = match &command {
        Commands::Fix(_) => ("fix", None, None),
        Commands::Check(_) => ("check", None, None),
        Commands::Test(_) => ("test", None, None),
        Commands::Bench(_) => ("bench", None, None),
        Commands::Build(_) => ("build", None, None),
        Commands::Run(_) => ("run", None, None),
        Commands::Stack { .. } => ("stack", None, None),
        Commands::Db { .. } => ("db", None, None),
        Commands::Jobs(_) => ("jobs", None, None),
        Commands::Status(_) => ("status", None, None),
        Commands::Deps(_) => ("deps", None, None),
        Commands::History(_) => ("history", None, None),
        Commands::Snapshot(_) => ("snapshot", None, None),
        Commands::Contracts(_) => ("contracts", None, None),
        Commands::Docs(_) => ("docs", None, None),
        Commands::Coverage(_) => ("coverage", None, None),
        Commands::Xtr(_) => ("xtr", None, None),
        Commands::Vm(_) => ("vm", None, None),
    };

    // Track invocation in history
    let history_db = open_history_db();
    let invocation_id = if command_name != "completions" && command_name != "status" {
        history_db.as_ref().ok().and_then(|db| {
            db.start_invocation(command_name, subcommand, profile, None)
                .ok()
        })
    } else {
        None
    };

    // Create context with invocation ID for diagnostics recording
    let ctx = CommandContext::with_invocation_id(
        OutputWriter::new(cli.global.output_format()),
        invocation_id,
    )
    .with_background(cli.global.is_background());

    let result = match command {
        Commands::Fix(cmd) => cmd.execute(&ctx),
        Commands::Check(cmd) => cmd.execute(&ctx),
        Commands::Test(cmd) => cmd.execute(&ctx),
        Commands::Bench(cmd) => cmd.execute(&ctx),
        Commands::Build(cmd) => cmd.execute(&ctx),
        Commands::Run(cmd) => cmd.execute(&ctx),
        Commands::Stack { cmd } => commands::StackCommand { subcommand: cmd }.execute(&ctx),
        Commands::Db { cmd } => commands::DbCommand { subcommand: cmd }.execute(&ctx),
        Commands::Jobs(cmd) => cmd.execute(&ctx),
        Commands::Status(cmd) => cmd.execute(&ctx),
        Commands::Deps(cmd) => cmd.execute(&ctx),
        Commands::History(cmd) => cmd.execute(&ctx),
        Commands::Snapshot(cmd) => cmd.execute(&ctx),
        Commands::Contracts(cmd) => cmd.execute(&ctx),
        Commands::Docs(cmd) => cmd.execute(&ctx),
        Commands::Coverage(cmd) => cmd.execute(&ctx),
        Commands::Xtr(cmd) => cmd.execute(&ctx),
        Commands::Vm(cmd) => cmd.execute(&ctx),
    };

    // Update history
    if let Some(id) = invocation_id {
        if let Ok(db) = history_db {
            let status = match &result {
                Ok(_) => crate::history::InvocationStatus::Success,
                Err(_) => crate::history::InvocationStatus::Failed,
            };
            let duration = match &result {
                Ok(res) => res.duration_secs.unwrap_or(ctx.elapsed().as_secs_f64()),
                Err(_) => ctx.elapsed().as_secs_f64(),
            };
            let _ = db.finish_invocation(id, status, None, duration);
        }
    }

    match result {
        Ok(res) => {
            res.print(ctx.writer(), command_name);
            if res.status == crate::output::Status::Failed
                || res.status == crate::output::Status::Partial
            {
                anyhow::bail!("Command failed with status: {:?}", res.status);
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}

/// List all available commands using clap introspection.
fn list_commands(format: OutputFormat) -> Result<()> {
    use clap::CommandFactory;
    use serde::Serialize;

    #[derive(Serialize)]
    struct CommandInfo {
        name: String,
        about: Option<String>,
        subcommands: Vec<CommandInfo>,
        hidden: bool,
    }

    fn extract_commands(cmd: &clap::Command) -> Vec<CommandInfo> {
        cmd.get_subcommands()
            .map(|sub| CommandInfo {
                name: sub.get_name().to_string(),
                about: sub.get_about().map(std::string::ToString::to_string),
                subcommands: extract_commands(sub),
                hidden: sub.is_hide_set(),
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
            if cmd.hidden {
                continue;
            }
            let about = cmd.about.as_deref().unwrap_or("");
            println!("  {:16} {}", cmd.name, about);

            for sub in &cmd.subcommands {
                if sub.hidden {
                    continue;
                }
                let sub_about = sub.about.as_deref().unwrap_or("");
                println!("    {:14} {}", sub.name, sub_about);
            }
        }
    }

    Ok(())
}
