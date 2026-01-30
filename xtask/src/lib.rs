// Allow xtask to reference itself as ::xtask for macro-generated code
extern crate self as xtask;

use anyhow::Result;
pub const DEFAULT_TEST_MATERIAL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
use clap::{Parser, Subcommand};

mod affected;
pub mod bench;
pub mod command;
pub mod commands;
mod config;
pub mod deps;
pub mod graph;
pub mod history;
pub mod jobs;
pub mod output;
pub mod process;
pub mod infra;
pub mod resources;
#[cfg(feature = "sandbox")]
pub mod sandbox;
#[cfg(feature = "sandbox")]
pub use sandbox::{EventOverrides, Sandbox, TestContext, TestResult};
pub mod tls;
mod tools;

use command::{CommandContext, XtaskCommand};
use commands::*;
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
}

impl GlobalOpts {
    /// Get the effective output format.
    pub fn output_format(&self) -> OutputFormat {
        if self.json {
            OutputFormat::Json
        } else {
            self.format
        }
    }
}

#[derive(Parser)]
#[command(author, version, about = "Developer tasks for the Sinex workspace")]
struct Cli {
    #[command(flatten)]
    global: GlobalOpts,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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
    /// VM and NixOS operations
    Vm(VmCommand),
    /// Infrastructure and secrets
    Infra(InfraCommand),
    /// Analyze codebase (deps, graph, history)
    Analyze(AnalyzeCommand),
    /// Background Job Management
    Jobs(JobsCommand),
    /// CI Pipelines
    Ci(CiCommand),
    /// TLS certificate management
    #[command(subcommand)]
    Tls(TlsCommand),
    /// Workspace status and service health
    Status(StatusCommand),
    /// Generate Shell Completions
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

pub fn run_cli() -> Result<()> {
    let cli = Cli::parse();
    let ctx = CommandContext::new(OutputWriter::new(cli.global.output_format()));

    // Dispatch
    let (command_name, subcommand, profile) = match &cli.command {
        Commands::Fix(_) => ("fix", None, None),
        Commands::Check(_) => ("check", None, None),
        Commands::Test(_) => ("test", None, None),
        Commands::Bench(_) => ("bench", None, None),
        Commands::Build(_) => ("build", None, None),
        Commands::Stack { .. } => ("stack", None, None),
        Commands::Db { .. } => ("db", None, None),
        Commands::Vm(_) => ("vm", None, None),
        Commands::Infra(_) => ("infra", None, None),
        Commands::Analyze(_) => ("analyze", None, None),
        Commands::Jobs(_) => ("jobs", None, None),
        Commands::Ci(_) => ("ci", None, None),
        Commands::Tls(_) => ("tls", None, None),
        Commands::Status(_) => ("status", None, None),
        Commands::Completions { .. } => ("completions", None, None),
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

    let result = match cli.command {
        Commands::Fix(cmd) => cmd.execute(&ctx),
        Commands::Check(cmd) => cmd.execute(&ctx),
        Commands::Test(cmd) => cmd.execute(&ctx),
        Commands::Bench(cmd) => cmd.execute(&ctx),
        Commands::Build(cmd) => cmd.execute(&ctx),
        Commands::Stack { cmd } => commands::StackCommand { subcommand: cmd }.execute(&ctx),
        Commands::Db { cmd } => commands::DbCommand { subcommand: cmd }.execute(&ctx),
        Commands::Vm(cmd) => cmd.execute(&ctx),
        Commands::Infra(cmd) => cmd.execute(&ctx),
        Commands::Analyze(cmd) => cmd.execute(&ctx),
        Commands::Jobs(cmd) => cmd.execute(&ctx),
        Commands::Ci(cmd) => cmd.execute(&ctx),
        Commands::Tls(cmd) => cmd.execute(&ctx),
        Commands::Status(cmd) => cmd.execute(&ctx),
        Commands::Completions { shell } => {
            let shell_mapped = match shell {
                clap_complete::Shell::Bash => commands::completions::Shell::Bash,
                clap_complete::Shell::Zsh => commands::completions::Shell::Zsh,
                clap_complete::Shell::Fish => commands::completions::Shell::Fish,
                clap_complete::Shell::PowerShell => commands::completions::Shell::PowerShell,
                _ => commands::completions::Shell::Bash,
            };

            // Generate completions
            use clap::CommandFactory;
            let cmd = Cli::command();
            commands::CompletionsCommand::generate_completions(shell_mapped, cmd)?;

            Ok(crate::command::CommandResult::success())
        }
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
            res.print(&ctx.writer(), command_name);
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
