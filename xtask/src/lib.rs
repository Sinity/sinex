use anyhow::Result;
use clap::{Parser, Subcommand};

mod affected;
pub mod bench;
pub mod command;
pub mod commands;
mod config;
pub mod deps;
pub mod devtools;
pub mod graph;
pub mod history;
pub mod jobs;
pub mod output;
pub mod process;
pub mod resources;
pub mod tls;
mod tools;

use command::{CommandContext, XtaskCommand};
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
    /// Manage local development environment (db, nats, vm, tls)
    Stack {
        #[command(subcommand)]
        cmd: commands::stack::StackSubcommand,
    },
    /// Quality Assurance (test, lint, check, bench)
    Qa {
        #[command(subcommand)]
        cmd: commands::qa::QaSubcommand,
    },
    /// Codebase Analysis (deps, graph, history)
    Analyze {
        #[command(subcommand)]
        cmd: commands::analyze::AnalyzeSubcommand,
    },
    /// Developer Inner Loop (build, run, generate)
    Dev {
        #[command(subcommand)]
        cmd: commands::dev::DevSubcommand,
    },
    /// Database Management (migrate, schema, setup)
    Db {
        #[command(subcommand)]
        cmd: commands::db::DbSubcommand,
    },
    /// CI Pipelines
    Ci {
        #[command(subcommand)]
        cmd: commands::ci::CiSubcommand,
    },
    /// Background Job Management
    Jobs {
        #[command(subcommand)]
        cmd: commands::jobs::JobsSubcommand,
    },
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
        Commands::Stack { .. } => ("stack", None, None),
        Commands::Qa { .. } => ("qa", None, None),
        Commands::Analyze { .. } => ("analyze", None, None),
        Commands::Dev { .. } => ("dev", None, None),
        Commands::Db { .. } => ("db", None, None),
        Commands::Ci { .. } => ("ci", None, None),
        Commands::Jobs { .. } => ("jobs", None, None),
        Commands::Completions { .. } => ("completions", None, None),
    };

    // Track invocation in history
    let history_db = open_history_db();
    let invocation_id = if command_name != "completions" {
        history_db.as_ref().ok().and_then(|db| {
            db.start_invocation(command_name, subcommand, profile, None)
                .ok()
        })
    } else {
        None
    };

    let result = match cli.command {
        Commands::Stack { cmd } => commands::StackCommand { subcommand: cmd }.execute(&ctx),
        Commands::Qa { cmd } => commands::QaCommand { subcommand: cmd }.execute(&ctx),
        Commands::Analyze { cmd } => commands::AnalyzeCommand { subcommand: cmd }.execute(&ctx),
        Commands::Dev { cmd } => commands::DevCommand { subcommand: cmd }.execute(&ctx),
        Commands::Db { cmd } => commands::DbCommand { subcommand: cmd }.execute(&ctx),
        Commands::Ci { cmd } => commands::CiCommand { subcommand: cmd }.execute(&ctx),
        Commands::Jobs { cmd } => commands::JobsCommand { subcommand: cmd }.execute(&ctx),
        Commands::Completions { shell } => {
            // Map clap shell to completions shell if needed, or if types match (enums match by name?)
            // But they are distinct types. Convert by matching.
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
                Ok(res) => res.duration_secs.unwrap_or(0.0),
                Err(_) => 0.0,
            };
            let _ = db.finish_invocation(id, status, None, duration);
        }
    }

    match result {
        Ok(res) => {
            res.print(&ctx.writer(), command_name);
            Ok(())
        }
        Err(err) => Err(err),
    }
}

fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}
