//! `sinexd`: one context built in `main`; admission and the writer task; the
//! commit-log reader; the confirmed-stream publisher; the NATS ingress
//! consumer; the automaton driver; replay; the API (architecture §7.4).
mod backfill;
mod daemon;
mod keys;
mod writer;

#[cfg(test)]
mod testdb;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sinexd", about = "The Sinex daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon.
    Run {
        #[arg(long)]
        database: String,
        #[arg(long)]
        store: PathBuf,
    },
    /// Administration that needs the database but not the running daemon.
    Admin {
        #[command(subcommand)]
        command: Admin,
    },
    /// Read sources once to the end, through the writer, and report counts and
    /// rates. For first imports and measurement.
    Backfill {
        #[arg(long)]
        database: String,
        /// Content store directory.
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        atuin: Option<PathBuf>,
        #[arg(long)]
        zsh: Option<PathBuf>,
        #[arg(long, default_value = "localhost")]
        host: String,
    },
}

#[derive(Subcommand)]
enum Admin {
    /// Create the schema in an empty database, or confirm it is current.
    Schema {
        #[arg(long)]
        database: String,
    },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Run { database, store } => daemon::run(database, store).await,
        Command::Admin {
            command: Admin::Schema { database },
        } => sinex_schema::apply_url(&database)
            .await
            .map(|applied| println!("{applied}"))
            .map_err(|e| e.to_string()),
        Command::Backfill {
            database,
            store,
            atuin,
            zsh,
            host,
        } => {
            backfill::run(backfill::Options {
                database,
                store,
                atuin,
                zsh,
                host,
            })
            .await
        }
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sinexd: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
