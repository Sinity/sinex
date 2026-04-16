//! NixOS VM test suite binary for sinex.
//!
//! Runs inside a NixOS VM and asserts behavioral invariants against live
//! services (`PostgreSQL`, sinex-ingestd) using typed queries and process checks.
//! Called from testScript with:
//!
//!   `su - postgres -c "DATABASE_URL=postgresql:///sinex ${suite}/bin/run-suite --category smoke"`

use clap::Parser;
use color_eyre::eyre::{Result, bail};

mod categories;
mod runner;

#[derive(Parser)]
#[command(name = "run-suite", about = "NixOS VM test suite for sinex")]
struct Args {
    /// Test category: smoke | integration | all | concurrency |
    ///   chaos-network-partition | chaos-process-restart | chaos-clock-skew
    #[arg(long, default_value = "smoke")]
    category: String,

    /// `PostgreSQL` connection URL
    #[arg(long, env = "DATABASE_URL", default_value = "postgresql:///sinex")]
    database_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();

    println!("sinex-vm-test-suite  category={}", args.category);
    println!("────────────────────────────────────────────");

    let mut runner = runner::TestRunner::new();

    match args.category.as_str() {
        "smoke" => categories::smoke::run(&mut runner, &args.database_url).await?,
        "integration" | "all" => {
            categories::smoke::run(&mut runner, &args.database_url).await?;
            categories::integration::run(&mut runner, &args.database_url).await?;
        }
        "concurrency" => categories::concurrency::run(&mut runner)?,
        "chaos-network-partition" => {
            categories::chaos_network_partition::run(&mut runner, &args.database_url).await?;
        }
        "chaos-process-restart" => {
            categories::chaos_process_restart::run(&mut runner, &args.database_url).await?;
        }
        "chaos-clock-skew" => {
            categories::chaos_clock_skew::run(&mut runner, &args.database_url).await?;
        }
        other => bail!(
            "Unknown category: {other}. Valid: smoke, integration, all, concurrency, \
             chaos-network-partition, chaos-process-restart, chaos-clock-skew"
        ),
    }

    runner.finish()
}
