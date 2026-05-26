//! NixOS VM test suite binary for sinex.
//!
//! Runs inside a NixOS VM and asserts behavioral invariants against live
//! services (`PostgreSQL`, sinex-ingestd) using typed queries and process checks.
//! Called from testScript with the VM-provided `SINEX_TEST_DB_NAME` environment,
//! or an explicit `DATABASE_URL` override when a scenario needs a different DB.

use clap::Parser;
use color_eyre::eyre::{Result, bail};
use std::env;

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
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
}

fn default_database_url() -> String {
    let database_name = env::var("SINEX_TEST_DB_NAME").unwrap_or_else(|_| "sinex".to_string());
    format!("postgresql://sinex@127.0.0.1/{database_name}")
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();

    println!("sinex-vm-test-suite  category={}", args.category);
    println!("────────────────────────────────────────────");

    let database_url = args.database_url.unwrap_or_else(default_database_url);

    let mut runner = runner::TestRunner::new();

    match args.category.as_str() {
        "smoke" => categories::smoke::run(&mut runner, &database_url).await?,
        "integration" | "all" => {
            categories::smoke::run(&mut runner, &database_url).await?;
            categories::integration::run(&mut runner, &database_url).await?;
        }
        "concurrency" => categories::concurrency::run(&mut runner),
        "chaos-network-partition" => {
            categories::chaos_network_partition::run(&mut runner, &database_url).await?;
        }
        "chaos-process-restart" => {
            categories::chaos_process_restart::run(&mut runner, &database_url).await?;
        }
        "chaos-clock-skew" => {
            categories::chaos_clock_skew::run(&mut runner, &database_url).await?;
        }
        other => bail!(
            "Unknown category: {other}. Valid: smoke, integration, all, concurrency, \
             chaos-network-partition, chaos-process-restart, chaos-clock-skew"
        ),
    }

    runner.finish()
}
