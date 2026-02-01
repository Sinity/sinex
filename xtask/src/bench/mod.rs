mod config;
mod environment;
mod history;
mod modes;
mod reports;
mod runner;
mod stats;

pub use config::{BenchConfig, BenchMode};

use anyhow::Result;
use console::style;

pub fn run(config: BenchConfig) -> Result<()> {
    println!("{}", style("━━━━ NEXTEST BENCHMARK ━━━━").bold().cyan());
    println!();

    let ctx = runner::BenchContext::new(config)?;

    match ctx.config.mode {
        config::BenchMode::Sweeps => modes::sweep_mode(&ctx),
        config::BenchMode::Refine => modes::refine_mode(&ctx),
        config::BenchMode::Bisect => modes::bisect_mode(&ctx),
        config::BenchMode::Stress => modes::stress_mode(&ctx),
        config::BenchMode::Soak => modes::soak_mode(&ctx),
    }
}
