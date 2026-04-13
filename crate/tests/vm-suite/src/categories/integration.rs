//! Integration tests — deeper behavioral invariants beyond smoke.
//!
//! The Rust-side integration category is intentionally empty today. The wider VM
//! scenario set still lives in the exported NixOS checks rather than this in-VM
//! binary.

use color_eyre::eyre::Result;

use crate::runner::TestRunner;

pub async fn run(runner: &mut TestRunner, _database_url: &str) -> Result<()> {
    println!("\n── Integration tests ──────────────────────────");
    runner.pass("no Rust-side integration assertions wired");
    Ok(())
}
