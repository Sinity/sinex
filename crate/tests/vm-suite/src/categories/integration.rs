//! Integration tests — deeper behavioral invariants beyond smoke.
//!
//! The Rust-side integration category is intentionally empty today. The wider VM
//! scenario set still lives in the exported NixOS checks rather than this in-VM
//! binary.

use crate::runner::TestRunner;

pub fn run(runner: &mut TestRunner, _database_url: &str) {
    println!("\n── Integration tests ──────────────────────────");
    runner.pass("no Rust-side integration assertions wired");
}
