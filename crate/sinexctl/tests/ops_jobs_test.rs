//! CLI integration tests for `sinexctl ops jobs`.
//!
//! These tests verify argument parsing, help text, and subcommand structure
//! without requiring a running gateway.

use assert_cmd::Command;
use assert_cmd::cargo;
use predicates::prelude::*;
use xtask::sandbox::sinex_test;

/// Helper to create a sinexctl command
fn sinexctl() -> Command {
    Command::new(cargo::cargo_bin!("sinexctl"))
}

#[sinex_test]
async fn ops_help_includes_jobs_subcommand() -> TestResult<()> {
    sinexctl()
        .args(["ops", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("jobs"))
        .stdout(predicate::str::contains("job"));
    Ok(())
}

#[sinex_test]
async fn ops_jobs_help_shows_list_and_show() -> TestResult<()> {
    sinexctl()
        .args(["ops", "jobs", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("list"))
        .stdout(predicate::str::contains("show"));
    Ok(())
}

#[sinex_test]
async fn ops_jobs_list_help_shows_expected_flags() -> TestResult<()> {
    sinexctl()
        .args(["ops", "jobs", "list", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--kind"))
        .stdout(predicate::str::contains("--status"))
        .stdout(predicate::str::contains("--limit"));
    Ok(())
}

#[sinex_test]
async fn ops_jobs_show_help_shows_operation_id_arg() -> TestResult<()> {
    sinexctl()
        .args(["ops", "jobs", "show", "--help"])
        .assert()
        .success()
        // clap renders the positional in usage as the upper-cased token `<OPERATION_ID>`.
        .stdout(predicate::str::contains("OPERATION_ID"));
    Ok(())
}
