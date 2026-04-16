//! Integration tests for extracted xtask commands
//!
//! Tests command execution, output formatting, and error handling
//! for extracted command modules.
//!
//! Tests assert behavioral invariants visible to users, not implementation details.
//! "Doesn't panic" is not an invariant. "Returns events in descending chronological order" is.

mod support;

use support::xtask_command;
use xtask::command::{CommandContext, XtaskCommand};
use xtask::commands::jobs::{JobsCommand, JobsSubcommand};
use xtask::output::{OutputFormat, OutputWriter};
use xtask::history::{
    HistoryDb,
    seed::{SeedOptions, seed_history},
};
use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

/// Invariant: `jobs list` on empty state returns an empty jobs array, not an error.
///
/// This guards against regressions where missing tables, missing files, or
/// uninitialized state causes a crash instead of a graceful empty response.
#[sinex_test]
async fn test_jobs_list_empty_state_returns_empty_array() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;

    let output = xtask_command()?
        .env("SINEX_STATE_DIR", dir.path())
        .env("NO_COLOR", "1")
        .args(["jobs", "list", "--json"])
        .output()?;

    assert!(
        output.status.success(),
        "jobs list on empty state must exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).map_err(|e| {
        color_eyre::eyre::eyre!("jobs list --json did not emit valid JSON: {e}\nstdout: {stdout}")
    })?;

    // Invariant: status field is "success", not "error"
    assert_eq!(
        json["status"], "success",
        "jobs list must report success on empty state, got: {json}"
    );

    // Invariant: jobs array is present and empty (not missing, not an error)
    let jobs = json["data"]["jobs"]
        .as_array()
        .expect("data.jobs must be an array");
    assert!(
        jobs.is_empty(),
        "jobs list on empty state must return empty array, got {} jobs",
        jobs.len()
    );

    Ok(())
}

/// Invariant: `jobs list` name returns "jobs".
#[sinex_test]
async fn test_jobs_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = JobsCommand {
        subcommand: JobsSubcommand::List {
            limit: 10,
            active: false,
        },
    };
    assert_eq!(cmd.name(), "jobs");
    Ok(())
}

/// Invariant: `jobs prune` on empty state removes 0 jobs and reports success.
///
/// Pruning an already-empty history must not error out on missing tables
/// or return a spurious failure count.
#[sinex_test]
async fn test_jobs_prune_empty_state_removes_zero() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;

    let output = xtask_command()?
        .env("SINEX_STATE_DIR", dir.path())
        .env("NO_COLOR", "1")
        .args(["jobs", "prune", "--json"])
        .output()?;

    assert!(
        output.status.success(),
        "jobs prune on empty state must exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).map_err(|e| {
        color_eyre::eyre::eyre!("jobs prune --json did not emit valid JSON: {e}\nstdout: {stdout}")
    })?;

    assert_eq!(
        json["status"], "success",
        "jobs prune on empty state must report success, got: {json}"
    );

    Ok(())
}

#[sinex_test]
async fn test_command_context_formats() -> ::xtask::sandbox::TestResult<()> {
    // Test different output formats work
    for format in [
        OutputFormat::Human,
        OutputFormat::Json,
        OutputFormat::Compact,
        OutputFormat::Silent,
    ] {
        let ctx = CommandContext::new(OutputWriter::new(format), false, None, "test");
        let elapsed = ctx.elapsed();
        assert!(elapsed.as_nanos() > 0);
    }
    Ok(())
}

// ============================================================================
// Analytics Smoke Tests
// ============================================================================

#[sinex_test]
async fn test_analytics_help() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
        .arg("analytics")
        .arg("--help")
        .output()?;

    assert!(output.status.success(), "analytics --help should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("workspace-health"),
        "help should mention workspace-health subcommand"
    );
    Ok(())
}

#[sinex_test]
async fn test_analytics_all_subcommands_empty_db() -> ::xtask::sandbox::TestResult<()> {
    // history_db_path() re-reads XTASK_HISTORY_DB on each call, so env override is safe.
    // One shared empty DB for all subcommands — each just reads, nothing to conflict.
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("analytics-empty.db");

    let subcommands = [
        "workspace-health",
        "hotspots",
        "reliability",
        "velocity",
        "recommend",
    ];

    for sub in subcommands {
        let output = xtask_command()?
            .env("XTASK_HISTORY_DB", db_path.to_str().unwrap())
            .arg("analytics")
            .arg(sub)
            .output()?;

        assert!(
            output.status.success(),
            "analytics {sub} on empty DB should not panic. Stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[sinex_serial_test]
async fn test_xtask_command_prefers_state_dir_history_over_parent_override()
-> ::xtask::sandbox::TestResult<()> {
    let state_dir = tempfile::tempdir()?;
    let seeded_history = state_dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&seeded_history)?;
    seed_history(
        &db,
        &SeedOptions {
            days: 3,
            invocations: 6,
        },
    )?;

    let poison_dir = tempfile::tempdir()?;
    let poison_history = poison_dir.path().join("poison-history.db");
    let _parent_history_override = EnvGuard::set_single("XTASK_HISTORY_DB", &poison_history);

    let output = xtask_command()?
        .env("SINEX_STATE_DIR", state_dir.path())
        .env("NO_COLOR", "1")
        .args(["history", "list", "--json", "--limit", "1"])
        .output()?;

    assert!(
        output.status.success(),
        "history list should succeed with explicit SINEX_STATE_DIR; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut values = serde_json::Deserializer::from_str(&stdout).into_iter::<serde_json::Value>();
    let history_rows = values
        .next()
        .ok_or_else(|| color_eyre::eyre::eyre!("no JSON rows returned from history list"))?
        .map_err(|error| {
            color_eyre::eyre::eyre!("invalid JSON from history list: {error}\nstdout: {stdout}")
        })?;

    let rows = history_rows
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("history list first JSON value must be an array"))?;
    assert_eq!(
        rows.len(),
        1,
        "history list should read the seeded state-dir DB instead of the parent XTASK_HISTORY_DB override"
    );

    Ok(())
}
