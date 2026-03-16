//! Integration tests for extracted xtask commands
//!
//! Tests command execution, output formatting, and error handling
//! for commands extracted during Phase 2 refactoring.
//!
//! Tests assert behavioral invariants visible to users, not implementation details.
//! "Doesn't panic" is not an invariant. "Returns events in descending chronological order" is.

use std::process::Command;
use xtask::command::{CommandContext, CommandResult, XtaskCommand};
use xtask::commands::jobs::{JobsCommand, JobsSubcommand};
use xtask::output::{OutputFormat, OutputWriter};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_jobs_list_command() -> ::xtask::sandbox::TestResult<()> {
    let cmd = JobsCommand {
        subcommand: JobsSubcommand::List {
            limit: 10,
            active: false,
        },
    };
    assert_eq!(cmd.name(), "jobs");

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, None, "test");
    let result = cmd.execute(&ctx).await;

    // List should not fail (even if no jobs exist)
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_jobs_prune_command() -> ::xtask::sandbox::TestResult<()> {
    let cmd = JobsCommand {
        subcommand: JobsSubcommand::Prune { older_than: 30 },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, None, "test");
    let result = cmd.execute(&ctx).await;

    // Prune should succeed (even if no jobs to prune)
    assert!(result.is_ok());
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
    let output = Command::new("xtask")
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
        let output = Command::new("xtask")
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
