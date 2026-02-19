//! Integration tests for extracted xtask commands
//!
//! Tests command execution, output formatting, and error handling
//! for commands extracted during Phase 2 refactoring.

use xtask::command::{CommandContext, CommandResult, XtaskCommand};
#[cfg(feature = "sandbox")]
use xtask::commands::ci::{CiCommand, CiSubcommand};
use xtask::commands::jobs::{JobsCommand, JobsSubcommand};
use xtask::output::{OutputFormat, OutputWriter};
use xtask::sandbox::sinex_test;

#[cfg(feature = "sandbox")]
#[sinex_test]
fn test_ci_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = CiCommand {
        subcommand: CiSubcommand::Workspace {
            target_dir: "/tmp".to_string(),
        },
    };
    assert_eq!(cmd.name(), "ci");
    Ok(())
}

#[cfg(feature = "sandbox")]
#[sinex_test]
fn test_ci_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = CiCommand {
        subcommand: CiSubcommand::Workspace {
            target_dir: "/tmp".to_string(),
        },
    };
    let metadata = cmd.metadata();

    assert_eq!(metadata.category, Some("test".to_string()));
    assert!(metadata.timeout.is_some());
    Ok(())
}

#[sinex_test]
async fn test_jobs_list_command() -> ::xtask::sandbox::TestResult<()> {
    let cmd = JobsCommand {
        subcommand: JobsSubcommand::List { limit: 10 },
    };
    assert_eq!(cmd.name(), "jobs");

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, false, None);
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

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, false, None);
    let result = cmd.execute(&ctx).await;

    // Prune should succeed (even if no jobs to prune)
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
fn test_command_result_formatting() -> ::xtask::sandbox::TestResult<()> {
    // Test that CommandResult can be created and used
    let result = CommandResult::success()
        .with_message("Test completed")
        .with_details(vec!["Step 1 done", "Step 2 done"]);

    assert!(result.is_success());
    assert_eq!(result.message, Some("Test completed".to_string()));
    assert_eq!(result.details.len(), 2);
    Ok(())
}

#[sinex_test]
fn test_command_context_formats() -> ::xtask::sandbox::TestResult<()> {
    // Test different output formats work
    for format in [
        OutputFormat::Human,
        OutputFormat::Json,
        OutputFormat::Compact,
        OutputFormat::Silent,
    ] {
        let ctx = CommandContext::new(OutputWriter::new(format), false, false, None);
        let elapsed = ctx.elapsed();
        assert!(elapsed.as_nanos() > 0);
    }
    Ok(())
}
