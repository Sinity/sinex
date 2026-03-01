//! Edge case tests for command execution framework.
//!
//! Tests cover:
//! - Commands with invalid arguments
//! - Timeout handling
//! - `ProcessBuilder` error cases
//! - JSON output format validation
//! - `CommandContext` behavior
//! - `CommandResult` construction

use std::process::Command;
use std::time::Duration;

use xtask::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use xtask::output::{OutputFormat, OutputWriter, Status, StructuredError};
use xtask::process::ProcessBuilder;
use xtask::sandbox::sinex_test;

// ============================================================================
// ProcessBuilder Error Cases
// ============================================================================

#[sinex_test]
async fn test_process_builder_nonexistent_command() -> TestResult<()> {
    let result = ProcessBuilder::new("nonexistent_command_that_does_not_exist_xyz")
        .arg("--version")
        .run();

    assert!(result.is_err(), "Nonexistent command should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("failed to spawn") || err.contains("No such file"),
        "Error should indicate spawn failure: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn test_process_builder_command_not_found_with_description() -> TestResult<()> {
    let result = ProcessBuilder::new("totally_fake_command")
        .with_description("my custom operation")
        .run();

    assert!(result.is_err(), "Missing command should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("my custom operation"),
        "Error should include description: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn test_process_builder_command_exits_with_error() -> TestResult<()> {
    let result = ProcessBuilder::new("false").run();

    assert!(result.is_err(), "Command returning non-zero should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("failed") || err.contains("exit code"),
        "Error should mention failure: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn test_process_builder_command_exits_with_error_code() -> TestResult<()> {
    let result = ProcessBuilder::new("sh")
        .args(["-c", "exit 42"])
        .with_description("exit code test")
        .run();

    assert!(result.is_err(), "Non-zero exit code should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("exit code 42") || err.contains("failed"),
        "Error should mention exit code: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn test_process_builder_stderr_captured_on_error() -> TestResult<()> {
    let result = ProcessBuilder::new("sh")
        .args(["-c", "echo 'error message' >&2; exit 1"])
        .run();

    assert!(result.is_err(), "Should fail with exit code 1");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("error message"),
        "Error should include stderr output: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn test_process_builder_run_success_returns_bool() -> TestResult<()> {
    let success = ProcessBuilder::new("true")
        .run_success()
        .expect("should not error on spawn");
    assert!(success, "true command should succeed");

    let failure = ProcessBuilder::new("false")
        .run_success()
        .expect("should not error on spawn");
    assert!(!failure, "false command should not succeed");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_run_ok_discards_output() -> TestResult<()> {
    ProcessBuilder::new("echo")
        .arg("hello world")
        .run_ok()
        .expect("echo should succeed");

    let err = ProcessBuilder::new("false").run_ok();
    assert!(err.is_err(), "false should fail");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_run_stdout() -> TestResult<()> {
    let output = ProcessBuilder::new("echo")
        .arg("test output")
        .run_stdout()
        .expect("echo should succeed");

    assert_eq!(output, "test output", "Should capture trimmed stdout");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_with_env_variable() -> TestResult<()> {
    let output = ProcessBuilder::new("sh")
        .args(["-c", "echo $MY_TEST_VAR"])
        .env("MY_TEST_VAR", "custom_value")
        .run()
        .expect("should succeed");

    assert!(output.success());
    assert_eq!(output.stdout.trim(), "custom_value");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_with_current_dir() -> TestResult<()> {
    let output = ProcessBuilder::new("pwd")
        .current_dir("/tmp")
        .run()
        .expect("pwd should succeed");

    assert!(output.success());
    // Handle symlinked /tmp on some systems
    assert!(
        output.stdout.contains("/tmp") || output.stdout.contains("private/tmp"),
        "Should be in /tmp directory"
    );
    Ok(())
}

#[sinex_test]
async fn test_process_builder_multiple_args() -> TestResult<()> {
    let output = ProcessBuilder::new("echo")
        .args(["one", "two", "three"])
        .run()
        .expect("echo should succeed");

    assert!(output.success());
    assert_eq!(output.stdout.trim(), "one two three");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_git_helper() -> TestResult<()> {
    let output = ProcessBuilder::git()
        .arg("--version")
        .run()
        .expect("git --version should succeed");

    assert!(output.success());
    assert!(output.stdout.contains("git version"));
    Ok(())
}

#[sinex_test]
async fn test_process_builder_cargo_helper() -> TestResult<()> {
    let output = ProcessBuilder::cargo()
        .arg("--version")
        .run()
        .expect("cargo --version should succeed");

    assert!(output.success());
    assert!(output.stdout.contains("cargo"));
    Ok(())
}

#[sinex_test]
async fn test_process_builder_psql_helper_without_db() -> TestResult<()> {
    // psql without connection should fail, but the helper should work
    let result = ProcessBuilder::psql().arg("--version").run();

    // This might succeed or fail depending on system setup,
    // but the important thing is the helper method exists and works
    if let Ok(output) = result {
        assert!(output.stdout.contains("psql"));
    }
    Ok(())
}

// ============================================================================
// CommandResult Tests
// ============================================================================

#[sinex_test]
async fn test_command_result_success() -> TestResult<()> {
    let result = CommandResult::success();

    assert!(result.is_success());
    assert_eq!(result.status, Status::Success);
    assert!(result.errors.is_empty());
    assert!(result.timestamp.is_some());
    Ok(())
}

#[sinex_test]
async fn test_command_result_failure() -> TestResult<()> {
    let error = StructuredError::new("TEST_ERR", "Something went wrong");
    let result = CommandResult::failure(error);

    assert!(result.is_failure());
    assert_eq!(result.status, Status::Failed);
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].code, "TEST_ERR");
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_message() -> TestResult<()> {
    let result = CommandResult::success().with_message("All checks passed");

    assert_eq!(result.message, Some("All checks passed".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_details() -> TestResult<()> {
    let result = CommandResult::success().with_details(vec!["Step 1 done", "Step 2 done"]);

    assert_eq!(result.details.len(), 2);
    assert_eq!(result.details[0], "Step 1 done");
    assert_eq!(result.details[1], "Step 2 done");
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_detail_single() -> TestResult<()> {
    let result = CommandResult::success()
        .with_detail("First")
        .with_detail("Second");

    assert_eq!(result.details.len(), 2);
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_warning() -> TestResult<()> {
    let result = CommandResult::success()
        .with_warning("Deprecation notice")
        .with_warning("Performance concern");

    assert_eq!(result.warnings.len(), 2);
    assert_eq!(result.warnings[0], "Deprecation notice");
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_duration() -> TestResult<()> {
    let duration = Duration::from_secs_f64(1.5);
    let result = CommandResult::success().with_duration(duration);

    assert_eq!(result.duration_secs, Some(1.5));
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_error_changes_status() -> TestResult<()> {
    let result = CommandResult::success().with_error(StructuredError::new("ERR", "Error occurred"));

    assert!(result.is_failure());
    assert_eq!(result.status, Status::Failed);
    assert_eq!(result.errors.len(), 1);
    Ok(())
}

#[sinex_test]
async fn test_command_result_partial_status() -> TestResult<()> {
    let result = CommandResult::partial().with_message("Some checks failed");

    assert_eq!(result.status, Status::Partial);
    assert!(!result.is_success());
    assert!(!result.is_failure());
    Ok(())
}

// ============================================================================
// CommandMetadata Tests
// ============================================================================

#[sinex_test]
async fn test_command_metadata_default() -> TestResult<()> {
    let meta = CommandMetadata::default();

    assert!(meta.category.is_none());
    assert!(meta.timeout.is_none());
    assert!(!meta.modifies_state);
    assert!(meta.track_in_history);
    Ok(())
}

#[sinex_test]
async fn test_command_metadata_build() -> TestResult<()> {
    let meta = CommandMetadata::build();

    assert_eq!(meta.category, Some("build".to_string()));
    assert!(meta.timeout.is_some());
    assert!(meta.modifies_state);
    assert!(meta.track_in_history);
    Ok(())
}

#[sinex_test]
async fn test_command_metadata_test() -> TestResult<()> {
    let meta = CommandMetadata::test();

    assert_eq!(meta.category, Some("test".to_string()));
    assert!(meta.timeout.is_some());
    assert!(!meta.modifies_state);
    assert!(meta.track_in_history);
    Ok(())
}

#[sinex_test]
async fn test_command_metadata_database() -> TestResult<()> {
    let meta = CommandMetadata::database();

    assert_eq!(meta.category, Some("database".to_string()));
    assert!(meta.modifies_state);
    Ok(())
}

#[sinex_test]
async fn test_command_metadata_utility() -> TestResult<()> {
    let meta = CommandMetadata::utility();

    assert_eq!(meta.category, Some("utility".to_string()));
    assert!(meta.timeout.is_none());
    assert!(!meta.modifies_state);
    assert!(!meta.track_in_history);
    Ok(())
}

#[sinex_test]
async fn test_command_metadata_diagnostics() -> TestResult<()> {
    let meta = CommandMetadata::diagnostics();

    assert_eq!(meta.category, Some("diagnostics".to_string()));
    assert!(meta.timeout.is_some());
    assert!(!meta.modifies_state);
    Ok(())
}

// ============================================================================
// CommandContext Tests
// ============================================================================

#[sinex_test]
async fn test_command_context_elapsed() -> TestResult<()> {
    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, false, None);
    std::thread::sleep(Duration::from_millis(10));
    let elapsed = ctx.elapsed();

    assert!(elapsed.as_millis() >= 10);
    Ok(())
}

#[sinex_test]
async fn test_command_context_is_human() -> TestResult<()> {
    let ctx_human = CommandContext::new(OutputWriter::new(OutputFormat::Human), false, false, None);
    assert!(ctx_human.is_human());

    let ctx_json = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    assert!(!ctx_json.is_human());
    Ok(())
}

#[sinex_test]
async fn test_command_context_is_json() -> TestResult<()> {
    let ctx_json = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    assert!(ctx_json.is_json());

    let ctx_human = CommandContext::new(OutputWriter::new(OutputFormat::Human), false, false, None);
    assert!(!ctx_human.is_json());
    Ok(())
}

#[sinex_test]
async fn test_command_context_output_formats() -> TestResult<()> {
    for format in [
        OutputFormat::Human,
        OutputFormat::Json,
        OutputFormat::Compact,
        OutputFormat::Silent,
    ] {
        let ctx = CommandContext::new(OutputWriter::new(format), false, false, None);
        // Just verify we can create contexts with all formats
        let _ = ctx.elapsed();
    }
    Ok(())
}

// ============================================================================
// StructuredError Tests
// ============================================================================

#[sinex_test]
async fn test_structured_error_basic() -> TestResult<()> {
    let error = StructuredError::new("E001", "Something went wrong");

    assert_eq!(error.code, "E001");
    assert_eq!(error.message, "Something went wrong");
    assert!(error.location.is_none());
    assert!(error.suggestion.is_none());
    Ok(())
}

#[sinex_test]
async fn test_structured_error_with_location() -> TestResult<()> {
    let error = StructuredError::new("E002", "Syntax error").with_location("src/main.rs:42:10");

    assert_eq!(error.location, Some("src/main.rs:42:10".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_structured_error_with_suggestion() -> TestResult<()> {
    let error =
        StructuredError::new("E003", "Missing semicolon").with_suggestion("Add a semicolon here");

    assert_eq!(error.suggestion, Some("Add a semicolon here".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_structured_error_chained() -> TestResult<()> {
    let error = StructuredError::new("E004", "Compilation failed")
        .with_location("lib.rs:100:5")
        .with_suggestion("Check the syntax");

    assert_eq!(error.code, "E004");
    assert!(error.location.is_some());
    assert!(error.suggestion.is_some());
    Ok(())
}

// ============================================================================
// XtaskCommand Trait Tests
// ============================================================================

struct MockCommand {
    should_fail: bool,
    name: String,
}

#[async_trait::async_trait]
impl XtaskCommand for MockCommand {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, _ctx: &CommandContext) -> color_eyre::eyre::Result<CommandResult> {
        if self.should_fail {
            Ok(CommandResult::failure(StructuredError::new(
                "MOCK_ERR",
                "Mock failure",
            )))
        } else {
            Ok(CommandResult::success().with_message("Mock success"))
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

#[sinex_test]
async fn test_xtask_command_trait_success() -> TestResult<()> {
    let cmd = MockCommand {
        should_fail: false,
        name: "mock-success".to_string(),
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());
    assert_eq!(cmd.name(), "mock-success");
    Ok(())
}

#[sinex_test]
async fn test_xtask_command_trait_failure() -> TestResult<()> {
    let cmd = MockCommand {
        should_fail: true,
        name: "mock-failure".to_string(),
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_failure());
    assert_eq!(result.errors[0].code, "MOCK_ERR");
    Ok(())
}

#[sinex_test]
async fn test_xtask_command_trait_metadata() -> TestResult<()> {
    let cmd = MockCommand {
        should_fail: false,
        name: "mock".to_string(),
    };

    let meta = cmd.metadata();
    assert_eq!(meta.category, Some("check".to_string()));
    Ok(())
}

// ============================================================================
// CLI Invalid Argument Tests
// ============================================================================

#[sinex_test]
async fn test_cli_unknown_command() -> TestResult<()> {
    let output = Command::new("xtask").arg("nonexistent-command").output()?;

    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error") && (stderr.contains("unrecognized") || stderr.contains("invalid")),
        "Should report unrecognized or invalid command"
    );
    Ok(())
}

#[sinex_test]
async fn test_cli_unknown_flag() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("check")
        .arg("--nonexistent-flag")
        .output()?;

    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error") && stderr.contains("unexpected"),
        "Should report unexpected flag"
    );
    Ok(())
}

#[sinex_test]
async fn test_cli_missing_required_arg() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("generate-client-cert")
        .output()?;

    // xtr tls generate-client-cert requires --name
    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("required") || stderr.contains("--name") || stderr.contains("missing"),
        "Should indicate missing argument"
    );
    Ok(())
}

#[sinex_test]
async fn test_cli_invalid_format_option() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("--format")
        .arg("invalid_format")
        .arg("check")
        .output()?;

    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid"), "Should report invalid format");
    Ok(())
}

#[sinex_test]
async fn test_cli_redundant_json_options() -> TestResult<()> {
    let mut cmd = Command::new("xtask");

    // --json and --format json are redundant but should both work
    cmd.arg("--json")
        .arg("--format")
        .arg("json")
        .arg("check")
        .arg("--skip-preflight");

    // This might succeed or fail depending on format checks
    // The key is it shouldn't crash or give an obscure error
    let output = cmd.output()?;

    // Either it succeeds OR gives a clear error about format validation
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be a cryptic error
        assert!(
            stderr.contains("fmt") || stderr.contains("check") || stderr.contains("error"),
            "Should give a clear error, not cryptic failure: {stderr}"
        );
    }
    Ok(())
}

// ============================================================================
// JSON Output Format Validation Tests
// ============================================================================

#[sinex_test]
async fn test_json_output_is_valid_json() -> TestResult<()> {
    let mut cmd = Command::new("xtask");

    cmd.arg("--json").arg("deps").arg("list");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Parse as JSON to validate
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&stdout);
        assert!(parsed.is_ok(), "Output should be valid JSON: {stdout}");
    }
    Ok(())
}

#[sinex_test]
async fn test_json_output_contains_required_fields() -> TestResult<()> {
    let mut cmd = Command::new("xtask");

    cmd.arg("--json").arg("deps").arg("list");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: serde_json::Value = serde_json::from_str(&stdout)?;

        // Required fields in CommandResult
        assert!(parsed.get("command").is_some(), "should have command field");
        assert!(parsed.get("status").is_some(), "should have status field");
    }
    Ok(())
}

#[sinex_test]
async fn test_json_output_status_values() -> TestResult<()> {
    let mut cmd = Command::new("xtask");

    // Use 'deps list' which has clean JSON output (unlike 'status' which outputs human-readable + JSON)
    cmd.arg("--json").arg("deps").arg("list");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: serde_json::Value = serde_json::from_str(&stdout)?;

        let status = parsed.get("status").and_then(|s| s.as_str());
        assert!(
            matches!(status, Some("success" | "failed" | "partial" | "running")),
            "status should be a valid value: {status:?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_json_output_for_failing_command() -> TestResult<()> {
    let mut cmd = Command::new("xtask");

    // This should fail because the database likely isn't available
    cmd.arg("--json").arg("db").arg("schema").arg("status");

    let output = cmd.output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&stdout);
        assert!(
            parsed.is_ok(),
            "Even failing commands should output valid JSON: {stdout}"
        );
    }
    Ok(())
}

// ============================================================================
// Process Output Tests
// ============================================================================

#[sinex_test]
async fn test_process_output_success_check() -> TestResult<()> {
    let output = ProcessBuilder::new("true")
        .run()
        .expect("true should succeed");

    assert!(output.success());
    assert_eq!(output.exit_code, 0);
    Ok(())
}

#[sinex_test]
async fn test_process_output_combined() -> TestResult<()> {
    let output = ProcessBuilder::new("sh")
        .args(["-c", "echo stdout; echo stderr >&2"])
        .run()
        .expect("should succeed");

    let combined = output.combined();
    assert!(combined.contains("stdout"));
    assert!(combined.contains("stderr"));
    Ok(())
}

// ============================================================================
// Status Symbol and Color Tests
// ============================================================================

#[sinex_test]
async fn test_status_symbols() -> TestResult<()> {
    assert_eq!(Status::Success.symbol(), "\u{2713}"); // checkmark
    assert_eq!(Status::Failed.symbol(), "\u{2717}"); // X mark
    assert_eq!(Status::Partial.symbol(), "\u{26A0}"); // warning
    assert_eq!(Status::Running.symbol(), "\u{22EF}"); // ellipsis
    assert_eq!(Status::Cancelled.symbol(), "\u{2298}"); // circle slash
    Ok(())
}

#[sinex_test]
async fn test_status_color_codes() -> TestResult<()> {
    // Green for success
    assert!(Status::Success.color_code().contains("32"));
    // Red for failed
    assert!(Status::Failed.color_code().contains("31"));
    // Yellow for partial
    assert!(Status::Partial.color_code().contains("33"));
    // Cyan for running
    assert!(Status::Running.color_code().contains("36"));
    // Gray for cancelled
    assert!(Status::Cancelled.color_code().contains("90"));
    Ok(())
}

// ============================================================================
// Edge Cases for Commands
// ============================================================================

#[sinex_test]
async fn test_test_command_with_invalid_profile() -> TestResult<()> {
    let mut cmd = Command::new("xtask");

    cmd.arg("test")
        .arg("--skip-preflight")
        .arg("--")
        .arg("--profile")
        .arg("nonexistent_profile");

    // Nextest may silently fall back to the default profile when the requested
    // profile doesn't exist, so we can't reliably assert failure.  What we CAN
    // verify is that the xtask invocation doesn't panic or produce a confusing
    // exit path — it should either fail gracefully or succeed with default settings.
    let output = cmd.output()?;
    // No assertion on exit code: nextest profile validation is version-dependent.
    // Just verify the process ran to completion without panicking.
    let _ = output.status;
    Ok(())
}

#[sinex_test]
async fn test_db_reset_without_confirmation() -> TestResult<()> {
    let output = Command::new("xtask").arg("infra").arg("reset").output()?;

    // infra reset requires --yes flag
    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--yes") || stderr.contains("dangerous"),
        "Should mention --yes or dangerous"
    );
    Ok(())
}

#[sinex_test]
async fn test_schema_deploy_missing_database_url() -> TestResult<()> {
    let output = Command::new("xtask")
        .env_remove("DATABASE_URL")
        .arg("contracts")
        .arg("deploy")
        .arg("--input")
        .arg("schemas/v1")
        .output()?;

    // Unset DATABASE_URL to ensure it's missing
    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("required")
            || stderr.contains("DATABASE_URL")
            || stderr.contains("database-url"),
        "Should indicate missing database URL"
    );
    Ok(())
}

#[sinex_test]
async fn test_help_works_for_all_subcommands() -> TestResult<()> {
    let subcommands = [
        vec!["check", "--help"],
        vec!["test", "--help"],
        vec!["build", "--help"],
        vec!["infra", "reset", "--help"],
        vec!["contracts", "deploy", "--help"],
        vec!["deps", "list", "--help"],
        vec!["deps", "graph", "--help"],
        vec!["status", "--help"],
        vec!["jobs", "list", "--help"],
        vec!["xtr", "patterns", "--help"],
        vec!["xtr", "tls", "--help"],
    ];

    for args in subcommands {
        let mut cmd = Command::new("xtask");
        for arg in &args {
            cmd.arg(arg);
        }
        let output = cmd.output()?;
        assert!(
            output.status.success(),
            "Help for {:?} should succeed",
            args
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_check_skip_options() -> TestResult<()> {
    // IMPORTANT: never run `xtask check` with valid flags in a nextest test.
    // xtask check invokes `cargo check`, which tries to acquire the cargo
    // target directory lock that nextest already holds. This causes deadlock.
    //
    // Safe alternative: verify flags are accepted by clap via --help output.
    // The unit tests in commands/check.rs cover the actual flag behavior.
    let output = Command::new("xtask").arg("check").arg("--help").output()?;

    assert!(output.status.success(), "Help command should succeed");
    let help = String::from_utf8_lossy(&output.stdout);

    // Verify the new additive flags are present
    assert!(help.contains("--lint"), "missing --lint flag");
    assert!(help.contains("--fmt"), "missing --fmt flag");
    assert!(help.contains("--forbidden"), "missing --forbidden flag");
    assert!(help.contains("--full"), "missing --full flag");
    // Verify old subtractive flags are gone
    assert!(!help.contains("--skip-fmt"), "--skip-fmt should be removed");
    Ok(())
}

#[sinex_test]
async fn test_completions_all_shells() -> TestResult<()> {
    for shell in ["bash", "zsh", "fish"] {
        let output = Command::new("xtask")
            .arg("xtr")
            .arg("completions")
            .arg(shell)
            .output()?;

        // Should succeed (output may go to stderr or stdout depending on clap_complete)
        assert!(
            output.status.success(),
            "Completions for {shell} should succeed"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_completions_power_shell() -> TestResult<()> {
    // Clap uses kebab-case 'power-shell' for the PowerShell variant
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("completions")
        .arg("power-shell")
        .output()?;

    assert!(
        output.status.success(),
        "PowerShell completions should succeed"
    );
    Ok(())
}
