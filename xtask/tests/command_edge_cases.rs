//! Edge case tests for command execution framework.
//!
//! Tests cover:
//! - Commands with invalid arguments
//! - Timeout handling
//! - `ProcessBuilder` error cases
//! - JSON output format validation
//! - `CommandContext` behavior
//! - `CommandResult` construction

use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::time::Duration;

use xtask::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use xtask::output::{OutputFormat, OutputWriter, Status, StructuredError};
use xtask::process::ProcessBuilder;

// ============================================================================
// ProcessBuilder Error Cases
// ============================================================================

#[test]
fn test_process_builder_nonexistent_command() {
    let result = ProcessBuilder::new("nonexistent_command_that_does_not_exist_xyz")
        .arg("--version")
        .run();

    assert!(result.is_err(), "Nonexistent command should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("failed to spawn") || err.contains("No such file"),
        "Error should indicate spawn failure: {err}"
    );
}

#[test]
fn test_process_builder_command_not_found_with_description() {
    let result = ProcessBuilder::new("totally_fake_command")
        .with_description("my custom operation")
        .run();

    assert!(result.is_err(), "Missing command should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("my custom operation"),
        "Error should include description: {err}"
    );
}

#[test]
fn test_process_builder_command_exits_with_error() {
    let result = ProcessBuilder::new("false").run();

    assert!(result.is_err(), "Command returning non-zero should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("failed") || err.contains("exit code"),
        "Error should mention failure: {err}"
    );
}

#[test]
fn test_process_builder_command_exits_with_error_code() {
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
}

#[test]
fn test_process_builder_stderr_captured_on_error() {
    let result = ProcessBuilder::new("sh")
        .args(["-c", "echo 'error message' >&2; exit 1"])
        .run();

    assert!(result.is_err(), "Should fail with exit code 1");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("error message"),
        "Error should include stderr output: {err}"
    );
}

#[test]
fn test_process_builder_run_success_returns_bool() {
    let success = ProcessBuilder::new("true")
        .run_success()
        .expect("should not error on spawn");
    assert!(success, "true command should succeed");

    let failure = ProcessBuilder::new("false")
        .run_success()
        .expect("should not error on spawn");
    assert!(!failure, "false command should not succeed");
}

#[test]
fn test_process_builder_run_ok_discards_output() {
    ProcessBuilder::new("echo")
        .arg("hello world")
        .run_ok()
        .expect("echo should succeed");

    let err = ProcessBuilder::new("false").run_ok();
    assert!(err.is_err(), "false should fail");
}

#[test]
fn test_process_builder_run_stdout() {
    let output = ProcessBuilder::new("echo")
        .arg("test output")
        .run_stdout()
        .expect("echo should succeed");

    assert_eq!(output, "test output", "Should capture trimmed stdout");
}

#[test]
fn test_process_builder_with_env_variable() {
    let output = ProcessBuilder::new("sh")
        .args(["-c", "echo $MY_TEST_VAR"])
        .env("MY_TEST_VAR", "custom_value")
        .run()
        .expect("should succeed");

    assert!(output.success());
    assert_eq!(output.stdout.trim(), "custom_value");
}

#[test]
fn test_process_builder_with_current_dir() {
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
}

#[test]
fn test_process_builder_multiple_args() {
    let output = ProcessBuilder::new("echo")
        .args(["one", "two", "three"])
        .run()
        .expect("echo should succeed");

    assert!(output.success());
    assert_eq!(output.stdout.trim(), "one two three");
}

#[test]
fn test_process_builder_git_helper() {
    let output = ProcessBuilder::git()
        .arg("--version")
        .run()
        .expect("git --version should succeed");

    assert!(output.success());
    assert!(output.stdout.contains("git version"));
}

#[test]
fn test_process_builder_cargo_helper() {
    let output = ProcessBuilder::cargo()
        .arg("--version")
        .run()
        .expect("cargo --version should succeed");

    assert!(output.success());
    assert!(output.stdout.contains("cargo"));
}

#[test]
fn test_process_builder_psql_helper_without_db() {
    // psql without connection should fail, but the helper should work
    let result = ProcessBuilder::psql().arg("--version").run();

    // This might succeed or fail depending on system setup,
    // but the important thing is the helper method exists and works
    match result {
        Ok(output) => assert!(output.stdout.contains("psql")),
        Err(_) => {} // OK if psql not available
    }
}

// ============================================================================
// CommandResult Tests
// ============================================================================

#[test]
fn test_command_result_success() {
    let result = CommandResult::success();

    assert!(result.is_success());
    assert_eq!(result.status, Status::Success);
    assert!(result.errors.is_empty());
    assert!(result.timestamp.is_some());
}

#[test]
fn test_command_result_failure() {
    let error = StructuredError::new("TEST_ERR", "Something went wrong");
    let result = CommandResult::failure(error);

    assert!(result.is_failure());
    assert_eq!(result.status, Status::Failed);
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].code, "TEST_ERR");
}

#[test]
fn test_command_result_with_message() {
    let result = CommandResult::success().with_message("All checks passed");

    assert_eq!(result.message, Some("All checks passed".to_string()));
}

#[test]
fn test_command_result_with_details() {
    let result = CommandResult::success().with_details(vec!["Step 1 done", "Step 2 done"]);

    assert_eq!(result.details.len(), 2);
    assert_eq!(result.details[0], "Step 1 done");
    assert_eq!(result.details[1], "Step 2 done");
}

#[test]
fn test_command_result_with_detail_single() {
    let result = CommandResult::success()
        .with_detail("First")
        .with_detail("Second");

    assert_eq!(result.details.len(), 2);
}

#[test]
fn test_command_result_with_warning() {
    let result = CommandResult::success()
        .with_warning("Deprecation notice")
        .with_warning("Performance concern");

    assert_eq!(result.warnings.len(), 2);
    assert_eq!(result.warnings[0], "Deprecation notice");
}

#[test]
fn test_command_result_with_duration() {
    let duration = Duration::from_secs_f64(1.5);
    let result = CommandResult::success().with_duration(duration);

    assert_eq!(result.duration_secs, Some(1.5));
}

#[test]
fn test_command_result_with_error_changes_status() {
    let result = CommandResult::success().with_error(StructuredError::new("ERR", "Error occurred"));

    assert!(result.is_failure());
    assert_eq!(result.status, Status::Failed);
    assert_eq!(result.errors.len(), 1);
}

#[test]
fn test_command_result_partial_status() {
    let result = CommandResult::partial().with_message("Some checks failed");

    assert_eq!(result.status, Status::Partial);
    assert!(!result.is_success());
    assert!(!result.is_failure());
}

// ============================================================================
// CommandMetadata Tests
// ============================================================================

#[test]
fn test_command_metadata_default() {
    let meta = CommandMetadata::default();

    assert!(meta.category.is_none());
    assert!(meta.timeout.is_none());
    assert!(!meta.modifies_state);
    assert!(meta.track_in_history);
}

#[test]
fn test_command_metadata_build() {
    let meta = CommandMetadata::build();

    assert_eq!(meta.category, Some("build".to_string()));
    assert!(meta.timeout.is_some());
    assert!(meta.modifies_state);
    assert!(meta.track_in_history);
}

#[test]
fn test_command_metadata_test() {
    let meta = CommandMetadata::test();

    assert_eq!(meta.category, Some("test".to_string()));
    assert!(meta.timeout.is_some());
    assert!(!meta.modifies_state);
    assert!(meta.track_in_history);
}

#[test]
fn test_command_metadata_database() {
    let meta = CommandMetadata::database();

    assert_eq!(meta.category, Some("database".to_string()));
    assert!(meta.modifies_state);
}

#[test]
fn test_command_metadata_utility() {
    let meta = CommandMetadata::utility();

    assert_eq!(meta.category, Some("utility".to_string()));
    assert!(meta.timeout.is_none());
    assert!(!meta.modifies_state);
    assert!(!meta.track_in_history);
}

#[test]
fn test_command_metadata_diagnostics() {
    let meta = CommandMetadata::diagnostics();

    assert_eq!(meta.category, Some("diagnostics".to_string()));
    assert!(meta.timeout.is_some());
    assert!(!meta.modifies_state);
}

// ============================================================================
// CommandContext Tests
// ============================================================================

#[test]
fn test_command_context_elapsed() {
    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, false, None);
    std::thread::sleep(Duration::from_millis(10));
    let elapsed = ctx.elapsed();

    assert!(elapsed.as_millis() >= 10);
}

#[test]
fn test_command_context_is_human() {
    let ctx_human = CommandContext::new(OutputWriter::new(OutputFormat::Human), false, false, None);
    assert!(ctx_human.is_human());

    let ctx_json = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    assert!(!ctx_json.is_human());
}

#[test]
fn test_command_context_is_json() {
    let ctx_json = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    assert!(ctx_json.is_json());

    let ctx_human = CommandContext::new(OutputWriter::new(OutputFormat::Human), false, false, None);
    assert!(!ctx_human.is_json());
}

#[test]
fn test_command_context_output_formats() {
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
}

// ============================================================================
// StructuredError Tests
// ============================================================================

#[test]
fn test_structured_error_basic() {
    let error = StructuredError::new("E001", "Something went wrong");

    assert_eq!(error.code, "E001");
    assert_eq!(error.message, "Something went wrong");
    assert!(error.location.is_none());
    assert!(error.suggestion.is_none());
}

#[test]
fn test_structured_error_with_location() {
    let error = StructuredError::new("E002", "Syntax error").with_location("src/main.rs:42:10");

    assert_eq!(error.location, Some("src/main.rs:42:10".to_string()));
}

#[test]
fn test_structured_error_with_suggestion() {
    let error =
        StructuredError::new("E003", "Missing semicolon").with_suggestion("Add a semicolon here");

    assert_eq!(error.suggestion, Some("Add a semicolon here".to_string()));
}

#[test]
fn test_structured_error_chained() {
    let error = StructuredError::new("E004", "Compilation failed")
        .with_location("lib.rs:100:5")
        .with_suggestion("Check the syntax");

    assert_eq!(error.code, "E004");
    assert!(error.location.is_some());
    assert!(error.suggestion.is_some());
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

    async fn execute(&self, _ctx: &CommandContext) -> anyhow::Result<CommandResult> {
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

#[tokio::test]
async fn test_xtask_command_trait_success() {
    let cmd = MockCommand {
        should_fail: false,
        name: "mock-success".to_string(),
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, false, None);
    let result = cmd.execute(&ctx).await.expect("execute should not error");

    assert!(result.is_success());
    assert_eq!(cmd.name(), "mock-success");
}

#[tokio::test]
async fn test_xtask_command_trait_failure() {
    let cmd = MockCommand {
        should_fail: true,
        name: "mock-failure".to_string(),
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, false, None);
    let result = cmd.execute(&ctx).await.expect("execute should not error");

    assert!(result.is_failure());
    assert_eq!(result.errors[0].code, "MOCK_ERR");
}

#[test]
fn test_xtask_command_trait_metadata() {
    let cmd = MockCommand {
        should_fail: false,
        name: "mock".to_string(),
    };

    let meta = cmd.metadata();
    assert_eq!(meta.category, Some("check".to_string()));
}

// ============================================================================
// CLI Invalid Argument Tests
// ============================================================================

#[test]
fn test_cli_unknown_command() {
    let mut cmd = cargo_bin_cmd!("xtask");

    cmd.arg("nonexistent-command");

    cmd.assert().failure().stderr(
        predicate::str::contains("error")
            .and(predicate::str::contains("unrecognized").or(predicate::str::contains("invalid"))),
    );
}

#[test]
fn test_cli_unknown_flag() {
    let mut cmd = cargo_bin_cmd!("xtask");

    cmd.arg("check").arg("--nonexistent-flag");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("error").and(predicate::str::contains("unexpected")));
}

#[test]
fn test_cli_missing_required_arg() {
    let mut cmd = cargo_bin_cmd!("xtask");

    // stack tls generate-client-cert requires --name
    cmd.arg("stack").arg("tls").arg("generate-client-cert");

    cmd.assert().failure().stderr(
        predicate::str::contains("required")
            .or(predicate::str::contains("--name"))
            .or(predicate::str::contains("missing")),
    );
}

#[test]
fn test_cli_invalid_format_option() {
    let mut cmd = cargo_bin_cmd!("xtask");

    cmd.arg("--format").arg("invalid_format").arg("check");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("invalid"));
}

#[test]
fn test_cli_redundant_json_options() {
    let mut cmd = cargo_bin_cmd!("xtask");

    // --json and --format json are redundant but should both work
    cmd.arg("--json").arg("--format").arg("json").arg("check");

    // This might succeed or fail depending on format checks
    // The key is it shouldn't crash or give an obscure error
    let output = cmd.output().expect("command should run");

    // Either it succeeds OR gives a clear error about format validation
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be a cryptic error
        assert!(
            stderr.contains("fmt") || stderr.contains("check") || stderr.contains("error"),
            "Should give a clear error, not cryptic failure: {stderr}"
        );
    }
}

// ============================================================================
// JSON Output Format Validation Tests
// ============================================================================

#[test]
fn test_json_output_is_valid_json() {
    let mut cmd = cargo_bin_cmd!("xtask");

    cmd.arg("--json").arg("deps").arg("list");

    let output = cmd.output().expect("command should run");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Parse as JSON to validate
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&stdout);
        assert!(parsed.is_ok(), "Output should be valid JSON: {stdout}");
    }
}

#[test]
fn test_json_output_contains_required_fields() {
    let mut cmd = cargo_bin_cmd!("xtask");

    cmd.arg("--json").arg("deps").arg("list");

    let output = cmd.output().expect("command should run");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: serde_json::Value =
            serde_json::from_str(&stdout).expect("should be valid JSON");

        // Required fields in CommandResult
        assert!(parsed.get("command").is_some(), "should have command field");
        assert!(parsed.get("status").is_some(), "should have status field");
    }
}

#[test]
fn test_json_output_status_values() {
    let mut cmd = cargo_bin_cmd!("xtask");

    // Use 'deps list' which has clean JSON output (unlike 'status' which outputs human-readable + JSON)
    cmd.arg("--json").arg("deps").arg("list");

    let output = cmd.output().expect("command should run");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: serde_json::Value =
            serde_json::from_str(&stdout).expect("should be valid JSON");

        let status = parsed.get("status").and_then(|s| s.as_str());
        assert!(
            matches!(status, Some("success" | "failed" | "partial" | "running")),
            "status should be a valid value: {status:?}"
        );
    }
}

#[test]
fn test_json_output_for_failing_command() {
    let mut cmd = cargo_bin_cmd!("xtask");

    // This should fail because the database likely isn't available
    cmd.arg("--json").arg("db").arg("schema").arg("status");

    let output = cmd.output().expect("command should run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&stdout);
        assert!(
            parsed.is_ok(),
            "Even failing commands should output valid JSON: {stdout}"
        );
    }
}

// ============================================================================
// Process Output Tests
// ============================================================================

#[test]
fn test_process_output_success_check() {
    let output = ProcessBuilder::new("true")
        .run()
        .expect("true should succeed");

    assert!(output.success());
    assert_eq!(output.exit_code, 0);
}

#[test]
fn test_process_output_combined() {
    let output = ProcessBuilder::new("sh")
        .args(["-c", "echo stdout; echo stderr >&2"])
        .run()
        .expect("should succeed");

    let combined = output.combined();
    assert!(combined.contains("stdout"));
    assert!(combined.contains("stderr"));
}

// ============================================================================
// Status Symbol and Color Tests
// ============================================================================

#[test]
fn test_status_symbols() {
    assert_eq!(Status::Success.symbol(), "\u{2713}"); // checkmark
    assert_eq!(Status::Failed.symbol(), "\u{2717}"); // X mark
    assert_eq!(Status::Partial.symbol(), "\u{26A0}"); // warning
    assert_eq!(Status::Running.symbol(), "\u{22EF}"); // ellipsis
    assert_eq!(Status::Cancelled.symbol(), "\u{2298}"); // circle slash
}

#[test]
fn test_status_color_codes() {
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
}

// ============================================================================
// Edge Cases for Commands
// ============================================================================

#[test]
fn test_test_command_with_invalid_profile() {
    let mut cmd = cargo_bin_cmd!("xtask");

    cmd.arg("test")
        .arg("--")
        .arg("--profile")
        .arg("nonexistent_profile");

    // Should fail because nextest won't find the profile
    let output = cmd.output().expect("command should run");
    // The command should fail or warn about the invalid profile
    // (exact behavior depends on nextest error handling)
    assert!(
        !output.status.success()
            || String::from_utf8_lossy(&output.stderr).contains("profile")
            || String::from_utf8_lossy(&output.stderr).contains("not found")
    );
}

#[test]
fn test_db_reset_without_confirmation() {
    let mut cmd = cargo_bin_cmd!("xtask");

    // db reset requires --yes flag
    cmd.arg("stack").arg("reset");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("--yes").or(predicate::str::contains("dangerous")));
}

#[test]
fn test_schema_deploy_missing_database_url() {
    let mut cmd = cargo_bin_cmd!("xtask");

    // Unset DATABASE_URL to ensure it's missing
    cmd.env_remove("DATABASE_URL");

    cmd.arg("db")
        .arg("schema")
        .arg("deploy")
        .arg("--input")
        .arg("schemas/v1");

    cmd.assert().failure().stderr(
        predicate::str::contains("required")
            .or(predicate::str::contains("DATABASE_URL"))
            .or(predicate::str::contains("database-url")),
    );
}

#[test]
fn test_help_works_for_all_subcommands() {
    let subcommands = [
        vec!["check", "--help"],
        vec!["test", "--help"],
        vec!["build", "--help"],
        vec!["stack", "reset", "--help"],
        vec!["contracts", "deploy", "--help"],
        vec!["deps", "list", "--help"],
        vec!["deps", "graph", "--help"],
        vec!["stack", "doctor", "--help"],
        vec!["jobs", "list", "--help"],
        vec!["xtr", "patterns", "--help"],
        vec!["xtr", "ci", "--help"],
    ];

    for args in subcommands {
        let mut cmd = cargo_bin_cmd!("xtask");
        for arg in &args {
            cmd.arg(arg);
        }
        cmd.assert().success();
    }
}

#[test]
fn test_check_skip_options() {
    let mut cmd = cargo_bin_cmd!("xtask");

    cmd.arg("check")
        .arg("--skip-fmt")
        .arg("--lint=false")
        .arg("--forbidden=false");

    // With everything skipped, should succeed quickly (doing nothing)
    cmd.assert().success();
}

#[test]
fn test_completions_all_shells() {
    for shell in ["bash", "zsh", "fish"] {
        let mut cmd = cargo_bin_cmd!("xtask");
        cmd.arg("xtr").arg("completions").arg(shell);

        // Should succeed (output may go to stderr or stdout depending on clap_complete)
        cmd.assert().success();
    }
}

#[test]
fn test_completions_power_shell() {
    let mut cmd = cargo_bin_cmd!("xtask");
    // Clap uses kebab-case 'power-shell' for the PowerShell variant
    cmd.arg("xtr").arg("completions").arg("power-shell");

    cmd.assert().success();
}
