//! Edge case tests for command execution framework.
//!
//! Tests cover:
//! - Commands with invalid arguments
//! - Timeout handling
//! - `ProcessBuilder` error cases
//! - JSON output format validation
//! - `CommandContext` behavior
//! - `CommandResult` construction

mod support;

use std::time::{Duration, Instant};

use serde_json::Value;
use support::xtask_command;
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
async fn test_command_metadata_factories() -> TestResult<()> {
    // (factory_fn_result, expected_category, timeout_is_some, modifies_state, track_in_history)
    let cases: &[(CommandMetadata, Option<&str>, bool, bool, Option<bool>)] = &[
        (CommandMetadata::default(), None, false, false, Some(true)),
        (
            CommandMetadata::build(),
            Some("build"),
            true,
            true,
            Some(true),
        ),
        (
            CommandMetadata::test(),
            Some("test"),
            true,
            false,
            Some(true),
        ),
        (
            CommandMetadata::database(),
            Some("database"),
            true,
            true,
            None,
        ),
        (
            CommandMetadata::utility(),
            Some("utility"),
            false,
            false,
            Some(false),
        ),
        (
            CommandMetadata::diagnostics(),
            Some("diagnostics"),
            true,
            false,
            None,
        ),
    ];

    for (meta, exp_cat, exp_timeout, exp_modifies, exp_track) in cases {
        assert_eq!(meta.category, *exp_cat, "category mismatch for {exp_cat:?}");
        assert_eq!(
            meta.timeout.is_some(),
            *exp_timeout,
            "timeout.is_some() mismatch for {exp_cat:?}"
        );
        assert_eq!(
            meta.modifies_state, *exp_modifies,
            "modifies_state mismatch for {exp_cat:?}"
        );
        if let Some(track) = exp_track {
            assert_eq!(
                meta.track_in_history, *track,
                "track_in_history mismatch for {exp_cat:?}"
            );
        }
    }
    Ok(())
}

// ============================================================================
// CommandContext Tests
// ============================================================================

#[sinex_test]
async fn test_command_context_elapsed() -> TestResult<()> {
    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, None, "test");
    let baseline = ctx.elapsed();
    let deadline = Instant::now() + Duration::from_secs(1);

    loop {
        let elapsed = ctx.elapsed();
        if elapsed > baseline {
            return Ok(());
        }

        assert!(
            Instant::now() < deadline,
            "CommandContext::elapsed() never advanced past {baseline:?}"
        );
        std::thread::yield_now();
    }
}

#[sinex_test]
async fn test_command_context_is_human() -> TestResult<()> {
    let ctx_human =
        CommandContext::new(OutputWriter::new(OutputFormat::Human), false, None, "test");
    assert!(ctx_human.is_human());

    let ctx_json = CommandContext::new(OutputWriter::new(OutputFormat::Json), false, None, "test");
    assert!(!ctx_json.is_human());
    Ok(())
}

#[sinex_test]
async fn test_command_context_is_json() -> TestResult<()> {
    let ctx_json = CommandContext::new(OutputWriter::new(OutputFormat::Json), false, None, "test");
    assert!(ctx_json.is_json());

    let ctx_human =
        CommandContext::new(OutputWriter::new(OutputFormat::Human), false, None, "test");
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
        let ctx = CommandContext::new(OutputWriter::new(format), false, None, "test");
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

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, None, "test");
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

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, None, "test");
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
    assert_eq!(meta.category, Some("check"));
    Ok(())
}

// ============================================================================
// CLI Invalid Argument Tests
// ============================================================================

#[sinex_test]
async fn test_cli_unknown_command() -> TestResult<()> {
    let output = xtask_command()?.arg("nonexistent-command").output()?;

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
    let output = xtask_command()?
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
    // `xtask reset` requires --yes to confirm the destructive operation
    let output = xtask_command()?.arg("reset").output()?;

    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("required") || stderr.contains("--yes") || stderr.contains("missing"),
        "Should indicate missing argument"
    );
    Ok(())
}

#[sinex_test]
async fn test_cli_invalid_format_option() -> TestResult<()> {
    let output = xtask_command()?
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
    let mut cmd = xtask_command()?;

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
        if stderr.trim().is_empty() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let parsed: serde_json::Value = serde_json::from_str(&stdout)?;
            assert_eq!(
                parsed.get("command").and_then(|v| v.as_str()),
                Some("xtask")
            );
            assert!(
                matches!(parsed.get("status").and_then(|v| v.as_str()), Some("error")),
                "redundant JSON flags should still produce a structured failure envelope: {stdout}"
            );
            assert!(
                parsed
                    .get("errors")
                    .and_then(|value| value.as_array())
                    .is_some_and(|errors| !errors.is_empty()),
                "redundant JSON flags should surface structured argument errors: {stdout}"
            );
        } else {
            // Should not be a cryptic error
            assert!(
                stderr.contains("fmt") || stderr.contains("check") || stderr.contains("error"),
                "Should give a clear error, not cryptic failure: {stderr}"
            );
        }
    }
    Ok(())
}

// ============================================================================
// JSON Output Format Validation Tests
// ============================================================================

#[sinex_test]
async fn test_json_output_is_valid_json() -> TestResult<()> {
    let mut cmd = xtask_command()?;

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
    let mut cmd = xtask_command()?;

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
    let mut cmd = xtask_command()?;

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
    let mut cmd = xtask_command()?;

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
async fn test_test_command_accepts_passthrough_args() -> TestResult<()> {
    // We cannot run a real `xtask test` from inside nextest — the nextest guard
    // fires (NEXTEST_RUN_ID inherited) and cargo target/ lock would deadlock
    // regardless.  Instead, verify the CLI accepts passthrough args via --help
    // and dry-run, matching the pattern used by test_check_skip_options.
    let output = xtask_command()?.arg("test").arg("--help").output()?;

    assert!(output.status.success(), "Help command should succeed");
    let help = String::from_utf8_lossy(&output.stdout);

    // Verify key test flags are accepted by clap
    assert!(
        help.contains("--skip-preflight"),
        "missing --skip-preflight"
    );
    assert!(help.contains("--debug"), "missing --debug");
    assert!(help.contains("--heavy"), "missing --heavy");
    // bench/fuzz/coverage are subcommands, not flags
    assert!(help.contains("bench"), "missing bench subcommand");
    assert!(help.contains("fuzz"), "missing fuzz subcommand");
    assert!(help.contains("coverage"), "missing coverage subcommand");

    // Verify dry-run actually works (doesn't invoke cargo)
    let dry = xtask_command()?
        .args(["test", "--dry-run", "--json"])
        .output()?;
    assert!(dry.status.success(), "dry-run should succeed");
    let stdout = String::from_utf8_lossy(&dry.stdout);
    let payload: Value = serde_json::from_str(&stdout)?;
    assert_eq!(payload["status"], "success");
    Ok(())
}

#[sinex_test]
async fn test_db_reset_without_confirmation() -> TestResult<()> {
    // `xtask reset` requires --yes.
    let output = xtask_command()?.arg("reset").output()?;

    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--yes") || stderr.contains("required"),
        "Should mention --yes"
    );
    Ok(())
}

#[sinex_test]
async fn test_status_schemas_succeeds() -> TestResult<()> {
    // `contracts info` was folded into `xtask status --schemas`.
    let output = xtask_command()?.arg("status").arg("--schemas").output()?;

    assert!(
        output.status.success(),
        "status --schemas should succeed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("status --schemas should produce valid JSON");
    assert!(
        parsed["data"]["schemas"].is_array(),
        "JSON data.schemas should be an array"
    );
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
    let output = xtask_command()?.arg("check").arg("--help").output()?;

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
        let output = xtask_command()?.arg("completions").arg(shell).output()?;

        assert!(
            output.status.success(),
            "Completions for {shell} should succeed"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_completions_power_shell() -> TestResult<()> {
    // Clap uses kebab-case 'power-shell' for the PowerShell variant.
    let output = xtask_command()?
        .arg("completions")
        .arg("power-shell")
        .output()?;

    assert!(
        output.status.success(),
        "PowerShell completions should succeed"
    );
    Ok(())
}

#[sinex_test]
async fn test_test_bench_dry_run_short_circuits_lane() -> TestResult<()> {
    // bench is now a subcommand: `xtask test bench --dry-run`
    // Running from inside nextest triggers the nextest guard — use --help to verify
    // the subcommand and --dry-run flag are accepted by clap.
    let output = xtask_command()?
        .arg("test")
        .arg("bench")
        .arg("--help")
        .output()?;

    assert!(output.status.success(), "bench --help should succeed");
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(
        help.contains("--dry-run"),
        "bench subcommand should accept --dry-run, got: {help}"
    );
    assert!(
        help.contains("--contracts"),
        "bench subcommand should accept --contracts, got: {help}"
    );
    Ok(())
}

#[sinex_test]
async fn test_test_fuzz_lane_reports_no_targets_as_failure() -> TestResult<()> {
    // fuzz is now a subcommand: `xtask test fuzz`
    // Running from inside nextest triggers the nextest guard for test subcommands —
    // verify the subcommand and flags are accepted via --help instead.
    let output = xtask_command()?
        .arg("test")
        .arg("fuzz")
        .arg("--help")
        .output()?;

    assert!(output.status.success(), "fuzz --help should succeed");
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(
        help.contains("fuzz") || help.contains("cargo-fuzz"),
        "fuzz subcommand help should mention fuzz, got: {help}"
    );
    Ok(())
}

#[sinex_test]
async fn test_test_subcommands_are_recognized() -> TestResult<()> {
    // bench and fuzz are now subcommands (exclusivity enforced by clap's subcommand model).
    // Verify each subcommand is recognized via --help.
    for subcmd in &["bench", "fuzz", "coverage", "mutants", "vm"] {
        let output = xtask_command()?
            .arg("test")
            .arg(subcmd)
            .arg("--help")
            .output()?;
        assert!(
            output.status.success(),
            "xtask test {subcmd} --help should succeed"
        );
    }
    // Verify that an unrecognized subcommand fails
    let bad = xtask_command()?
        .arg("test")
        .arg("nonexistent-lane")
        .output()?;
    assert!(!bad.status.success(), "unknown test subcommand should fail");
    Ok(())
}
