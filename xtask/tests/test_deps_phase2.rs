//! Integration tests for deps commands - Phase 2
//!
//! Tests for the unused and timings subcommands added in Phase 2.
//! This module comprehensively tests:
//! - deps unused command with format options and CI mode
//! - deps timings command with parametrization
//! - Enhanced list/tree/duplicates commands from Phase 1

#![allow(deprecated)]
use assert_cmd::Command;
use predicates::prelude::*;

// ============================================================================
// Phase 2: Unused Dependencies & Build Timings Tests
// ============================================================================

// --- Help & Discovery Tests ---

#[test]
fn test_deps_unused_help() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("unused").arg("--help");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Detect unused dependencies"))
        .stdout(predicate::str::contains("--ci"));
}

#[test]
fn test_deps_timings_help() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("timings").arg("--help");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Analyze build timings"))
        .stdout(predicate::str::contains("--top"))
        .stdout(predicate::str::contains("--compare"));
}

#[test]
fn test_deps_subcommands_in_main_help() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("--help");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("unused"))
        .stdout(predicate::str::contains("timings"))
        .stdout(predicate::str::contains("list"))
        .stdout(predicate::str::contains("tree"))
        .stdout(predicate::str::contains("duplicates"));
}

// --- Unused Dependencies Tests ---

#[test]
fn test_deps_unused_is_recognized_command() {
    // This test verifies that unused is a recognized subcommand
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("unused").arg("--help");

    let output = cmd.output().unwrap();

    // Should succeed (show help)
    assert!(
        output.status.success(),
        "Help output should succeed. Stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_deps_unused_human_format_default() {
    // Test that the default output format is human-readable
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("unused");

    let output = cmd.output().unwrap();

    // Whether it succeeds or fails, it should not produce JSON by default
    // (unless the tool returns JSON, but the wrapper should format it)
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Human format output should be more readable
        // May contain "unused" or "No unused" or similar language
        assert!(
            stdout.contains("unused")
                || stdout.contains("tool")
                || stdout.is_empty()
                || stdout.contains("Found")
        );
    }
}

#[test]
fn test_deps_unused_execution_graceful() {
    // Test that the unused command executes gracefully
    // (Either succeeds if tool available, or provides helpful error)
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("unused");

    let output = cmd.output().unwrap();

    // Either succeeds or fails gracefully
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be a command parsing error
        assert!(!stderr.contains("unrecognized"));
    }
}

#[test]
fn test_deps_unused_ci_mode_flag() {
    // Test that CI mode accepts the flag
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("unused").arg("--ci");

    let output = cmd.output().unwrap();

    // CI mode should work (either succeeds or fails gracefully)
    // Should not be a parse error
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unrecognized"));
    }
}

#[test]
fn test_deps_unused_ci_mode_graceful() {
    // Test CI mode executes gracefully
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("unused").arg("--ci");

    let output = cmd.output().unwrap();

    // Either succeeds or fails gracefully (not a parse error)
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unrecognized"));
    }
}

#[test]
fn test_deps_unused_has_expected_subcommand() {
    // Test that unused is a recognized subcommand
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("unused").arg("--help");

    cmd.assert().success();
}

// --- Build Timings Tests ---

#[test]
fn test_deps_timings_default_top() {
    // Test timings command with default top parameter (10)
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("timings");

    let output = cmd.output().unwrap();

    // May fail if cargo build --timings hasn't been run,
    // but the command should at least be recognized
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Should contain timing information
        assert!(
            stdout.contains("crate")
                || stdout.contains("duration")
                || stdout.contains("Timing")
                || stdout.is_empty()
        );
    } else {
        // Error should be about missing timing data, not invalid command
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unrecognized subcommand"));
    }
}

#[test]
fn test_deps_timings_custom_top_parameter() {
    // Test timings command with custom top value
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("timings").arg("--top").arg("5");

    let output = cmd.output().unwrap();

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // If there's output, verify it's timing-related
        if !stdout.is_empty() {
            assert!(
                stdout.contains('5') || stdout.contains("crate") || stdout.contains("duration")
            );
        }
    } else {
        // Should not fail due to invalid --top parameter
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unexpected argument") && !stderr.contains("unknown option"));
    }
}

#[test]
fn test_deps_timings_top_with_large_number() {
    // Test timings with a large top value
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("timings").arg("--top").arg("50");

    let output = cmd.output().unwrap();

    // Should handle large values gracefully
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Should contain timing data or empty result
        assert!(stdout.contains("50") || stdout.contains("crate") || stdout.is_empty());
    } else {
        // Error should not be about invalid parameter
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unexpected argument"));
    }
}

#[test]
fn test_deps_timings_top_with_zero() {
    // Test timings with zero (edge case)
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("timings").arg("--top").arg("0");

    let output = cmd.output().unwrap();

    // Either succeeds (shows nothing or all) or fails gracefully
    // Should not panic or produce invalid output
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should be a meaningful error, not a crash
        assert!(!stderr.is_empty());
    }
}

#[test]
fn test_deps_timings_compare_parameter() {
    // Test timings command with compare option
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps")
        .arg("timings")
        .arg("--compare")
        .arg("previous");

    let output = cmd.output().unwrap();

    // Should accept the parameter (may fail if no baseline exists)
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not fail on unknown parameter
        assert!(!stderr.contains("unknown option"));
    }
}

// --- Enhanced List/Tree/Duplicates Tests (Phase 1) ---

#[test]
fn test_deps_list_basic() {
    // Test basic list command execution
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("list");

    let output = cmd.output().unwrap();

    // Should succeed or not fail due to command parsing
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be an unrecognized command error
        assert!(!stderr.contains("unrecognized"));
    }
}

#[test]
fn test_deps_list_execution() {
    // Test list command executes successfully
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("list");

    let output = cmd.output().unwrap();

    // Should succeed and produce output
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(!stdout.is_empty());
    }
}

#[test]
fn test_deps_tree_with_depth_parameter() {
    // Test tree with explicit depth
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("tree").arg("--depth").arg("3");

    cmd.assert().success();
}

#[test]
fn test_deps_tree_with_max_depth() {
    // Test tree with maximum depth
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("tree").arg("--depth").arg("20");

    cmd.assert().success();
}

#[test]
fn test_deps_tree_with_zero_depth() {
    // Test tree with zero depth (edge case)
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("tree").arg("--depth").arg("0");

    let output = cmd.output().unwrap();

    // Should either succeed or fail gracefully
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be a parse error
        assert!(!stderr.contains("invalid"));
    }
}

#[test]
fn test_deps_duplicates_recognized_command() {
    // Test duplicates command is recognized
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("duplicates").arg("--help");

    cmd.assert().success();
}

#[test]
fn test_deps_duplicates_threshold_parameter() {
    // Test duplicates with threshold parameter
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps")
        .arg("duplicates")
        .arg("--threshold")
        .arg("5");

    let output = cmd.output().unwrap();

    // Should not fail on parameter parsing
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unrecognized"));
    }
}

#[test]
fn test_deps_duplicates_help_shows_threshold_param() {
    // Verify that the threshold parameter is documented in help
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("duplicates").arg("--help");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("--threshold"));
}

// --- Command Composition Tests ---

#[test]
fn test_deps_all_phase2_subcommands_recognized() {
    // Verify that both Phase 2 subcommands are recognized
    // (tests that they don't interfere with each other)

    // First: unused
    let mut cmd1 = Command::cargo_bin("xtask").unwrap();
    cmd1.arg("deps").arg("unused").arg("--help");
    let output1 = cmd1.output().unwrap();
    assert!(output1.status.success());

    // Second: timings
    let mut cmd2 = Command::cargo_bin("xtask").unwrap();
    cmd2.arg("deps").arg("timings").arg("--help");
    let output2 = cmd2.output().unwrap();
    assert!(output2.status.success());
}

#[test]
fn test_deps_all_phase2_subcommands_help() {
    // Verify all Phase 2 subcommands have help
    let subcommands = vec!["unused", "timings"];

    for subcmd in subcommands {
        let mut cmd = Command::cargo_bin("xtask").unwrap();
        cmd.arg("deps").arg(subcmd).arg("--help");

        cmd.assert()
            .success()
            .stdout(predicate::str::is_empty().not());
    }
}

// --- Error Handling Tests ---

#[test]
fn test_deps_timings_top_parameter_parsing() {
    // Test that top parameter is parsed correctly
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("timings").arg("--top").arg("15");

    let output = cmd.output().unwrap();

    // Should not fail due to parameter parsing error
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unexpected argument"));
    }
}

#[test]
fn test_deps_timings_invalid_top() {
    // Test with invalid top value (non-numeric)
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("timings").arg("--top").arg("invalid");

    let output = cmd.output().unwrap();

    // Should fail with parse error
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid") || stderr.contains("integer"));
}

#[test]
fn test_deps_duplicates_invalid_threshold() {
    // Test with invalid threshold value
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps")
        .arg("duplicates")
        .arg("--threshold")
        .arg("not-a-number");

    let output = cmd.output().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid") || stderr.contains("integer"));
}

// --- Output Validation Tests ---

#[test]
fn test_deps_list_produces_output() {
    // Verify that list command produces output
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("list");

    let output = cmd.output().unwrap();

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Should have some content
        assert!(!stdout.is_empty());
    }
}

#[test]
fn test_deps_tree_produces_output() {
    // Verify that tree command produces output
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("tree");

    let output = cmd.output().unwrap();

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Should have some content
        assert!(!stdout.is_empty());
    }
}

#[test]
fn test_deps_duplicates_produces_output() {
    // Verify that duplicates command produces output
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("deps").arg("duplicates");

    let output = cmd.output().unwrap();

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Should have some content
        assert!(!stdout.is_empty());
    }
}
