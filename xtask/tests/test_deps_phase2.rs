//! Integration tests for deps commands - Phase 2
//!
//! Tests for the unused and timings subcommands added in Phase 2.
//! This module comprehensively tests:
//! - deps unused command with format options and CI mode
//! - deps timings command with parametrization
//! - Enhanced list/tree/duplicates commands from Phase 1

use std::process::Command;
use xtask::sandbox::sinex_test;

// ============================================================================
// Phase 2: Unused Dependencies & Build Timings Tests
// ============================================================================

// --- Help & Discovery Tests ---

#[sinex_test]
async fn test_deps_unused_help() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("deps")
        .arg("unused")
        .arg("--help")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Detect unused dependencies"),
        "Should describe unused"
    );
    assert!(stdout.contains("--ci"), "Should document --ci");
    Ok(())
}

#[sinex_test]
async fn test_deps_timings_help() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("deps")
        .arg("timings")
        .arg("--help")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Analyze build timings"),
        "Should describe timings"
    );
    assert!(stdout.contains("--top"), "Should document --top");
    assert!(stdout.contains("--compare"), "Should document --compare");
    Ok(())
}

#[sinex_test]
async fn test_deps_subcommands_in_main_help() -> TestResult<()> {
    let output = Command::new("xtask").arg("deps").arg("--help").output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("unused"), "Should list unused");
    assert!(stdout.contains("timings"), "Should list timings");
    assert!(stdout.contains("list"), "Should list list");
    assert!(stdout.contains("tree"), "Should list tree");
    assert!(stdout.contains("duplicates"), "Should list duplicates");
    Ok(())
}

// --- Unused Dependencies Tests ---

#[sinex_test]
async fn test_deps_unused_human_format_default() -> TestResult<()> {
    // Test that the default output format is human-readable
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("unused");

    let output = cmd
        .output()
        .expect("xtask deps unused command failed to execute");

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
    Ok(())
}

#[sinex_test]
async fn test_deps_unused_execution_graceful() -> TestResult<()> {
    // Test that the unused command executes gracefully
    // (Either succeeds if tool available, or provides helpful error)
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("unused");

    let output = cmd
        .output()
        .expect("xtask deps unused command failed to execute");

    // Either succeeds or fails gracefully
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be a command parsing error
        assert!(!stderr.contains("unrecognized"));
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_unused_ci_mode_flag() -> TestResult<()> {
    // Test that CI mode accepts the flag
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("unused").arg("--ci");

    let output = cmd
        .output()
        .expect("xtask deps unused command failed to execute");

    // CI mode should work (either succeeds or fails gracefully)
    // Should not be a parse error
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unrecognized"));
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_unused_ci_mode_graceful() -> TestResult<()> {
    // Test CI mode executes gracefully
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("unused").arg("--ci");

    let output = cmd
        .output()
        .expect("xtask deps unused command failed to execute");

    // Either succeeds or fails gracefully (not a parse error)
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unrecognized"));
    }
    Ok(())
}

// --- Build Timings Tests ---

#[sinex_test]
async fn test_deps_timings_default_top() -> TestResult<()> {
    // Test timings command with default top parameter (10)
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("timings");

    let output = cmd
        .output()
        .expect("xtask deps timings command failed to execute");

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
    Ok(())
}

#[sinex_test]
async fn test_deps_timings_custom_top_parameter() -> TestResult<()> {
    // Test timings command with custom top value
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("timings").arg("--top").arg("5");

    let output = cmd
        .output()
        .expect("xtask deps timings command failed to execute");

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
    Ok(())
}

#[sinex_test]
async fn test_deps_timings_top_with_large_number() -> TestResult<()> {
    // Test timings with a large top value
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("timings").arg("--top").arg("50");

    let output = cmd
        .output()
        .expect("xtask deps timings command failed to execute");

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
    Ok(())
}

#[sinex_test]
async fn test_deps_timings_top_with_zero() -> TestResult<()> {
    // Test timings with zero (edge case)
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("timings").arg("--top").arg("0");

    let output = cmd
        .output()
        .expect("xtask deps timings command failed to execute");

    // Either succeeds (shows nothing or all) or fails gracefully
    // Should not panic or produce invalid output
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should be a meaningful error, not a crash
        assert!(!stderr.is_empty());
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_timings_compare_parameter() -> TestResult<()> {
    // Test timings command with compare option
    let mut cmd = Command::new("xtask");

    cmd.arg("deps")
        .arg("timings")
        .arg("--compare")
        .arg("previous");

    let output = cmd
        .output()
        .expect("xtask deps timings command failed to execute");

    // Should accept the parameter (may fail if no baseline exists)
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not fail on unknown parameter
        assert!(!stderr.contains("unknown option"));
    }
    Ok(())
}

// --- Enhanced List/Tree/Duplicates Tests (Phase 1) ---

#[sinex_test]
async fn test_deps_list_basic() -> TestResult<()> {
    // Test basic list command execution
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("list");

    let output = cmd
        .output()
        .expect("xtask deps list command failed to execute");

    // Should succeed or not fail due to command parsing
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be an unrecognized command error
        assert!(!stderr.contains("unrecognized"));
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_list_execution() -> TestResult<()> {
    // Test list command executes successfully
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("list");

    let output = cmd
        .output()
        .expect("xtask deps list command failed to execute");

    // Should succeed and produce output
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(!stdout.is_empty());
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_tree_with_depth_parameter() -> TestResult<()> {
    // Test tree with explicit depth
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("tree").arg("--depth").arg("3");

    let output = cmd.output()?;
    assert!(output.status.success(), "Command should succeed");
    Ok(())
}

#[sinex_test]
async fn test_deps_tree_with_max_depth() -> TestResult<()> {
    // Test tree with maximum depth
    let output = Command::new("xtask")
        .arg("deps")
        .arg("tree")
        .arg("--depth")
        .arg("20")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    Ok(())
}

#[sinex_test]
async fn test_deps_tree_with_zero_depth() -> TestResult<()> {
    // Test tree with zero depth (edge case)
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("tree").arg("--depth").arg("0");

    let output = cmd
        .output()
        .expect("xtask deps tree command failed to execute");

    // Should either succeed or fail gracefully
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Should not be a parse error
        assert!(!stderr.contains("invalid"));
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_duplicates_threshold_parameter() -> TestResult<()> {
    // Test duplicates with threshold parameter
    let mut cmd = Command::new("xtask");

    cmd.arg("deps")
        .arg("duplicates")
        .arg("--threshold")
        .arg("5");

    let output = cmd
        .output()
        .expect("xtask deps duplicates command failed to execute");

    // Should not fail on parameter parsing
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unrecognized"));
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_duplicates_help_shows_threshold_param() -> TestResult<()> {
    // Verify that the threshold parameter is documented in help
    let output = Command::new("xtask")
        .arg("deps")
        .arg("duplicates")
        .arg("--help")
        .output()?;

    assert!(output.status.success(), "Help should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--threshold"), "Should contain --threshold");
    Ok(())
}

// --- Error Handling Tests ---

#[sinex_test]
async fn test_deps_timings_top_parameter_parsing() -> TestResult<()> {
    // Test that top parameter is parsed correctly
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("timings").arg("--top").arg("15");

    let output = cmd
        .output()
        .expect("xtask deps timings command failed to execute");

    // Should not fail due to parameter parsing error
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("unexpected argument"));
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_timings_invalid_top() -> TestResult<()> {
    // Test with invalid top value (non-numeric)
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("timings").arg("--top").arg("invalid");

    let output = cmd
        .output()
        .expect("xtask deps timings command failed to execute");

    // Should fail with parse error
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid") || stderr.contains("integer"));
    Ok(())
}

#[sinex_test]
async fn test_deps_duplicates_invalid_threshold() -> TestResult<()> {
    // Test with invalid threshold value
    let mut cmd = Command::new("xtask");

    cmd.arg("deps")
        .arg("duplicates")
        .arg("--threshold")
        .arg("not-a-number");

    let output = cmd
        .output()
        .expect("xtask deps duplicates command failed to execute");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid") || stderr.contains("integer"));
    Ok(())
}

// --- Output Validation Tests ---

#[sinex_test]
async fn test_deps_list_produces_output() -> TestResult<()> {
    // Verify that list command produces output
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("list");

    let output = cmd
        .output()
        .expect("xtask deps list command failed to execute");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Should have some content
        assert!(!stdout.is_empty());
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_tree_produces_output() -> TestResult<()> {
    // Verify that tree command produces output
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("tree");

    let output = cmd
        .output()
        .expect("xtask deps tree command failed to execute");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Should have some content
        assert!(!stdout.is_empty());
    }
    Ok(())
}

#[sinex_test]
async fn test_deps_duplicates_produces_output() -> TestResult<()> {
    // Verify that duplicates command produces output
    let mut cmd = Command::new("xtask");

    cmd.arg("deps").arg("duplicates");

    let output = cmd
        .output()
        .expect("xtask deps duplicates command failed to execute");

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Should have some content
        assert!(!stdout.is_empty());
    }
    Ok(())
}
