//! Extended integration tests for deps commands.
//!
//! Covers:
//! - `deps unused` format options and CI mode
//! - `deps timings` parametrization
//! - broader `deps list/tree/duplicates` behavior

mod support;

use clap::Parser;
use support::xtask_command;
use xtask::Cli;
use xtask::sandbox::sinex_test;

// --- Help & Discovery Tests ---

#[sinex_test]
async fn test_deps_unused_help() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
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
async fn test_deps_timings_help() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
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
async fn test_deps_subcommands_in_main_help() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?.arg("deps").arg("--help").output()?;

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
async fn test_deps_unused_human_format_default() -> ::xtask::sandbox::TestResult<()> {
    // Test that the default output format is human-readable
    let mut cmd = xtask_command()?;

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
async fn test_deps_unused_execution_graceful() -> ::xtask::sandbox::TestResult<()> {
    // Test that the unused command executes gracefully
    // (Either succeeds if tool available, or provides helpful error)
    let mut cmd = xtask_command()?;

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
async fn test_deps_unused_ci_mode_flag() -> ::xtask::sandbox::TestResult<()> {
    // Test that CI mode accepts the flag
    let mut cmd = xtask_command()?;

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

// --- Additional List/Tree/Duplicates Tests ---

#[sinex_test]
async fn test_deps_list_basic() -> ::xtask::sandbox::TestResult<()> {
    // Test basic list command execution
    let output = xtask_command()?
        .arg("deps")
        .arg("list")
        .output()
        .expect("xtask deps list command failed to execute");

    assert!(output.status.success(), "deps list should succeed");
    Ok(())
}

#[sinex_test]
async fn test_deps_tree_with_depth_parameter() -> ::xtask::sandbox::TestResult<()> {
    // Test tree with explicit depth
    let mut cmd = xtask_command()?;

    cmd.arg("deps").arg("tree").arg("--depth").arg("3");

    let output = cmd.output()?;
    assert!(output.status.success(), "Command should succeed");
    Ok(())
}

#[sinex_test]
async fn test_deps_tree_with_max_depth() -> ::xtask::sandbox::TestResult<()> {
    // Test tree with maximum depth
    let output = xtask_command()?
        .arg("deps")
        .arg("tree")
        .arg("--depth")
        .arg("20")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    Ok(())
}

#[sinex_test]
async fn test_deps_tree_with_zero_depth() -> ::xtask::sandbox::TestResult<()> {
    // Test tree with zero depth (edge case)
    let mut cmd = xtask_command()?;

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
async fn test_deps_duplicates_threshold_parameter() -> ::xtask::sandbox::TestResult<()> {
    // Test duplicates with threshold parameter
    let mut cmd = xtask_command()?;

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
async fn test_deps_duplicates_help_shows_threshold_param() -> ::xtask::sandbox::TestResult<()> {
    // Verify that the threshold parameter is documented in help
    let output = xtask_command()?
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
async fn test_deps_timings_top_parameter_parsing() -> ::xtask::sandbox::TestResult<()> {
    Cli::try_parse_from(["xtask", "deps", "timings", "--top", "15"])?;
    Ok(())
}

#[sinex_test]
async fn test_deps_timings_invalid_top() -> ::xtask::sandbox::TestResult<()> {
    let Err(error) = Cli::try_parse_from(["xtask", "deps", "timings", "--top", "invalid"]) else {
        return Err(color_eyre::eyre::eyre!(
            "invalid --top should fail during clap parsing"
        ));
    };
    let rendered = error.to_string();
    assert!(rendered.contains("invalid") || rendered.contains("integer"));
    Ok(())
}

#[sinex_test]
async fn test_deps_duplicates_invalid_threshold() -> ::xtask::sandbox::TestResult<()> {
    // Test with invalid threshold value
    let mut cmd = xtask_command()?;

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
