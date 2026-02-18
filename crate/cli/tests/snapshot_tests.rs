//! Snapshot tests for sinexctl output formatting
//!
//! These tests use insta for snapshot management.
//! Run `cargo insta review` to review/accept snapshot changes.

use assert_cmd::cargo;
use xtask::sandbox::sinex_test;
use std::process::Command;

/// Helper to create a sinexctl command
fn sinexctl() -> Command {
    Command::new(cargo::cargo_bin!("sinexctl"))
}

#[sinex_test]
fn snapshot_bash_completions_structure() -> TestResult<()> {
    let output = sinexctl()
        .args(["completions", "bash"])
        .output()
        .expect("Failed to generate bash completions");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify key structural elements exist
    assert!(
        stdout.contains("_sinexctl"),
        "Should define _sinexctl function"
    );
    assert!(
        stdout.contains("complete"),
        "Should have complete directive"
    );
    assert!(
        stdout.contains("query") || stdout.contains("COMPREPLY"),
        "Should reference commands"
    );
    Ok(())
}

#[sinex_test]
fn snapshot_fish_completions_structure() -> TestResult<()> {
    let output = sinexctl()
        .args(["completions", "fish"])
        .output()
        .expect("Failed to generate fish completions");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify key structural elements
    assert!(
        stdout.contains("complete -c sinexctl"),
        "Should have fish complete command"
    );
    Ok(())
}

#[sinex_test]
fn snapshot_zsh_completions_structure() -> TestResult<()> {
    let output = sinexctl()
        .args(["completions", "zsh"])
        .output()
        .expect("Failed to generate zsh completions");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify key structural elements
    assert!(
        stdout.contains("#compdef"),
        "Should have zsh compdef header"
    );
    assert!(stdout.contains("sinexctl"), "Should reference sinexctl");
    Ok(())
}

#[sinex_test]
fn test_config_show_json_is_valid() -> TestResult<()> {
    let output = sinexctl()
        .args(["config", "show", "-f", "json"])
        .output()
        .expect("Failed to run sinexctl config show");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // The output may have additional info after the JSON, so extract just the JSON part
    // JSON ends with a closing brace
    let json_str = if let Some(end_idx) = stdout.rfind('}') {
        &stdout[..=end_idx]
    } else {
        &stdout
    };

    // Verify output is valid JSON
    let json: serde_json::Value =
        serde_json::from_str(json_str).expect("Output should be valid JSON");

    // Verify expected fields exist
    assert!(json.get("rpc_url").is_some(), "Should have rpc_url field");
    assert!(json.get("timeout").is_some(), "Should have timeout field");
    assert!(
        json.get("default_format").is_some(),
        "Should have default_format field"
    );
    Ok(())
}

#[sinex_test]
fn test_config_show_yaml_is_valid() -> TestResult<()> {
    let output = sinexctl()
        .args(["config", "show", "-f", "yaml"])
        .output()
        .expect("Failed to run sinexctl config show -f yaml");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify output contains expected YAML keys
    assert!(stdout.contains("rpc_url:"), "Should have rpc_url in YAML");
    assert!(stdout.contains("timeout:"), "Should have timeout in YAML");
    Ok(())
}
