//! Snapshot tests for sinexctl output formatting
//!
//! These tests use insta for snapshot management.
//! Run `cargo insta review` to review/accept snapshot changes.

use assert_cmd::cargo;
use std::process::Command;
use xtask::sandbox::sinex_test;

/// Helper to create a sinexctl command
fn sinexctl() -> Command {
    Command::new(cargo::cargo_bin!("sinexctl"))
}

#[sinex_test]
async fn snapshot_structured_completion_shape() -> TestResult<()> {
    let output = sinexctl()
        .args([
            "_complete",
            "--line",
            "sinexctl events source:wm",
            "--cursor",
            "24",
            "--format",
            "json",
        ])
        .output()
        .expect("Failed to run structured completion endpoint");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"candidates\""),
        "Should return candidates"
    );
    assert!(
        stdout.contains("source:wm.hyprland"),
        "Should complete sources"
    );
    Ok(())
}

#[sinex_test]
async fn test_config_show_json_is_valid() -> TestResult<()> {
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
async fn test_config_show_yaml_is_valid() -> TestResult<()> {
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
