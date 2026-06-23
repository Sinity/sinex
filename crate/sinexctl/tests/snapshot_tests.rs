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
    let line = "sinexctl events source:wm";
    let cursor = line.len().to_string();
    let output = sinexctl()
        .args([
            "_complete",
            "--line",
            line,
            "--cursor",
            cursor.as_str(),
            "--format",
            "json",
        ])
        .output()
        .expect("Failed to run structured completion endpoint");

    assert!(
        output.status.success(),
        "_complete json should succeed; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let response: serde_json::Value = serde_json::from_slice(&output.stdout)
        .expect("_complete --format json must emit a single JSON document");
    assert_eq!(response["schema_version"], 1);
    assert_eq!(response["line"], line);
    assert_eq!(response["cursor"], line.len());
    assert_eq!(response["active_token"], "source:wm");

    let candidates = response["candidates"]
        .as_array()
        .expect("completion response must contain an array of candidates");
    let hyprland = candidates
        .iter()
        .find(|candidate| candidate["value"] == "source:wm.hyprland")
        .expect("structured completion should include wm.hyprland source candidate");

    for field in [
        "value",
        "insert",
        "replace_start",
        "replace_end",
        "display",
        "kind",
        "group",
        "description",
        "stale",
        "danger",
        "privacy",
        "score",
    ] {
        assert!(
            hyprland.get(field).is_some(),
            "completion candidate must expose stable `{field}` field: {hyprland}"
        );
    }
    assert_eq!(hyprland["stale"], true);
    assert_eq!(
        hyprland["privacy"],
        "schema-key-only; payload values not sampled"
    );
    Ok(())
}

#[sinex_test]
async fn test_config_show_json_is_valid() -> TestResult<()> {
    let output = sinexctl()
        .args(["config", "show", "-f", "json"])
        .output()
        .expect("Failed to run sinexctl config show");

    assert!(
        output.status.success(),
        "config show -f json should succeed; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim())
        .expect("config show -f json must emit one JSON document with no trailing prose");

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
