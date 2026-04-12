//! Integration tests for deps commands

mod support;

use color_eyre::eyre::Result;
use support::xtask_command;

#[test]
fn test_deps_list_non_tty() -> Result<()> {
    // Tests run in non-TTY → JSON is the natural output. Verify JSON structure.
    let output = xtask_command()?.arg("deps").arg("list").output()?;

    assert!(
        output.status.success(),
        "deps list failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("deps list should produce valid JSON in non-TTY");
    assert!(
        parsed["data"]["count"].as_i64().unwrap_or(0) > 0,
        "JSON data.count should be positive"
    );
    assert!(
        parsed["data"]["packages"]
            .as_array()
            .is_some_and(|a| a.iter().any(|p| p["name"].as_str() == Some("xtask"))),
        "JSON data.packages should include 'xtask'"
    );
    Ok(())
}

#[test]
fn test_deps_list_json() -> Result<()> {
    let output = xtask_command()?
        .arg("deps")
        .arg("list")
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "deps list --json failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("deps list --json should output valid JSON");
    assert!(
        parsed["data"]["packages"].is_array(),
        "JSON data.packages should be an array"
    );
    assert!(
        parsed["data"]["count"].as_u64().unwrap_or(0) > 0,
        "JSON data.count should be non-zero"
    );
    Ok(())
}

#[test]
fn test_deps_tree_no_package() -> Result<()> {
    let output = xtask_command()?.arg("deps").arg("tree").output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("deps tree should produce valid JSON in non-TTY");
    let tree = parsed["data"]
        .as_str()
        .expect("deps tree JSON should expose rendered tree in data");
    assert!(
        tree.contains("sinex-workspace"),
        "workspace tree should contain the synthetic workspace root"
    );
    assert!(
        tree.contains("xtask"),
        "workspace tree should contain xtask"
    );
    Ok(())
}

#[test]
fn test_deps_tree_with_valid_package() -> Result<()> {
    let output = xtask_command()?
        .arg("deps")
        .arg("tree")
        .arg("--package")
        .arg("xtask")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .expect("deps tree --package should produce valid JSON in non-TTY");
    let tree = parsed["data"]
        .as_str()
        .expect("deps tree --package JSON should expose rendered tree in data");
    assert!(
        tree.contains("xtask"),
        "package tree should include the requested package"
    );
    Ok(())
}

#[test]
fn test_deps_tree_with_invalid_package() -> Result<()> {
    let output = xtask_command()?
        .arg("deps")
        .arg("tree")
        .arg("--package")
        .arg("nonexistent-package-xyz")
        .output()?;

    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found in workspace"),
        "Should indicate package not found"
    );
    assert!(
        stderr.contains("Available packages"),
        "Should list available packages"
    );
    Ok(())
}

#[test]
fn test_deps_duplicates_default() -> Result<()> {
    let output = xtask_command()?.arg("deps").arg("duplicates").output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("duplicate"), "Should mention duplicates");
    Ok(())
}

#[test]
fn test_deps_duplicates_custom_threshold() -> Result<()> {
    let output = xtask_command()?
        .arg("deps")
        .arg("duplicates")
        .arg("--threshold")
        .arg("5")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("duplicate"), "Should mention duplicates");
    Ok(())
}
