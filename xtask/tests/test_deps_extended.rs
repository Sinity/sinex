//! Focused behavior tests for dependency-analysis commands.

mod support;

use clap::Parser;
use support::xtask_command;
use xtask::Cli;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_deps_tree_zero_depth_reports_truncation() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
        .arg("deps")
        .arg("tree")
        .arg("--depth")
        .arg("0")
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "deps tree --depth 0 failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let tree = parsed["data"]["tree"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("deps tree JSON data.tree should be a string"))?;

    assert!(
        tree.contains("xtask"),
        "tree should include workspace packages"
    );
    assert!(
        tree.contains("(max depth)"),
        "tree should make depth truncation visible"
    );
    assert_eq!(parsed["data"]["depth"].as_u64(), Some(0));
    Ok(())
}

#[sinex_test]
async fn test_deps_duplicates_threshold_filters_report() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
        .arg("deps")
        .arg("duplicates")
        .arg("--threshold")
        .arg("1000")
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "deps duplicates --threshold 1000 failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let data = parsed["data"]
        .as_object()
        .ok_or_else(|| color_eyre::eyre::eyre!("deps duplicates JSON data should be an object"))?;
    assert_eq!(data.get("threshold"), Some(&serde_json::json!(1000)));
    assert_eq!(data.get("count"), Some(&serde_json::json!(0)));
    let duplicates = data
        .get("duplicates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| color_eyre::eyre::eyre!("data.duplicates should be an array"))?;
    assert!(
        duplicates.is_empty(),
        "high threshold should filter every duplicate from the structured report"
    );
    Ok(())
}

#[sinex_test]
async fn test_deps_duplicates_json_includes_version_roots() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
        .arg("deps")
        .arg("duplicates")
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "deps duplicates --json failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let duplicates = parsed["data"]["duplicates"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("data.duplicates should be an array"))?;
    let duplicate = duplicates
        .iter()
        .find(|duplicate| duplicate["version_details"].is_array())
        .ok_or_else(|| color_eyre::eyre::eyre!("expected at least one duplicate detail"))?;
    let details = duplicate["version_details"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("version_details should be an array"))?;

    assert!(
        details.len() >= 2,
        "duplicate version details should include each duplicate version"
    );
    assert!(
        details.iter().any(|detail| detail["workspace_roots"]
            .as_array()
            .is_some_and(|roots| !roots.is_empty())),
        "at least one duplicate version should be reachable from a workspace root"
    );
    Ok(())
}

#[sinex_test]
async fn test_deps_tree_omits_disabled_optional_backend() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
        .arg("deps")
        .arg("tree")
        .arg("--package")
        .arg("sinexctl")
        .arg("--depth")
        .arg("4")
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "deps tree for sinexctl failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let tree = parsed["data"]["tree"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("deps tree JSON data.tree should be a string"))?;

    assert!(
        !tree.contains("ratatui-termwiz"),
        "tree should not report ratatui's disabled termwiz backend as an active dependency"
    );
    assert!(
        !tree.contains("termwiz"),
        "tree should not report the disabled termwiz backend closure"
    );
    Ok(())
}

#[sinex_test]
async fn test_deps_duplicates_ignore_inactive_optional_versions() -> ::xtask::sandbox::TestResult<()>
{
    let output = xtask_command()?
        .arg("deps")
        .arg("duplicates")
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "deps duplicates --json failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let duplicates = parsed["data"]["duplicates"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("data.duplicates should be an array"))?;
    let names = duplicates
        .iter()
        .filter_map(|duplicate| duplicate["name"].as_str())
        .collect::<Vec<_>>();

    for inactive_name in ["bit-set", "bit-vec", "fixedbitset"] {
        assert!(
            !names.contains(&inactive_name),
            "inactive optional dependency '{inactive_name}' should not be reported as duplicate debt"
        );
    }
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
    let Err(error) =
        Cli::try_parse_from(["xtask", "deps", "duplicates", "--threshold", "not-a-number"])
    else {
        return Err(color_eyre::eyre::eyre!(
            "invalid --threshold should fail during clap parsing"
        ));
    };
    let rendered = error.to_string();
    assert!(rendered.contains("invalid") || rendered.contains("integer"));
    Ok(())
}
