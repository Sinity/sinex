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
    let tree = parsed["data"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("deps tree JSON data should be a string"))?;

    assert!(
        tree.contains("xtask"),
        "tree should include workspace packages"
    );
    assert!(
        tree.contains("(max depth)"),
        "tree should make depth truncation visible"
    );
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
