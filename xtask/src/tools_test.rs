use super::*;
use crate::sandbox::sinex_test;
use ::xtask::sandbox::EnvGuard;
use std::fs;
use std::os::unix::fs::PermissionsExt;

fn write_executable_script(
    path: &std::path::Path,
    body: &str,
) -> ::xtask::sandbox::TestResult<()> {
    fs::write(path, body)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[sinex_test]
async fn test_unavailable_tool_info() -> TestResult<()> {
    let info = ToolInfo {
        path: PathBuf::from("missing-tool"),
        version: String::from("not found"),
        probe_issue: None,
    };
    assert_eq!(info.version, "not found");
    assert_eq!(info.path, PathBuf::from("missing-tool"));
    assert!(info.probe_issue.is_none());
    Ok(())
}

#[sinex_test]
async fn test_check_tool_exists() -> TestResult<()> {
    // Use a tool that's guaranteed to exist (cargo)
    let result = ToolManager::check_tool("cargo");
    assert!(result.is_ok());
    let info = result?;
    assert!(info.path.exists());
    assert!(!info.version.is_empty());
    assert!(info.probe_issue.is_none());
    Ok(())
}

#[sinex_test]
async fn test_check_tool_not_exists() -> TestResult<()> {
    // Use a tool that definitely doesn't exist
    let result = ToolManager::check_tool("nonexistent-tool-xyz-12345");
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_get_tool_version_success() -> TestResult<()> {
    // Test get_tool_version with cargo which should succeed
    let result = ToolManager::check_tool("cargo");
    assert!(result.is_ok());
    let info = result?;
    assert!(!info.version.is_empty());
    assert!(!info.version.contains("unknown"));
    assert!(info.probe_issue.is_none());
    Ok(())
}

#[sinex_test]
async fn test_check_tool_preserves_version_probe_failures() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let tool_path = temp.path().join("broken-tool");
    write_executable_script(&tool_path, "#!/bin/sh\nexit 7\n")?;

    let original_path = std::env::var("PATH").unwrap_or_default();
    let combined_path = format!("{}:{original_path}", temp.path().display());
    let mut env = EnvGuard::new();
    env.set("PATH", combined_path);

    let info = ToolManager::check_tool("broken-tool")?;
    assert_eq!(info.version, "unknown");
    assert!(
        info.probe_issue
            .as_deref()
            .is_some_and(|message| message.contains("--version"))
    );
    Ok(())
}

#[sinex_test]
async fn test_install_guidance_known_tool() -> TestResult<()> {
    let guidance = ToolManager::install_guidance("cargo-audit");
    assert!(guidance.contains("cargo-audit"));
    assert!(guidance.contains("nix-shell"));
    assert!(guidance.contains("configuration.nix"));
    Ok(())
}

#[sinex_test]
async fn test_install_guidance_unknown_tool() -> TestResult<()> {
    let guidance = ToolManager::install_guidance("unknown-tool-xyz");
    assert!(guidance.contains("No installation guidance"));
    assert!(guidance.contains("nix-shell -p unknown-tool-xyz"));
    Ok(())
}

#[sinex_test]
async fn test_install_guidance_graphviz() -> TestResult<()> {
    let guidance = ToolManager::install_guidance("dot");
    assert!(guidance.contains("graphviz"));
    assert!(guidance.contains("nix-shell"));
    Ok(())
}

#[sinex_test]
async fn test_check_required_tools_all_present() -> TestResult<()> {
    // Test with a tool we know exists
    let missing = ToolManager::check_required_tools(&["cargo"]);
    assert!(missing.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_check_required_tools_some_missing() -> TestResult<()> {
    // Test with mix of existing and non-existing tools
    let missing = ToolManager::check_required_tools(&["cargo", "nonexistent-tool-xyz-12345"]);

    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].0, "nonexistent-tool-xyz-12345");
    assert!(missing[0].1.contains("nix-shell"));
    Ok(())
}

#[sinex_test]
async fn test_check_required_tools_empty_list() -> TestResult<()> {
    let missing = ToolManager::check_required_tools(&[]);
    assert!(missing.is_empty());
    Ok(())
}
