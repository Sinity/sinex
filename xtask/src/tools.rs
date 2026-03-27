//! External tool detection and management
//!
//! This module handles detection of tools like cargo-audit, cargo-deny,
//! cargo-machete, and provides NixOS-specific installation guidance.

use color_eyre::eyre::{Result, WrapErr, bail};
use std::path::PathBuf;
use std::process::Command;
use which::which;

/// Information about an external tool
#[derive(Debug, Clone)]
pub struct ToolInfo {
    /// Absolute path to the tool binary
    pub path: PathBuf,
    /// Version string (from --version or similar)
    pub version: String,
    /// Additional probe detail when the binary exists but validation was incomplete
    pub probe_issue: Option<String>,
}

/// Manages detection and validation of external tools
pub struct ToolManager;

impl ToolManager {
    /// Check if a tool exists in PATH and get its version
    ///
    /// # Arguments
    /// * `name` - Tool name (e.g., "cargo-audit", "cargo-deny", "cargo")
    ///
    /// # Returns
    /// * `Ok(ToolInfo)` if tool found and version retrieved
    /// * `Err` if tool not found in PATH
    ///
    /// # Example
    /// ```ignore
    /// use xtask::tools::ToolManager;
    /// let info = ToolManager::check_tool("cargo").unwrap();
    /// assert!(info.probe_issue.is_none());
    /// ```
    pub(crate) fn check_tool(name: &str) -> Result<ToolInfo> {
        // Try to find tool in PATH using which crate
        let path = which(name).with_context(|| format!("Tool '{name}' not found in PATH"))?;

        // Get version by running --version
        let (version, probe_issue) = match Self::get_tool_version(name, &path) {
            Ok(version) => (version, None),
            Err(error) => (String::from("unknown"), Some(error.to_string())),
        };

        Ok(ToolInfo {
            path,
            version,
            probe_issue,
        })
    }

    /// Get tool version by running `<tool> --version`
    ///
    /// # Arguments
    /// * `name` - Tool name for error messages
    /// * `path` - Absolute path to tool binary
    ///
    /// # Returns
    /// Version string (first line of --version output)
    fn get_tool_version(name: &str, path: &PathBuf) -> Result<String> {
        let output = Command::new(path)
            .arg("--version")
            .output()
            .with_context(|| format!("Failed to run '{name} --version'"))?;

        if !output.status.success() {
            bail!("'{name}' --version exited with non-zero status");
        }

        let version_output = String::from_utf8_lossy(&output.stdout);
        let version = version_output
            .lines()
            .next()
            .unwrap_or("unknown")
            .trim()
            .to_string();

        Ok(version)
    }

    /// Get installation guidance for a missing tool
    ///
    /// Provides NixOS-specific installation commands for common tools.
    ///
    /// # Arguments
    /// * `name` - Tool name (e.g., "cargo-audit", "graphviz")
    ///
    /// # Returns
    /// Installation command string for NixOS/nix-shell
    ///
    /// # Example
    /// ```ignore
    /// use xtask::tools::ToolManager;
    /// let guidance = ToolManager::install_guidance("cargo-audit");
    /// assert!(guidance.contains("nix"));
    /// ```
    pub(crate) fn install_guidance(name: &str) -> String {
        let nix_package = match name {
            "cargo-audit" => "cargo-audit",
            "cargo-deny" => "cargo-deny",
            "cargo-machete" => "cargo-machete",
            "cargo-udeps" => "cargo-udeps",
            "dot" | "graphviz" => "graphviz",
            "trivy" => "trivy",
            _ => {
                return format!(
                    "No installation guidance available for '{name}'.\n\
                     For NixOS, try: nix-shell -p {name}"
                );
            }
        };

        format!(
            "Install '{name}' with Nix:\n\
             \n\
             Temporary shell:\n  \
               nix-shell -p {nix_package}\n\
             \n\
             Persistent (add to configuration.nix or home-manager):\n  \
               environment.systemPackages = [ pkgs.{nix_package} ];"
        )
    }

    /// Check multiple tools and return missing ones with guidance
    ///
    /// Validates a list of required tools and provides installation guidance
    /// for any that are missing from the system PATH.
    ///
    /// # Arguments
    /// * `tools` - Slice of tool names to check
    ///
    /// # Returns
    /// Vector of (`tool_name`, `installation_guidance`) for missing tools.
    /// Empty vector means all tools are available.
    ///
    /// # Example
    /// ```ignore
    /// use xtask::tools::ToolManager;
    /// let missing = ToolManager::check_required_tools(&["cargo", "nonexistent"]).unwrap();
    /// assert_eq!(missing.len(), 1);
    /// assert_eq!(missing[0].0, "nonexistent");
    /// ```
    pub(crate) fn check_required_tools(tools: &[&str]) -> Vec<(String, String)> {
        let mut missing = Vec::new();

        for tool in tools {
            if Self::check_tool(tool).is_err() {
                let guidance = Self::install_guidance(tool);
                missing.push((tool.to_string(), guidance));
            }
        }

        missing
    }
}

#[cfg(test)]
mod tests {
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
}
