//! External tool detection and management
//!
//! This module handles detection of tools like cargo-audit, cargo-deny,
//! cargo-machete, and provides NixOS-specific installation guidance.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;
use which::which;

/// Information about an external tool
#[derive(Debug, Clone)]
pub struct ToolInfo {
    /// Absolute path to the tool binary
    #[allow(dead_code)]
    pub path: PathBuf,
    /// Version string (from --version or similar)
    #[allow(dead_code)]
    pub version: String,
    /// Whether the tool is available and functional
    #[allow(dead_code)]
    pub is_available: bool,
}

impl ToolInfo {
    /// Create a new ToolInfo for an unavailable tool
    #[allow(dead_code)]
    pub fn unavailable(name: &str) -> Self {
        Self {
            path: PathBuf::from(name),
            version: String::from("not found"),
            is_available: false,
        }
    }
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
    /// ```no_run
    /// use xtask::tools::ToolManager;
    /// let info = ToolManager::check_tool("cargo").unwrap();
    /// assert!(info.is_available);
    /// ```
    pub fn check_tool(name: &str) -> Result<ToolInfo> {
        // Try to find tool in PATH using which crate
        let path = which(name).with_context(|| format!("Tool '{}' not found in PATH", name))?;

        // Get version by running --version
        let version =
            Self::get_tool_version(name, &path).unwrap_or_else(|_| String::from("unknown"));

        Ok(ToolInfo {
            path,
            version,
            is_available: true,
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
            .with_context(|| format!("Failed to run '{} --version'", name))?;

        if !output.status.success() {
            anyhow::bail!("'{}' --version exited with non-zero status", name);
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
    /// ```
    /// use xtask::tools::ToolManager;
    /// let guidance = ToolManager::install_guidance("cargo-audit");
    /// assert!(guidance.contains("nix"));
    /// ```
    pub fn install_guidance(name: &str) -> String {
        let nix_package = match name {
            "cargo-audit" => "cargo-audit",
            "cargo-deny" => "cargo-deny",
            "cargo-machete" => "cargo-machete",
            "cargo-udeps" => "cargo-udeps",
            "dot" | "graphviz" => "graphviz",
            "trivy" => "trivy",
            _ => {
                return format!(
                    "No installation guidance available for '{}'.\n\
                     For NixOS, try: nix-shell -p {}",
                    name, name
                );
            }
        };

        format!(
            "Install '{}' with Nix:\n\
             \n\
             Temporary shell:\n  \
               nix-shell -p {}\n\
             \n\
             Persistent (add to configuration.nix or home-manager):\n  \
               environment.systemPackages = [ pkgs.{} ];",
            name, nix_package, nix_package
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
    /// Vector of (tool_name, installation_guidance) for missing tools.
    /// Empty vector means all tools are available.
    ///
    /// # Example
    /// ```no_run
    /// use xtask::tools::ToolManager;
    /// let missing = ToolManager::check_required_tools(&["cargo", "nonexistent"]).unwrap();
    /// assert_eq!(missing.len(), 1);
    /// assert_eq!(missing[0].0, "nonexistent");
    /// ```
    #[allow(dead_code)]
    pub fn check_required_tools(tools: &[&str]) -> Result<Vec<(String, String)>> {
        let mut missing = Vec::new();

        for tool in tools {
            if Self::check_tool(tool).is_err() {
                let guidance = Self::install_guidance(tool);
                missing.push((tool.to_string(), guidance));
            }
        }

        Ok(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unavailable_tool_info() {
        let info = ToolInfo::unavailable("missing-tool");
        assert!(!info.is_available);
        assert_eq!(info.version, "not found");
        assert_eq!(info.path, PathBuf::from("missing-tool"));
    }

    #[test]
    fn test_check_tool_exists() {
        // Use a tool that's guaranteed to exist (cargo)
        let result = ToolManager::check_tool("cargo");
        assert!(result.is_ok());
        let info = result.unwrap();
        assert!(info.is_available);
        assert!(info.path.exists());
        assert!(!info.version.is_empty());
    }

    #[test]
    fn test_check_tool_not_exists() {
        // Use a tool that definitely doesn't exist
        let result = ToolManager::check_tool("nonexistent-tool-xyz-12345");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_tool_version_success() {
        // Test get_tool_version with cargo which should succeed
        let result = ToolManager::check_tool("cargo");
        assert!(result.is_ok());
        let info = result.unwrap();
        assert!(!info.version.is_empty());
        assert!(!info.version.contains("unknown"));
    }

    #[test]
    fn test_install_guidance_known_tool() {
        let guidance = ToolManager::install_guidance("cargo-audit");
        assert!(guidance.contains("cargo-audit"));
        assert!(guidance.contains("nix-shell"));
        assert!(guidance.contains("configuration.nix"));
    }

    #[test]
    fn test_install_guidance_unknown_tool() {
        let guidance = ToolManager::install_guidance("unknown-tool-xyz");
        assert!(guidance.contains("No installation guidance"));
        assert!(guidance.contains("nix-shell -p unknown-tool-xyz"));
    }

    #[test]
    fn test_install_guidance_graphviz() {
        let guidance = ToolManager::install_guidance("dot");
        assert!(guidance.contains("graphviz"));
        assert!(guidance.contains("nix-shell"));
    }

    #[test]
    fn test_check_required_tools_all_present() {
        // Test with a tool we know exists
        let missing = ToolManager::check_required_tools(&["cargo"]).unwrap();
        assert!(missing.is_empty());
    }

    #[test]
    fn test_check_required_tools_some_missing() {
        // Test with mix of existing and non-existing tools
        let missing =
            ToolManager::check_required_tools(&["cargo", "nonexistent-tool-xyz-12345"]).unwrap();

        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].0, "nonexistent-tool-xyz-12345");
        assert!(missing[0].1.contains("nix-shell"));
    }

    #[test]
    fn test_check_required_tools_empty_list() {
        let missing = ToolManager::check_required_tools(&[]).unwrap();
        assert!(missing.is_empty());
    }
}
