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
#[path = "tools_test.rs"]
mod tests;
