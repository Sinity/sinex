//! Unused dependency detection
//!
//! Integrates with cargo-machete or cargo-udeps to find unused dependencies.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::process::Command;

use crate::tools::ToolManager;

/// Result of unused dependency detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnusedReport {
    /// List of unused dependencies
    pub unused: Vec<UnusedDependency>,
    /// Tool used for detection
    pub tool: String,
}

/// An unused dependency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnusedDependency {
    /// Package that has the unused dependency
    pub package: String,
    /// Name of the unused dependency
    pub dependency: String,
}

/// Detector for unused dependencies
pub struct UnusedDetector;

impl UnusedDetector {
    /// Detect unused dependencies using available tool
    ///
    /// Tries cargo-machete first (faster), falls back to cargo-udeps.
    /// Returns error if neither tool is available.
    ///
    /// # Returns
    /// UnusedReport with list of unused dependencies and tool name
    ///
    /// # Errors
    /// Returns error if:
    /// - No detection tool is available
    /// - Tool execution fails
    /// - Output parsing fails
    ///
    /// # Example
    /// ```no_run
    /// use xtask::deps::UnusedDetector;
    /// let report = UnusedDetector::detect()?;
    /// println!("Found {} unused dependencies", report.unused.len());
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn detect() -> Result<UnusedReport> {
        // Try cargo-machete first (faster, simpler)
        if ToolManager::check_tool("cargo-machete").is_ok() {
            return Self::detect_with_machete();
        }

        // Fall back to cargo-udeps (requires nightly)
        if ToolManager::check_tool("cargo-udeps").is_ok() {
            return Self::detect_with_udeps();
        }

        // Neither tool available - provide installation guidance
        anyhow::bail!(
            "No unused dependency detection tool available.\n\n{}\n\nAlternatively:\n{}",
            ToolManager::install_guidance("cargo-machete"),
            ToolManager::install_guidance("cargo-udeps")
        )
    }

    /// Detect using cargo-machete
    ///
    /// Runs `cargo machete --format json` and parses output.
    ///
    /// # Returns
    /// UnusedReport with unused dependencies found by cargo-machete
    ///
    /// # Errors
    /// Returns error if cargo-machete execution fails or output parsing fails
    fn detect_with_machete() -> Result<UnusedReport> {
        // Run cargo machete with JSON output
        let output = Command::new("cargo")
            .arg("machete")
            .arg("--format")
            .arg("json")
            .output()
            .context("Failed to execute cargo-machete")?;

        // Check if command succeeded
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("cargo-machete failed: {}", stderr);
        }

        // Parse JSON output
        let stdout = String::from_utf8_lossy(&output.stdout);
        Self::parse_machete_output(&stdout)
    }

    /// Detect using cargo-udeps
    ///
    /// Runs `cargo +nightly udeps --output json` and parses output.
    /// Requires nightly toolchain.
    ///
    /// # Returns
    /// UnusedReport with unused dependencies found by cargo-udeps
    ///
    /// # Errors
    /// Returns error if:
    /// - Nightly toolchain not installed
    /// - cargo-udeps execution fails
    /// - Output parsing fails
    fn detect_with_udeps() -> Result<UnusedReport> {
        // Run cargo udeps with JSON output (requires nightly)
        let output = Command::new("cargo")
            .arg("+nightly")
            .arg("udeps")
            .arg("--output")
            .arg("json")
            .output()
            .context("Failed to execute cargo-udeps (requires nightly toolchain)")?;

        // Check if command succeeded
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!(
                "cargo-udeps failed: {}\n\nNote: cargo-udeps requires nightly: rustup install nightly",
                stderr
            );
        }

        // Parse JSON output
        let stdout = String::from_utf8_lossy(&output.stdout);
        Self::parse_udeps_output(&stdout)
    }

    /// Parse cargo-machete JSON output
    ///
    /// Expects JSON format with structure:
    /// ```json
    /// {
    ///   "unused": [
    ///     {
    ///       "package": "crate-name",
    ///       "dependencies": ["dep1", "dep2", ...]
    ///     }
    ///   ]
    /// }
    /// ```
    ///
    /// # Errors
    /// Returns error if JSON parsing fails
    fn parse_machete_output(json_str: &str) -> Result<UnusedReport> {
        #[derive(Deserialize)]
        struct MacheteOutput {
            unused: Vec<MacheteUnused>,
        }

        #[derive(Deserialize)]
        struct MacheteUnused {
            package: String,
            dependencies: Vec<String>,
        }

        let output: MacheteOutput =
            serde_json::from_str(json_str).context("Failed to parse cargo-machete JSON output")?;

        let mut unused_deps = Vec::new();

        for entry in output.unused {
            for dep in entry.dependencies {
                unused_deps.push(UnusedDependency {
                    package: entry.package.clone(),
                    dependency: dep,
                });
            }
        }

        Ok(UnusedReport {
            unused: unused_deps,
            tool: "cargo-machete".to_string(),
        })
    }

    /// Parse cargo-udeps JSON output
    ///
    /// Parses the JSON output from `cargo udeps --output json`.
    /// The output format is: `{ "unused_deps": { "package_name": ["dep1", "dep2"], ... } }`
    ///
    /// # Arguments
    /// * `json_str` - The JSON output from cargo-udeps
    ///
    /// # Returns
    /// UnusedReport with flattened list of unused dependencies
    ///
    /// # Errors
    /// Returns error if JSON parsing fails
    fn parse_udeps_output(json_str: &str) -> Result<UnusedReport> {
        #[derive(Deserialize)]
        struct UdepsOutput {
            unused_deps: std::collections::HashMap<String, Vec<String>>,
        }

        let output: UdepsOutput =
            serde_json::from_str(json_str).context("Failed to parse cargo-udeps JSON output")?;

        let mut unused_deps = Vec::new();

        for (package, dependencies) in output.unused_deps {
            for dep in dependencies {
                unused_deps.push(UnusedDependency {
                    package: package.clone(),
                    dependency: dep,
                });
            }
        }

        Ok(UnusedReport {
            unused: unused_deps,
            tool: "cargo-udeps".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_machete_output_empty() {
        let json = r#"{"unused":[]}"#;
        let report = UnusedDetector::parse_machete_output(json).unwrap();

        assert_eq!(report.unused.len(), 0);
        assert_eq!(report.tool, "cargo-machete");
    }

    #[test]
    fn test_parse_machete_output_single_package() {
        let json = r#"{
            "unused": [
                {
                    "package": "sinex-core",
                    "dependencies": ["serde", "tokio"]
                }
            ]
        }"#;

        let report = UnusedDetector::parse_machete_output(json).unwrap();

        assert_eq!(report.unused.len(), 2);
        assert_eq!(report.tool, "cargo-machete");
        assert_eq!(report.unused[0].package, "sinex-core");
        assert_eq!(report.unused[0].dependency, "serde");
        assert_eq!(report.unused[1].package, "sinex-core");
        assert_eq!(report.unused[1].dependency, "tokio");
    }

    #[test]
    fn test_parse_machete_output_multiple_packages() {
        let json = r#"{
            "unused": [
                {
                    "package": "sinex-core",
                    "dependencies": ["serde"]
                },
                {
                    "package": "sinex-gateway",
                    "dependencies": ["anyhow", "tokio"]
                }
            ]
        }"#;

        let report = UnusedDetector::parse_machete_output(json).unwrap();

        assert_eq!(report.unused.len(), 3);
        assert_eq!(report.unused[0].package, "sinex-core");
        assert_eq!(report.unused[1].package, "sinex-gateway");
        assert_eq!(report.unused[2].package, "sinex-gateway");
    }

    #[test]
    fn test_parse_machete_output_invalid_json() {
        let json = "not valid json";
        let result = UnusedDetector::parse_machete_output(json);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_udeps_output_empty() {
        let json = r#"{"unused_deps":{}}"#;
        let report = UnusedDetector::parse_udeps_output(json).unwrap();

        assert_eq!(report.unused.len(), 0);
        assert_eq!(report.tool, "cargo-udeps");
    }

    #[test]
    fn test_parse_udeps_output_single_package() {
        let json = r#"{
            "unused_deps": {
                "sinex-core": ["serde", "tokio"]
            }
        }"#;

        let report = UnusedDetector::parse_udeps_output(json).unwrap();

        assert_eq!(report.unused.len(), 2);
        assert_eq!(report.tool, "cargo-udeps");

        // Check both dependencies are present (order may vary due to HashMap)
        let deps: Vec<_> = report
            .unused
            .iter()
            .map(|d| d.dependency.as_str())
            .collect();
        assert!(deps.contains(&"serde"));
        assert!(deps.contains(&"tokio"));
    }

    #[test]
    fn test_parse_udeps_output_invalid_json() {
        let json = "not valid json";
        let result = UnusedDetector::parse_udeps_output(json);

        assert!(result.is_err());
    }
}
