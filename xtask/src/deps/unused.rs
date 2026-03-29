//! Unused dependency detection
//!
//! Integrates with cargo-machete or cargo-udeps to find unused dependencies.

use crate::process::ProcessBuilder;
use color_eyre::eyre::{Result, WrapErr, bail};
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
    /// `UnusedReport` with list of unused dependencies and tool name
    ///
    /// # Errors
    /// Returns error if:
    /// - No detection tool is available
    /// - Tool execution fails
    /// - Output parsing fails
    ///
    /// # Example
    /// ```no_run
    /// use xtask::deps::unused::UnusedDetector;
    /// let report = UnusedDetector::detect()?;
    /// println!("Found {} unused dependencies", report.unused.len());
    /// # Ok::<(), color_eyre::eyre::Report>(())
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
        bail!(
            "No unused dependency detection tool available.\n\n{}\n\nAlternatively:\n{}",
            ToolManager::install_guidance("cargo-machete"),
            ToolManager::install_guidance("cargo-udeps")
        )
    }

    /// Detect using cargo-machete
    ///
    /// Runs `cargo-machete` directly and parses text output.
    ///
    /// # Returns
    /// `UnusedReport` with unused dependencies found by cargo-machete
    ///
    /// # Errors
    /// Returns error if cargo-machete execution fails or output parsing fails
    fn detect_with_machete() -> Result<UnusedReport> {
        // Run cargo-machete directly (not via "cargo machete" which has issues)
        let output = Command::new("cargo-machete")
            .output()
            .context("Failed to execute cargo-machete")?;

        // Parse text output (exit code 1 = found unused deps, 0 = none found, 2 = error)
        let stdout = String::from_utf8_lossy(&output.stdout);

        // Exit code 2 is an error
        if output.status.code() == Some(2) {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("cargo-machete failed: {stdout}{stderr}");
        }

        Self::parse_machete_stdout(&stdout)
    }

    /// Detect using cargo-udeps
    ///
    /// Runs `cargo +nightly udeps --output json` and parses output.
    /// Requires nightly toolchain.
    ///
    /// # Returns
    /// `UnusedReport` with unused dependencies found by cargo-udeps
    ///
    /// # Errors
    /// Returns error if:
    /// - Nightly toolchain not installed
    /// - cargo-udeps execution fails
    /// - Output parsing fails
    fn detect_with_udeps() -> Result<UnusedReport> {
        // Run cargo udeps with JSON output (requires nightly)
        let output = ProcessBuilder::cargo()
            .args(["+nightly", "udeps", "--output", "json"])
            .with_description("cargo udeps")
            .run()
            .context("Failed to execute cargo-udeps (requires nightly toolchain)")?;

        // Parse JSON output
        Self::parse_udeps_output(&output.stdout)
    }

    /// Parse cargo-machete text output
    ///
    /// Parses text format:
    /// ```text
    /// package-name -- ./path/to/Cargo.toml:
    ///     dep1
    ///     dep2
    /// ```
    ///
    /// # Errors
    /// Returns error if output format is unexpected
    fn parse_machete_stdout(stdout: &str) -> Result<UnusedReport> {
        let trimmed = stdout.trim_start();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            return Self::parse_machete_output(stdout)
                .context("cargo-machete emitted JSON-looking output that failed to parse");
        }

        Self::parse_machete_text_output(stdout)
    }

    fn parse_machete_text_output(text: &str) -> Result<UnusedReport> {
        let mut unused_deps = Vec::new();
        let mut current_package: Option<String> = None;

        for line in text.lines() {
            let line = line.trim();

            // Skip empty lines and info messages
            if line.is_empty()
                || line.starts_with("Analyzing")
                || line.starts_with("cargo-machete found")
                || line.starts_with("Done")
            {
                continue;
            }

            // Package line: "package-name -- ./path/to/Cargo.toml:"
            if line.contains("-- ") && line.ends_with(':') {
                let (package_name, _) = line.split_once("-- ").ok_or_else(|| {
                    color_eyre::eyre::eyre!(
                        "cargo-machete package line was missing expected delimiter: {line}"
                    )
                })?;
                let package_name = package_name.trim();
                if package_name.is_empty() {
                    bail!("cargo-machete reported an empty package name: {line}");
                }
                current_package = Some(package_name.to_string());
            }
            // Dependency line (indented): "    dep-name"
            else if let Some(ref package) = current_package {
                let dep_name = line.trim();
                if !dep_name.is_empty() {
                    unused_deps.push(UnusedDependency {
                        package: package.clone(),
                        dependency: dep_name.to_string(),
                    });
                }
            } else {
                bail!("cargo-machete emitted a dependency line before any package header: {line}");
            }
        }

        Ok(UnusedReport {
            unused: unused_deps,
            tool: "cargo-machete".to_string(),
        })
    }

    /// Parse cargo-machete JSON output (for future use if JSON format is added)
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
    /// `UnusedReport` with flattened list of unused dependencies
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
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_parse_machete_output_empty() -> TestResult<()> {
        let json = r#"{"unused":[]}"#;
        let report = UnusedDetector::parse_machete_output(json).unwrap();

        assert_eq!(report.unused.len(), 0);
        assert_eq!(report.tool, "cargo-machete");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_machete_output_single_package() -> TestResult<()> {
        let json = r#"{
            "unused": [
                {
                    "package": "sinex-db",
                    "dependencies": ["serde", "tokio"]
                }
            ]
        }"#;

        let report = UnusedDetector::parse_machete_output(json).unwrap();

        assert_eq!(report.unused.len(), 2);
        assert_eq!(report.tool, "cargo-machete");
        assert_eq!(report.unused[0].package, "sinex-db");
        assert_eq!(report.unused[0].dependency, "serde");
        assert_eq!(report.unused[1].package, "sinex-db");
        assert_eq!(report.unused[1].dependency, "tokio");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_machete_output_multiple_packages() -> TestResult<()> {
        let json = r#"{
            "unused": [
                {
                    "package": "sinex-db",
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
        assert_eq!(report.unused[0].package, "sinex-db");
        assert_eq!(report.unused[1].package, "sinex-gateway");
        assert_eq!(report.unused[2].package, "sinex-gateway");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_machete_output_invalid_json() -> TestResult<()> {
        let json = "not valid json";
        let result = UnusedDetector::parse_machete_output(json);

        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_machete_stdout_text_output() -> TestResult<()> {
        let report = UnusedDetector::parse_machete_stdout(
            "sinex-db -- ./crate/lib/sinex-db/Cargo.toml:\n    serde\n    tokio\n",
        )?;

        assert_eq!(report.tool, "cargo-machete");
        assert_eq!(report.unused.len(), 2);
        assert_eq!(report.unused[0].package, "sinex-db");
        assert_eq!(report.unused[0].dependency, "serde");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_machete_stdout_rejects_malformed_json() -> TestResult<()> {
        let error = UnusedDetector::parse_machete_stdout("{not valid json")
            .expect_err("malformed JSON-looking output should fail");
        assert!(format!("{error:#}").contains("failed to parse"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_machete_text_output_rejects_dependency_before_header() -> TestResult<()> {
        let error = UnusedDetector::parse_machete_text_output("serde\n")
            .expect_err("dependency without package header should fail");
        assert!(
            format!("{error:#}")
                .contains("cargo-machete emitted a dependency line before any package header")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_machete_text_output_rejects_empty_package_name() -> TestResult<()> {
        let error = UnusedDetector::parse_machete_text_output(" -- ./Cargo.toml:\n    serde\n")
            .expect_err("empty package header should fail");
        assert!(format!("{error:#}").contains("empty package name"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_udeps_output_empty() -> TestResult<()> {
        let json = r#"{"unused_deps":{}}"#;
        let report = UnusedDetector::parse_udeps_output(json).unwrap();

        assert_eq!(report.unused.len(), 0);
        assert_eq!(report.tool, "cargo-udeps");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_udeps_output_single_package() -> TestResult<()> {
        let json = r#"{
            "unused_deps": {
                "sinex-db": ["serde", "tokio"]
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
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_udeps_output_invalid_json() -> TestResult<()> {
        let json = "not valid json";
        let result = UnusedDetector::parse_udeps_output(json);

        assert!(result.is_err());
        Ok(())
    }
}
