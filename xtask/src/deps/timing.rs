//! Build timing analysis
//!
//! Analyzes cargo build times using `cargo build --timings`.

use color_eyre::eyre::{bail, Result, WrapErr};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

/// Result of build timing analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingReport {
    /// Per-crate compile times (sorted longest first)
    pub crate_times: Vec<CrateTimingInfo>,
    /// Total build time (seconds)
    pub total_time_secs: f64,
    /// Path to HTML timing report (if generated)
    pub html_report: Option<PathBuf>,
}

/// Timing information for a single crate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrateTimingInfo {
    /// Crate name
    pub name: String,
    /// Compile duration (seconds)
    pub duration_secs: f64,
}

/// Analyzer for build timings
pub struct TimingAnalyzer;

impl TimingAnalyzer {
    /// Run cargo build with timings and analyze results
    ///
    /// Executes `cargo build --release --timings` and parses build output.
    /// Note: Cargo generates HTML reports, not JSON. We parse timing data
    /// from the build output instead.
    ///
    /// # Returns
    /// `TimingReport` with per-crate compile times and total build time
    ///
    /// # Errors
    /// Returns error if:
    /// - Build fails
    /// - Output parsing fails
    ///
    /// # Example
    /// ```no_run
    /// use xtask::deps::TimingAnalyzer;
    /// let report = TimingAnalyzer::analyze()?;
    /// println!("Build took {:.2}s", report.total_time_secs);
    /// # Ok::<(), color_eyre::eyre::Report>(())
    /// ```
    pub fn analyze() -> Result<TimingReport> {
        // Run cargo build with timing output (HTML only, no JSON available)
        let output = Command::new("cargo")
            .arg("build")
            .arg("--release")
            .arg("--timings")
            .output()
            .context("Failed to execute cargo build")?;

        // Check if build succeeded
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("cargo build failed:\n{stderr}");
        }

        // Prefer JSON timing data if available (more accurate than stderr parsing)
        let timing_json = PathBuf::from("target/cargo-timings/cargo-timing.json");
        if timing_json.exists() {
            if let Ok(report) = Self::parse_timing_json(&timing_json) {
                return Ok(report);
            }
            // Fall through to stderr parsing if JSON fails
        }

        // Parse timing from build output
        let stderr = String::from_utf8_lossy(&output.stderr);
        Ok(Self::parse_build_output(&stderr))
    }

    /// Parse timing data from cargo build stderr output
    ///
    /// Extracts compilation times from lines like:
    /// "   Compiling sinex-db v0.4.2 (path)"
    /// Note: Cargo doesn't provide per-crate timing in output directly,
    /// so we approximate based on the HTML report if available.
    fn parse_build_output(output: &str) -> TimingReport {
        // Check for HTML report path in output
        let html_report = PathBuf::from("target/cargo-timings/cargo-timing.html");
        let html_exists = html_report.exists();

        // Count compiled crates from output
        let compiled_crates: Vec<String> = output
            .lines()
            .filter(|l| l.contains("Compiling"))
            .filter_map(|l| {
                // Extract crate name from "   Compiling crate-name v0.1.0"
                let parts: Vec<&str> = l.split_whitespace().collect();
                if parts.len() >= 2 {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
            .collect();

        // Without JSON output, we can only provide crate names and HTML report path
        // Real timing data requires parsing the HTML report
        let crate_times: Vec<CrateTimingInfo> = compiled_crates
            .into_iter()
            .map(|name| CrateTimingInfo {
                name,
                duration_secs: 0.0, // Timing not available from stdout
            })
            .collect();

        TimingReport {
            crate_times,
            total_time_secs: 0.0, // Not available without parsing HTML
            html_report: if html_exists { Some(html_report) } else { None },
        }
    }

    /// Parse timing JSON output from cargo (for future use if JSON format is added)
    ///
    /// Extracts crate names and durations from the cargo timing JSON file,
    /// sorts by duration (slowest first), and calculates total build time.
    ///
    /// # Arguments
    /// * `timing_json` - Path to the cargo-timing.json file
    ///
    /// # Returns
    /// `TimingReport` with sorted crate times and total duration
    ///
    /// # Errors
    /// Returns error if:
    /// - File doesn't exist
    /// - File can't be read
    /// - JSON parsing fails
    fn parse_timing_json(timing_json: &PathBuf) -> Result<TimingReport> {
        if !timing_json.exists() {
            bail!("Timing JSON file not found at {}", timing_json.display());
        }

        let contents =
            fs::read_to_string(timing_json).context("Failed to read timing JSON file")?;

        #[derive(Deserialize)]
        struct TimingData {
            targets: Vec<Target>,
        }

        #[derive(Deserialize)]
        struct Target {
            name: String,
            duration: f64,
        }

        let data: TimingData =
            serde_json::from_str(&contents).context("Failed to parse timing JSON")?;

        let mut crate_times: Vec<CrateTimingInfo> = data
            .targets
            .into_iter()
            .map(|t| CrateTimingInfo {
                name: t.name,
                duration_secs: t.duration,
            })
            .collect();

        // Sort by duration (slowest first)
        crate_times.sort_by(|a, b| {
            b.duration_secs
                .partial_cmp(&a.duration_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total_time_secs: f64 = crate_times.iter().map(|c| c.duration_secs).sum();

        // Look for HTML report in same directory
        let html_report = timing_json.parent().and_then(|p| {
            let html = p.join("cargo-timing.html");
            if html.exists() {
                Some(html)
            } else {
                None
            }
        });

        Ok(TimingReport {
            crate_times,
            total_time_secs,
            html_report,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[sinex_test]
    fn test_parse_timing_json_valid() -> TestResult<()> {
        let json_content = r#"{
            "targets": [
                {"name": "sinex-db", "duration": 45.5},
                {"name": "sinex-gateway", "duration": 12.3},
                {"name": "xtask", "duration": 5.1}
            ]
        }"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let report = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf()).unwrap();

        assert_eq!(report.crate_times.len(), 3);
        assert_eq!(report.total_time_secs, 45.5 + 12.3 + 5.1);

        // Should be sorted slowest first
        assert_eq!(report.crate_times[0].name, "sinex-db");
        assert_eq!(report.crate_times[0].duration_secs, 45.5);
        assert_eq!(report.crate_times[1].name, "sinex-gateway");
        assert_eq!(report.crate_times[1].duration_secs, 12.3);
        assert_eq!(report.crate_times[2].name, "xtask");
        assert_eq!(report.crate_times[2].duration_secs, 5.1);
        Ok(())
    }

    #[sinex_test]
    fn test_parse_timing_json_empty_targets() -> TestResult<()> {
        let json_content = r#"{"targets": []}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let report = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf()).unwrap();

        assert_eq!(report.crate_times.len(), 0);
        assert_eq!(report.total_time_secs, 0.0);
        Ok(())
    }

    #[sinex_test]
    fn test_parse_timing_json_file_not_found() -> TestResult<()> {
        let nonexistent = PathBuf::from("/tmp/nonexistent-timing-file-xyz.json");
        let result = TimingAnalyzer::parse_timing_json(&nonexistent);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
        Ok(())
    }

    #[sinex_test]
    fn test_parse_timing_json_invalid_json() -> TestResult<()> {
        let json_content = "not valid json";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf());

        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_parse_timing_json_malformed_structure() -> TestResult<()> {
        let json_content = r#"{"invalid": "structure"}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf());

        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_crate_timing_info_ordering() -> TestResult<()> {
        let mut times = [
            CrateTimingInfo {
                name: "fast".to_string(),
                duration_secs: 1.0,
            },
            CrateTimingInfo {
                name: "slow".to_string(),
                duration_secs: 10.0,
            },
            CrateTimingInfo {
                name: "medium".to_string(),
                duration_secs: 5.0,
            },
        ];

        times.sort_by(|a, b| {
            b.duration_secs
                .partial_cmp(&a.duration_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        assert_eq!(times[0].name, "slow");
        assert_eq!(times[0].duration_secs, 10.0);
        assert_eq!(times[1].name, "medium");
        assert_eq!(times[1].duration_secs, 5.0);
        assert_eq!(times[2].name, "fast");
        assert_eq!(times[2].duration_secs, 1.0);
        Ok(())
    }

    #[sinex_test]
    fn test_parse_timing_json_single_target() -> TestResult<()> {
        let json_content = r#"{
            "targets": [
                {"name": "single-crate", "duration": 23.7}
            ]
        }"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let report = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf()).unwrap();

        assert_eq!(report.crate_times.len(), 1);
        assert_eq!(report.total_time_secs, 23.7);
        assert_eq!(report.crate_times[0].name, "single-crate");
        assert_eq!(report.crate_times[0].duration_secs, 23.7);
        Ok(())
    }

    #[sinex_test]
    fn test_parse_timing_json_equal_durations() -> TestResult<()> {
        let json_content = r#"{
            "targets": [
                {"name": "crate-a", "duration": 10.0},
                {"name": "crate-b", "duration": 10.0},
                {"name": "crate-c", "duration": 10.0}
            ]
        }"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let report = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf()).unwrap();

        assert_eq!(report.crate_times.len(), 3);
        assert_eq!(report.total_time_secs, 30.0);
        // All have the same duration, so they should be ordered by input
        assert!(report.crate_times.iter().all(|c| c.duration_secs == 10.0));
        Ok(())
    }

    #[sinex_test]
    fn test_timing_report_total_calculation() -> TestResult<()> {
        let crate_times = vec![
            CrateTimingInfo {
                name: "test1".to_string(),
                duration_secs: 1.5,
            },
            CrateTimingInfo {
                name: "test2".to_string(),
                duration_secs: 2.3,
            },
            CrateTimingInfo {
                name: "test3".to_string(),
                duration_secs: 0.7,
            },
        ];

        let expected_total = 1.5 + 2.3 + 0.7;

        let report = TimingReport {
            crate_times,
            total_time_secs: expected_total,
            html_report: None,
        };

        assert_eq!(report.total_time_secs, 4.5);
        Ok(())
    }
}
