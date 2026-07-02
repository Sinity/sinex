//! Build timing analysis
//!
//! Analyzes cargo build times using Cargo's `--timings` support.

use crate::process::ProcessBuilder;
use color_eyre::eyre::{Result, WrapErr, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

/// Result of build timing analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingReport {
    /// Cargo arguments used to generate this timing report
    pub cargo_args: Vec<String>,
    /// Package selected for the timing run, if any
    pub package: Option<String>,
    /// Cargo profile selected for the timing run
    pub profile: String,
    /// Whether the selected package was cleaned before timing
    pub cleaned_package: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingOptions {
    pub package: Option<String>,
    pub profile: String,
    pub clean_package: bool,
}

impl Default for TimingOptions {
    fn default() -> Self {
        Self {
            package: None,
            profile: "dev".to_string(),
            clean_package: false,
        }
    }
}

impl TimingOptions {
    fn cargo_args(&self) -> Vec<String> {
        let mut args = vec!["build".to_string(), "--timings".to_string()];
        match self.profile.as_str() {
            "dev" => {}
            "release" => args.push("--release".to_string()),
            profile => {
                args.push("--profile".to_string());
                args.push(profile.to_string());
            }
        }
        if let Some(package) = &self.package {
            args.push("-p".to_string());
            args.push(package.clone());
        }
        args
    }
}

impl TimingAnalyzer {
    /// Run cargo build with timings and analyze results
    ///
    /// Executes `cargo build --timings` by default and parses Cargo's HTML
    /// timing report. Cargo embeds the unit timing data as JSON inside that
    /// report. Use `analyze_with_options` for package/profile-specific timing.
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
        Self::analyze_with_options(&TimingOptions::default())
    }

    pub fn analyze_with_options(options: &TimingOptions) -> Result<TimingReport> {
        let cargo_args = options.cargo_args();
        let cargo_args_ref = cargo_args.iter().map(String::as_str).collect::<Vec<_>>();

        if options.clean_package {
            let Some(package) = &options.package else {
                bail!("--clean-package requires --package so the clean scope is explicit");
            };
            ProcessBuilder::cargo()
                .args(["clean", "-p", package.as_str()])
                .with_description("cargo clean package")
                .run()
                .context("Failed to execute cargo clean")?;
        }

        // Run cargo build with timing output (HTML only, no JSON available)
        let output = ProcessBuilder::cargo()
            .args(cargo_args_ref)
            .with_description("cargo build")
            .run()
            .context("Failed to execute cargo build")?;

        // Prefer JSON timing data if Cargo adds it in the future.
        let timing_json =
            crate::config::workspace_target_dir().join("cargo-timings/cargo-timing.json");
        if timing_json.exists()
            && let Ok(report) = Self::parse_timing_json_with_context(
                &timing_json,
                cargo_args.clone(),
                options.package.clone(),
                options.profile.clone(),
                options.clean_package,
            )
        {
            return Ok(report);
        }

        // Current Cargo emits HTML with embedded JSON unit data.
        let timing_html =
            crate::config::workspace_target_dir().join("cargo-timings/cargo-timing.html");
        if timing_html.exists()
            && let Ok(report) = Self::parse_timing_html_with_context(
                &timing_html,
                cargo_args.clone(),
                options.package.clone(),
                options.profile.clone(),
                options.clean_package,
            )
        {
            return Ok(report);
        }

        // Parse timing from build output
        Ok(Self::parse_build_output(
            &output.stderr,
            cargo_args,
            options.package.clone(),
            options.profile.clone(),
            options.clean_package,
        ))
    }

    /// Parse timing data from cargo build stderr output
    ///
    /// Extracts compilation times from lines like:
    /// "   Compiling sinex-db v0.4.2 (path)"
    /// Note: Cargo doesn't provide per-crate timing in output directly,
    /// so we approximate based on the HTML report if available.
    fn parse_build_output(
        output: &str,
        cargo_args: Vec<String>,
        package: Option<String>,
        profile: String,
        cleaned_package: bool,
    ) -> TimingReport {
        // Check for HTML report path in output
        let html_report =
            crate::config::workspace_target_dir().join("cargo-timings/cargo-timing.html");
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
            cargo_args,
            package,
            profile,
            cleaned_package,
            crate_times,
            total_time_secs: 0.0, // Not available without parsing HTML
            html_report: if html_exists { Some(html_report) } else { None },
        }
    }

    /// Parse timing data embedded in Cargo's HTML timing report.
    #[cfg(test)]
    fn parse_timing_html(timing_html: &PathBuf) -> Result<TimingReport> {
        Self::parse_timing_html_with_context(
            timing_html,
            Vec::new(),
            None,
            "unknown".to_string(),
            false,
        )
    }

    fn parse_timing_html_with_context(
        timing_html: &PathBuf,
        cargo_args: Vec<String>,
        package: Option<String>,
        profile: String,
        cleaned_package: bool,
    ) -> Result<TimingReport> {
        if !timing_html.exists() {
            bail!("Timing HTML file not found at {}", timing_html.display());
        }

        let contents =
            fs::read_to_string(timing_html).context("Failed to read timing HTML file")?;
        let unit_json = Self::extract_js_array(&contents, "const UNIT_DATA")
            .context("Failed to find UNIT_DATA in timing HTML")?;

        #[derive(Deserialize)]
        struct UnitData {
            name: String,
            start: f64,
            duration: f64,
        }

        let units: Vec<UnitData> =
            serde_json::from_str(unit_json).context("Failed to parse timing UNIT_DATA")?;

        let mut durations_by_name = BTreeMap::<String, f64>::new();
        let mut total_time_secs = 0.0_f64;
        for unit in units {
            if unit.duration > 0.0 {
                *durations_by_name.entry(unit.name).or_default() += unit.duration;
            }
            total_time_secs = total_time_secs.max(unit.start + unit.duration);
        }

        let mut crate_times: Vec<CrateTimingInfo> = durations_by_name
            .into_iter()
            .map(|(name, duration_secs)| CrateTimingInfo {
                name,
                duration_secs,
            })
            .collect();

        crate_times.sort_by(|a, b| {
            b.duration_secs
                .partial_cmp(&a.duration_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(TimingReport {
            cargo_args,
            package,
            profile,
            cleaned_package,
            crate_times,
            total_time_secs,
            html_report: Some(timing_html.clone()),
        })
    }

    fn extract_js_array<'a>(contents: &'a str, declaration: &str) -> Result<&'a str> {
        let declaration_start = contents
            .find(declaration)
            .ok_or_else(|| color_eyre::eyre::eyre!("{declaration} declaration not found"))?;
        let after_declaration = &contents[declaration_start..];
        let array_start = after_declaration
            .find('[')
            .ok_or_else(|| color_eyre::eyre::eyre!("{declaration} array start not found"))?;
        let array_contents = &after_declaration[array_start..];
        let array_end = array_contents
            .find("\n];")
            .ok_or_else(|| color_eyre::eyre::eyre!("{declaration} array end not found"))?;
        Ok(&array_contents[..array_end + 2])
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
    #[cfg(test)]
    fn parse_timing_json(timing_json: &PathBuf) -> Result<TimingReport> {
        Self::parse_timing_json_with_context(
            timing_json,
            Vec::new(),
            None,
            "unknown".to_string(),
            false,
        )
    }

    fn parse_timing_json_with_context(
        timing_json: &PathBuf,
        cargo_args: Vec<String>,
        package: Option<String>,
        profile: String,
        cleaned_package: bool,
    ) -> Result<TimingReport> {
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
            if html.exists() { Some(html) } else { None }
        });

        Ok(TimingReport {
            cargo_args,
            package,
            profile,
            cleaned_package,
            crate_times,
            total_time_secs,
            html_report,
        })
    }
}

#[cfg(test)]
#[path = "timing_test.rs"]
mod tests;
