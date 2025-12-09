//! Benchmark Results - Storage and Analysis of Benchmark Data
//!
//! This module provides structures and utilities for collecting, storing, and
//! analyzing benchmark results. It supports both timing measurements (from Divan)
//! and hardware counter measurements (future support for Iai).
//!
//! # Architecture
//!
//! Results are stored in a hierarchical structure:
//! - `BenchmarkRun`: Complete benchmark session with environment info
//! - `BenchmarkResult`: Individual benchmark result with measurements
//! - Statistical analysis and comparison utilities
//!
//! # Storage Format
//!
//! Results are persisted as JSON files with the naming convention:
//! `{timestamp}-{branch}-{commit}.json`
//!
//! This allows for:
//! - Historical tracking
//! - Cross-branch comparisons
//! - Regression detection

use crate::TestResult;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A complete benchmark run with metadata and results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRun {
    // Metadata
    /// Timestamp when the benchmark run started
    pub timestamp: DateTime<Utc>,
    /// Git commit hash
    pub git_commit: String,
    /// Git branch name
    pub git_branch: String,
    /// Whether the git working directory had uncommitted changes
    pub git_dirty: bool,

    // Environment
    /// Hostname where benchmarks were run
    pub hostname: String,
    /// CPU model information
    pub cpu_model: String,
    /// Number of CPU cores
    pub cpu_count: usize,
    /// Total system memory in GB
    pub memory_gb: f64,
    /// PostgreSQL version
    pub postgres_version: String,
    /// Rust compiler version
    pub rust_version: String,

    // Results
    /// All benchmark results from this run
    pub benchmarks: Vec<BenchmarkResult>,
}

/// Individual benchmark result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Benchmark name (e.g., "bench_insert_event")
    pub name: String,
    /// Suite/module name (e.g., "sinex_core::db::events")
    pub suite: String,
    /// Dataset used (e.g., "small", "medium", "large")
    pub dataset: String,
    /// Timestamp of this specific benchmark
    pub timestamp: DateTime<Utc>,

    // I/O benchmark measurements
    /// Cold cache timing in nanoseconds
    pub cold_cache_ns: Option<u64>,
    /// Warm cache timing in nanoseconds
    pub warm_cache_ns: Option<u64>,
    /// Number of samples collected
    pub samples: Option<usize>,
    /// Mean time in nanoseconds
    pub mean_ns: Option<u64>,
    /// Median time in nanoseconds
    pub median_ns: Option<u64>,
    /// Standard deviation in nanoseconds
    pub std_dev_ns: Option<u64>,

    // CPU benchmark measurements (future Iai support)
    /// CPU instruction count
    pub instructions: Option<u64>,
    /// L1 cache accesses
    pub l1_accesses: Option<u64>,
    /// L2 cache accesses
    pub l2_accesses: Option<u64>,
    /// RAM accesses
    pub ram_accesses: Option<u64>,
    /// Estimated CPU cycles
    pub estimated_cycles: Option<u64>,
}

impl BenchmarkRun {
    /// Create a new benchmark run with current environment info
    pub fn new() -> TestResult<Self> {
        Ok(Self {
            timestamp: Utc::now(),
            git_commit: get_git_commit()?,
            git_branch: get_git_branch()?,
            git_dirty: is_git_dirty()?,
            hostname: get_hostname()?,
            cpu_model: get_cpu_model()?,
            cpu_count: get_cpu_count(),
            memory_gb: get_memory_gb()?,
            postgres_version: get_postgres_version()?,
            rust_version: get_rust_version()?,
            benchmarks: Vec::new(),
        })
    }

    /// Collect results from current benchmark session
    ///
    /// This reads results from various sources:
    /// - Divan JSON output (if available)
    /// - Iai results (future support)
    /// - Manual recordings via BenchContext
    pub fn collect() -> TestResult<Self> {
        let mut run = Self::new()?;

        // Collect from divan results if available
        if let Ok(divan_results) = read_divan_results() {
            run.benchmarks.extend(divan_results);
        }

        // Future: collect from iai results
        // if let Ok(iai_results) = read_iai_results() {
        //     run.benchmarks.extend(iai_results);
        // }

        Ok(run)
    }

    /// Save results to a file
    ///
    /// Results are saved to `target/benchmarks/{timestamp}-{branch}-{commit}.json`
    /// with validated path operations for security.
    pub fn save(&self) -> TestResult<Utf8PathBuf> {
        use crate::path_validation::validate_test_path;

        let dir = Utf8PathBuf::from("target/benchmarks");

        // Validate the target directory path
        validate_test_path(dir.as_str())
            .map_err(|e| color_eyre::eyre::eyre!("Invalid benchmark directory path: {}", e))?;

        std::fs::create_dir_all(&dir)?;

        let filename = format!(
            "{}-{}-{}.json",
            self.timestamp.format("%Y%m%d-%H%M%S"),
            self.git_branch.replace('/', "-"),
            &self.git_commit[..8.min(self.git_commit.len())]
        );

        let path = dir.join(filename);

        // Validate the final file path
        validate_test_path(path.as_str())
            .map_err(|e| color_eyre::eyre::eyre!("Invalid benchmark file path: {}", e))?;

        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, json)?;

        Ok(path)
    }

    /// Load results from a file
    /// Path is validated for security before loading.
    pub fn load(path: &Utf8Path) -> TestResult<Self> {
        use crate::path_validation::validate_test_path;

        // Validate the file path before reading
        validate_test_path(path.as_str())
            .map_err(|e| color_eyre::eyre::eyre!("Invalid benchmark file path: {}", e))?;

        let json = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&json)?)
    }

    /// Compare with another benchmark run
    pub fn compare_with(&self, baseline: &Self) -> ComparisonReport {
        let mut comparisons = Vec::new();

        for current in &self.benchmarks {
            // Find matching benchmark in baseline
            if let Some(baseline_bench) = baseline
                .benchmarks
                .iter()
                .find(|b| b.name == current.name && b.dataset == current.dataset)
            {
                comparisons.push(BenchmarkComparison::new(baseline_bench, current));
            }
        }

        ComparisonReport {
            current_run: self.clone(),
            baseline_run: baseline.clone(),
            comparisons,
        }
    }
}

/// Comparison between two benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkComparison {
    /// Benchmark name
    pub name: String,
    /// Dataset used
    pub dataset: String,
    /// Baseline mean time
    pub baseline_mean_ns: u64,
    /// Current mean time
    pub current_mean_ns: u64,
    /// Percentage change (positive = slower, negative = faster)
    pub change_percent: f64,
    /// Whether this is a statistically significant change
    pub significant: bool,
}

impl BenchmarkComparison {
    /// Create a comparison between baseline and current results
    fn new(baseline: &BenchmarkResult, current: &BenchmarkResult) -> Self {
        let baseline_mean = baseline.mean_ns.unwrap_or(0);
        let current_mean = current.mean_ns.unwrap_or(0);

        let change_percent = if baseline_mean > 0 {
            ((current_mean as f64 - baseline_mean as f64) / baseline_mean as f64) * 100.0
        } else {
            0.0
        };

        // Simple significance check - could be improved with proper statistics
        let significant = change_percent.abs() > 5.0;

        Self {
            name: current.name.clone(),
            dataset: current.dataset.clone(),
            baseline_mean_ns: baseline_mean,
            current_mean_ns: current_mean,
            change_percent,
            significant,
        }
    }

    /// Check if this is a regression (significant slowdown)
    pub fn is_regression(&self) -> bool {
        self.significant && self.change_percent > 0.0
    }

    /// Check if this is an improvement (significant speedup)
    pub fn is_improvement(&self) -> bool {
        self.significant && self.change_percent < 0.0
    }
}

/// Report comparing two benchmark runs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    /// Current benchmark run
    pub current_run: BenchmarkRun,
    /// Baseline benchmark run for comparison
    pub baseline_run: BenchmarkRun,
    /// Individual benchmark comparisons
    pub comparisons: Vec<BenchmarkComparison>,
}

impl ComparisonReport {
    /// Check if there are any regressions
    pub fn has_regressions(&self) -> bool {
        self.comparisons.iter().any(|c| c.is_regression())
    }

    /// Get all regressions
    pub fn regressions(&self) -> Vec<&BenchmarkComparison> {
        self.comparisons
            .iter()
            .filter(|c| c.is_regression())
            .collect()
    }

    /// Get all improvements
    pub fn improvements(&self) -> Vec<&BenchmarkComparison> {
        self.comparisons
            .iter()
            .filter(|c| c.is_improvement())
            .collect()
    }

    /// Generate a markdown report
    pub fn to_markdown(&self) -> String {
        let mut report = String::new();

        report.push_str("# Benchmark Comparison Report\n\n");
        report.push_str(&format!(
            "**Current**: {} ({})\n",
            &self.current_run.git_commit[..8.min(self.current_run.git_commit.len())],
            self.current_run.git_branch
        ));
        report.push_str(&format!(
            "**Baseline**: {} ({})\n\n",
            &self.baseline_run.git_commit[..8.min(self.baseline_run.git_commit.len())],
            self.baseline_run.git_branch
        ));

        // Regressions
        let regressions = self.regressions();
        if !regressions.is_empty() {
            report.push_str("## ❌ Regressions\n\n");
            report.push_str("| Benchmark | Dataset | Baseline | Current | Change |\n");
            report.push_str("|-----------|---------|----------|---------|--------|\n");
            for reg in regressions {
                report.push_str(&format!(
                    "| {} | {} | {:.2}ms | {:.2}ms | **+{:.1}%** |\n",
                    reg.name,
                    reg.dataset,
                    reg.baseline_mean_ns as f64 / 1_000_000.0,
                    reg.current_mean_ns as f64 / 1_000_000.0,
                    reg.change_percent
                ));
            }
            report.push_str("\n");
        }

        // Improvements
        let improvements = self.improvements();
        if !improvements.is_empty() {
            report.push_str("## ✅ Improvements\n\n");
            report.push_str("| Benchmark | Dataset | Baseline | Current | Change |\n");
            report.push_str("|-----------|---------|----------|---------|--------|\n");
            for imp in improvements {
                report.push_str(&format!(
                    "| {} | {} | {:.2}ms | {:.2}ms | **{:.1}%** |\n",
                    imp.name,
                    imp.dataset,
                    imp.baseline_mean_ns as f64 / 1_000_000.0,
                    imp.current_mean_ns as f64 / 1_000_000.0,
                    imp.change_percent
                ));
            }
            report.push_str("\n");
        }

        // No significant changes
        let unchanged: Vec<_> = self.comparisons.iter().filter(|c| !c.significant).collect();
        if !unchanged.is_empty() {
            report.push_str("## ➖ No Significant Changes\n\n");
            report.push_str(&format!(
                "{} benchmarks showed no significant change.\n",
                unchanged.len()
            ));
        }

        report
    }
}

// Environment detection functions

fn get_git_commit() -> TestResult<String> {
    Ok(std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()?
        .stdout
        .into_iter()
        .map(|b| b as char)
        .collect::<String>()
        .trim()
        .to_string())
}

fn get_git_branch() -> TestResult<String> {
    Ok(std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()?
        .stdout
        .into_iter()
        .map(|b| b as char)
        .collect::<String>()
        .trim()
        .to_string())
}

fn is_git_dirty() -> TestResult<bool> {
    let output = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()?;
    Ok(!output.stdout.is_empty())
}

fn get_hostname() -> TestResult<String> {
    #[cfg(feature = "bench")]
    {
        Ok(hostname::get()?.to_string_lossy().to_string())
    }
    #[cfg(not(feature = "bench"))]
    {
        Ok("unknown".to_string())
    }
}

fn get_cpu_model() -> TestResult<String> {
    #[cfg(all(feature = "bench", target_os = "linux"))]
    {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo")?;
        for line in cpuinfo.lines() {
            if line.starts_with("model name") {
                return Ok(line
                    .split(':')
                    .nth(1)
                    .unwrap_or("unknown")
                    .trim()
                    .to_string());
            }
        }
    }
    Ok("unknown".to_string())
}

fn get_cpu_count() -> usize {
    #[cfg(feature = "bench")]
    {
        num_cpus::get()
    }
    #[cfg(not(feature = "bench"))]
    {
        1
    }
}

fn get_memory_gb() -> TestResult<f64> {
    #[cfg(feature = "bench")]
    {
        use sysinfo::System;
        let mut sys = System::new_all();
        sys.refresh_all();
        Ok(sys.total_memory() as f64 / 1024.0 / 1024.0 / 1024.0)
    }
    #[cfg(not(feature = "bench"))]
    {
        Ok(0.0)
    }
}

fn get_postgres_version() -> TestResult<String> {
    // This would need a database connection to query
    // For now, return a placeholder
    Ok("unknown".to_string())
}

fn get_rust_version() -> TestResult<String> {
    Ok(std::process::Command::new("rustc")
        .args(["--version"])
        .output()?
        .stdout
        .into_iter()
        .map(|b| b as char)
        .collect::<String>()
        .trim()
        .to_string())
}

/// Read results from Divan JSON output
fn read_divan_results() -> TestResult<Vec<BenchmarkResult>> {
    // This would parse target/divan-results.json if it exists
    // For now, return empty vec
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;

    #[sinex_test]
    fn test_benchmark_comparison() -> TestResult<()> {
        let baseline = BenchmarkResult {
            name: "test_query".to_string(),
            suite: "db".to_string(),
            dataset: "small".to_string(),
            timestamp: Utc::now(),
            mean_ns: Some(1_000_000), // 1ms
            median_ns: Some(950_000),
            std_dev_ns: Some(50_000),
            samples: Some(100),
            cold_cache_ns: None,
            warm_cache_ns: None,
            instructions: None,
            l1_accesses: None,
            l2_accesses: None,
            ram_accesses: None,
            estimated_cycles: None,
        };

        let current = BenchmarkResult {
            mean_ns: Some(1_100_000), // 1.1ms (10% slower)
            ..baseline.clone()
        };

        let comparison = BenchmarkComparison::new(&baseline, &current);
        assert_eq!(comparison.change_percent, 10.0);
        assert!(comparison.is_regression());
        assert!(!comparison.is_improvement());
        Ok(())
    }

    #[sinex_test]
    fn test_environment_detection() -> TestResult<()> {
        // These should not panic
        let _ = get_git_commit();
        let _ = get_git_branch();
        let _ = is_git_dirty();
        let _ = get_hostname();
        let _ = get_cpu_model();
        assert!(get_cpu_count() > 0);
        let _ = get_memory_gb();
        let _ = get_rust_version();
        Ok(())
    }

    #[sinex_test]
    fn test_markdown_generation() -> TestResult<()> {
        let baseline_run = BenchmarkRun {
            timestamp: Utc::now(),
            git_commit: "abcd1234".to_string(),
            git_branch: "main".to_string(),
            git_dirty: false,
            hostname: "bench-host".to_string(),
            cpu_model: "Test CPU".to_string(),
            cpu_count: 8,
            memory_gb: 16.0,
            postgres_version: "15.0".to_string(),
            rust_version: "1.70.0".to_string(),
            benchmarks: vec![BenchmarkResult {
                name: "query_events".to_string(),
                suite: "db".to_string(),
                dataset: "small".to_string(),
                timestamp: Utc::now(),
                mean_ns: Some(1_000_000),
                median_ns: Some(950_000),
                std_dev_ns: Some(50_000),
                samples: Some(100),
                cold_cache_ns: None,
                warm_cache_ns: None,
                instructions: None,
                l1_accesses: None,
                l2_accesses: None,
                ram_accesses: None,
                estimated_cycles: None,
            }],
        };

        let current_run = BenchmarkRun {
            git_commit: "efgh5678".to_string(),
            git_branch: "feature".to_string(),
            benchmarks: vec![BenchmarkResult {
                mean_ns: Some(1_100_000), // 10% regression
                ..baseline_run.benchmarks[0].clone()
            }],
            ..baseline_run.clone()
        };

        let report = current_run.compare_with(&baseline_run);
        let markdown = report.to_markdown();

        assert!(markdown.contains("Regressions"));
        assert!(markdown.contains("query_events"));
        assert!(markdown.contains("+10.0%"));
        Ok(())
    }
}
