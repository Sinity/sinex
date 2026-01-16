use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Args)]
pub struct BenchConfig {
    /// Benchmark mode
    #[arg(long, default_value = "sweeps")]
    pub mode: BenchMode,

    /// Nextest profile to use
    #[arg(long, default_value = "fast")]
    pub profile: String,

    /// Number of runs per configuration
    #[arg(long, default_value = "3")]
    pub runs: u32,

    /// Thread counts to test (comma-separated)
    #[arg(long, value_delimiter = ',', default_values_t = vec![12, 24])]
    pub threads: Vec<u32>,

    /// Baseline directory to compare against
    #[arg(long)]
    pub baseline: Option<PathBuf>,

    /// Regression threshold percentage (default: 10%)
    #[arg(long, default_value = "10.0")]
    pub regression_threshold_pct: f64,

    /// SQLite history database path
    #[arg(long)]
    pub history_db: Option<PathBuf>,

    /// Number of history points to include per scenario when reporting trends
    #[arg(long, default_value = "5")]
    pub history_trend_limit: usize,

    /// Generate markdown report
    #[arg(long)]
    pub report_md: bool,

    /// Generate HTML report
    #[arg(long)]
    pub report_html: bool,

    /// Tag git commit with results
    #[arg(long)]
    pub git_tag: bool,

    /// Dry run (compile only, no test execution)
    #[arg(long)]
    pub dry_run: bool,

    /// GitHub Actions mode (emit annotations)
    #[arg(long)]
    pub gha: bool,

    /// Bisect mode: known good commit
    #[arg(long)]
    pub bisect_good: Option<String>,

    /// Bisect mode: known bad commit
    #[arg(long)]
    pub bisect_bad: Option<String>,

    /// Stress mode: maximum iterations before giving up
    #[arg(long, default_value = "100")]
    pub stress_limit: u32,

    /// Soak mode: duration in seconds
    #[arg(long, default_value = "3600")]
    pub soak_duration: u64,

    /// Output directory (defaults to test-results/bench-nextest-TIMESTAMP)
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Refine mode: number of top thread counts to explore in detail
    #[arg(long, default_value = "3")]
    pub refine_top_threads: usize,

    /// Refine mode: only refine configs within this % of best (e.g., 10 = within 10%)
    #[arg(long, default_value = "10.0")]
    pub refine_threshold_pct: f64,

    /// Refine mode: number of runs for initial quick sweep
    #[arg(long, default_value = "1")]
    pub refine_sweep_runs: u32,

    /// Target package(s) to test (comma-separated, or 'workspace' for all)
    #[arg(long, default_value = "workspace")]
    pub target: String,

    /// Continue running other scenarios even if one fails
    #[arg(long)]
    pub continue_on_fail: bool,

    /// Don't use nextest --no-fail-fast (allow early exit on test failure)
    #[arg(long)]
    pub fail_fast: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BenchMode {
    /// Sweep through configuration matrices
    Sweeps,
    /// Two-phase optimization: quick sweep → find top N → detailed sweep
    Refine,
    /// Git bisect to find performance regression
    Bisect,
    /// Stress test (run until failure)
    Stress,
    /// Soak test (run for extended duration)
    Soak,
}

impl std::fmt::Display for BenchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchMode::Sweeps => write!(f, "sweeps"),
            BenchMode::Refine => write!(f, "refine"),
            BenchMode::Bisect => write!(f, "bisect"),
            BenchMode::Stress => write!(f, "stress"),
            BenchMode::Soak => write!(f, "soak"),
        }
    }
}
