use super::{config::BenchConfig, environment::Environment, stats::RunStats};
use crate::process::ProcessBuilder;
use color_eyre::eyre::{Result, WrapErr, bail};
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock as Lazy;
use std::{
    env,
    path::PathBuf,
    time::{Duration, Instant},
};

static BENCH_TIMESTAMP_FORMAT: Lazy<Vec<time::format_description::BorrowedFormatItem<'static>>> =
    Lazy::new(|| {
        time::format_description::parse("[year][month][day]-[hour][minute][second]")
            .expect("static format string is valid")
    });

pub(super) struct BenchContext {
    pub config: BenchConfig,
    pub output_dir: PathBuf,
    pub environment: Environment,
}

impl BenchContext {
    pub(super) fn new(config: BenchConfig) -> Result<Self> {
        let timestamp = time::OffsetDateTime::now_utc()
            .format(&*BENCH_TIMESTAMP_FORMAT)
            .unwrap_or_else(|_| "unknown".to_string());
        let output_dir = config
            .output
            .clone()
            .unwrap_or_else(|| default_bench_output_dir(&timestamp.clone()));

        std::fs::create_dir_all(&output_dir).with_context(|| {
            format!(
                "Failed to create output directory: {}",
                output_dir.display()
            )
        })?;

        let environment = Environment::capture();
        let env_file = output_dir.join("environment.txt");
        environment.write_to_file(&env_file)?;

        Ok(Self {
            config,
            output_dir,
            environment,
        })
    }

    pub(super) fn compile(&self) -> Result<Duration> {
        println!("{}", style("Compiling workspace...").cyan().bold());

        let start = Instant::now();
        let mut builder = ProcessBuilder::cargo().args([
            "nextest",
            "run",
            "--config-file",
            ".config/nextest.toml",
            "--no-run",
        ]);

        if self.config.target == "workspace" {
            builder = builder.arg("--workspace");
        } else {
            for pkg in self
                .config
                .target
                .split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
            {
                builder = builder.arg("-p").arg(pkg);
            }
        }

        builder
            .with_description("cargo nextest compile")
            .inherit_output()
            .run_ok()
            .context("Failed to execute cargo nextest run --no-run")?;

        let elapsed = start.elapsed();

        println!(
            "{} Compiled in {}",
            style("✓").green(),
            style(format!("{:.1}s", elapsed.as_secs_f64())).cyan()
        );

        Ok(elapsed)
    }
}

fn default_bench_output_dir(timestamp: &str) -> PathBuf {
    let repo_cache_dir = crate::config::workspace_root().join(".sinex").join("cache");
    let base_dir = env::var_os("SINEX_TEST_RESULTS_DIR")
        .map_or_else(|| repo_cache_dir.join("test-results"), PathBuf::from);

    base_dir.join(format!("bench-nextest-{timestamp}"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Scenario {
    pub threads: u32,
}

impl Scenario {
    pub(super) fn key(&self) -> String {
        format!("t={}", self.threads)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RunResult {
    pub success: bool,
    pub elapsed_ms: f64,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioResult {
    pub scenario: Scenario,
    pub runs: Vec<RunResult>,
    pub stats: RunStats,
}

pub(super) struct BenchRunner<'a> {
    pub ctx: &'a BenchContext,
}

impl<'a> BenchRunner<'a> {
    pub(super) fn new(ctx: &'a BenchContext) -> Self {
        Self { ctx }
    }

    pub(super) fn run_scenario(
        &self,
        scenario: &Scenario,
        run_index: usize,
        total_runs: usize,
    ) -> Result<RunResult> {
        if self.ctx.config.dry_run {
            return Ok(RunResult {
                success: true,
                elapsed_ms: 0.0,
                stdout: String::new(),
                stderr: String::new(),
            });
        }

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.cyan} [{elapsed_precise}] {msg}")
                .expect("valid progress bar template"),
        );
        pb.set_message(format!(
            "Run {}/{} | {}",
            run_index + 1,
            total_runs,
            scenario.key()
        ));
        pb.enable_steady_tick(Duration::from_millis(100));

        let mut builder = ProcessBuilder::cargo().args([
            "nextest",
            "run",
            "--config-file",
            ".config/nextest.toml",
        ]);

        // Target specification
        if self.ctx.config.target == "workspace" {
            builder = builder.arg("--workspace");
        } else {
            for pkg in self.ctx.config.target.split(',') {
                builder = builder.arg("-p").arg(pkg.trim());
            }
        }

        builder = builder
            .arg("--profile")
            .arg(&self.ctx.config.profile)
            .arg("--test-threads")
            .arg(scenario.threads.to_string());

        // Fail-fast control
        if !self.ctx.config.fail_fast {
            builder = builder.arg("--no-fail-fast");
        }

        let start = Instant::now();
        let output = builder
            .with_description("cargo nextest run")
            .run_capture()?;
        let elapsed = start.elapsed();

        pb.finish_and_clear();

        let success = output.success();
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

        let status_icon = if success {
            style("✓").green()
        } else {
            style("✗").red()
        };

        println!(
            "{} Run {}/{} | {} | {:.1}ms",
            status_icon,
            run_index + 1,
            total_runs,
            scenario.key(),
            elapsed_ms
        );

        Ok(RunResult {
            success,
            elapsed_ms,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    pub(super) fn run_scenario_multiple(&self, scenario: &Scenario) -> Result<ScenarioResult> {
        let runs_count = self.ctx.config.runs as usize;
        let mut runs = Vec::new();
        let mut failures = 0;

        println!();
        println!(
            "{} Testing scenario: {}",
            style("▶").cyan().bold(),
            style(&scenario.key()).yellow()
        );

        for i in 0..runs_count {
            let result = self.run_scenario(scenario, i, runs_count)?;

            if !result.success {
                failures += 1;
                if !self.ctx.config.continue_on_fail {
                    bail!(
                        "Scenario {} failed on run {}/{}. Use --continue-on-fail to ignore failures.",
                        scenario.key(),
                        i + 1,
                        runs_count
                    );
                }
            }

            runs.push(result);
        }

        let samples: Vec<f64> = runs.iter().map(|r| r.elapsed_ms).collect();
        let stats = RunStats::from_samples(&samples);

        println!(
            "{} {}",
            style("📊").cyan(),
            style(&stats.format_summary()).dim()
        );

        if failures > 0 {
            println!(
                "{} {}/{} runs failed",
                style("⚠").yellow(),
                failures,
                runs_count
            );
        }

        Ok(ScenarioResult {
            scenario: scenario.clone(),
            runs,
            stats,
        })
    }
}

pub(super) fn generate_scenarios(config: &BenchConfig) -> Vec<Scenario> {
    config
        .threads
        .iter()
        .copied()
        .map(|threads| Scenario { threads })
        .collect()
}
