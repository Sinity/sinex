use super::{config::BenchConfig, environment::Environment, stats::RunStats};
use crate::process::ProcessBuilder;
use color_eyre::eyre::{Result, WrapErr, bail};
use console::style;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock as Lazy;
use std::{
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
        guard_db_benchmark_resources(&config)?;

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

fn guard_db_benchmark_resources(config: &BenchConfig) -> Result<()> {
    if config.db_pool_sizes.is_empty() || config.dry_run {
        return Ok(());
    }

    let conflicts = active_heavy_processes();
    if !config.allow_contended_host && !conflicts.is_empty() {
        let details = conflicts
            .iter()
            .take(8)
            .map(|process| format!("  pid {}: {}", process.pid, process.command))
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "Refusing DB benchmark while another heavy development process is active:\n{}\n\
             Wait for those jobs or stop them before running the DB pool matrix.",
            details
        );
    }

    if !config.allow_contended_host {
        let pressure = crate::resources::PressureRecommendation::capture();
        if let Some(error) = pressure.broad_start_error("DB benchmark") {
            bail!("{error}");
        }
    }

    Ok(())
}

#[derive(Debug)]
struct ActiveProcess {
    pid: u32,
    command: String,
}

fn active_heavy_processes() -> Vec<ActiveProcess> {
    let self_pid = std::process::id();
    let mut processes = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return processes;
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(pid) = file_name.to_str().and_then(|name| name.parse::<u32>().ok()) else {
            continue;
        };
        if pid == self_pid {
            continue;
        }

        let cmdline_path = entry.path().join("cmdline");
        let Ok(raw) = std::fs::read(&cmdline_path) else {
            continue;
        };
        if raw.is_empty() {
            continue;
        }
        let command = String::from_utf8_lossy(&raw).replace('\0', " ");
        let argv0 = command.split_whitespace().next().unwrap_or_default();
        let executable = std::path::Path::new(argv0)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(argv0);
        if heavy_development_command(executable, &command) {
            processes.push(ActiveProcess { pid, command });
        }
    }

    processes
}

fn heavy_development_command(executable: &str, command: &str) -> bool {
    let command = command.to_ascii_lowercase();
    matches!(
        executable,
        "cargo"
            | "cargo-nextest"
            | "rustc"
            | "rustdoc"
            | "pytest"
            | "uv"
            | "nix"
            | "nix-build"
            | "nixos-rebuild"
    ) || executable.starts_with("mold")
        || command.contains("polylogue")
        || command.contains(" xtask ")
}

fn default_bench_output_dir(timestamp: &str) -> PathBuf {
    let base_dir = std::env::var_os("SINEX_TEST_RESULTS_DIR").map_or_else(
        || crate::config::config().cache_dir.join("test-results"),
        PathBuf::from,
    );

    base_dir.join(format!("bench-nextest-{timestamp}"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Scenario {
    pub threads: u32,
    pub package: String,
    pub db_pool_size: Option<u32>,
}

impl Scenario {
    pub(super) fn key(&self) -> String {
        let base = if self.package.is_empty() {
            format!("t={}", self.threads)
        } else {
            format!("{}:t={}", self.package, self.threads)
        };
        match self.db_pool_size {
            Some(size) => format!("{base}:db_pool={size}"),
            None => base,
        }
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

        eprintln!("Run {}/{} | {}", run_index + 1, total_runs, scenario.key());

        let builder = if let Some(db_pool_size) = scenario.db_pool_size {
            let exe = std::env::current_exe()
                .context("failed to resolve current xtask executable for db benchmark")?;
            let mut builder =
                ProcessBuilder::new(exe.to_string_lossy()).args(["test", "--ephemeral-postgres"]);
            if self.ctx.config.target == "workspace" {
                builder = builder.arg("--all");
            } else {
                for pkg in self.ctx.config.target.split(',') {
                    builder = builder.arg("-p").arg(pkg.trim());
                }
            }
            builder
                .arg("--threads")
                .arg(scenario.threads.to_string())
                .env("SINEX_TEST_DB_POOL_SIZE", db_pool_size.to_string())
        } else {
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

            builder
        };

        let start = Instant::now();
        let output = builder
            .with_description("cargo nextest run")
            .run_capture()?;
        let elapsed = start.elapsed();

        let success = output.success();
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

        if !success {
            eprintln!("Scenario {} failed.", scenario.key());
            emit_failure_stream("stdout", &output.stdout);
            emit_failure_stream("stderr", &output.stderr);
        }

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
    let db_pool_sizes: Vec<Option<u32>> = if config.db_pool_sizes.is_empty() {
        vec![None]
    } else {
        config.db_pool_sizes.iter().copied().map(Some).collect()
    };

    config
        .threads
        .iter()
        .copied()
        .flat_map(|threads| {
            db_pool_sizes
                .iter()
                .copied()
                .map(move |db_pool_size| Scenario {
                    threads,
                    package: String::new(),
                    db_pool_size,
                })
        })
        .collect()
}

fn emit_failure_stream(name: &str, content: &str) {
    if content.trim().is_empty() {
        return;
    }

    const MAX_CHARS: usize = 32_000;
    let char_count = content.chars().count();
    if char_count <= MAX_CHARS {
        eprintln!("--- child {name} ---\n{content}");
        return;
    }

    let tail: String = content
        .chars()
        .rev()
        .take(MAX_CHARS)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    eprintln!("--- child {name} (last {MAX_CHARS} chars of {char_count}) ---\n{tail}");
}
