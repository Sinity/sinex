//! Verification command surface for conformance, replay determinism, and perf budgets.

use crate::bench::{self, BenchConfig, BenchMode};
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::{config, workspace_root};
use crate::output::Status;
use crate::process::ProcessBuilder;
use color_eyre::eyre::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, clap::Args)]
pub struct VerifyCommand {
    #[command(subcommand)]
    pub subcommand: VerifySubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum VerifySubcommand {
    /// Core conformance checks for command surface and runtime invariants.
    Conformance,
    /// Deterministic replay-kernel checks.
    ReplayLab {
        /// Optional deterministic seed (forwarded to property-test env).
        #[arg(long)]
        seed: Option<u64>,
    },
    /// Run perf sweeps and enforce contract budgets.
    Perf {
        /// Nextest profile.
        #[arg(long, default_value = "fast")]
        profile: String,
        /// Runs per thread scenario.
        #[arg(long, default_value_t = 2)]
        runs: u32,
        /// Thread scenarios.
        #[arg(long, value_delimiter = ',', default_values_t = vec![12, 24])]
        threads: Vec<u32>,
        /// Target package list (comma-delimited) or `workspace`.
        #[arg(long, default_value = "workspace")]
        target: String,
        /// Contract file path.
        #[arg(long)]
        contracts: Option<PathBuf>,
        /// Output directory for verify artifacts.
        #[arg(long)]
        output_dir: Option<PathBuf>,
        /// History DB path for benchmark series.
        #[arg(long)]
        history_db: Option<PathBuf>,
    },
    /// Print summary from a perf report JSON.
    Report {
        /// Report file path (defaults to latest pointer).
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Compare two perf reports.
    Compare {
        #[arg(long)]
        current: PathBuf,
        #[arg(long)]
        previous: PathBuf,
    },
    /// Run conformance + replay-lab + perf in order.
    All {
        #[arg(long)]
        seed: Option<u64>,
        #[arg(long, default_value = "fast")]
        profile: String,
        #[arg(long, default_value_t = 2)]
        runs: u32,
        #[arg(long, value_delimiter = ',', default_values_t = vec![12, 24])]
        threads: Vec<u32>,
        #[arg(long, default_value = "workspace")]
        target: String,
        #[arg(long)]
        contracts: Option<PathBuf>,
        #[arg(long)]
        output_dir: Option<PathBuf>,
        #[arg(long)]
        history_db: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScenarioMeasurement {
    median_ms: f64,
    p95_ms: f64,
    throughput_runs_per_sec: f64,
    sample_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BudgetCheck {
    name: String,
    passed: bool,
    detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResolvedThresholds {
    max_median_ms: Option<f64>,
    max_p95_ms: Option<f64>,
    min_throughput_runs_per_sec: Option<f64>,
    median_regression_pct: Option<f64>,
    p95_regression_pct: Option<f64>,
    throughput_regression_pct: Option<f64>,
    enforce_baseline: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScenarioVerification {
    scenario_key: String,
    threads: u32,
    current: ScenarioMeasurement,
    baseline: Option<ScenarioMeasurement>,
    thresholds: ResolvedThresholds,
    checks: Vec<BudgetCheck>,
    passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerfVerificationReport {
    generated_at: String,
    profile: String,
    runs: u32,
    threads: Vec<u32>,
    bench_output_dir: String,
    history_db: String,
    contracts_path: String,
    latest_run_id: i64,
    passed: bool,
    failure_count: usize,
    scenarios: Vec<ScenarioVerification>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerfLatestPointer {
    updated_at: String,
    report_path: String,
    metrics_path: String,
    run_id: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PerfContractsFile {
    #[serde(default)]
    defaults: PerfThresholds,
    #[serde(default)]
    scenarios: BTreeMap<String, PerfThresholds>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PerfThresholds {
    max_median_ms: Option<f64>,
    max_p95_ms: Option<f64>,
    min_throughput_runs_per_sec: Option<f64>,
    median_regression_pct: Option<f64>,
    p95_regression_pct: Option<f64>,
    throughput_regression_pct: Option<f64>,
    enforce_baseline: Option<bool>,
}

#[derive(Debug, Clone)]
struct ScenarioRow {
    scenario_key: String,
    threads: u32,
    current: ScenarioMeasurement,
    baseline: Option<ScenarioMeasurement>,
}

#[derive(Debug, Clone)]
struct PerfArgs {
    profile: String,
    runs: u32,
    threads: Vec<u32>,
    target: String,
    contracts: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    history_db: Option<PathBuf>,
}

#[async_trait::async_trait]
impl XtaskCommand for VerifyCommand {
    fn name(&self) -> &'static str {
        "verify"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            VerifySubcommand::Conformance => execute_conformance(ctx),
            VerifySubcommand::ReplayLab { seed } => execute_replay_lab(*seed, ctx),
            VerifySubcommand::Perf {
                profile,
                runs,
                threads,
                target,
                contracts,
                output_dir,
                history_db,
            } => execute_perf(
                PerfArgs {
                    profile: profile.clone(),
                    runs: *runs,
                    threads: threads.clone(),
                    target: target.clone(),
                    contracts: contracts.clone(),
                    output_dir: output_dir.clone(),
                    history_db: history_db.clone(),
                },
                ctx,
            ),
            VerifySubcommand::Report { report } => execute_report(report.clone(), ctx),
            VerifySubcommand::Compare { current, previous } => {
                execute_compare(current, previous, ctx)
            }
            VerifySubcommand::All {
                seed,
                profile,
                runs,
                threads,
                target,
                contracts,
                output_dir,
                history_db,
            } => {
                execute_conformance(ctx)?;
                execute_replay_lab(*seed, ctx)?;
                execute_perf(
                    PerfArgs {
                        profile: profile.clone(),
                        runs: *runs,
                        threads: threads.clone(),
                        target: target.clone(),
                        contracts: contracts.clone(),
                        output_dir: output_dir.clone(),
                        history_db: history_db.clone(),
                    },
                    ctx,
                )
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("verification".to_string()),
            timeout: Some(Duration::from_mins(30)),
            modifies_state: true,
            track_in_history: true,
        }
    }
}

fn execute_conformance(ctx: &CommandContext) -> Result<CommandResult> {
    ProcessBuilder::cargo()
        .args([
            "test",
            "-p",
            "sinex-node-sdk",
            "runtime::stream::kernel::tests::validate_pull_consumer_config_reports_mismatch",
            "--",
            "--nocapture",
        ])
        .with_description("verify conformance: kernel invariant tests")
        .inherit_output()
        .run_ok()?;

    ProcessBuilder::cargo()
        .args([
            "test",
            "-p",
            "xtask",
            "--test",
            "command_consistency",
            "--",
            "--nocapture",
        ])
        .with_description("verify conformance: xtask command shape test")
        .inherit_output()
        .run_ok()?;

    Ok(CommandResult::success()
        .with_message("Conformance checks passed")
        .with_duration(ctx.elapsed()))
}

fn execute_replay_lab(seed: Option<u64>, ctx: &CommandContext) -> Result<CommandResult> {
    let mut command = ProcessBuilder::cargo()
        .args([
            "test",
            "-p",
            "sinex-node-sdk",
            "runtime::stream::kernel::tests::replay_publish_envelope_is_deterministic_for_fixed_timestamp",
            "--",
            "--nocapture",
        ])
        .with_description("verify replay-lab: deterministic replay envelope")
        .inherit_output();

    if let Some(seed) = seed {
        command = command.env("SINEX_PROPTEST_SEED", seed.to_string());
    }

    command.run_ok()?;

    Ok(CommandResult::success()
        .with_message("Replay lab checks passed")
        .with_duration(ctx.elapsed()))
}

fn execute_perf(args: PerfArgs, ctx: &CommandContext) -> Result<CommandResult> {
    let cfg = config();
    cfg.ensure_state_dir()
        .with_context(|| "failed to ensure state directory for verify")?;

    let contracts_path = args
        .contracts
        .unwrap_or_else(|| workspace_root().join("config/verify/perf-contracts.toml"));
    let output_root = args
        .output_dir
        .unwrap_or_else(|| cfg.state_dir.join("verify-perf"));
    let history_db = args
        .history_db
        .unwrap_or_else(|| cfg.state_dir.join("bench-verify-history.db"));

    fs::create_dir_all(&output_root).with_context(|| {
        format!(
            "failed to create verify output dir {}",
            output_root.display()
        )
    })?;

    let stamp = sinex_primitives::temporal::Timestamp::now()
        .format_rfc3339()
        .replace(':', "")
        .replace('-', "")
        .replace('T', "_")
        .replace('Z', "");
    let bench_output_dir = output_root.join(format!("bench-{stamp}"));

    let bench_cfg = BenchConfig {
        mode: BenchMode::Sweeps,
        profile: args.profile.clone(),
        runs: args.runs,
        threads: args.threads.clone(),
        baseline: None,
        regression_threshold_pct: 10.0,
        history_db: Some(history_db.clone()),
        history_trend_limit: 5,
        report_md: true,
        report_html: true,
        git_tag: false,
        dry_run: false,
        gha: false,
        bisect_good: None,
        bisect_bad: None,
        stress_limit: 100,
        soak_duration: 3600,
        output: Some(bench_output_dir.clone()),
        verbose: false,
        refine_top_threads: 3,
        refine_threshold_pct: 10.0,
        refine_sweep_runs: 1,
        target: args.target,
        continue_on_fail: false,
        fail_fast: false,
    };

    bench::run(bench_cfg).with_context(|| "benchmark execution failed during verify perf")?;

    let contracts = load_contracts(&contracts_path)?;
    let (latest_run_id, scenario_rows) = load_latest_run_rows(&history_db)?;

    if scenario_rows.is_empty() {
        bail!("verify perf did not produce any scenario rows");
    }

    let mut scenarios = Vec::with_capacity(scenario_rows.len());
    for row in scenario_rows {
        scenarios.push(evaluate_scenario(&row, &contracts));
    }

    let passed = scenarios.iter().all(|s| s.passed);
    let failure_count = scenarios.iter().filter(|s| !s.passed).count();

    let report = PerfVerificationReport {
        generated_at: sinex_primitives::temporal::Timestamp::now().format_rfc3339(),
        profile: args.profile,
        runs: args.runs,
        threads: args.threads,
        bench_output_dir: bench_output_dir.display().to_string(),
        history_db: history_db.display().to_string(),
        contracts_path: contracts_path.display().to_string(),
        latest_run_id,
        passed,
        failure_count,
        scenarios,
    };

    let report_path = output_root.join("verify-perf-report.json");
    let metrics_path = output_root.join("verify-perf-metrics.prom");
    let latest_path = cfg.state_dir.join("verify-perf-latest.json");

    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("failed to write {}", report_path.display()))?;
    fs::write(&metrics_path, render_prometheus(&report))
        .with_context(|| format!("failed to write {}", metrics_path.display()))?;

    let latest = PerfLatestPointer {
        updated_at: sinex_primitives::temporal::Timestamp::now().format_rfc3339(),
        report_path: report_path.display().to_string(),
        metrics_path: metrics_path.display().to_string(),
        run_id: latest_run_id,
    };
    fs::write(&latest_path, serde_json::to_vec_pretty(&latest)?)
        .with_context(|| format!("failed to write {}", latest_path.display()))?;

    if ctx.is_human() {
        println!("Verify perf report: {}", report_path.display());
        println!("Prometheus metrics: {}", metrics_path.display());
        if !report.passed {
            println!(
                "Perf contracts failed in {} scenario(s)",
                report.failure_count
            );
            for scenario in report.scenarios.iter().filter(|s| !s.passed) {
                println!("  - {}", scenario.scenario_key);
                for check in scenario.checks.iter().filter(|c| !c.passed) {
                    println!("    * {}: {}", check.name, check.detail);
                }
            }
        }
    }

    let mut result = if report.passed {
        CommandResult::success().with_message("Perf verification passed")
    } else {
        CommandResult::partial().with_message("Perf verification failed budget gates")
    };

    result
        .details
        .push(format!("report={}", report_path.display()));
    result
        .details
        .push(format!("metrics={}", metrics_path.display()));
    result.details.push(format!("run_id={latest_run_id}"));
    result.duration_secs = Some(ctx.elapsed().as_secs_f64());

    if !report.passed {
        result.status = Status::Failed;
    }

    Ok(result)
}

fn execute_report(report: Option<PathBuf>, ctx: &CommandContext) -> Result<CommandResult> {
    let path = resolve_report_path(report)?;
    let report: PerfVerificationReport = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("failed to parse perf report {}", path.display()))?;

    if ctx.is_human() {
        println!("Report: {}", path.display());
        println!("Generated: {}", report.generated_at);
        println!("Run ID: {}", report.latest_run_id);
        println!("Status: {}", if report.passed { "pass" } else { "fail" });
        for scenario in &report.scenarios {
            println!(
                "  {} median={:.1}ms p95={:.1}ms throughput={:.2} runs/s status={}",
                scenario.scenario_key,
                scenario.current.median_ms,
                scenario.current.p95_ms,
                scenario.current.throughput_runs_per_sec,
                if scenario.passed { "pass" } else { "fail" }
            );
        }
    }

    Ok(CommandResult::success()
        .with_message(format!("Loaded perf report from {}", path.display()))
        .with_duration(ctx.elapsed()))
}

fn execute_compare(current: &Path, previous: &Path, ctx: &CommandContext) -> Result<CommandResult> {
    let current_report: PerfVerificationReport = serde_json::from_slice(
        &fs::read(current).with_context(|| format!("failed to read {}", current.display()))?,
    )
    .with_context(|| format!("failed to parse {}", current.display()))?;
    let previous_report: PerfVerificationReport = serde_json::from_slice(
        &fs::read(previous).with_context(|| format!("failed to read {}", previous.display()))?,
    )
    .with_context(|| format!("failed to parse {}", previous.display()))?;

    let previous_map: BTreeMap<&str, &ScenarioVerification> = previous_report
        .scenarios
        .iter()
        .map(|s| (s.scenario_key.as_str(), s))
        .collect();

    let mut details = Vec::new();
    for scenario in &current_report.scenarios {
        if let Some(prev) = previous_map.get(scenario.scenario_key.as_str()) {
            let median_delta = percent_increase(scenario.current.median_ms, prev.current.median_ms);
            let p95_delta = percent_increase(scenario.current.p95_ms, prev.current.p95_ms);
            let throughput_delta = percent_drop(
                prev.current.throughput_runs_per_sec,
                scenario.current.throughput_runs_per_sec,
            );
            details.push(format!(
                "{} median={:+.2}% p95={:+.2}% throughput_drop={:+.2}%",
                scenario.scenario_key, median_delta, p95_delta, throughput_delta
            ));
        }
    }

    if ctx.is_human() {
        println!("Comparing {} -> {}", previous.display(), current.display());
        for line in &details {
            println!("  {line}");
        }
    }

    Ok(CommandResult::success()
        .with_message("Compared perf reports")
        .with_details(details)
        .with_duration(ctx.elapsed()))
}

fn resolve_report_path(path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = path {
        return Ok(path);
    }

    let latest_path = config().state_dir.join("verify-perf-latest.json");
    let latest: PerfLatestPointer = serde_json::from_slice(
        &fs::read(&latest_path)
            .with_context(|| format!("failed to read {}", latest_path.display()))?,
    )
    .with_context(|| format!("failed to parse {}", latest_path.display()))?;

    Ok(PathBuf::from(latest.report_path))
}

fn load_contracts(path: &Path) -> Result<PerfContractsFile> {
    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read contracts file {}", path.display()))?;
    toml::from_str(&data).with_context(|| format!("failed to parse {}", path.display()))
}

fn load_latest_run_rows(history_db: &Path) -> Result<(i64, Vec<ScenarioRow>)> {
    let conn = Connection::open(history_db)
        .with_context(|| format!("failed to open history db {}", history_db.display()))?;

    let latest_run_id: i64 = conn
        .query_row("SELECT id FROM runs ORDER BY id DESC LIMIT 1", [], |row| {
            row.get(0)
        })
        .with_context(|| "benchmark history does not contain any run")?;

    let mut stmt = conn.prepare(
        "SELECT threads, median_ms, p95_ms, throughput_runs_per_sec, sample_count
         FROM results
         WHERE run_id = ?1
         ORDER BY threads ASC",
    )?;

    let rows: Vec<(u32, f64, f64, f64, usize)> = stmt
        .query_map(params![latest_run_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut out = Vec::with_capacity(rows.len());
    for (threads, median_ms, p95_ms, throughput_runs_per_sec, sample_count) in rows {
        let baseline = load_baseline(&conn, latest_run_id, threads)?;
        out.push(ScenarioRow {
            scenario_key: format!("t={threads}"),
            threads,
            current: ScenarioMeasurement {
                median_ms,
                p95_ms,
                throughput_runs_per_sec,
                sample_count,
            },
            baseline,
        });
    }

    Ok((latest_run_id, out))
}

fn load_baseline(
    conn: &Connection,
    latest_run_id: i64,
    threads: u32,
) -> Result<Option<ScenarioMeasurement>> {
    conn.query_row(
        "SELECT median_ms, p95_ms, throughput_runs_per_sec, sample_count
         FROM results
         WHERE threads = ?1
           AND run_id != ?2
         ORDER BY id DESC
         LIMIT 1",
        params![threads, latest_run_id],
        |row| {
            Ok(ScenarioMeasurement {
                median_ms: row.get(0)?,
                p95_ms: row.get(1)?,
                throughput_runs_per_sec: row.get(2)?,
                sample_count: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn evaluate_scenario(row: &ScenarioRow, contracts: &PerfContractsFile) -> ScenarioVerification {
    let thresholds = resolve_thresholds(
        &contracts.defaults,
        contracts.scenarios.get(row.scenario_key.as_str()),
    );

    let mut checks = Vec::new();

    if let Some(max) = thresholds.max_median_ms {
        let passed = row.current.median_ms <= max;
        checks.push(BudgetCheck {
            name: "max_median_ms".to_string(),
            passed,
            detail: format!(
                "current {:.2}ms <= limit {:.2}ms",
                row.current.median_ms, max
            ),
        });
    }

    if let Some(max) = thresholds.max_p95_ms {
        let passed = row.current.p95_ms <= max;
        checks.push(BudgetCheck {
            name: "max_p95_ms".to_string(),
            passed,
            detail: format!("current {:.2}ms <= limit {:.2}ms", row.current.p95_ms, max),
        });
    }

    if let Some(min) = thresholds.min_throughput_runs_per_sec {
        let passed = row.current.throughput_runs_per_sec >= min;
        checks.push(BudgetCheck {
            name: "min_throughput_runs_per_sec".to_string(),
            passed,
            detail: format!(
                "current {:.2} runs/s >= limit {:.2} runs/s",
                row.current.throughput_runs_per_sec, min
            ),
        });
    }

    match &row.baseline {
        Some(baseline) => {
            if let Some(limit) = thresholds.median_regression_pct {
                let pct = percent_increase(row.current.median_ms, baseline.median_ms);
                let passed = pct <= limit;
                checks.push(BudgetCheck {
                    name: "median_regression_pct".to_string(),
                    passed,
                    detail: format!("median regression {:.2}% <= limit {:.2}%", pct, limit),
                });
            }

            if let Some(limit) = thresholds.p95_regression_pct {
                let pct = percent_increase(row.current.p95_ms, baseline.p95_ms);
                let passed = pct <= limit;
                checks.push(BudgetCheck {
                    name: "p95_regression_pct".to_string(),
                    passed,
                    detail: format!("p95 regression {:.2}% <= limit {:.2}%", pct, limit),
                });
            }

            if let Some(limit) = thresholds.throughput_regression_pct {
                let pct = percent_drop(
                    baseline.throughput_runs_per_sec,
                    row.current.throughput_runs_per_sec,
                );
                let passed = pct <= limit;
                checks.push(BudgetCheck {
                    name: "throughput_regression_pct".to_string(),
                    passed,
                    detail: format!("throughput drop {:.2}% <= limit {:.2}%", pct, limit),
                });
            }
        }
        None if thresholds.enforce_baseline => checks.push(BudgetCheck {
            name: "baseline_required".to_string(),
            passed: false,
            detail: "baseline required but no prior run exists".to_string(),
        }),
        None => {}
    }

    let passed = checks.iter().all(|c| c.passed);

    ScenarioVerification {
        scenario_key: row.scenario_key.clone(),
        threads: row.threads,
        current: row.current.clone(),
        baseline: row.baseline.clone(),
        thresholds,
        checks,
        passed,
    }
}

fn resolve_thresholds(
    defaults: &PerfThresholds,
    scenario: Option<&PerfThresholds>,
) -> ResolvedThresholds {
    let s = scenario.cloned().unwrap_or_default();

    ResolvedThresholds {
        max_median_ms: s.max_median_ms.or(defaults.max_median_ms),
        max_p95_ms: s.max_p95_ms.or(defaults.max_p95_ms),
        min_throughput_runs_per_sec: s
            .min_throughput_runs_per_sec
            .or(defaults.min_throughput_runs_per_sec),
        median_regression_pct: s.median_regression_pct.or(defaults.median_regression_pct),
        p95_regression_pct: s.p95_regression_pct.or(defaults.p95_regression_pct),
        throughput_regression_pct: s
            .throughput_regression_pct
            .or(defaults.throughput_regression_pct),
        enforce_baseline: s
            .enforce_baseline
            .or(defaults.enforce_baseline)
            .unwrap_or(false),
    }
}

fn percent_increase(current: f64, baseline: f64) -> f64 {
    if baseline <= 0.0 {
        return 0.0;
    }
    ((current - baseline) / baseline) * 100.0
}

fn percent_drop(baseline: f64, current: f64) -> f64 {
    if baseline <= 0.0 {
        return 0.0;
    }
    ((baseline - current) / baseline) * 100.0
}

fn render_prometheus(report: &PerfVerificationReport) -> String {
    let mut lines = vec![
        "# HELP verify_perf_overall_pass Overall pass status of verify perf run".to_string(),
        "# TYPE verify_perf_overall_pass gauge".to_string(),
        format!("verify_perf_overall_pass {}", i32::from(report.passed)),
        "# HELP verify_perf_scenario_pass Scenario pass status".to_string(),
        "# TYPE verify_perf_scenario_pass gauge".to_string(),
        "# HELP verify_perf_median_ms Scenario median runtime in milliseconds".to_string(),
        "# TYPE verify_perf_median_ms gauge".to_string(),
        "# HELP verify_perf_p95_ms Scenario p95 runtime in milliseconds".to_string(),
        "# TYPE verify_perf_p95_ms gauge".to_string(),
        "# HELP verify_perf_throughput_runs_per_sec Scenario throughput in runs per second"
            .to_string(),
        "# TYPE verify_perf_throughput_runs_per_sec gauge".to_string(),
    ];

    for scenario in &report.scenarios {
        let key = &scenario.scenario_key;
        lines.push(format!(
            "verify_perf_scenario_pass{{scenario=\"{}\"}} {}",
            key,
            i32::from(scenario.passed)
        ));
        lines.push(format!(
            "verify_perf_median_ms{{scenario=\"{}\"}} {:.6}",
            key, scenario.current.median_ms
        ));
        lines.push(format!(
            "verify_perf_p95_ms{{scenario=\"{}\"}} {:.6}",
            key, scenario.current.p95_ms
        ));
        lines.push(format!(
            "verify_perf_throughput_runs_per_sec{{scenario=\"{}\"}} {:.6}",
            key, scenario.current.throughput_runs_per_sec
        ));
    }

    lines.push(String::new());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentage_helpers_are_stable() {
        assert!((percent_increase(110.0, 100.0) - 10.0).abs() < f64::EPSILON);
        assert!((percent_drop(100.0, 92.0) - 8.0).abs() < f64::EPSILON);
        assert_eq!(percent_increase(100.0, 0.0), 0.0);
        assert_eq!(percent_drop(0.0, 100.0), 0.0);
    }

    #[test]
    fn prometheus_render_contains_expected_metrics() {
        let report = PerfVerificationReport {
            generated_at: "2026-01-01T00:00:00Z".to_string(),
            profile: "fast".to_string(),
            runs: 2,
            threads: vec![12],
            bench_output_dir: "/tmp/bench".to_string(),
            history_db: "/tmp/history.db".to_string(),
            contracts_path: "/tmp/contracts.toml".to_string(),
            latest_run_id: 42,
            passed: true,
            failure_count: 0,
            scenarios: vec![ScenarioVerification {
                scenario_key: "t=12".to_string(),
                threads: 12,
                current: ScenarioMeasurement {
                    median_ms: 100.0,
                    p95_ms: 120.0,
                    throughput_runs_per_sec: 8.5,
                    sample_count: 2,
                },
                baseline: None,
                thresholds: ResolvedThresholds {
                    max_median_ms: None,
                    max_p95_ms: None,
                    min_throughput_runs_per_sec: None,
                    median_regression_pct: None,
                    p95_regression_pct: None,
                    throughput_regression_pct: None,
                    enforce_baseline: false,
                },
                checks: vec![],
                passed: true,
            }],
        };

        let rendered = render_prometheus(&report);
        assert!(rendered.contains("verify_perf_overall_pass 1"));
        assert!(rendered.contains("verify_perf_scenario_pass{scenario=\"t=12\"} 1"));
        assert!(rendered.contains("verify_perf_median_ms{scenario=\"t=12\"} 100.000000"));
    }
}
