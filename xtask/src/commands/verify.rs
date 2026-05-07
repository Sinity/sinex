//! Verification command surface for conformance, replay determinism, and perf budgets.

use crate::bench::{self, BenchConfig, BenchMode};
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::{config, workspace_root};
use crate::output::Status;
use color_eyre::eyre::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Verify phase plans and performance contracts.
#[derive(Debug, Clone, clap::Args)]
pub struct VerifyCommand {
    #[command(subcommand)]
    pub subcommand: VerifySubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum VerifySubcommand {
    /// Inspect and validate the phase verification manifest.
    Plan {
        /// Select one phase by id.
        #[arg(long)]
        phase: Option<String>,
        /// Show all phases.
        #[arg(long)]
        all: bool,
        /// Validate the manifest contract and exit.
        #[arg(long)]
        check: bool,
        /// Manifest path.
        #[arg(long)]
        manifest: Option<PathBuf>,
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
    /// Run perf only.
    All {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PhaseVerificationManifest {
    version: u32,
    phases: Vec<PhaseVerificationPhase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PhaseVerificationPhase {
    id: String,
    title: String,
    issues: Vec<u64>,
    required_checks: Vec<String>,
    #[serde(default)]
    boundary_checks: Vec<String>,
    #[serde(default)]
    impact_gates: Vec<PhaseImpactGate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PhaseImpactGate {
    impact: String,
    commands: Vec<String>,
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
pub struct PerfArgs {
    pub profile: String,
    pub runs: u32,
    pub threads: Vec<u32>,
    pub target: String,
    pub contracts: Option<PathBuf>,
    pub output_dir: Option<PathBuf>,
    pub history_db: Option<PathBuf>,
}

impl XtaskCommand for VerifyCommand {
    fn name(&self) -> &'static str {
        "verify"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            VerifySubcommand::Plan {
                phase,
                all,
                check,
                manifest,
            } => execute_phase_plan(phase.clone(), *all, *check, manifest.clone(), ctx),
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
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("verification"),
            timeout: Some(Duration::from_mins(30)),
            modifies_state: true,
            track_in_history: true,
            history_access: crate::command::HistoryAccessMode::ReadWrite,
        }
    }
}

fn default_phase_manifest_path() -> PathBuf {
    workspace_root().join("xtask/config/phase-verification.json")
}

fn execute_phase_plan(
    phase: Option<String>,
    all: bool,
    check: bool,
    manifest_path: Option<PathBuf>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let path = manifest_path.unwrap_or_else(default_phase_manifest_path);
    let manifest = load_phase_manifest(&path)?;
    validate_phase_manifest(&manifest)?;

    let selected_phases: Vec<PhaseVerificationPhase> = if let Some(phase_id) = phase {
        let selected: Vec<_> = manifest
            .phases
            .iter()
            .filter(|phase| phase.id == phase_id)
            .cloned()
            .collect();
        if selected.is_empty() {
            bail!("phase `{phase_id}` does not exist in {}", path.display());
        }
        selected
    } else {
        manifest.phases.clone()
    };

    if ctx.is_human() {
        if check {
            println!("Phase verification manifest is valid: {}", path.display());
        } else {
            let render_all = all || selected_phases.len() > 1;
            if render_all {
                println!("Phase verification manifest: {}", path.display());
            }
            for phase in &selected_phases {
                println!("{} — {}", phase.id, phase.title);
                println!("  issues: {}", render_issues(&phase.issues));
                println!("  required:");
                for command in &phase.required_checks {
                    println!("    - {command}");
                }
                if !phase.boundary_checks.is_empty() {
                    println!("  boundary:");
                    for command in &phase.boundary_checks {
                        println!("    - {command}");
                    }
                }
                if !phase.impact_gates.is_empty() {
                    println!("  impact:");
                    for gate in &phase.impact_gates {
                        println!("    {}:", gate.impact);
                        for command in &gate.commands {
                            println!("      - {command}");
                        }
                    }
                }
            }
        }
    }

    Ok(CommandResult::success()
        .with_message("Phase verification plan loaded")
        .with_detail(format!("manifest={}", path.display()))
        .with_detail(format!("phases={}", selected_phases.len()))
        .with_data(serde_json::json!({
            "manifest": path,
            "version": manifest.version,
            "checked": check,
            "phases": selected_phases,
        }))
        .with_duration(ctx.elapsed()))
}

fn render_issues(issues: &[u64]) -> String {
    issues
        .iter()
        .map(|issue| format!("#{issue}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn load_phase_manifest(path: &Path) -> Result<PhaseVerificationManifest> {
    let data = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read phase verification manifest {}",
            path.display()
        )
    })?;
    serde_json::from_str(&data).with_context(|| {
        format!(
            "failed to parse phase verification manifest {}",
            path.display()
        )
    })
}

fn validate_phase_manifest(manifest: &PhaseVerificationManifest) -> Result<()> {
    if manifest.version != 1 {
        bail!(
            "unsupported phase verification manifest version {}; expected 1",
            manifest.version
        );
    }
    if manifest.phases.is_empty() {
        bail!("phase verification manifest must define at least one phase");
    }

    let mut ids = BTreeSet::new();
    for phase in &manifest.phases {
        if phase.id.trim().is_empty() {
            bail!("phase id must not be empty");
        }
        if !ids.insert(phase.id.as_str()) {
            bail!("duplicate phase id `{}`", phase.id);
        }
        if phase.title.trim().is_empty() {
            bail!("phase `{}` must have a title", phase.id);
        }
        if phase.issues.is_empty() {
            bail!("phase `{}` must reference at least one issue", phase.id);
        }
        if phase.required_checks.is_empty() {
            bail!(
                "phase `{}` must define at least one required check",
                phase.id
            );
        }
        for command in &phase.required_checks {
            validate_supported_phase_command(command)?;
        }
        for command in &phase.boundary_checks {
            validate_supported_phase_command(command)?;
        }
        for gate in &phase.impact_gates {
            if gate.impact.trim().is_empty() {
                bail!("phase `{}` has an impact gate with an empty name", phase.id);
            }
            if gate.commands.is_empty() {
                bail!(
                    "phase `{}` impact gate `{}` must define at least one command",
                    phase.id,
                    gate.impact
                );
            }
            for command in &gate.commands {
                validate_supported_phase_command(command)?;
            }
        }
    }

    Ok(())
}

fn validate_supported_phase_command(command: &str) -> Result<()> {
    let tokens: Vec<_> = command.split_whitespace().collect();
    let Some(program) = tokens.first().copied() else {
        bail!("phase verification command must not be empty");
    };

    match program {
        "xtask" => validate_xtask_phase_command(&tokens[1..], command),
        "git" => validate_git_phase_command(&tokens[1..], command),
        _ => bail!("unsupported phase verification command `{command}`; use xtask or git surfaces"),
    }
}

fn validate_xtask_phase_command(tokens: &[&str], command: &str) -> Result<()> {
    let Some(root) = tokens.first().copied() else {
        bail!("xtask phase verification command is missing a subcommand: `{command}`");
    };

    const SUPPORTED_ROOTS: &[&str] = &["check", "test", "docs", "ci", "verify"];
    if !SUPPORTED_ROOTS.contains(&root) {
        bail!("unsupported xtask phase verification root `{root}` in `{command}`");
    }

    if let Some(pos) = tokens.iter().position(|token| *token == "--") {
        let nested = tokens[pos + 1..].join(" ");
        if !nested.is_empty() {
            validate_supported_phase_command(&nested)?;
        }
    }

    Ok(())
}

fn validate_git_phase_command(tokens: &[&str], command: &str) -> Result<()> {
    if tokens == ["diff", "--check"] {
        return Ok(());
    }
    bail!("unsupported git phase verification command `{command}`")
}

pub fn execute_perf(args: PerfArgs, ctx: &CommandContext) -> Result<CommandResult> {
    let cfg = config();
    cfg.ensure_state_dir()
        .with_context(|| "failed to ensure state directory for verify")?;

    let contracts_path = args
        .contracts
        .unwrap_or_else(|| workspace_root().join("xtask/config/perf-contracts.toml"));
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
        .replace([':', '-'], "")
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

    let stage = ctx.start_stage("bench");
    bench::run(bench_cfg).with_context(|| "benchmark execution failed during verify perf")?;
    ctx.finish_stage(stage, true);

    let stage = ctx.start_stage("verify");
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

    ctx.finish_stage(stage, report.passed);

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

pub fn execute_report(report: Option<PathBuf>, ctx: &CommandContext) -> Result<CommandResult> {
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

pub fn execute_compare(
    current: &Path,
    previous: &Path,
    ctx: &CommandContext,
) -> Result<CommandResult> {
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
                    detail: format!("median regression {pct:.2}% <= limit {limit:.2}%"),
                });
            }

            if let Some(limit) = thresholds.p95_regression_pct {
                let pct = percent_increase(row.current.p95_ms, baseline.p95_ms);
                let passed = pct <= limit;
                checks.push(BudgetCheck {
                    name: "p95_regression_pct".to_string(),
                    passed,
                    detail: format!("p95 regression {pct:.2}% <= limit {limit:.2}%"),
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
                    detail: format!("throughput drop {pct:.2}% <= limit {limit:.2}%"),
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
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn percentage_helpers_are_stable() -> ::xtask::sandbox::TestResult<()> {
        assert!((percent_increase(110.0, 100.0) - 10.0).abs() < f64::EPSILON);
        assert!((percent_drop(100.0, 92.0) - 8.0).abs() < f64::EPSILON);
        assert_eq!(percent_increase(100.0, 0.0), 0.0);
        assert_eq!(percent_drop(0.0, 100.0), 0.0);
        Ok(())
    }

    #[sinex_test]
    async fn prometheus_render_contains_expected_metrics() -> ::xtask::sandbox::TestResult<()> {
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
        Ok(())
    }

    fn valid_phase_manifest() -> PhaseVerificationManifest {
        PhaseVerificationManifest {
            version: 1,
            phases: vec![PhaseVerificationPhase {
                id: "1".to_string(),
                title: "Source worker foundation".to_string(),
                issues: vec![1054, 1128],
                required_checks: vec![
                    "git diff --check".to_string(),
                    "xtask test --dry-run --all --exclude sinex-e2e-tests".to_string(),
                ],
                boundary_checks: vec!["xtask ci postgres -- xtask ci schema-only".to_string()],
                impact_gates: vec![PhaseImpactGate {
                    impact: "schema".to_string(),
                    commands: vec!["xtask docs check".to_string()],
                }],
            }],
        }
    }

    #[sinex_test]
    async fn phase_manifest_validation_accepts_supported_commands()
    -> ::xtask::sandbox::TestResult<()> {
        validate_phase_manifest(&valid_phase_manifest())?;
        Ok(())
    }

    #[sinex_test]
    async fn phase_manifest_validation_rejects_duplicate_phase_ids()
    -> ::xtask::sandbox::TestResult<()> {
        let mut manifest = valid_phase_manifest();
        manifest.phases.push(manifest.phases[0].clone());

        let error = validate_phase_manifest(&manifest).expect_err("duplicate id must fail");
        assert!(format!("{error:#}").contains("duplicate phase id"));
        Ok(())
    }

    #[sinex_test]
    async fn phase_manifest_validation_rejects_empty_required_checks()
    -> ::xtask::sandbox::TestResult<()> {
        let mut manifest = valid_phase_manifest();
        manifest.phases[0].required_checks.clear();

        let error = validate_phase_manifest(&manifest).expect_err("empty checks must fail");
        assert!(format!("{error:#}").contains("must define at least one required check"));
        Ok(())
    }

    #[sinex_test]
    async fn phase_manifest_validation_rejects_unsupported_commands()
    -> ::xtask::sandbox::TestResult<()> {
        let mut manifest = valid_phase_manifest();
        manifest.phases[0]
            .required_checks
            .push("python -m pytest".to_string());

        let error = validate_phase_manifest(&manifest).expect_err("unsupported command must fail");
        assert!(format!("{error:#}").contains("unsupported phase verification command"));
        Ok(())
    }
}
