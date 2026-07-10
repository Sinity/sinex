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
use std::process::Command;
use std::time::Duration;

/// Verify phase plans and performance contracts.
#[derive(Debug, Clone, clap::Args)]
pub struct VerifyCommand {
    #[command(subcommand)]
    pub subcommand: VerifySubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum VerifySubcommand {
    /// Compile a bounded proof-obligation manifest without executing its commands.
    Obligations {
        /// Proof-obligation IR JSON manifest.
        manifest: PathBuf,
    },
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
    /// Verify a closed Bead's AC dispositions and execute its evidence commands.
    Closure {
        /// Bead id to verify (for example, sinex-e7e9).
        bead_id: String,
        /// Emit JSON output.
        #[arg(long)]
        json: bool,
        /// Dry-run: parse and print commands without executing them.
        #[arg(long)]
        dry_run: bool,
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
    #[serde(default)]
    evidence_manifest: Vec<PhaseEvidenceManifestItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PhaseImpactGate {
    impact: String,
    commands: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PhaseEvidenceManifestItem {
    ac_id: String,
    status: String,
    evidence_kind: String,
    surface: String,
    evidence: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    artifact: Option<String>,
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
            VerifySubcommand::Obligations { manifest } => {
                crate::commands::proof_obligations::execute(manifest, ctx)
            }
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
            VerifySubcommand::Closure {
                bead_id,
                json,
                dry_run,
            } => execute_closure(bead_id, *json, *dry_run, ctx),
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
                if !phase.evidence_manifest.is_empty() {
                    println!("  evidence manifest:");
                    for item in &phase.evidence_manifest {
                        println!(
                            "    - [{}] {} {} {} :: {}",
                            item.status,
                            item.ac_id,
                            item.evidence_kind,
                            item.surface,
                            item.evidence
                        );
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
        for error in validate_phase_evidence_manifest(phase) {
            bail!(
                "phase `{}` evidence manifest row `{}`: {}",
                phase.id,
                error.ac_id.as_deref().unwrap_or("<missing-ac>"),
                error.reason
            );
        }
    }

    Ok(())
}

fn validate_phase_evidence_manifest(
    phase: &PhaseVerificationPhase,
) -> Vec<ClosureEvidenceManifestError> {
    phase
        .evidence_manifest
        .iter()
        .flat_map(|item| {
            let closure_item = ClosureEvidenceManifestItem {
                source: format!("phase:{}", phase.id),
                ac_id: item.ac_id.clone(),
                status: normalize_manifest_status(&item.status),
                evidence_kind: item.evidence_kind.clone(),
                surface: item.surface.clone(),
                evidence: item.evidence.clone(),
                command: item.command.clone(),
                artifact: item.artifact.clone(),
            };
            validate_closure_evidence_manifest(std::slice::from_ref(&closure_item))
        })
        .collect()
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

    const SUPPORTED_ROOTS: &[&str] = &["check", "test", "docs", "schema", "verify"];
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
        stress_limit: 100,
        soak_duration: 3600,
        output: Some(bench_output_dir.clone()),
        verbose: false,
        refine_top_threads: 3,
        refine_threshold_pct: 10.0,
        refine_sweep_runs: 1,
        target: args.target,
        db_pool_sizes: Vec::new(),
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

// =============================================================================
// Closure verification gate
// =============================================================================

#[derive(Debug, Clone, Serialize)]
struct ClosureCommandResult {
    command: String,
    source: String,
    exit_code: i32,
    passed: bool,
    stdout_preview: String,
    stderr_preview: String,
}

#[derive(Debug, Clone, Serialize)]
struct ClosureCommand {
    command: String,
    source: String,
}

#[derive(Debug, Clone, Serialize)]
struct ClosureMatrixItem {
    source: String,
    status: String,
    text: String,
}

#[derive(Debug, Clone, Serialize)]
struct ClosureMatrixError {
    source: String,
    status: String,
    text: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct ClosureEvidenceManifestItem {
    source: String,
    ac_id: String,
    status: String,
    evidence_kind: String,
    surface: String,
    evidence: String,
    command: Option<String>,
    artifact: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ClosureEvidenceManifestError {
    source: String,
    ac_id: Option<String>,
    reason: String,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ClosureEvidence {
    commands: Vec<ClosureCommand>,
    matrix_items: Vec<ClosureMatrixItem>,
    manifest_items: Vec<ClosureEvidenceManifestItem>,
}

#[derive(Debug, Clone, Serialize)]
struct ClosureVerificationReport {
    bead_id: String,
    bead_status: String,
    acceptance_criteria_found: usize,
    dry_run: bool,
    commands_found: usize,
    commands_run: usize,
    commands_passed: usize,
    commands_failed: usize,
    matrix_items_found: usize,
    matrix_errors: Vec<ClosureMatrixError>,
    manifest_items_found: usize,
    manifest_errors: Vec<ClosureEvidenceManifestError>,
    overall_passed: bool,
    evidence_sources: Vec<String>,
    results: Vec<ClosureCommandResult>,
}

fn execute_closure(
    bead_id: &str,
    json: bool,
    dry_run: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let payload = fetch_bead_closure_payload(bead_id)?;
    let acceptance_criteria = extract_bead_acceptance_criteria(&payload.acceptance_criteria);
    let evidence = collect_closure_evidence(&payload);
    let commands = &evidence.commands;
    let evidence_sources = evidence
        .commands
        .iter()
        .map(|command| command.source.clone())
        .chain(evidence.matrix_items.iter().map(|item| item.source.clone()))
        .chain(
            evidence
                .manifest_items
                .iter()
                .map(|item| item.source.clone()),
        )
        .collect::<Vec<_>>();

    let matrix_errors = validate_closure_matrix_items(&evidence.matrix_items);
    let mut manifest_errors = validate_closure_evidence_readiness(&evidence);
    manifest_errors.extend(validate_bead_closure_contract(
        &payload,
        &acceptance_criteria,
        &evidence,
    ));

    if commands.is_empty() && evidence.matrix_items.is_empty() && evidence.manifest_items.is_empty()
    {
        let report = ClosureVerificationReport {
            bead_id: payload.id.clone(),
            bead_status: payload.status.clone(),
            acceptance_criteria_found: acceptance_criteria.len(),
            dry_run,
            commands_found: 0,
            commands_run: 0,
            commands_passed: 0,
            commands_failed: 0,
            matrix_items_found: 0,
            matrix_errors,
            manifest_items_found: 0,
            manifest_errors,
            overall_passed: false,
            evidence_sources,
            results: Vec::new(),
        };
        if json && !ctx.is_json() {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        let mut result = CommandResult::failure(crate::output::StructuredError::new(
            "CLOSURE_VERIFICATION_MISSING_EVIDENCE",
            format!(
                "bead {}: no closure evidence manifest or verification commands found in close_reason",
                payload.id
            ),
        ))
        .with_message(format!("bead {}: closure verification missing evidence", payload.id))
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed());
        if json && !ctx.is_json() {
            result.data = None;
            result = result.with_silent();
        }
        return Ok(result);
    }

    if ctx.is_human() && !json {
        println!(
            "Bead {}: {} verification command(s), {} closure matrix item(s), {} evidence manifest item(s) found",
            payload.id,
            commands.len(),
            evidence.matrix_items.len(),
            evidence.manifest_items.len()
        );
        if dry_run {
            println!("Dry-run mode — printing evidence without executing commands:");
            for command in commands {
                println!("  [{}] $ {}", command.source, command.command);
            }
            for item in &evidence.matrix_items {
                println!("  [{}] [{}] {}", item.source, item.status, item.text);
            }
            for error in &matrix_errors {
                println!(
                    "  [{}] matrix error [{}]: {} ({})",
                    error.source, error.status, error.text, error.reason
                );
            }
            for item in &evidence.manifest_items {
                println!(
                    "  [{}] [{}] {} {} {} :: {}",
                    item.source,
                    item.status,
                    item.ac_id,
                    item.evidence_kind,
                    item.surface,
                    item.evidence
                );
            }
            for error in &manifest_errors {
                println!(
                    "  [{}] manifest error{}: {}",
                    error.source,
                    error
                        .ac_id
                        .as_ref()
                        .map(|id| format!(" ac={id}"))
                        .unwrap_or_default(),
                    error.reason
                );
            }
        }
    }

    let mut results: Vec<ClosureCommandResult> = Vec::new();

    if !dry_run && matrix_errors.is_empty() && manifest_errors.is_empty() {
        for command in commands {
            let outcome = run_shell_command(command);
            if ctx.is_human() && !json {
                let tag = if outcome.passed { "PASS" } else { "FAIL" };
                println!("[{tag}] [{}] $ {}", outcome.source, outcome.command);
                if !outcome.passed && !outcome.stderr_preview.is_empty() {
                    println!("       stderr: {}", outcome.stderr_preview);
                }
            }
            results.push(outcome);
        }
    }

    let commands_run = results.len();
    let commands_passed = results.iter().filter(|r| r.passed).count();
    let commands_failed = results.iter().filter(|r| !r.passed).count();
    let overall_passed =
        commands_failed == 0 && matrix_errors.is_empty() && manifest_errors.is_empty();

    let report = ClosureVerificationReport {
        bead_id: payload.id.clone(),
        bead_status: payload.status.clone(),
        acceptance_criteria_found: acceptance_criteria.len(),
        dry_run,
        commands_found: commands.len(),
        commands_run,
        commands_passed,
        commands_failed,
        matrix_items_found: evidence.matrix_items.len(),
        matrix_errors: matrix_errors.clone(),
        manifest_items_found: evidence.manifest_items.len(),
        manifest_errors: manifest_errors.clone(),
        overall_passed,
        evidence_sources,
        results,
    };

    if json && !ctx.is_json() {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if ctx.is_human() && !dry_run {
        println!(
            "Bead {}: {commands_passed}/{commands_run} passed{}{}",
            payload.id,
            if commands_failed > 0 {
                format!(", {commands_failed} FAILED")
            } else {
                String::new()
            },
            if matrix_errors.is_empty() && manifest_errors.is_empty() {
                String::new()
            } else {
                format!(
                    ", {} closure matrix error(s), {} evidence manifest error(s)",
                    matrix_errors.len(),
                    manifest_errors.len()
                )
            }
        );
    }

    let mut result = if overall_passed || dry_run {
        CommandResult::success()
            .with_message(format!("bead {}: closure verification passed", payload.id))
    } else {
        CommandResult::failure(crate::output::StructuredError::new(
            "CLOSURE_VERIFICATION_FAILED",
            format!(
                "bead {}: {commands_failed} verification command(s) failed, {} closure matrix error(s), {} evidence manifest error(s)",
                payload.id,
                matrix_errors.len(),
                manifest_errors.len()
            ),
        ))
        .with_message(format!("bead {}: closure verification FAILED", payload.id))
    };

    result = result
        .with_detail(format!("bead_id={}", payload.id))
        .with_detail(format!("bead_status={}", payload.status))
        .with_detail(format!("acceptance_criteria={}", acceptance_criteria.len()))
        .with_detail(format!("commands_found={}", commands.len()))
        .with_detail(format!("commands_run={commands_run}"))
        .with_detail(format!("passed={commands_passed}"))
        .with_detail(format!("failed={commands_failed}"))
        .with_detail(format!("matrix_items={}", evidence.matrix_items.len()))
        .with_detail(format!("matrix_errors={}", matrix_errors.len()))
        .with_detail(format!("manifest_items={}", evidence.manifest_items.len()))
        .with_detail(format!("manifest_errors={}", manifest_errors.len()))
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed());

    if json && !ctx.is_json() {
        result.data = None;
        result = result.with_silent();
    }

    Ok(result)
}

#[derive(Debug, Deserialize)]
struct BeadClosurePayload {
    id: String,
    status: String,
    #[serde(default)]
    acceptance_criteria: String,
    #[serde(default)]
    close_reason: String,
}

fn collect_closure_evidence(payload: &BeadClosurePayload) -> ClosureEvidence {
    let source = "close_reason";
    let mut evidence = ClosureEvidence {
        commands: extract_closure_command_entries(&payload.close_reason, source),
        matrix_items: extract_closure_matrix_items(&payload.close_reason, source),
        manifest_items: extract_closure_evidence_manifest_items(&payload.close_reason, source),
    };

    for item in &evidence.manifest_items {
        let Some(command) = item.command.as_deref() else {
            continue;
        };
        let command = command.trim().trim_matches('`').trim();
        if !looks_like_runnable_command(command) || is_closure_verifier_self_command(command) {
            continue;
        }
        if !evidence
            .commands
            .iter()
            .any(|existing| existing.command == command)
        {
            evidence.commands.push(ClosureCommand {
                command: command.to_string(),
                source: format!("{source}:manifest:{}", item.ac_id),
            });
        }
    }

    evidence
}

fn fetch_bead_closure_payload(bead_id: &str) -> Result<BeadClosurePayload> {
    validate_bead_id(bead_id)?;
    let output = Command::new("bd")
        .args(["show", bead_id, "--json"])
        .output();

    let output = match output {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!("bd CLI not found; install Beads to use `xtask verify closure`");
        }
        Err(error) => bail!("failed to invoke bd CLI: {error}"),
        Ok(output) if !output.status.success() => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("bd show {bead_id} failed: {}", stderr.trim());
        }
        Ok(output) => output,
    };

    parse_bead_closure_payload(&output.stdout, bead_id)
}

fn validate_bead_id(bead_id: &str) -> Result<()> {
    if !looks_like_bead_id(bead_id) {
        bail!("closure verification requires a Bead string id such as `sinex-e7e9`");
    }
    Ok(())
}

fn looks_like_bead_id(candidate: &str) -> bool {
    let Some((prefix, suffix)) = candidate.split_once('-') else {
        return false;
    };
    !prefix.is_empty()
        && !suffix.is_empty()
        && prefix.chars().any(|ch| ch.is_ascii_alphabetic())
        && candidate
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '.' | '_'))
}

fn parse_bead_closure_payload(bytes: &[u8], expected_id: &str) -> Result<BeadClosurePayload> {
    let mut payloads: Vec<BeadClosurePayload> = serde_json::from_slice(bytes)
        .with_context(|| "bd show output is not valid Beads JSON")?;
    if payloads.len() != 1 {
        bail!(
            "bd show {expected_id} returned {} top-level records; expected exactly one",
            payloads.len()
        );
    }
    let payload = payloads.pop().expect("length checked above");
    if payload.id != expected_id {
        bail!(
            "bd show returned bead `{}` while `{expected_id}` was requested",
            payload.id
        );
    }
    Ok(payload)
}

fn extract_closure_command_entries(body: &str, source: &str) -> Vec<ClosureCommand> {
    let mut commands: Vec<String> = Vec::new();
    let mut in_verify_section = false;
    let mut in_code_block = false;
    let mut code_lang = String::new();

    for line in body.lines() {
        let trimmed = line.trim();

        // Detect section headings that indicate verification context.
        if !in_code_block && is_closure_evidence_heading(trimmed) {
            let heading_lower = trimmed.trim_start_matches('#').trim().to_lowercase();
            in_verify_section =
                heading_lower.contains("verif") || heading_lower.contains("closure");
            continue;
        }

        // Code block start/end.
        if trimmed.starts_with("```") {
            if in_code_block {
                in_code_block = false;
                code_lang.clear();
            } else {
                in_code_block = true;
                code_lang = trimmed.trim_start_matches('`').to_lowercase();
            }
            continue;
        }

        if in_code_block && in_verify_section {
            // Accept non-comment, non-empty lines from verify-section blocks
            // whose language is bash, sh, verify, shell, or unspecified.
            let lang_ok = code_lang.is_empty()
                || code_lang == "bash"
                || code_lang == "sh"
                || code_lang == "verify"
                || code_lang == "shell";

            if lang_ok && !trimmed.is_empty() && !trimmed.starts_with('#') {
                // Strip leading `$ ` prompt if present.
                let cmd = trimmed.strip_prefix("$ ").unwrap_or(trimmed);
                // Skip prose lines that happen to live inside a fenced block.
                // Without this guard, narrative lines like
                // `git push pre-push drift guard` or bare `xtask` get treated
                // as commands and always fail, producing false-positive
                // closure regressions (#1552).
                if looks_like_runnable_command(cmd) && !is_closure_verifier_self_command(cmd) {
                    commands.push(cmd.to_string());
                }
            }
        } else if !in_code_block && in_verify_section {
            // Bare `$ command` lines outside code blocks in a verify section.
            if let Some(cmd) = trimmed.strip_prefix("$ ") {
                if !is_closure_verifier_self_command(cmd) {
                    commands.push(cmd.to_string());
                }
            } else if let Some(cmd) = extract_inline_backtick_command(trimmed) {
                if !is_closure_verifier_self_command(&cmd) {
                    commands.push(cmd);
                }
            }
        }
    }

    commands
        .into_iter()
        .map(|command| ClosureCommand {
            command,
            source: source.to_string(),
        })
        .collect()
}

fn extract_closure_evidence_manifest_items(
    body: &str,
    source: &str,
) -> Vec<ClosureEvidenceManifestItem> {
    let mut items = Vec::new();
    let mut in_manifest_section = false;
    let mut header: Option<Vec<String>> = None;

    for line in body.lines() {
        let trimmed = line.trim();

        if is_closure_evidence_heading(trimmed) {
            let heading_lower = trimmed.trim_start_matches('#').trim().to_lowercase();
            in_manifest_section = heading_lower.contains("evidence manifest")
                || heading_lower.contains("closure evidence")
                || heading_lower.contains("acceptance matrix");
            header = None;
            continue;
        }

        if !in_manifest_section || !trimmed.starts_with('|') || !trimmed.ends_with('|') {
            continue;
        }

        let cells = parse_markdown_table_cells(trimmed);
        if cells.is_empty() || is_markdown_separator_row(&cells) {
            continue;
        }

        if header.is_none() && looks_like_closure_manifest_header(&cells) {
            header = Some(cells);
            continue;
        }

        let Some(header_cells) = header.as_deref() else {
            continue;
        };
        let Some(item) = parse_closure_manifest_row(header_cells, &cells, source) else {
            continue;
        };
        items.push(item);
    }

    items
}

fn parse_markdown_table_cells(line: &str) -> Vec<String> {
    line.trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_string())
        .collect()
}

fn is_markdown_separator_row(cells: &[String]) -> bool {
    cells.iter().all(|cell| {
        cell.chars()
            .all(|ch| ch == '-' || ch == ':' || ch.is_whitespace())
    })
}

fn looks_like_closure_manifest_header(cells: &[String]) -> bool {
    let normalized = cells
        .iter()
        .map(|cell| normalize_manifest_header(cell))
        .collect::<Vec<_>>();
    normalized.iter().any(|cell| cell == "ac_id")
        && normalized.iter().any(|cell| cell == "evidence_kind")
        && normalized.iter().any(|cell| cell == "surface")
        && normalized.iter().any(|cell| cell == "evidence")
        && normalized.iter().any(|cell| cell == "status")
}

fn normalize_manifest_header(cell: &str) -> String {
    let lower = cell.trim().to_lowercase();
    match lower.as_str() {
        "ac" | "ac id" | "criterion" | "acceptance criterion" | "acceptance" => "ac_id".to_string(),
        "kind" | "evidence kind" | "evidence_kind" => "evidence_kind".to_string(),
        "behavior surface" | "surface" => "surface".to_string(),
        "proof" | "evidence" => "evidence".to_string(),
        "cmd" | "command" | "commands" => "command".to_string(),
        "artifact" | "artifacts" => "artifact".to_string(),
        "state" | "status" => "status".to_string(),
        _ => lower.replace([' ', '-'], "_"),
    }
}

fn manifest_cell<'a>(header: &'a [String], cells: &'a [String], key: &str) -> Option<&'a str> {
    header
        .iter()
        .position(|cell| normalize_manifest_header(cell) == key)
        .and_then(|index| cells.get(index))
        .map(|cell| cell.trim())
}

fn parse_closure_manifest_row(
    header: &[String],
    cells: &[String],
    source: &str,
) -> Option<ClosureEvidenceManifestItem> {
    let ac_id = manifest_cell(header, cells, "ac_id")?;
    let status = manifest_cell(header, cells, "status")?;
    let evidence_kind = manifest_cell(header, cells, "evidence_kind")?;
    let surface = manifest_cell(header, cells, "surface")?;
    let evidence = manifest_cell(header, cells, "evidence")?;

    Some(ClosureEvidenceManifestItem {
        source: source.to_string(),
        ac_id: ac_id.to_string(),
        status: normalize_manifest_status(status),
        evidence_kind: evidence_kind.to_string(),
        surface: surface.to_string(),
        evidence: evidence.to_string(),
        command: optional_manifest_cell(header, cells, "command"),
        artifact: optional_manifest_cell(header, cells, "artifact"),
    })
}

fn optional_manifest_cell(header: &[String], cells: &[String], key: &str) -> Option<String> {
    manifest_cell(header, cells, key)
        .filter(|cell| !cell.trim().is_empty() && *cell != "-")
        .map(ToString::to_string)
}

fn normalize_manifest_status(status: &str) -> String {
    let lower = status.trim().to_lowercase();
    let words = lower
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect::<BTreeSet<_>>();
    if ["satisfied", "done", "pass", "passed", "checked"]
        .iter()
        .any(|word| words.contains(word))
    {
        "satisfied".to_string()
    } else if ["required", "require"]
        .iter()
        .any(|word| words.contains(word))
    {
        "required".to_string()
    } else if ["defer", "deferred", "owner", "tracked"]
        .iter()
        .any(|word| words.contains(word))
    {
        "deferred".to_string()
    } else if words.contains("misframed") {
        "misframed".to_string()
    } else if ["fail", "failed"]
        .iter()
        .any(|word| words.contains(word))
    {
        "failed".to_string()
    } else {
        lower
    }
}

fn extract_bead_acceptance_criteria(text: &str) -> Vec<String> {
    let mut criteria: Vec<String> = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty()
            || trimmed
                .trim_start_matches('#')
                .trim()
                .eq_ignore_ascii_case("acceptance criteria")
        {
            continue;
        }

        let bullet = trimmed
            .strip_prefix("- [ ] ")
            .or_else(|| trimmed.strip_prefix("- [x] "))
            .or_else(|| trimmed.strip_prefix("- [X] "))
            .or_else(|| trimmed.strip_prefix("- "))
            .or_else(|| trimmed.strip_prefix("* "))
            .or_else(|| strip_numbered_list_prefix(trimmed));
        if let Some(criterion) = bullet {
            criteria.push(criterion.trim().to_string());
        } else if line.chars().next().is_some_and(char::is_whitespace)
            && !criteria.is_empty()
        {
            let previous = criteria.last_mut().expect("non-empty checked above");
            previous.push(' ');
            previous.push_str(trimmed);
        } else {
            criteria.push(trimmed.to_string());
        }
    }
    criteria.retain(|criterion| !criterion.is_empty());
    criteria
}

fn strip_numbered_list_prefix(line: &str) -> Option<&str> {
    let (prefix, rest) = line.split_once(". ")?;
    prefix.chars().all(|ch| ch.is_ascii_digit()).then_some(rest)
}

fn validate_bead_closure_contract(
    payload: &BeadClosurePayload,
    acceptance_criteria: &[String],
    evidence: &ClosureEvidence,
) -> Vec<ClosureEvidenceManifestError> {
    let mut errors = Vec::new();
    if payload.status != "closed" {
        errors.push(ClosureEvidenceManifestError {
            source: "bd.status".to_string(),
            ac_id: None,
            reason: format!(
                "bead status is `{}`; closure verification requires `closed`",
                payload.status
            ),
        });
    }
    if acceptance_criteria.is_empty() {
        errors.push(ClosureEvidenceManifestError {
            source: "bd.acceptance_criteria".to_string(),
            ac_id: None,
            reason: "bead has no acceptance criteria to verify".to_string(),
        });
        return errors;
    }

    let mut covered = BTreeSet::new();
    for item in &evidence.manifest_items {
        let Some(ordinal) = manifest_ac_ordinal(&item.ac_id) else {
            errors.push(manifest_error(
                item,
                "Bead closure manifest AC ids must use AC-1, AC-2, ... in acceptance_criteria order",
            ));
            continue;
        };
        if ordinal == 0 || ordinal > acceptance_criteria.len() {
            errors.push(manifest_error(
                item,
                &format!(
                    "AC ordinal {ordinal} is outside the bead's {} acceptance criteria",
                    acceptance_criteria.len()
                ),
            ));
            continue;
        }
        if !covered.insert(ordinal) {
            errors.push(manifest_error(item, "duplicate disposition for this acceptance criterion"));
        }

        match item.status.as_str() {
            "satisfied" | "checked" => {
                if item.evidence_kind.trim().eq_ignore_ascii_case("docs") {
                    if item.command.is_none() && item.artifact.is_none() {
                        errors.push(manifest_error(
                            item,
                            "satisfied docs criteria require a runnable command or named artifact",
                        ));
                    }
                } else {
                    let runnable = item.command.as_deref().is_some_and(|command| {
                        let command = command.trim().trim_matches('`').trim();
                        looks_like_runnable_command(command)
                            && !is_closure_verifier_self_command(command)
                    });
                    if !runnable {
                        errors.push(manifest_error(
                            item,
                            "satisfied non-doc Bead criteria require a runnable command",
                        ));
                    }
                }
            }
            "deferred" => {
                let follow_up = format!(
                    "{} {}",
                    item.evidence,
                    item.artifact.as_deref().unwrap_or_default()
                );
                if !contains_bead_id(&follow_up) {
                    errors.push(manifest_error(
                        item,
                        "deferred criteria must name a durable follow-up Bead",
                    ));
                }
            }
            "misframed" => {}
            _ => errors.push(manifest_error(
                item,
                "Bead criteria require an explicit Satisfied, Deferred, or Misframed disposition",
            )),
        }
    }

    for (index, criterion) in acceptance_criteria.iter().enumerate() {
        let ordinal = index + 1;
        if !covered.contains(&ordinal) {
            errors.push(ClosureEvidenceManifestError {
                source: "bd.acceptance_criteria".to_string(),
                ac_id: Some(format!("AC-{ordinal}")),
                reason: format!("missing closure manifest disposition for `{criterion}`"),
            });
        }
    }

    errors
}

fn manifest_ac_ordinal(ac_id: &str) -> Option<usize> {
    let normalized = ac_id.trim().to_ascii_lowercase();
    let suffix = normalized.strip_prefix("ac").unwrap_or(&normalized);
    suffix
        .trim_start_matches(|ch| matches!(ch, ' ' | '-' | '_' | '#' | ':'))
        .parse()
        .ok()
}

fn contains_bead_id(text: &str) -> bool {
    text.split(|ch: char| {
        !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '.' | '_'))
    })
    .any(looks_like_bead_id)
}

fn validate_closure_evidence_readiness(
    evidence: &ClosureEvidence,
) -> Vec<ClosureEvidenceManifestError> {
    let mut errors = validate_closure_evidence_manifest(&evidence.manifest_items);

    if evidence.manifest_items.is_empty()
        && (!evidence.commands.is_empty() || !evidence.matrix_items.is_empty())
    {
        let mut sources = evidence
            .commands
            .iter()
            .map(|command| command.source.as_str())
            .chain(
                evidence
                    .matrix_items
                    .iter()
                    .map(|item| item.source.as_str()),
            )
            .collect::<Vec<_>>();
        sources.sort_unstable();
        sources.dedup();

        errors.push(ClosureEvidenceManifestError {
            source: if sources.is_empty() {
                "closure evidence".to_string()
            } else {
                sources.join(", ")
            },
            ac_id: None,
            reason: "closure verification requires a Closure Evidence Manifest row mapping each satisfied AC to behavior/API/runtime/schema/test evidence"
                .to_string(),
        });
    }

    errors
}

fn validate_closure_evidence_manifest(
    items: &[ClosureEvidenceManifestItem],
) -> Vec<ClosureEvidenceManifestError> {
    let mut errors = Vec::new();
    for item in items {
        if item.ac_id.trim().is_empty() {
            errors.push(manifest_error(item, "missing AC id"));
        }
        if item.evidence_kind.trim().is_empty() {
            errors.push(manifest_error(item, "missing evidence kind"));
        }
        if item.surface.trim().is_empty() {
            errors.push(manifest_error(item, "missing behavior surface"));
        }
        if item.evidence.trim().is_empty() {
            errors.push(manifest_error(item, "missing evidence description"));
        }
        if is_satisfied_manifest_status(&item.status) {
            errors.extend(validate_satisfied_manifest_item(item));
        }
    }
    errors
}

fn is_satisfied_manifest_status(status: &str) -> bool {
    matches!(
        status,
        "checked" | "satisfied" | "done" | "passed" | "required"
    )
}

fn validate_satisfied_manifest_item(
    item: &ClosureEvidenceManifestItem,
) -> Vec<ClosureEvidenceManifestError> {
    let mut errors = Vec::new();
    let kind = item.evidence_kind.trim().to_lowercase();
    let surface = item.surface.trim().to_lowercase();
    let evidence = item.evidence.trim().to_lowercase();
    let command = item.command.as_deref().unwrap_or("").trim().to_lowercase();

    let allowed_kinds = [
        "behavior",
        "contract",
        "runtime",
        "parser",
        "privacy",
        "disclosure",
        "replay",
        "schema",
        "cli",
        "api",
        "rpc",
        "typed-boundary",
        "vm",
        "harness",
        "docs",
    ];
    if !allowed_kinds.contains(&kind.as_str()) {
        errors.push(manifest_error(
            item,
            &format!("unsupported evidence kind `{}`", item.evidence_kind),
        ));
    }

    if surface == "source" || surface == "grep" || surface == "text" {
        errors.push(manifest_error(
            item,
            "satisfied evidence must name a behavior/API/runtime/schema/test surface, not source text",
        ));
    }

    if kind != "docs" && command.starts_with("rg ") && !command.contains("xtask ") {
        errors.push(manifest_error(
            item,
            "grep-only command cannot satisfy non-doc evidence",
        ));
    }

    if kind != "docs" && evidence.contains("source text") {
        errors.push(manifest_error(
            item,
            "source-text evidence cannot satisfy non-doc behavior evidence",
        ));
    }

    errors
}

fn manifest_error(
    item: &ClosureEvidenceManifestItem,
    reason: &str,
) -> ClosureEvidenceManifestError {
    ClosureEvidenceManifestError {
        source: item.source.clone(),
        ac_id: (!item.ac_id.trim().is_empty()).then(|| item.ac_id.clone()),
        reason: reason.to_string(),
    }
}

fn validate_closure_matrix_items(items: &[ClosureMatrixItem]) -> Vec<ClosureMatrixError> {
    items
        .iter()
        .filter_map(|item| closure_matrix_status_error(item))
        .collect()
}

fn closure_matrix_status_error(item: &ClosureMatrixItem) -> Option<ClosureMatrixError> {
    let status = item.status.trim().to_lowercase();
    let reason = match status.as_str() {
        "checked" | "satisfied" | "done" | "passed" | "deferred" | "misframed" => return None,
        "" => "closure matrix item has no status",
        "unchecked" => "unchecked acceptance criterion is not closed",
        "failed" | "fail" => "failed acceptance criterion is not closed",
        "required" => "required acceptance criterion still needs evidence or explicit deferral",
        "todo" | "open" | "missing" => "open acceptance criterion is not closed",
        "noted" => {
            "unrecognized matrix table status; use Satisfied, Deferred, Misframed, or Failed"
        }
        _ => "unrecognized matrix status; use a supported closed/deferred status",
    };
    Some(ClosureMatrixError {
        source: item.source.clone(),
        status: item.status.clone(),
        text: item.text.clone(),
        reason: reason.to_string(),
    })
}

fn extract_closure_matrix_items(body: &str, source: &str) -> Vec<ClosureMatrixItem> {
    let mut items = Vec::new();
    let mut in_matrix_section = false;

    for line in body.lines() {
        let trimmed = line.trim();

        if is_closure_evidence_heading(trimmed) {
            let heading_lower = trimmed.trim_start_matches('#').trim().to_lowercase();
            in_matrix_section = heading_lower.contains("acceptance")
                || heading_lower.contains("closure")
                || heading_lower.contains("criteria drift");
            continue;
        }

        if !in_matrix_section {
            continue;
        }

        let Some((status, text)) = parse_closure_matrix_line(trimmed) else {
            continue;
        };

        items.push(ClosureMatrixItem {
            source: source.to_string(),
            status,
            text,
        });
    }

    items
}

fn is_closure_evidence_heading(line: &str) -> bool {
    if line.starts_with('#') {
        return true;
    }

    let lower = line.trim_end_matches(':').to_lowercase();
    lower == "verification"
        || lower == "verification run"
        || lower == "verification commands"
        || lower == "closure verification"
        || lower == "closure verification commands"
        || lower == "acceptance matrix"
        || lower == "acceptance criteria drift"
        || lower.starts_with("closeout")
}

fn extract_inline_backtick_command(line: &str) -> Option<String> {
    let (_, rest) = line.split_once('`')?;
    let (candidate, _) = rest.split_once('`')?;
    let candidate = candidate.trim();
    if looks_like_shell_command(candidate) {
        Some(candidate.to_string())
    } else {
        None
    }
}

fn looks_like_shell_command(candidate: &str) -> bool {
    candidate.starts_with("xtask ")
        || candidate == "xtask"
        || candidate.starts_with("git ")
        || candidate.starts_with("gh ")
        || candidate.starts_with("bd ")
        || candidate.starts_with("rg ")
        || candidate.starts_with("nix ")
        || candidate.starts_with("SINEX_")
}

/// Stricter form of `looks_like_shell_command` used when scanning lines inside
/// fenced code blocks for closure-verification commands. A bare `xtask` or
/// other zero-argument invocation is almost certainly prose that happened to
/// land inside a fence; require at least a subcommand or argument. Surfaced by
/// #1552 — bare `xtask` and prose lines like `git push pre-push drift guard`
/// kept producing false-positive closure regressions.
fn looks_like_runnable_command(candidate: &str) -> bool {
    let parts: Vec<&str> = candidate.split_whitespace().collect();
    if parts.is_empty() {
        return false;
    }
    let head = parts[0];
    // For env-prefixed forms (SINEX_FOO=bar xtask ...), skip the prefix tokens
    // and re-check the first real command token.
    let cmd_idx = parts.iter().position(|tok| !tok.contains('='));
    let cmd = match cmd_idx.and_then(|i| parts.get(i)) {
        Some(c) => *c,
        None => return false,
    };
    let cmd_args = parts.len() - cmd_idx.unwrap_or(0) - 1;
    if cmd_args == 0 {
        // Bare command (e.g. `xtask`) — almost always prose at the start of a
        // sentence. Reject. Exception: simple non-xtask commands like `gh`,
        // `git` that print useful help are still useless for closure replay,
        // so reject those too.
        return false;
    }
    match cmd {
        "xtask" | "sinexctl" | "git" | "gh" | "bd" | "rg" | "nix" | "psql" | "nats" => {
            true
        }
        _ => looks_like_shell_command(head),
    }
}

fn is_closure_verifier_self_command(candidate: &str) -> bool {
    let parts: Vec<&str> = candidate.split_whitespace().collect();
    let Some(cmd_idx) = parts.iter().position(|tok| !tok.contains('=')) else {
        return false;
    };
    matches!(
        parts.get(cmd_idx..cmd_idx + 3),
        Some(["xtask", "verify", "closure"])
    )
}

fn parse_closure_matrix_line(line: &str) -> Option<(String, String)> {
    let body = line
        .strip_prefix("- ")
        .or_else(|| line.strip_prefix("* "))
        .unwrap_or(line)
        .trim();

    if let Some(item) = parse_markdown_closure_matrix_row(body) {
        return Some(item);
    }

    if let Some(rest) = body
        .strip_prefix("[x] ")
        .or_else(|| body.strip_prefix("[X] "))
    {
        return Some(("checked".to_string(), rest.trim().to_string()));
    }
    if let Some(rest) = body.strip_prefix("[ ] ") {
        let text = rest.trim();
        let lower = text.to_lowercase();
        let status = if lower.contains("defer")
            || lower.contains("tracked")
            || lower.contains("owner")
            || lower.contains("follow-up")
            || lower.contains("out-of-scope")
        {
            "deferred"
        } else if lower.contains("misframed") {
            "misframed"
        } else {
            "unchecked"
        };
        return Some((status.to_string(), text.to_string()));
    }
    if let Some(rest) = body.strip_prefix('✅') {
        return Some(("satisfied".to_string(), rest.trim().to_string()));
    }
    if let Some(rest) = body.strip_prefix('⏭') {
        return Some(("deferred".to_string(), rest.trim().to_string()));
    }
    if let Some(rest) = body.strip_prefix('❌') {
        return Some(("failed".to_string(), rest.trim().to_string()));
    }

    None
}

fn parse_markdown_closure_matrix_row(line: &str) -> Option<(String, String)> {
    if !line.starts_with('|') || !line.ends_with('|') {
        return None;
    }

    let cells: Vec<String> = parse_markdown_table_cells(line);
    if cells.len() < 2 {
        return None;
    }

    let lower_cells = cells
        .iter()
        .map(|cell| cell.to_lowercase())
        .collect::<Vec<_>>();
    if is_markdown_separator_row(&lower_cells) {
        return None;
    }
    if lower_cells.iter().any(|cell| cell == "status")
        && lower_cells
            .iter()
            .any(|cell| cell.contains("acceptance") || cell == "ac")
    {
        return None;
    }

    let status_cell = cells.last()?.trim();
    let status_lower = status_cell.to_lowercase();
    let status = if status_lower.contains("satisfied")
        || status_lower.contains("done")
        || status_lower.contains("fixed")
        || status_lower.contains('✅')
    {
        "satisfied"
    } else if status_lower.contains("defer")
        || status_lower.contains("tracked")
        || status_lower.contains("owner")
        || status_lower.contains("out-of-scope")
    {
        "deferred"
    } else if status_lower.contains("misframed") {
        "misframed"
    } else if status_lower.contains("fail") || status_lower.contains('❌') {
        "failed"
    } else {
        "noted"
    };

    let text = cells
        .iter()
        .take(cells.len().saturating_sub(1))
        .filter(|cell| !cell.is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join(" | ");

    if text.is_empty() {
        return None;
    }

    Some((status.to_string(), text))
}

/// Run a single shell command and capture its outcome.
fn run_shell_command(command: &ClosureCommand) -> ClosureCommandResult {
    let result = Command::new("sh").args(["-c", &command.command]).output();

    match result {
        Err(e) => ClosureCommandResult {
            command: command.command.clone(),
            source: command.source.clone(),
            exit_code: -1,
            passed: false,
            stdout_preview: String::new(),
            stderr_preview: e.to_string(),
        },
        Ok(out) => {
            let exit_code = out.status.code().unwrap_or(-1);
            let passed = out.status.success();
            let stdout_preview = preview_output(&out.stdout, 200);
            let stderr_preview = preview_output(&out.stderr, 200);
            ClosureCommandResult {
                command: command.command.clone(),
                source: command.source.clone(),
                exit_code,
                passed,
                stdout_preview,
                stderr_preview,
            }
        }
    }
}

/// Truncate raw bytes to a UTF-8 preview string, replacing invalid chars.
fn preview_output(bytes: &[u8], max_chars: usize) -> String {
    let s = String::from_utf8_lossy(bytes);
    let s = s.trim();
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_chars).collect();
        format!("{truncated}…")
    }
}

#[cfg(test)]
#[path = "verify_test.rs"]
mod tests;
