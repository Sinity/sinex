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
    /// Source-worker evidence gate: NixOS binding drift, parser registration,
    /// and privacy invocation.
    SourceWorker {
        /// Path to the JSON file exported by
        /// `config.services.sinex.sources.exportedJson` (from the NixOS module).
        ///
        /// When provided the binding-drift check compares Rust descriptor IDs
        /// against the live host configuration rather than the static example
        /// module.  Obtain the path with:
        ///
        ///   nix eval --raw \
        ///     .#nixosConfigurations.sinnix-prime.config.services.sinex.sources.exportedJson
        ///
        /// Default (absent): warn-only comparison against the static
        /// `nixos/modules/source-bindings.nix` example block.
        #[arg(long)]
        bindings_json: Option<PathBuf>,
        /// Emit JSON output.
        #[arg(long)]
        json: bool,
    },
    /// Operationalize the 2026-05-11 closure-verification policy: fetch an
    /// issue body via `gh`, extract AC checkboxes and shell code blocks marked
    /// `verify`, and run each command, reporting pass/fail per command.
    Closure {
        /// GitHub issue number to verify.
        issue: u64,
        /// Emit JSON output.
        #[arg(long)]
        json: bool,
        /// Dry-run: parse and print commands without executing them.
        #[arg(long)]
        dry_run: bool,
    },
    /// Summarize executable verification claims, runner commands, and deferrals.
    Claims {
        /// Emit JSON output.
        #[arg(long)]
        json: bool,
        /// Show advisory obligations as well as required obligations.
        #[arg(long)]
        advisory: bool,
        /// Include deferred obligations and exemptions.
        #[arg(long)]
        deferrals: bool,
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
            VerifySubcommand::SourceWorker {
                bindings_json,
                json,
            } => execute_source_worker(bindings_json.as_deref(), *json, ctx),
            VerifySubcommand::Closure {
                issue,
                json,
                dry_run,
            } => execute_closure(*issue, *json, *dry_run, ctx).await,
            VerifySubcommand::Claims {
                json,
                advisory,
                deferrals,
            } => execute_claims(*json, *advisory, *deferrals, ctx),
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

#[derive(Debug, Serialize)]
struct ClaimVerificationSummary {
    schema_version: u32,
    required: Vec<ClaimVerificationItem>,
    advisory: Vec<ClaimVerificationItem>,
    deferred: Vec<ClaimVerificationItem>,
    exemptions: Vec<ClaimExemptionItem>,
    errors: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ClaimVerificationItem {
    obligation_id: String,
    claim_id: String,
    subject: String,
    runner_binding_id: String,
    command: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct ClaimExemptionItem {
    exemption_id: String,
    subject: String,
    obligation_id: String,
    reason: String,
    expires: Option<String>,
}

fn execute_claims(
    json: bool,
    include_advisory: bool,
    include_deferrals: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let catalog = crate::proof_catalog::build_proof_catalog(&workspace_root())?;
    let validation = crate::proof_catalog::validate_proof_catalog(&catalog);
    let runner_by_id = catalog
        .runner_bindings
        .iter()
        .map(|runner| (runner.id, runner.command))
        .collect::<BTreeMap<_, _>>();

    let mut required = Vec::new();
    let mut advisory = Vec::new();
    let mut deferred = Vec::new();

    for obligation in &catalog.obligations {
        let command = runner_by_id
            .get(obligation.runner_binding_id)
            .copied()
            .unwrap_or("<missing-runner-command>");
        let item = ClaimVerificationItem {
            obligation_id: obligation.id.to_string(),
            claim_id: obligation.claim_id.to_string(),
            subject: obligation.subject.as_str().to_string(),
            runner_binding_id: obligation.runner_binding_id.to_string(),
            command: command.to_string(),
            reason: obligation.reason.to_string(),
        };

        match obligation.level {
            sinex_primitives::ProofObligationLevel::Required => required.push(item),
            sinex_primitives::ProofObligationLevel::Advisory => advisory.push(item),
            sinex_primitives::ProofObligationLevel::Deferred => deferred.push(item),
        }
    }

    let exemptions = catalog
        .exemptions
        .iter()
        .map(|exemption| ClaimExemptionItem {
            exemption_id: exemption.id.to_string(),
            subject: exemption.subject.as_str().to_string(),
            obligation_id: exemption.obligation_id.to_string(),
            reason: exemption.reason.to_string(),
            expires: exemption.expires.map(str::to_string),
        })
        .collect::<Vec<_>>();

    let summary = ClaimVerificationSummary {
        schema_version: catalog.schema_version,
        required,
        advisory,
        deferred,
        exemptions,
        errors: validation.errors,
        warnings: visible_claim_warnings(
            validation.warnings,
            include_advisory || include_deferrals,
        ),
    };

    let summary_data = serde_json::to_value(&summary)?;

    if json && !ctx.is_json() {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else if !json && ctx.is_human() {
        println!("Proof claims");
        println!("  required:  {}", summary.required.len());
        println!("  advisory:  {}", summary.advisory.len());
        println!("  deferred:  {}", summary.deferred.len());
        println!("  exemptions: {}", summary.exemptions.len());
        println!("  errors:    {}", summary.errors.len());
        println!("  warnings:  {}", summary.warnings.len());
        for item in &summary.required {
            println!(
                "  required  {}  {}  {}",
                item.obligation_id, item.claim_id, item.command
            );
        }
        if include_advisory {
            for item in &summary.advisory {
                println!(
                    "  advisory  {}  {}  {}",
                    item.obligation_id, item.claim_id, item.command
                );
            }
        }
        if include_deferrals {
            for item in &summary.deferred {
                println!(
                    "  deferred  {}  {}  {}",
                    item.obligation_id, item.claim_id, item.command
                );
            }
            for item in &summary.exemptions {
                println!(
                    "  exempt    {}  {}  {}",
                    item.exemption_id, item.obligation_id, item.reason
                );
            }
        }
    }

    let result = if summary.errors.is_empty() {
        CommandResult::success().with_message("verification claim catalog is valid")
    } else {
        CommandResult::failure(crate::output::StructuredError::new(
            "PROOF_CLAIMS_INVALID",
            "verification claim catalog has errors",
        ))
        .with_message("verification claim catalog has errors")
    };

    if ctx.is_json() {
        Ok(result.with_data(summary_data))
    } else if json {
        Ok(result.with_silent())
    } else {
        Ok(result)
    }
}

fn visible_claim_warnings(warnings: Vec<String>, include_demoted_detail: bool) -> Vec<String> {
    if include_demoted_detail {
        return warnings;
    }
    warnings
        .into_iter()
        .filter(|warning| !is_local_source_unit_tag_warning(warning))
        .collect()
}

fn is_local_source_unit_tag_warning(warning: &str) -> bool {
    warning.contains(" local source-unit verification tag")
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
        allow_contended_host: false,
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
// A3.1 — source-worker integrity gate
// =============================================================================

/// Outcome of a single source-worker integrity check.
#[derive(Debug, Clone, Serialize)]
struct SwCheck {
    name: &'static str,
    status: SwCheckStatus,
    detail: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    items: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SwCheckStatus {
    Pass,
    Warn,
    Fail,
}

impl SwCheck {
    fn pass(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: SwCheckStatus::Pass,
            detail: detail.into(),
            items: Vec::new(),
        }
    }

    fn warn(name: &'static str, detail: impl Into<String>, items: Vec<String>) -> Self {
        Self {
            name,
            status: SwCheckStatus::Warn,
            detail: detail.into(),
            items,
        }
    }

    fn fail(name: &'static str, detail: impl Into<String>, items: Vec<String>) -> Self {
        Self {
            name,
            status: SwCheckStatus::Fail,
            detail: detail.into(),
            items,
        }
    }

    fn is_fail(&self) -> bool {
        self.status == SwCheckStatus::Fail
    }
}

#[derive(Debug, Clone, Serialize)]
struct SourceWorkerReport {
    overall: SwCheckStatus,
    checks: Vec<SwCheck>,
}

fn execute_source_worker(
    bindings_json: Option<&Path>,
    json: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let root = workspace_root();
    let mut checks: Vec<SwCheck> = Vec::new();

    // A3.1.1 — SourceUnitDescriptor inventory vs NixOS source-bindings drift
    checks.push(check_sw_binding_drift(&root, bindings_json));

    // A3.1.2 — Registered parsers smoke
    checks.push(check_sw_registered_parsers(&root));

    // A3.1.3 — Privacy invocation: every Sensitive/Secret source unit must invoke
    // the privacy engine or declare an explicit escape hatch.
    checks.push(check_sw_privacy_invocation(&root));

    let overall = if checks.iter().any(SwCheck::is_fail) {
        SwCheckStatus::Fail
    } else if checks.iter().any(|c| c.status == SwCheckStatus::Warn) {
        SwCheckStatus::Warn
    } else {
        SwCheckStatus::Pass
    };

    let report = SourceWorkerReport {
        overall: overall.clone(),
        checks: checks.clone(),
    };

    if json && !ctx.is_json() {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if !json && ctx.is_human() {
        for check in &checks {
            let tag = match check.status {
                SwCheckStatus::Pass => "PASS",
                SwCheckStatus::Warn => "WARN",
                SwCheckStatus::Fail => "FAIL",
            };
            println!("[{tag}] {} — {}", check.name, check.detail);
            for item in &check.items {
                println!("       {item}");
            }
        }
    }

    // Warnings are advisory by design: they surface pre-existing drift (e.g. rust-only
    // source units without NixOS bindings) that the PR being verified did not introduce.
    // Returning Partial here would make every PR fail CI as soon as any drift accumulates
    // anywhere in the source-unit catalog, defeating the gate's purpose. Only true Fail
    // states (registered parser or privacy invocation regressions, plus live binding drift
    // when --bindings-json is supplied) block the PR.
    let mut result = match &overall {
        SwCheckStatus::Pass => {
            CommandResult::success().with_message("source-worker integrity: all checks passed")
        }
        SwCheckStatus::Warn => CommandResult::success()
            .with_message("source-worker integrity: warnings present (advisory)"),
        SwCheckStatus::Fail => CommandResult::failure(crate::output::StructuredError::new(
            "SOURCE_WORKER_INTEGRITY",
            "source-worker integrity: one or more checks failed",
        ))
        .with_message("source-worker integrity: FAILED"),
    };

    result = result
        .with_detail(format!("checks={}", checks.len()))
        .with_detail(format!(
            "failed={}",
            checks.iter().filter(|c| c.is_fail()).count()
        ))
        .with_detail(format!(
            "warned={}",
            checks
                .iter()
                .filter(|c| c.status == SwCheckStatus::Warn)
                .count()
        ))
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed());

    if json && !ctx.is_json() {
        result.data = None;
        result = result.with_silent();
    }

    Ok(result)
}

/// A3.1.1 — SourceUnitDescriptor inventory vs NixOS source-bindings drift.
///
/// Two modes:
///
/// **Static (default):** extracts `sourceUnitId = "..."` string literals from
/// `nixos/modules/source-bindings.nix`. This works without a running Nix
/// evaluation but only covers the example/option doc block, not the live host
/// configuration.
///
/// **Live (--bindings-json <path>):** parses the JSON exported by the NixOS
/// option `config.services.sinex.sources.exportedJson` (a `pkgs.writeText`
/// derivation). This reflects the actual host configuration.  Obtain the path:
///
/// ```text
/// nix eval --raw \
///   .#nixosConfigurations.sinnix-prime.config.services.sinex.sources.exportedJson
/// ```
///
/// In live mode the check fails on rust-only IDs (descriptors without a host
/// binding) and warns on nix-only IDs (host bindings without a Rust
/// descriptor). Static mode only checks the Nix module's examples for unresolved
/// descriptor references; it is not a host inventory and should not report every
/// descriptor as rust-only.
fn check_sw_binding_drift(root: &Path, bindings_json: Option<&Path>) -> SwCheck {
    // Collect Nix-side source-unit IDs.
    let (nix_ids, mode_label): (BTreeSet<String>, &str) = if let Some(json_path) = bindings_json {
        // Live mode: parse the exported JSON.
        let json_src = match fs::read_to_string(json_path) {
            Ok(s) => s,
            Err(e) => {
                return SwCheck::fail(
                    "binding_drift",
                    format!("cannot read bindings JSON {}: {e}", json_path.display()),
                    Vec::new(),
                );
            }
        };
        let parsed: serde_json::Value = match serde_json::from_str(&json_src) {
            Ok(v) => v,
            Err(e) => {
                return SwCheck::fail(
                    "binding_drift",
                    format!("cannot parse bindings JSON {}: {e}", json_path.display()),
                    Vec::new(),
                );
            }
        };
        // Shape: { "bindings": [{ "sourceUnitId": "..." | null, ... }] }
        let ids: BTreeSet<String> = parsed["bindings"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|b| b["sourceUnitId"].as_str())
            .filter(|s| !s.is_empty())
            .map(std::string::ToString::to_string)
            .collect();
        (ids, "live host config")
    } else {
        // Static mode: grep the module file for `sourceUnitId = "..."`.
        let bindings_nix = root.join("nixos/modules/source-bindings.nix");
        let nix_source = match fs::read_to_string(&bindings_nix) {
            Ok(s) => s,
            Err(e) => {
                return SwCheck::fail(
                    "binding_drift",
                    format!("cannot read {}: {e}", bindings_nix.display()),
                    Vec::new(),
                );
            }
        };
        let value_pattern =
            regex::Regex::new(r#"sourceUnitId\s*=\s*"([^"]+)""#).expect("static regex is valid");
        let ids: BTreeSet<String> = value_pattern
            .captures_iter(&nix_source)
            .map(|cap| cap[1].to_string())
            .collect();
        (ids, "static module example")
    };

    // Collect from compile-time SourceUnitDescriptor inventory.
    let rust_ids: BTreeSet<String> = sinex_primitives::proof::all_source_units()
        .map(|d| d.id.to_string())
        .collect();

    let nix_only: Vec<String> = nix_ids.difference(&rust_ids).cloned().collect();
    let rust_only: Vec<String> = if bindings_json.is_some() {
        rust_ids.difference(&nix_ids).cloned().collect()
    } else {
        Vec::new()
    };

    if nix_only.is_empty() && rust_only.is_empty() {
        return SwCheck::pass(
            "binding_drift",
            format!(
                "{} source-unit IDs matched between {} and Rust descriptors",
                nix_ids.len(),
                mode_label
            ),
        );
    }

    let mut items = Vec::new();
    for id in &nix_only {
        items.push(format!("nix-only (no Rust descriptor): {id}"));
    }
    for id in &rust_only {
        items.push(format!("rust-only (no NixOS binding): {id}"));
    }

    if bindings_json.is_some() && !rust_only.is_empty() {
        // Live mode: rust-only is a hard failure (descriptor without host binding).
        SwCheck::fail(
            "binding_drift",
            format!(
                "{} nix-only, {} rust-only (live host drift)",
                nix_only.len(),
                rust_only.len()
            ),
            items,
        )
    } else {
        // Static mode (or live mode with no rust-only): warn-only.
        SwCheck::warn(
            "binding_drift",
            format!(
                "{} nix-only, {} rust-only ({})",
                nix_only.len(),
                rust_only.len(),
                mode_label
            ),
            items,
        )
    }
}

/// A3.1.2 — Registered parsers smoke: list every `register_parser!` call in
/// the workspace and surface source_unit_id + parser type for drift visibility.
fn check_sw_registered_parsers(root: &Path) -> SwCheck {
    // Static grep across crate/core/sinex-source-worker and crate/lib/sinex-node-sdk.
    let search_roots = [
        root.join("crate/core/sinex-source-worker"),
        root.join("crate/lib/sinex-node-sdk"),
    ];

    let register_pattern =
        regex::Regex::new(r#"register_parser!\s*\(\s*"([^"]+)"\s*,\s*(\w+)\s*\)"#)
            .expect("static regex");

    let mut registrations: Vec<String> = Vec::new();

    for search_root in &search_roots {
        scan_rs_files_for_pattern(search_root, &register_pattern, &mut registrations);
    }

    if registrations.is_empty() {
        SwCheck::warn(
            "registered_parsers",
            "no register_parser! calls found in source-worker or sdk",
            Vec::new(),
        )
    } else {
        SwCheck::pass(
            "registered_parsers",
            format!("{} parser registration(s) found", registrations.len()),
        )
    }
}

/// Walk `.rs` files under `dir`, extract capture group 1 and 2 from `pattern`
/// as `"source_unit_id -> TypeName"` strings, push to `out`.
fn scan_rs_files_for_pattern(dir: &Path, pattern: &regex::Regex, out: &mut Vec<String>) {
    let Ok(rd) = fs::read_dir(dir) else { return };
    for entry in rd.filter_map(std::result::Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            scan_rs_files_for_pattern(&path, pattern, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let Ok(contents) = fs::read_to_string(&path) else {
                continue;
            };
            for cap in pattern.captures_iter(&contents) {
                let source_unit_id = &cap[1];
                let parser_type = &cap[2];
                out.push(format!("{source_unit_id} -> {parser_type}"));
            }
        }
    }
}

/// A3.1.3 — Privacy invocation: every `register_source_unit!` block that declares
/// a non-Public privacy tier must invoke the privacy engine in the same file.
///
/// Scanning targets: the entire `crate/core/sinex-source-worker/src/` tree and
/// `crate/lib/sinex-node-sdk/src/parser/` (where parsers may live after the fold).
///
/// Indicators (any one satisfies the gate):
/// - `privacy::engine(`
/// - `privacy::process(`
/// - `privacy::process_json(`
/// - `ProcessingContext::` (imperative parsers that use a context variant)
/// - `default_privacy_context =` (declarative `#[source_record]` DSL attribute)
/// - `#[allow(missing_privacy_invocation` (explicit escape hatch)
fn check_sw_privacy_invocation(root: &Path) -> SwCheck {
    const NON_PUBLIC_TIERS: &[&str] = &[
        "PrivacyTier::Sensitive",
        "PrivacyTier::Secret",
        "SuPrivacyTier::Sensitive",
        "SuPrivacyTier::Secret",
    ];
    const PRIVACY_INDICATORS: &[&str] = &[
        "privacy::engine(",
        "privacy::process(",
        "privacy::process_json(",
        "ProcessingContext::",
        "default_privacy_context =",
        "#[allow(missing_privacy_invocation",
    ];

    let search_roots = [
        root.join("crate/core/sinex-source-worker/src"),
        root.join("crate/lib/sinex-node-sdk/src/parser"),
    ];

    let mut violations: Vec<String> = Vec::new();

    for search_root in &search_roots {
        collect_privacy_violations(
            search_root,
            NON_PUBLIC_TIERS,
            PRIVACY_INDICATORS,
            &mut violations,
        );
    }

    if violations.is_empty() {
        SwCheck::pass(
            "privacy_invocation",
            "all non-Public source units invoke the privacy engine",
        )
    } else {
        SwCheck::fail(
            "privacy_invocation",
            format!(
                "{} file(s) have non-Public source units without a privacy invocation",
                violations.len()
            ),
            violations,
        )
    }
}

/// Walk `.rs` files under `dir` and collect privacy-gate violations.
///
/// Only the `crate/core/sinex-source-worker/` and `crate/lib/sinex-node-sdk/src/parser/`
/// trees are scanned — not the whole workspace. Descriptor-only source units
/// (e.g. blob-storage in sinex-primitives) are only registered there to describe
/// infra-internal event types and are not caught by this gate.
fn collect_privacy_violations(
    dir: &Path,
    non_public_tiers: &[&str],
    privacy_indicators: &[&str],
    out: &mut Vec<String>,
) {
    let Ok(rd) = fs::read_dir(dir) else { return };
    for entry in rd.filter_map(std::result::Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            collect_privacy_violations(&path, non_public_tiers, privacy_indicators, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let Ok(contents) = fs::read_to_string(&path) else {
                continue;
            };

            // Only examine files that register a source unit.
            if !contents.contains("register_source_unit!") {
                continue;
            }

            // Only flag files with a non-Public privacy tier.
            let has_non_public = non_public_tiers.iter().any(|t| contents.contains(t));
            if !has_non_public {
                continue;
            }

            // Pass if any privacy indicator is present.
            let has_invocation = privacy_indicators.iter().any(|ind| contents.contains(ind));
            if has_invocation {
                continue;
            }

            // Also check siblings in the same directory (lib.rs + sibling pattern).
            let has_sibling_invocation = path
                .parent()
                .and_then(|parent| fs::read_dir(parent).ok())
                .is_some_and(|rd| {
                    rd.filter_map(std::result::Result::ok)
                        .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
                        .any(|e| {
                            fs::read_to_string(e.path())
                                .is_ok_and(|c| privacy_indicators.iter().any(|ind| c.contains(ind)))
                        })
                });

            if has_sibling_invocation {
                continue;
            }

            // Extract id for the error message.
            let id = extract_unit_id_from_contents(&contents);
            out.push(format!(
                "{}: source unit '{}' has non-Public privacy tier but no privacy invocation \
                 (add privacy::engine(, ProcessingContext::, default_privacy_context =, or \
                 #[allow(missing_privacy_invocation, reason = \"...\")])",
                path.display(),
                id,
            ));
        }
    }
}

/// Extract the first `id: "..."` value from a source-unit descriptor block.
fn extract_unit_id_from_contents(contents: &str) -> String {
    for line in contents.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("id:") {
            let rest = rest.trim();
            if let Some(inner) = rest.strip_prefix('"')
                && let Some(id) = inner.split('"').next()
                && !id.is_empty()
            {
                return id.to_string();
            }
        }
    }
    "<unknown>".to_string()
}

// =============================================================================
// A3.2 — closure verification gate
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

#[derive(Debug, Clone, Default, Serialize)]
struct ClosureEvidence {
    commands: Vec<ClosureCommand>,
    matrix_items: Vec<ClosureMatrixItem>,
}

#[derive(Debug, Clone, Serialize)]
struct ClosureVerificationReport {
    issue: u64,
    dry_run: bool,
    commands_found: usize,
    commands_run: usize,
    commands_passed: usize,
    commands_failed: usize,
    matrix_items_found: usize,
    overall_passed: bool,
    evidence_sources: Vec<String>,
    results: Vec<ClosureCommandResult>,
}

async fn execute_closure(
    issue: u64,
    json: bool,
    dry_run: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let evidence = fetch_closure_evidence(issue)?;
    let commands = &evidence.commands;
    let evidence_sources = evidence
        .commands
        .iter()
        .map(|command| command.source.clone())
        .chain(evidence.matrix_items.iter().map(|item| item.source.clone()))
        .collect::<Vec<_>>();

    if commands.is_empty() && evidence.matrix_items.is_empty() {
        let report = ClosureVerificationReport {
            issue,
            dry_run,
            commands_found: 0,
            commands_run: 0,
            commands_passed: 0,
            commands_failed: 0,
            matrix_items_found: 0,
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
                "issue #{issue}: no verification commands or closure matrix found in issue body \
                 or comments"
            ),
        ))
        .with_message(format!(
            "issue #{issue}: closure verification missing evidence"
        ))
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
            "Issue #{issue}: {} verification command(s), {} closure matrix item(s) found",
            commands.len(),
            evidence.matrix_items.len()
        );
        if dry_run {
            println!("Dry-run mode — printing evidence without executing commands:");
            for command in commands {
                println!("  [{}] $ {}", command.source, command.command);
            }
            for item in &evidence.matrix_items {
                println!("  [{}] [{}] {}", item.source, item.status, item.text);
            }
        }
    }

    let mut results: Vec<ClosureCommandResult> = Vec::new();

    if !dry_run {
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
    let overall_passed = commands_failed == 0;

    let report = ClosureVerificationReport {
        issue,
        dry_run,
        commands_found: commands.len(),
        commands_run,
        commands_passed,
        commands_failed,
        matrix_items_found: evidence.matrix_items.len(),
        overall_passed,
        evidence_sources,
        results,
    };

    if json && !ctx.is_json() {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if ctx.is_human() && !dry_run {
        println!(
            "Issue #{issue}: {commands_passed}/{commands_run} passed{}",
            if commands_failed > 0 {
                format!(", {commands_failed} FAILED")
            } else {
                String::new()
            }
        );
    }

    let mut result = if overall_passed || dry_run {
        CommandResult::success()
            .with_message(format!("issue #{issue}: closure verification passed"))
    } else {
        CommandResult::failure(crate::output::StructuredError::new(
            "CLOSURE_VERIFICATION_FAILED",
            format!("issue #{issue}: {commands_failed} verification command(s) failed"),
        ))
        .with_message(format!("issue #{issue}: closure verification FAILED"))
    };

    result = result
        .with_detail(format!("issue={issue}"))
        .with_detail(format!("commands_found={}", commands.len()))
        .with_detail(format!("commands_run={commands_run}"))
        .with_detail(format!("passed={commands_passed}"))
        .with_detail(format!("failed={commands_failed}"))
        .with_detail(format!("matrix_items={}", evidence.matrix_items.len()))
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed());

    if json && !ctx.is_json() {
        result.data = None;
        result = result.with_silent();
    }

    Ok(result)
}

#[derive(Debug, Deserialize)]
struct ClosureIssuePayload {
    #[serde(default)]
    body: String,
    #[serde(default)]
    comments: Vec<ClosureIssueComment>,
}

#[derive(Debug, Deserialize)]
struct ClosureIssueComment {
    #[serde(default)]
    body: String,
    #[serde(rename = "createdAt")]
    created_at: String,
}

/// Fetch closure verification evidence from a GitHub issue body and comments.
fn fetch_closure_evidence(issue: u64) -> Result<ClosureEvidence> {
    let payload = fetch_issue_closure_payload(issue)?;
    Ok(collect_closure_evidence(&payload))
}

fn collect_closure_evidence(payload: &ClosureIssuePayload) -> ClosureEvidence {
    let mut evidence = ClosureEvidence::default();
    evidence
        .commands
        .extend(extract_closure_command_entries(&payload.body, "body"));
    evidence
        .matrix_items
        .extend(extract_closure_matrix_items(&payload.body, "body"));

    for (index, comment) in payload.comments.iter().enumerate() {
        let source = if comment.created_at.is_empty() {
            format!("comment[{index}]")
        } else {
            format!("comment[{index}]@{}", comment.created_at)
        };
        evidence
            .commands
            .extend(extract_closure_command_entries(&comment.body, &source));
        evidence
            .matrix_items
            .extend(extract_closure_matrix_items(&comment.body, &source));
    }

    evidence
}

/// Fetch issue body and comments via the `gh` CLI.
fn fetch_issue_closure_payload(issue: u64) -> Result<ClosureIssuePayload> {
    let output = Command::new("gh")
        .args([
            "issue",
            "view",
            &issue.to_string(),
            "--json",
            "body,comments",
        ])
        .output();

    match output {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            bail!(
                "gh CLI not found; install GitHub CLI (https://cli.github.com/) to use \
                 `xtask verify closure`"
            )
        }
        Err(e) => bail!("failed to invoke gh CLI: {e}"),
        Ok(out) if !out.status.success() => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            bail!("gh issue view #{issue} failed: {stderr}")
        }
        Ok(out) => serde_json::from_slice(&out.stdout)
            .with_context(|| "gh issue view output is not valid closure JSON"),
    }
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
            // Accept all non-comment, non-empty lines from verify-section blocks
            // whose language is bash, sh, verify, or unspecified.
            let lang_ok = code_lang.is_empty()
                || code_lang == "bash"
                || code_lang == "sh"
                || code_lang == "verify"
                || code_lang == "shell";

            if lang_ok && !trimmed.is_empty() && !trimmed.starts_with('#') {
                // Strip leading `$ ` prompt if present.
                let cmd = trimmed.strip_prefix("$ ").unwrap_or(trimmed);
                commands.push(cmd.to_string());
            }
        } else if !in_code_block && in_verify_section {
            // Bare `$ command` lines outside code blocks in a verify section.
            if let Some(cmd) = trimmed.strip_prefix("$ ") {
                commands.push(cmd.to_string());
            } else if let Some(cmd) = extract_inline_backtick_command(trimmed) {
                commands.push(cmd);
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
        || lower == "acceptance criteria drift"
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
        || candidate.starts_with("rg ")
        || candidate.starts_with("nix ")
        || candidate.starts_with("SINEX_")
}

fn parse_closure_matrix_line(line: &str) -> Option<(String, String)> {
    let body = line
        .strip_prefix("- ")
        .or_else(|| line.strip_prefix("* "))
        .unwrap_or(line)
        .trim();

    if let Some(rest) = body
        .strip_prefix("[x] ")
        .or_else(|| body.strip_prefix("[X] "))
    {
        return Some(("checked".to_string(), rest.trim().to_string()));
    }
    if let Some(rest) = body.strip_prefix("[ ] ") {
        let text = rest.trim();
        let lower = text.to_lowercase();
        let status = if lower.contains("defer") {
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

    #[sinex_test]
    async fn claim_warnings_hide_local_tags_until_demoted_detail_requested()
    -> ::xtask::sandbox::TestResult<()> {
        let warnings = vec![
            "source_unit:terminal carries 2 local source-unit verification tag(s): anchor, timestamp"
                .to_string(),
            "required proof subject `runtime_unit:*` has no runner binding".to_string(),
        ];

        assert_eq!(
            visible_claim_warnings(warnings.clone(), false),
            vec!["required proof subject `runtime_unit:*` has no runner binding".to_string()]
        );
        assert_eq!(visible_claim_warnings(warnings.clone(), true), warnings);
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

    // ==========================================================================
    // A3 — source-worker and closure subcommand unit tests
    // ==========================================================================

    #[sinex_test]
    async fn extract_closure_commands_returns_empty_for_no_verify_section()
    -> ::xtask::sandbox::TestResult<()> {
        let body = "## Summary\nSome text.\n\n```bash\necho hello\n```\n";
        let cmds = extract_closure_command_entries(body, "body");
        assert!(
            cmds.is_empty(),
            "no verify section should yield no commands, got: {cmds:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn extract_closure_commands_finds_commands_in_verify_section()
    -> ::xtask::sandbox::TestResult<()> {
        let body = "## Closure verification commands\n\n```bash\ngit log --oneline -3\nxtask verify source-worker\n```\n";
        let cmds = extract_closure_command_entries(body, "body");
        assert_eq!(cmds.len(), 2, "expected 2 commands, got: {cmds:?}");
        assert!(cmds[0].command.contains("git log"));
        assert!(cmds[1].command.contains("xtask verify"));
        Ok(())
    }

    #[sinex_test]
    async fn extract_closure_commands_strips_dollar_prompt() -> ::xtask::sandbox::TestResult<()> {
        let body = "## Verification\n\n```bash\n$ git show HEAD --stat\n```\n";
        let cmds = extract_closure_command_entries(body, "body");
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].command, "git show HEAD --stat");
        Ok(())
    }

    #[sinex_test]
    async fn extract_closure_commands_ignores_comment_lines() -> ::xtask::sandbox::TestResult<()> {
        let body = "## Verification\n\n```bash\n# this is a comment\nxtask check\n```\n";
        let cmds = extract_closure_command_entries(body, "body");
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].command, "xtask check");
        Ok(())
    }

    #[sinex_test]
    async fn extract_closure_command_entries_preserve_source_location()
    -> ::xtask::sandbox::TestResult<()> {
        let body = "## Verification\n\n```bash\nxtask check -p xtask\n```\n";
        let cmds = extract_closure_command_entries(body, "comment[0]@2026-05-19T00:00:00Z");
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].command, "xtask check -p xtask");
        assert_eq!(cmds[0].source, "comment[0]@2026-05-19T00:00:00Z");
        Ok(())
    }

    #[sinex_test]
    async fn claims_json_mode_returns_enveloped_data() -> ::xtask::sandbox::TestResult<()> {
        let ctx = CommandContext::new(
            crate::output::OutputWriter::new(crate::output::OutputFormat::Json),
            false,
            None,
            "verify",
        );
        let result = execute_claims(true, false, false, &ctx)?;

        assert!(matches!(result.status, Status::Success));
        let data = result
            .data
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("claims result missing structured data"))?;
        assert_eq!(data["schema_version"], 1);
        assert!(
            data["required"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
        );
        assert!(data["errors"].as_array().is_some_and(Vec::is_empty));
        Ok(())
    }

    #[sinex_test]
    async fn source_worker_json_mode_returns_enveloped_data() -> ::xtask::sandbox::TestResult<()> {
        let ctx = CommandContext::new(
            crate::output::OutputWriter::new(crate::output::OutputFormat::Json),
            false,
            None,
            "verify",
        );
        let result = execute_source_worker(None, true, &ctx)?;

        assert!(matches!(result.status, Status::Success));
        let data = result.data.as_ref().ok_or_else(|| {
            color_eyre::eyre::eyre!("source-worker result missing structured data")
        })?;
        assert_eq!(data["overall"], "pass");
        assert_eq!(data["checks"].as_array().map(Vec::len), Some(3));
        Ok(())
    }

    #[sinex_test]
    async fn extract_closure_commands_finds_inline_comment_verification()
    -> ::xtask::sandbox::TestResult<()> {
        let body = "\
Verification:

- `SINEX_PREFLIGHT_SKIP_DISK_CHECK=1 xtask check -p sinexctl --allow-contended-host` - passed.
- `xtask test -p sinexctl -E 'test(mcp)' --allow-contended-host` - passed.
";
        let cmds = extract_closure_command_entries(body, "body");
        assert_eq!(cmds.len(), 2);
        assert!(
            cmds[0]
                .command
                .starts_with("SINEX_PREFLIGHT_SKIP_DISK_CHECK=1 xtask check")
        );
        assert!(cmds[1].command.starts_with("xtask test -p sinexctl"));
        Ok(())
    }

    #[sinex_test]
    async fn collect_closure_evidence_includes_comment_commands() -> ::xtask::sandbox::TestResult<()>
    {
        let payload = ClosureIssuePayload {
            body: "## Summary\nNo command here.".to_string(),
            comments: vec![ClosureIssueComment {
                body: "## Verification\n\n```bash\nxtask check -p xtask\n```".to_string(),
                created_at: "2026-05-19T00:00:00Z".to_string(),
            }],
        };
        let evidence = collect_closure_evidence(&payload);
        assert_eq!(evidence.commands.len(), 1);
        assert_eq!(evidence.commands[0].command, "xtask check -p xtask");
        assert_eq!(
            evidence.commands[0].source,
            "comment[0]@2026-05-19T00:00:00Z"
        );
        Ok(())
    }

    #[sinex_test]
    async fn collect_closure_evidence_is_empty_without_commands_or_matrix()
    -> ::xtask::sandbox::TestResult<()> {
        let payload = ClosureIssuePayload {
            body: "## Summary\nText-only issue discussion.".to_string(),
            comments: vec![ClosureIssueComment {
                body: "Still no verification evidence.".to_string(),
                created_at: String::new(),
            }],
        };
        let evidence = collect_closure_evidence(&payload);
        assert!(evidence.commands.is_empty());
        assert!(evidence.matrix_items.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn extract_closure_matrix_items_reports_checkbox_status()
    -> ::xtask::sandbox::TestResult<()> {
        let body = "\
## Acceptance Criteria Drift

- [x] AC #1 satisfied by PR
- [ ] AC #2 deferred to #123
- [ ] AC #3 still unclear
";
        let items = extract_closure_matrix_items(body, "body");
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].status, "checked");
        assert_eq!(items[1].status, "deferred");
        assert_eq!(items[2].status, "unchecked");
        Ok(())
    }

    #[sinex_test]
    async fn preview_output_truncates_long_text() -> ::xtask::sandbox::TestResult<()> {
        let long = "a".repeat(300);
        let preview = preview_output(long.as_bytes(), 200);
        assert!(
            preview.chars().count() <= 210,
            "preview too long: {}",
            preview.chars().count()
        );
        assert!(preview.ends_with('…'), "should end with ellipsis");
        Ok(())
    }

    #[sinex_test]
    async fn preview_output_preserves_short_text() -> ::xtask::sandbox::TestResult<()> {
        let short = b"hello world";
        let preview = preview_output(short, 200);
        assert_eq!(preview, "hello world");
        Ok(())
    }

    #[sinex_test]
    async fn sw_check_is_fail_only_for_fail_status() -> ::xtask::sandbox::TestResult<()> {
        let pass = SwCheck::pass("x", "ok");
        let warn = SwCheck::warn("x", "meh", Vec::new());
        let fail = SwCheck::fail("x", "bad", Vec::new());
        assert!(!pass.is_fail());
        assert!(!warn.is_fail());
        assert!(fail.is_fail());
        Ok(())
    }
}
