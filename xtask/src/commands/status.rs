//! Status command - workspace health and recent activity
//!
//! Unified command for workspace status with options:
//! - Default: Full status (infra + services + jobs + recent activity)
//! - `--summary`: Rich multi-section MOTD
//! - `--watch`: Live updates

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{
    DiagnosticCounts, HistoryAnalysis, HistoryDb, Invocation, InvocationStatus, Recommendation,
    VelocityTrend, WorkspaceHealthReport,
};
use crate::infra::probe::{NatsProbe, PostgresProbe, probe_nats, probe_postgres};
use crate::jobs::JobManager;
use crate::runtime_metrics::{IngestdStatus, RuntimeAssessment, RuntimeMetrics};
use crate::session::{WatchAction, WatchLoop};
use color_eyre::eyre::{Result, WrapErr};
use console::style;
use serde::Serialize;
use sinex_primitives::DeploymentReadinessDescriptor;
use std::any::Any;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, clap::Args)]
pub struct StatusCommand {
    /// Watch for changes (live updates)
    #[arg(short, long)]
    pub watch: bool,

    /// Rich multi-section MOTD
    #[arg(long)]
    pub summary: bool,

    /// Show event payload schema information
    #[arg(long)]
    pub schemas: bool,
}

/// Structured status output for JSON mode
#[derive(Debug, Serialize)]
struct StatusOutput {
    infrastructure: InfrastructureStatus,
    services: Vec<ServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<RuntimeMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_assessment: Option<RuntimeAssessment>,
    history: HistoryStatusOutput,
    jobs: JobsStatus,
    recent_activity: Vec<ActivityEntry>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct InfrastructureStatus {
    postgres: ComponentStatus,
    nats: ComponentStatus,
}

#[derive(Debug, Serialize)]
struct ComponentStatus {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ServiceStatus {
    name: String,
    status: ServiceRunStatus,
    probe: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ServiceRunStatus {
    Running,
    Stopped,
    Unknown,
}

const CORE_SERVICE_NAMES: [&str; 2] = ["sinex-gateway", "sinex-ingestd"];

fn probe_service_status(service_name: &str) -> ServiceStatus {
    let output = std::process::Command::new("pgrep")
        .arg("-x")
        .arg(service_name)
        .output();
    probe_service_status_with(service_name, output)
}

fn probe_service_status_with(
    service_name: &str,
    output: std::io::Result<std::process::Output>,
) -> ServiceStatus {
    let (status, pid, message) = match output {
        Ok(output) if output.status.success() && !output.stdout.is_empty() => {
            let pid_str = String::from_utf8_lossy(&output.stdout);
            match pid_str
                .lines()
                .next()
                .and_then(|line| line.trim().parse().ok())
            {
                Some(pid) => (ServiceRunStatus::Running, Some(pid), None),
                None => (
                    ServiceRunStatus::Unknown,
                    None,
                    Some(format!(
                        "process probe returned unreadable pid output: {}",
                        pid_str.trim()
                    )),
                ),
            }
        }
        Ok(output) if output.status.code() == Some(1) => (
            ServiceRunStatus::Stopped,
            None,
            Some("exact process-name probe found no matching process".to_string()),
        ),
        Ok(output) => (
            ServiceRunStatus::Unknown,
            None,
            Some(format!(
                "process probe exited with status {}{}",
                output.status,
                render_probe_stderr(&output)
            )),
        ),
        Err(error) => (
            ServiceRunStatus::Unknown,
            None,
            Some(format!("failed to run process probe: {error}")),
        ),
    };

    ServiceStatus {
        name: service_name.to_string(),
        status,
        probe: "process_exact_name",
        pid,
        message,
    }
}

fn render_probe_stderr(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr.trim();
    if detail.is_empty() {
        String::new()
    } else {
        format!(" ({detail})")
    }
}

fn collect_core_service_statuses() -> Vec<ServiceStatus> {
    CORE_SERVICE_NAMES
        .iter()
        .map(|service_name| probe_service_status(service_name))
        .collect()
}

fn resolve_runtime_metrics_database_url(database_url: Option<&str>) -> Result<Option<String>> {
    let descriptor = if database_url.is_some() {
        None
    } else {
        DeploymentReadinessDescriptor::load()
            .wrap_err("failed to load deployment readiness descriptor for runtime metrics")?
    };
    crate::commands::doctor::resolve_effective_database_probe_url(
        database_url,
        descriptor.as_ref(),
        "runtime metrics",
    )
    .map(|value| value.map(|(url, _source)| url))
}

fn collect_runtime_metrics(runtime_db_url: Result<Option<String>>) -> Option<RuntimeMetrics> {
    match runtime_db_url {
        Ok(Some(url)) => match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => Some(rt.block_on(crate::runtime_metrics::query_runtime_metrics(&url))),
            Err(error) => Some(RuntimeMetrics::query_failure(format!(
                "failed to build runtime probe executor: {error}"
            ))),
        },
        Ok(None) => None,
        Err(error) => Some(RuntimeMetrics::query_failure(error.to_string())),
    }
}

fn describe_thread_panic(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else {
        "non-string panic payload".to_string()
    }
}

fn recover_runtime_metrics_thread(
    result: std::thread::Result<Option<RuntimeMetrics>>,
) -> Option<RuntimeMetrics> {
    match result {
        Ok(metrics) => metrics,
        Err(payload) => Some(RuntimeMetrics::query_failure(format!(
            "runtime metrics collection thread panicked: {}",
            describe_thread_panic(&*payload)
        ))),
    }
}

#[derive(Debug, Serialize)]
struct JobsStatus {
    active: usize,
    recent_failures: usize,
}

#[derive(Debug, Clone, Serialize)]
struct HistoryStatusOutput {
    status: String,
    synthetic: bool,
    recent_invocations: usize,
    diagnostic_errors: usize,
    diagnostic_warnings: usize,
    fixable_diagnostics: usize,
    flaky_tests: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct ActivityEntry {
    command: String,
    status: String,
    duration_secs: f64,
    timestamp: String,
}

/// Summary (MOTD) output structure
#[derive(Debug, Serialize)]
struct SummaryOutput {
    health: String,
    /// Condensed single-field grade: "ok" | "warn" | "error" | "infra"
    health_indicator: String,
    summary: String,
    infrastructure: SummaryInfraHealth,
    last_commands: SummaryLastCommands,
    diagnostics: SummaryDiagnostics,
    active_jobs: usize,
    git: SummaryGitState,
    warnings: Vec<String>,
    history: HistoryStatusOutput,
    // --- Rich fields ---
    #[serde(skip_serializing_if = "Option::is_none")]
    health_score: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    velocity: Option<Vec<VelocityTrendOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recommendations: Option<Vec<RecommendationOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<RuntimeMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    services: Option<Vec<ServiceStatus>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_commit: Option<CommitInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stash_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    files_changed: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uncommitted_count: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SummaryRuntimeImpact {
    Healthy,
    Degraded,
    Unhealthy,
}

fn classify_runtime_summary_impact(metrics: &RuntimeMetrics) -> SummaryRuntimeImpact {
    if metrics.query_error.is_some()
        || matches!(
            metrics.ingestd_status,
            IngestdStatus::Down | IngestdStatus::Unknown
        )
    {
        SummaryRuntimeImpact::Unhealthy
    } else if !metrics.assessment().warnings.is_empty() {
        SummaryRuntimeImpact::Degraded
    } else {
        SummaryRuntimeImpact::Healthy
    }
}

fn classify_summary_health(
    pg_ready: bool,
    nats_ready: bool,
    history_available: bool,
    history_diag_errors: usize,
    history_diag_warnings: usize,
    history_synthetic: bool,
    has_job_issues: bool,
    last_test_failed: bool,
    last_check_failed: bool,
    runtime_impact: Option<SummaryRuntimeImpact>,
    has_warnings: bool,
) -> (&'static str, &'static str) {
    let runtime_unhealthy =
        runtime_impact.is_some_and(|impact| matches!(impact, SummaryRuntimeImpact::Unhealthy));
    let health = if !pg_ready
        || !nats_ready
        || !history_available
        || history_diag_errors > 0
        || last_test_failed
        || last_check_failed
        || runtime_unhealthy
    {
        "unhealthy"
    } else if has_warnings {
        "degraded"
    } else {
        "healthy"
    };

    let indicator = if !pg_ready || !nats_ready {
        "infra"
    } else if !history_available || history_diag_errors > 0 || runtime_unhealthy {
        "error"
    } else if history_synthetic || has_job_issues || history_diag_warnings > 0 || has_warnings {
        "warn"
    } else {
        "ok"
    };

    (health, indicator)
}

fn runtime_query_error_message(metrics: &RuntimeMetrics) -> Option<String> {
    metrics
        .query_error
        .as_ref()
        .map(|error| format!("Runtime metrics query failed: {error}"))
}

#[derive(Debug, Serialize)]
struct VelocityTrendOutput {
    command: String,
    recent_avg_secs: Option<f64>,
    delta_pct: Option<f64>,
    trend: String,
    sample_count: usize,
}

impl From<&VelocityTrend> for VelocityTrendOutput {
    fn from(v: &VelocityTrend) -> Self {
        Self {
            command: v.command.clone(),
            recent_avg_secs: v.recent_avg_secs,
            delta_pct: v.delta_pct,
            trend: v.trend.clone(),
            sample_count: v.sample_count,
        }
    }
}

#[derive(Debug, Serialize)]
struct RecommendationOutput {
    severity: String,
    category: String,
    description: String,
    action: String,
}

impl From<&Recommendation> for RecommendationOutput {
    fn from(r: &Recommendation) -> Self {
        Self {
            severity: r.severity.clone(),
            category: r.category.clone(),
            description: r.description.clone(),
            action: r.action.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
struct CommitInfo {
    hash: String,
    message: String,
    age_mins: i64,
}

#[derive(Debug, Serialize)]
struct SummaryDiagnostics {
    errors: usize,
    warnings: usize,
    /// Auto-fixable warnings (MachineApplicable)
    fixable: usize,
    /// Tests that passed on retry (flaky)
    flaky_tests: usize,
}

#[derive(Debug, Serialize)]
struct SummaryInfraHealth {
    postgres: bool,
    nats: bool,
}

#[derive(Debug, Serialize)]
struct SummaryLastCommands {
    check: Option<SummaryCommandInfo>,
    test: Option<SummaryCommandInfo>,
    build: Option<SummaryCommandInfo>,
}

#[derive(Debug, Serialize)]
struct SummaryCommandInfo {
    status: InvocationStatus,
    duration_secs: f64,
    age_mins: i64,
}

#[derive(Debug, Serialize)]
struct SummaryGitState {
    branch: Option<String>,
    dirty: bool,
    ahead: u32,
    behind: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

impl XtaskCommand for StatusCommand {
    fn name(&self) -> &'static str {
        "status"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        if self.schemas {
            Ok(execute_schemas(ctx))
        } else if self.summary {
            execute_summary(ctx)
        } else {
            execute_full_status(self.watch, ctx).await
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
    }
}

/// Show event payload schema information (formerly `contracts info describe-schemas`)
fn execute_schemas(ctx: &CommandContext) -> CommandResult {
    use sinex_schema::schema_registry::SINEX_SCHEMAS;

    let schemas: Vec<_> = SINEX_SCHEMAS
        .iter()
        .map(|s| serde_json::json!({ "name": s.name, "description": s.description }))
        .collect();

    if ctx.is_human() {
        println!("Event payload schemas:");
        for schema in SINEX_SCHEMAS {
            println!("  {:30} {}", schema.name, schema.description);
        }
    }

    CommandResult::success()
        .with_message(format!("{} event payload schemas", schemas.len()))
        .with_duration(ctx.elapsed())
        .with_data(serde_json::json!({ "schemas": schemas }))
}

// ─── Data Collection ────────────────────────────────────────────────────────

/// Expanded git state for rich MOTD
struct GitState {
    branch: Option<String>,
    dirty: bool,
    ahead: u32,
    behind: u32,
    probe_message: Option<String>,
    last_commit_hash: Option<String>,
    last_commit_message: Option<String>,
    last_commit_age_mins: Option<i64>,
    stash_count: usize,
    files_changed: Option<String>,
    uncommitted_count: usize,
}

fn summarize_git_probe_output(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }

    format!("exit status {}", output.status)
}

fn record_git_probe_issue(probe_issues: &mut Vec<String>, args: &[&str], detail: impl Into<String>) {
    probe_issues.push(format!("git {} failed: {}", args.join(" "), detail.into()));
}

fn run_git_output(cwd: &Path, probe_issues: &mut Vec<String>, args: &[&str]) -> Option<std::process::Output> {
    match std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
    {
        Ok(output) if output.status.success() => Some(output),
        Ok(output) => {
            record_git_probe_issue(probe_issues, args, summarize_git_probe_output(&output));
            None
        }
        Err(error) => {
            record_git_probe_issue(probe_issues, args, error.to_string());
            None
        }
    }
}

fn probe_git_state(cwd: &Path) -> GitState {
    let mut probe_issues = Vec::new();

    let branch = run_git_output(cwd, &mut probe_issues, &["branch", "--show-current"]).and_then(
        |output| {
            let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
            (!branch.is_empty()).then_some(branch)
        },
    );

    let porcelain_output = run_git_output(cwd, &mut probe_issues, &["status", "--porcelain"]);
    let dirty = porcelain_output
        .as_ref()
        .is_some_and(|output| !output.stdout.is_empty());
    let uncommitted_count = porcelain_output
        .as_ref()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|line| !line.is_empty())
                .count()
        })
        .unwrap_or(0);

    let (ahead, behind) = run_git_output(
        cwd,
        &mut probe_issues,
        &["rev-list", "--left-right", "--count", "HEAD...@{u}"],
    )
    .map_or((0, 0), |output| {
        parse_git_upstream_counts(&String::from_utf8_lossy(&output.stdout), &mut probe_issues)
    });

    let commit = run_git_output(cwd, &mut probe_issues, &["log", "-1", "--format=%h\t%s\t%ct"])
        .and_then(|output| {
            let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let parts: Vec<&str> = text.splitn(3, '\t').collect();
            if parts.len() == 3 {
                Some((
                    parts[0].to_string(),
                    parts[1].to_string(),
                    parts[2].to_string(),
                ))
            } else {
                record_git_probe_issue(
                    &mut probe_issues,
                    &["log", "-1", "--format=%h\t%s\t%cr"],
                    format!("unexpected output: {text}"),
                );
                None
            }
        });

    let stash_count = run_git_output(cwd, &mut probe_issues, &["stash", "list"])
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|line| !line.is_empty())
                .count()
        })
        .unwrap_or(0);

    let files_changed = run_git_output(cwd, &mut probe_issues, &["diff", "--shortstat", "HEAD"])
        .and_then(|output| {
            let shortstat = String::from_utf8_lossy(&output.stdout).trim().to_string();
            (!shortstat.is_empty()).then_some(shortstat)
        });

    let now_unix_ts = current_unix_timestamp_secs();
    let last_age = commit.as_ref().and_then(|(_, _, commit_unix_ts)| match now_unix_ts {
        Some(now_unix_ts) => parse_git_commit_age_mins(commit_unix_ts, now_unix_ts).or_else(|| {
            record_git_probe_issue(
                &mut probe_issues,
                &["log", "-1", "--format=%h\t%s\t%ct"],
                format!("unexpected commit timestamp: {commit_unix_ts}"),
            );
            None
        }),
        None => {
            record_git_probe_issue(
                &mut probe_issues,
                &["log", "-1", "--format=%h\t%s\t%ct"],
                "system clock is before the Unix epoch".to_string(),
            );
            None
        }
    });
    let last_hash = commit.as_ref().map(|(hash, _, _)| hash.clone());
    let last_msg = commit.as_ref().map(|(_, message, _)| message.clone());

    GitState {
        branch,
        dirty,
        ahead,
        behind,
        probe_message: (!probe_issues.is_empty()).then(|| probe_issues.join("; ")),
        last_commit_hash: last_hash,
        last_commit_message: last_msg,
        last_commit_age_mins: last_age,
        stash_count,
        files_changed,
        uncommitted_count,
    }
}

/// Active job detail for rich MOTD
struct ActiveJobDetail {
    command: String,
    elapsed_secs: f64,
}

#[derive(Default)]
struct JobsSnapshot {
    active: Vec<crate::jobs::Job>,
    recent: Vec<crate::jobs::Job>,
    issues: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct HistorySnapshot {
    available: bool,
    recent: Vec<Invocation>,
    diag_counts: DiagnosticCounts,
    error_packages: Vec<String>,
    flaky_count: usize,
    is_synthetic: bool,
    health_report: Option<WorkspaceHealthReport>,
    velocity: Vec<VelocityTrend>,
    recommendations: Vec<Recommendation>,
    issues: Vec<String>,
}

impl HistorySnapshot {
    fn unavailable(message: String) -> Self {
        Self {
            available: false,
            issues: vec![message],
            ..Self::default()
        }
    }

    fn status(&self) -> &'static str {
        if !self.available {
            "unavailable"
        } else if self.is_synthetic {
            "synthetic"
        } else if self.issues.is_empty() {
            "available"
        } else {
            "degraded"
        }
    }

    fn message(&self) -> Option<String> {
        (!self.issues.is_empty()).then(|| self.issues.join("; "))
    }

    fn output(&self) -> HistoryStatusOutput {
        HistoryStatusOutput {
            status: self.status().to_string(),
            synthetic: self.is_synthetic,
            recent_invocations: self.recent.len(),
            diagnostic_errors: self.diag_counts.errors,
            diagnostic_warnings: self.diag_counts.warnings,
            fixable_diagnostics: self.diag_counts.fixable,
            flaky_tests: self.flaky_count,
            message: self.message(),
        }
    }
}

/// All collected summary data
struct SummaryData {
    pg_probe: PostgresProbe,
    nats_probe: NatsProbe,
    services: Vec<ServiceStatus>,
    git: GitState,
    active_job_details: Vec<ActiveJobDetail>,
    active_job_count: usize,
    history: HistorySnapshot,
    job_issues: Vec<String>,
    runtime_metrics: Option<RuntimeMetrics>,
}

fn collect_jobs_snapshot(recent_limit: usize) -> JobsSnapshot {
    let jobs_dir = config().jobs_dir();
    let mut snapshot = JobsSnapshot::default();
    let manager = match JobManager::new(jobs_dir.clone()) {
        Ok(manager) => manager,
        Err(error) => {
            snapshot.issues.push(format!(
                "Jobs state unavailable at {}: {error}",
                jobs_dir.display()
            ));
            return snapshot;
        }
    };

    match manager.list_active() {
        Ok(active) => snapshot.active = active,
        Err(error) => snapshot.issues.push(format!(
            "Failed to read active jobs from {}: {error}",
            jobs_dir.display()
        )),
    }

    match manager.list_recent(recent_limit) {
        Ok(recent) => snapshot.recent = recent,
        Err(error) => snapshot.issues.push(format!(
            "Failed to read recent jobs from {}: {error}",
            jobs_dir.display()
        )),
    }

    snapshot
}

fn explain_history_db_open_failure(ctx: &CommandContext) -> String {
    match HistoryDb::open(ctx.history_db_path()) {
        Ok(_) => format!(
            "History DB became unavailable at {}",
            ctx.history_db_path().display()
        ),
        Err(error) => format!(
            "Failed to open history DB at {}: {error}",
            ctx.history_db_path().display()
        ),
    }
}

fn collect_history_snapshot(
    ctx: &CommandContext,
    recent_limit: usize,
    include_analytics: bool,
) -> HistorySnapshot {
    use crate::history::DiagnosticQuery;

    let Some(result) = ctx.try_with_history_db(|db: &HistoryDb| {
        let mut snapshot = HistorySnapshot {
            available: true,
            is_synthetic: db.is_synthetic,
            ..HistorySnapshot::default()
        };

        match db.get_recent(recent_limit, None) {
            Ok(recent) => snapshot.recent = recent,
            Err(error) => snapshot
                .issues
                .push(format!("Failed to read recent command history: {error}")),
        }

        match db.get_current_diagnostic_counts() {
            Ok(counts) => snapshot.diag_counts = counts,
            Err(error) => snapshot
                .issues
                .push(format!("Failed to read current diagnostics: {error}")),
        }

        match db.get_flaky_tests(50) {
            Ok(flaky) => snapshot.flaky_count = flaky.len(),
            Err(error) => snapshot
                .issues
                .push(format!("Failed to read flaky-test history: {error}")),
        }

        match DiagnosticQuery::new().level("error").limit(50).run(db) {
            Ok(diags) => {
                let mut pkgs: Vec<String> =
                    diags.iter().filter_map(|d| d.package.clone()).collect();
                pkgs.sort();
                pkgs.dedup();
                snapshot.error_packages = pkgs;
            }
            Err(error) => snapshot
                .issues
                .push(format!("Failed to read error-package history: {error}")),
        }

        if include_analytics {
            let analysis = HistoryAnalysis::new(db);
            match analysis.workspace_health_report() {
                Ok(report) => snapshot.health_report = Some(report),
                Err(error) => snapshot.issues.push(format!(
                    "Failed to compute workspace health report: {error}"
                )),
            }
            match analysis.velocity_trends() {
                Ok(velocity) => snapshot.velocity = velocity,
                Err(error) => snapshot
                    .issues
                    .push(format!("Failed to compute velocity trends: {error}")),
            }
            match analysis.recommendations() {
                Ok(recommendations) => snapshot.recommendations = recommendations,
                Err(error) => snapshot.issues.push(format!(
                    "Failed to compute workspace recommendations: {error}"
                )),
            }
        }

        Ok(snapshot)
    }) else {
        return HistorySnapshot::unavailable(explain_history_db_open_failure(ctx));
    };

    match result {
        Ok(snapshot) => snapshot,
        Err(error) => HistorySnapshot::unavailable(format!(
            "History DB query failed at {}: {error}",
            ctx.history_db_path().display()
        )),
    }
}

/// Collect all data for --summary in parallel threads.
fn collect_summary_data(ctx: &CommandContext) -> SummaryData {
    let cfg = config();

    std::thread::scope(|s| {
        // Thread 1: Infrastructure + services
        let infra_handle = s.spawn(move || {
            let pg = probe_postgres();
            let nats = probe_nats();

            let services = collect_core_service_statuses();

            (pg, nats, services)
        });

        // Thread 2: Runtime metrics from Postgres
        let runtime_db_url = resolve_runtime_metrics_database_url(cfg.database_url.as_deref());
        let runtime_metrics_handle = s.spawn(move || collect_runtime_metrics(runtime_db_url));

        // Thread 3: History snapshot
        let history_handle = s.spawn(move || collect_history_snapshot(ctx, 50, true));

        // Thread 4: Git state (expanded for rich mode)
        let git_handle = s.spawn(move || match std::env::current_dir() {
            Ok(cwd) => probe_git_state(&cwd),
            Err(error) => GitState {
                branch: None,
                dirty: false,
                ahead: 0,
                behind: 0,
                probe_message: Some(format!("failed to determine current directory for git probe: {error}")),
                last_commit_hash: None,
                last_commit_message: None,
                last_commit_age_mins: None,
                stash_count: 0,
                files_changed: None,
                uncommitted_count: 0,
            },
        });

        // Main thread: jobs
        let jobs = collect_jobs_snapshot(20);
        let active_jobs_list = jobs.active;
        let active_job_count = active_jobs_list.len();

        let now_instant = time::OffsetDateTime::now_utc();
        let active_job_details: Vec<ActiveJobDetail> = active_jobs_list
            .iter()
            .map(|j| {
                let elapsed = (now_instant - j.started_at).as_seconds_f64();
                ActiveJobDetail {
                    command: if j.args.is_empty() {
                        j.command.clone()
                    } else {
                        format!("{} {}", j.command, j.args.join(" "))
                    },
                    elapsed_secs: elapsed.max(0.0),
                }
            })
            .collect();

        // Collect thread results
        let (pg_probe, nats_probe, services) = match infra_handle.join() {
            Ok(result) => result,
            Err(payload) => {
                let message =
                    format!("infra probe thread panicked: {}", describe_thread_panic(&*payload));
                (
                    PostgresProbe {
                        running: false,
                        accepting_connections: false,
                        latency_ms: 0,
                        message: Some(message.clone()),
                    },
                    NatsProbe {
                        running: false,
                        reachable: false,
                        latency_ms: 0,
                        port: 4222,
                        message: Some(message),
                    },
                    vec![],
                )
            }
        };
        let git = git_handle.join().unwrap_or_else(|payload| GitState {
            branch: None,
            dirty: false,
            ahead: 0,
            behind: 0,
            probe_message: Some(format!(
                "git probe thread panicked: {}",
                describe_thread_panic(&*payload)
            )),
            last_commit_hash: None,
            last_commit_message: None,
            last_commit_age_mins: None,
            stash_count: 0,
            files_changed: None,
            uncommitted_count: 0,
        });
        let runtime_metrics = recover_runtime_metrics_thread(runtime_metrics_handle.join());
        let history = history_handle.join().unwrap_or_else(|payload| {
            HistorySnapshot::unavailable(format!(
                "history collection thread panicked: {}",
                describe_thread_panic(&*payload)
            ))
        });

        SummaryData {
            pg_probe,
            nats_probe,
            services,
            git,
            active_job_details,
            active_job_count,
            history,
            job_issues: jobs.issues,
            runtime_metrics,
        }
    })
}

fn current_unix_timestamp_secs() -> Option<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
}

/// Parse a git commit timestamp (`%ct`) into a relative age in minutes.
fn parse_git_commit_age_mins(commit_unix_ts: &str, now_unix_ts: i64) -> Option<i64> {
    let commit_unix_ts = commit_unix_ts.parse::<i64>().ok()?;
    Some((now_unix_ts - commit_unix_ts).max(0) / 60)
}

fn parse_git_upstream_counts(output: &str, probe_issues: &mut Vec<String>) -> (u32, u32) {
    let trimmed = output.trim();
    let parts: Vec<&str> = trimmed.split('\t').collect();
    if parts.len() != 2 {
        record_git_probe_issue(
            probe_issues,
            &["rev-list", "--left-right", "--count", "HEAD...@{u}"],
            format!("unexpected output: {trimmed}"),
        );
        return (0, 0);
    }

    let ahead = match parts[0].parse::<u32>() {
        Ok(value) => value,
        Err(error) => {
            record_git_probe_issue(
                probe_issues,
                &["rev-list", "--left-right", "--count", "HEAD...@{u}"],
                format!("invalid ahead count `{}`: {error}", parts[0]),
            );
            return (0, 0);
        }
    };
    let behind = match parts[1].parse::<u32>() {
        Ok(value) => value,
        Err(error) => {
            record_git_probe_issue(
                probe_issues,
                &["rev-list", "--left-right", "--count", "HEAD...@{u}"],
                format!("invalid behind count `{}`: {error}", parts[1]),
            );
            return (0, 0);
        }
    };

    (ahead, behind)
}

// ─── Summary / Compact execution ────────────────────────────────────────────

/// Execute --summary (rich multi-section MOTD)
fn execute_summary(ctx: &CommandContext) -> Result<CommandResult> {
    let data = collect_summary_data(ctx);

    let now = time::OffsetDateTime::now_utc();
    let get_last_command = |cmd: &str| -> Option<SummaryCommandInfo> {
        data.history
            .recent
            .iter()
            .find(|i| i.command == cmd && i.status != InvocationStatus::Running)
            .map(|i| {
                let age = now - i.started_at;
                SummaryCommandInfo {
                    status: i.status,
                    duration_secs: i.duration_secs.unwrap_or(0.0),
                    age_mins: age.whole_minutes(),
                }
            })
    };

    let last_check = get_last_command("check");
    let last_test = get_last_command("test");
    let last_build = get_last_command("build");
    let unavailable_services: Vec<&str> = data
        .services
        .iter()
        .filter(|service| !matches!(service.status, ServiceRunStatus::Running))
        .map(|service| service.name.as_str())
        .collect();

    // Build warnings
    let mut warnings = Vec::new();
    if !data.pg_probe.ready() {
        warnings.push(
            data.pg_probe
                .message
                .clone()
                .unwrap_or_else(|| "Postgres offline".to_string()),
        );
    }
    if !data.nats_probe.ready() {
        warnings.push(
            data.nats_probe
                .message
                .clone()
                .unwrap_or_else(|| "NATS offline".to_string()),
        );
    }
    if let Some(ref test) = last_test {
        if matches!(test.status, InvocationStatus::Failed) {
            warnings.push("Tests failing".to_string());
        }
        if test.age_mins > 60 {
            warnings.push(format!("Tests not run in {}h", test.age_mins / 60));
        }
    } else {
        warnings.push("No test runs recorded".to_string());
    }
    if let Some(ref check) = last_check
        && matches!(check.status, InvocationStatus::Failed)
    {
        warnings.push("Check failing".to_string());
    }
    if data.active_job_count > 3 {
        warnings.push(format!("{} jobs running", data.active_job_count));
    }
    if data.git.dirty {
        warnings.push("Uncommitted changes".to_string());
    }
    if let Some(message) = &data.git.probe_message {
        warnings.push(message.clone());
    }
    if !unavailable_services.is_empty() {
        warnings.push(format!(
            "Core services not healthy: {}",
            unavailable_services.join(", ")
        ));
    }
    if data.history.is_synthetic {
        warnings.push("History DB is seeded with synthetic data".to_string());
    }
    warnings.extend(data.history.issues.clone());
    warnings.extend(data.job_issues.clone());
    if let Some(runtime_metrics) = data.runtime_metrics.as_ref() {
        warnings.extend(runtime_metrics.assessment().warnings);
    }
    let runtime_impact = data
        .runtime_metrics
        .as_ref()
        .map(classify_runtime_summary_impact);

    let (health, health_indicator) = classify_summary_health(
        data.pg_probe.ready(),
        data.nats_probe.ready(),
        data.history.available,
        data.history.diag_counts.errors,
        data.history.diag_counts.warnings,
        data.history.is_synthetic,
        !data.job_issues.is_empty(),
        last_test
            .as_ref()
            .is_some_and(|t| matches!(t.status, InvocationStatus::Failed)),
        last_check
            .as_ref()
            .is_some_and(|c| matches!(c.status, InvocationStatus::Failed)),
        runtime_impact,
        !warnings.is_empty(),
    );

    // Summary line (always computed for JSON)
    let warns_str = if data.history.diag_counts.errors > 0 {
        format!(
            "{}e+{}w",
            data.history.diag_counts.errors, data.history.diag_counts.warnings
        )
    } else if data.history.diag_counts.warnings > 0 {
        format!("{}w", data.history.diag_counts.warnings)
    } else {
        "0".to_string()
    };
    let fixes_str = format!("{}f", data.history.diag_counts.fixable);
    let rt_fragment = data
        .runtime_metrics
        .as_ref()
        .map(|m| format!(" {}", m.summary_fragment()))
        .unwrap_or_default();
    let summary = format!(
        "infra:{} jobs:{} tests:{} warns:{} fixes:{}{} git:{}{}",
        if data.pg_probe.ready() && data.nats_probe.ready() {
            "ok"
        } else {
            "x"
        },
        data.active_job_count,
        last_test.as_ref().map_or("?", |t| {
            if matches!(t.status, InvocationStatus::Success) {
                "ok"
            } else {
                "x"
            }
        }),
        warns_str,
        fixes_str,
        rt_fragment,
        if data.git.dirty { "dirty" } else { "clean" },
        if data.history.is_synthetic {
            " [synthetic]"
        } else {
            ""
        },
    );

    let output = SummaryOutput {
        health: health.to_string(),
        health_indicator: health_indicator.to_string(),
        summary: summary.clone(),
        infrastructure: SummaryInfraHealth {
            postgres: data.pg_probe.ready(),
            nats: data.nats_probe.ready(),
        },
        last_commands: SummaryLastCommands {
            check: last_check,
            test: last_test,
            build: last_build,
        },
        diagnostics: SummaryDiagnostics {
            errors: data.history.diag_counts.errors,
            warnings: data.history.diag_counts.warnings,
            fixable: data.history.diag_counts.fixable,
            flaky_tests: data.history.flaky_count,
        },
        active_jobs: data.active_job_count,
        git: SummaryGitState {
            branch: data.git.branch.clone(),
            dirty: data.git.dirty,
            ahead: data.git.ahead,
            behind: data.git.behind,
            message: data.git.probe_message.clone(),
        },
        warnings: warnings.clone(),
        history: data.history.output(),
        // Rich fields
        health_score: data.history.health_report.as_ref().map(|r| r.score),
        velocity: if !data.history.velocity.is_empty() {
            Some(
                data.history
                    .velocity
                    .iter()
                    .map(VelocityTrendOutput::from)
                    .collect(),
            )
        } else {
            None
        },
        recommendations: if !data.history.recommendations.is_empty() {
            Some(
                data.history
                    .recommendations
                    .iter()
                    .map(RecommendationOutput::from)
                    .collect(),
            )
        } else {
            None
        },
        runtime: data.runtime_metrics.clone(),
        services: (!data.services.is_empty()).then(|| data.services.clone()),
        last_commit: data.git.last_commit_hash.as_ref().map(|hash| CommitInfo {
            hash: hash.clone(),
            message: data.git.last_commit_message.clone().unwrap_or_default(),
            age_mins: data.git.last_commit_age_mins.unwrap_or(0),
        }),
        stash_count: if data.git.stash_count > 0 {
            Some(data.git.stash_count)
        } else {
            None
        },
        files_changed: data.git.files_changed.clone(),
        uncommitted_count: if data.git.uncommitted_count > 0 {
            Some(data.git.uncommitted_count)
        } else {
            None
        },
    };

    if ctx.is_human() {
        let renderer = MotdRenderer::new(&output, &data);
        renderer.render();
        Ok(CommandResult::success().with_duration(ctx.elapsed()))
    } else {
        Ok(CommandResult::success()
            .with_data(serde_json::to_value(&output)?)
            .with_duration(ctx.elapsed()))
    }
}

// ─── MOTD Renderer ──────────────────────────────────────────────────────────

/// Visual width of section labels including leading/trailing spaces.
/// All sections use this for consistent left-column alignment.
/// Layout: "  label" (2 + up to 6) + padding to 11 = "  build    " etc.
const LABEL_COL: usize = 11;

struct MotdRenderer<'a> {
    width: usize,
    output: &'a SummaryOutput,
    data: &'a SummaryData,
}

impl<'a> MotdRenderer<'a> {
    fn new(output: &'a SummaryOutput, data: &'a SummaryData) -> Self {
        let width = console::Term::stdout().size().1.max(80).min(120) as usize;
        Self {
            width,
            output,
            data,
        }
    }

    fn render(&self) {
        // Header (always)
        self.render_header();

        // Infra + services (always)
        self.render_infra();

        // Build status (when any history exists)
        self.render_build();

        // Velocity trends (when meaningful data exists)
        self.render_velocity();

        // Recommendations (when critical/warning exist)
        self.render_recommendations();

        // Runtime metrics (when services are active)
        self.render_runtime();

        // Active jobs (when >0)
        self.render_jobs();

        // Git working directory (when notable)
        self.render_git();
    }

    // ─── Header ─────────────────────────────────────────────────────────

    fn render_header(&self) {
        let w = self.width;
        let inner = w - 2; // inside the box (excluding │ on each side)

        // Top border
        println!("┌{}┐", "─".repeat(inner));

        // Content: "  sinex" left, "score   branch" right
        let left = format!("  {}", style("sinex").bold());
        let left_vis = console::measure_text_width(&left);

        let score_part = match self.data.history.health_report.as_ref() {
            Some(r) => {
                let s = format!("{}/100", r.score);
                if r.score >= 80 {
                    style(s).green().to_string()
                } else if r.score >= 60 {
                    style(s).yellow().to_string()
                } else {
                    style(s).red().to_string()
                }
            }
            None => style("--/100").dim().to_string(),
        };

        let branch_raw = self.data.git.branch.as_deref().unwrap_or("-");
        let max_branch = 20;
        let branch_name = if branch_raw.len() > max_branch {
            format!("{}…", &branch_raw[..max_branch - 1])
        } else {
            branch_raw.to_string()
        };
        let branch_part = if self.data.git.dirty {
            style(&branch_name).bold().to_string()
        } else {
            style(&branch_name).dim().to_string()
        };

        // Ahead/behind inline after branch
        let mut ab_part = String::new();
        if self.data.git.ahead > 0 {
            ab_part.push_str(&format!(
                " {}",
                style(format!("↑{}", self.data.git.ahead)).cyan()
            ));
        }
        if self.data.git.behind > 0 {
            ab_part.push_str(&format!(
                " {}",
                style(format!("↓{}", self.data.git.behind)).red()
            ));
        }

        let right = format!("{}   {}{}  ", score_part, branch_part, ab_part);
        let right_vis = console::measure_text_width(&right);

        let padding = inner.saturating_sub(left_vis + right_vis);
        println!("│{}{}{}│", left, " ".repeat(padding), right);

        // Bottom border
        println!("└{}┘", "─".repeat(inner));
    }

    // ─── Infrastructure + Services ──────────────────────────────────────

    fn render_infra(&self) {
        let label = style("  infra").dim();

        let pg = if self.data.pg_probe.ready() {
            style("pg:ready").green().to_string()
        } else {
            style("pg:offline").red().bold().to_string()
        };

        let nats = if self.data.nats_probe.ready() {
            style("nats:reachable").green().to_string()
        } else {
            style("nats:offline").red().bold().to_string()
        };

        if self.data.services.is_empty() {
            println!("{label}    {pg}  {nats}");
        } else {
            let svc_parts: Vec<String> = self
                .data
                .services
                .iter()
                .map(|s| {
                    let short = s.name.strip_prefix("sinex-").unwrap_or(&s.name);
                    match s.status {
                        ServiceRunStatus::Running => {
                            style(format!("{short}:up")).green().to_string()
                        }
                        ServiceRunStatus::Stopped => {
                            style(format!("{short}:down")).red().to_string()
                        }
                        ServiceRunStatus::Unknown => {
                            style(format!("{short}:unknown")).yellow().to_string()
                        }
                    }
                })
                .collect();
            println!(
                "{label}    {pg}  {nats} {} {}",
                style("·").dim(),
                svc_parts.join("  ")
            );
        }
    }

    // ─── Build Status ───────────────────────────────────────────────────

    fn render_build(&self) {
        let cmds = &self.output.last_commands;
        let has_any = cmds.check.is_some() || cmds.test.is_some() || cmds.build.is_some();
        let show_history_note =
            self.output.history.synthetic || self.output.history.message.is_some();
        if !has_any && !show_history_note {
            return;
        }

        let label = style("  build").dim();
        let mut parts = Vec::new();

        for (name, cmd) in [
            ("check", &cmds.check),
            ("test", &cmds.test),
            ("build", &cmds.build),
        ] {
            if let Some(info) = cmd {
                let icon = if matches!(info.status, InvocationStatus::Success) {
                    style("✓").green().to_string()
                } else {
                    style("✗").red().to_string()
                };
                let age = format_age(info.age_mins);
                let dur = format!("{:.1}s", info.duration_secs);
                parts.push(format!(
                    "{} {} {} {}",
                    name,
                    icon,
                    style(age).dim(),
                    style(dur).dim()
                ));
            }
        }

        if parts.is_empty() {
            println!(
                "{label}    {}",
                style("no recorded xtask invocations").dim()
            );
        } else {
            println!("{label}    {}", parts.join("   "));
        }

        // Diagnostics sub-line — show what's wrong and where
        let d = &self.output.diagnostics;
        if d.errors > 0 || d.warnings > 0 {
            let mut diag_parts = Vec::new();

            if d.errors > 0 {
                // Include package names for context
                let err_label = if self.data.history.error_packages.len() == 1 {
                    format!(
                        "{} error in {}",
                        d.errors, self.data.history.error_packages[0]
                    )
                } else if self.data.history.error_packages.len() <= 3
                    && !self.data.history.error_packages.is_empty()
                {
                    format!(
                        "{} error{} in {}",
                        d.errors,
                        if d.errors == 1 { "" } else { "s" },
                        self.data.history.error_packages.join(", ")
                    )
                } else {
                    format!("{} error{}", d.errors, if d.errors == 1 { "" } else { "s" })
                };
                diag_parts.push(style(err_label).red().bold().to_string());
            }

            if d.warnings > 0 {
                diag_parts.push(
                    style(format!(
                        "{} warning{}",
                        d.warnings,
                        if d.warnings == 1 { "" } else { "s" }
                    ))
                    .yellow()
                    .to_string(),
                );
            }
            if d.fixable > 0 {
                diag_parts.push(style(format!("{} fixable", d.fixable)).yellow().to_string());
            }
            if d.flaky_tests > 0 {
                diag_parts.push(
                    style(format!("{} flaky", d.flaky_tests))
                        .yellow()
                        .to_string(),
                );
            }

            // Action hint: most specific useful command
            let action = if d.fixable > 0 {
                format!(
                    " {} {}",
                    style("→").dim(),
                    style("xtask fix --smart").cyan()
                )
            } else if d.errors > 0 {
                format!(
                    " {} {}",
                    style("→").dim(),
                    style("xtask history diagnostics --level error").cyan()
                )
            } else {
                String::new()
            };

            let sep = format!(" {} ", style("·").dim());
            let indent = " ".repeat(LABEL_COL);
            println!("{indent}{}{action}", diag_parts.join(&sep));
        }

        if self.output.history.synthetic {
            let indent = " ".repeat(LABEL_COL);
            println!(
                "{indent}{}",
                style("history DB is synthetic; trends and diagnostics are seeded").yellow()
            );
        } else if let Some(message) = self.output.history.message.as_deref() {
            let indent = " ".repeat(LABEL_COL);
            println!("{indent}{}", style(message).yellow());
        }
    }

    // ─── Velocity ───────────────────────────────────────────────────────

    fn render_velocity(&self) {
        let meaningful: Vec<_> = self
            .data
            .history
            .velocity
            .iter()
            .filter(|v| v.sample_count >= 4 && v.recent_avg_secs.is_some())
            .collect();

        if meaningful.is_empty() {
            return;
        }

        let label = style("  trend").dim();
        let parts: Vec<String> = meaningful
            .iter()
            .map(|v| {
                let avg = format!("~{:.1}s", v.recent_avg_secs.unwrap_or(0.0));
                let delta = match v.delta_pct {
                    Some(d) if d < -5.0 => style(format!("↓{:.0}%", d.abs())).green().to_string(),
                    Some(d) if d > 5.0 => style(format!("↑{:.0}%", d)).red().to_string(),
                    _ => style("→").dim().to_string(),
                };
                format!("{} {} {}", v.command, avg, delta)
            })
            .collect();

        println!("{label}    {}", parts.join("   "));
    }

    // ─── Recommendations ────────────────────────────────────────────────

    fn render_recommendations(&self) {
        let actionable: Vec<_> = self
            .data
            .history
            .recommendations
            .iter()
            .filter(|r| r.severity != "info")
            .collect();

        if actionable.is_empty() {
            return;
        }

        let max_show = 3;
        for (i, rec) in actionable.iter().take(max_show).enumerate() {
            let label_text = "  action";
            let label = if i == 0 {
                style(label_text).dim().to_string()
            } else {
                " ".repeat(label_text.len()).to_string()
            };

            let icon = if rec.severity == "critical" {
                style("✗").red().to_string()
            } else {
                style("⚠").yellow().to_string()
            };

            let action = style(&rec.action).cyan();
            println!(
                "{label}   {icon} {} {} {action}",
                rec.description,
                style("→").dim()
            );
        }

        if actionable.len() > max_show {
            let overflow = actionable.len() - max_show;
            let indent = " ".repeat(LABEL_COL);
            println!(
                "{indent}{} {}",
                style(format!("+{overflow} more")).dim(),
                style(format!("→ {}", "xtask analytics recommend")).cyan()
            );
        }
    }

    // ─── Runtime ────────────────────────────────────────────────────────

    fn render_runtime(&self) {
        let label = style("  runtime").dim();
        let Some(metrics) = &self.data.runtime_metrics else {
            println!(
                "{label}  {}",
                style("unavailable (runtime database target not configured)").dim()
            );
            return;
        };

        let assessment = metrics.assessment();
        let lag_high = assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("consumer lag is high"));
        let batch_high = assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("batch latency is high"));

        let status = match metrics.ingestd_status {
            IngestdStatus::Healthy => style("ingestd ok").green().to_string(),
            IngestdStatus::Stale => style("ingestd stale").yellow().to_string(),
            IngestdStatus::Down => style("ingestd down").red().to_string(),
            IngestdStatus::Unknown => style("ingestd unknown").dim().to_string(),
        };

        let lag = metrics
            .fresh_consumer_lag_pending()
            .map(|v| {
                let s = format!("{v:.0}");
                let colored = if lag_high {
                    style(s).red().to_string()
                } else if matches!(
                    assessment.status,
                    crate::runtime_metrics::RuntimeHealthStatus::Healthy
                ) {
                    style(s).green().to_string()
                } else {
                    style(s).yellow().to_string()
                };
                format!("lag {colored}")
            })
            .or_else(|| {
                metrics
                    .consumer_lag_age_secs
                    .filter(|_| metrics.consumer_lag_is_stale())
                    .map(|age| style(format!("lag stale ({age}s)")).yellow().to_string())
            })
            .unwrap_or_default();

        let batch = metrics
            .fresh_batch_latency_ms()
            .map(|v| {
                let summary = format!("batch {}ms", v as u64);
                if batch_high {
                    style(summary).red().to_string()
                } else if matches!(
                    assessment.status,
                    crate::runtime_metrics::RuntimeHealthStatus::Healthy
                ) {
                    style(summary).green().to_string()
                } else {
                    style(summary).yellow().to_string()
                }
            })
            .or_else(|| {
                metrics
                    .last_batch_latency_age_secs
                    .filter(|_| metrics.batch_latency_is_stale())
                    .map(|age| style(format!("batch stale ({age}s)")).yellow().to_string())
            })
            .unwrap_or_default();

        let heartbeat = metrics
            .last_heartbeat_age_secs
            .map(|secs| {
                let s = format!("heartbeat {}s ago", secs);
                if matches!(metrics.ingestd_status, IngestdStatus::Healthy) {
                    style(s).green().to_string()
                } else if matches!(metrics.ingestd_status, IngestdStatus::Stale) {
                    style(s).yellow().to_string()
                } else if matches!(metrics.ingestd_status, IngestdStatus::Down) {
                    style(s).red().to_string()
                } else {
                    style(s).dim().to_string()
                }
            })
            .unwrap_or_default();

        let query = metrics
            .query_error
            .as_ref()
            .map(|error| style(format!("query error ({error})")).red().to_string())
            .unwrap_or_default();

        let sep = style("·").dim();
        let parts: Vec<&str> = [
            status.as_str(),
            lag.as_str(),
            batch.as_str(),
            heartbeat.as_str(),
            query.as_str(),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();

        println!("{label}  {}", parts.join(&format!(" {sep} ")));
    }

    // ─── Active Jobs ────────────────────────────────────────────────────

    fn render_jobs(&self) {
        if self.data.active_job_details.is_empty() && self.data.job_issues.is_empty() {
            return;
        }

        let label = style("  jobs").dim();
        let count = self.data.active_job_details.len();
        let max_show = 3;

        let job_parts: Vec<String> = self
            .data
            .active_job_details
            .iter()
            .take(max_show)
            .map(|j| format!("{} ({}s)", j.command, j.elapsed_secs as u64))
            .collect();

        let sep = style("·").dim();
        let mut line = format!(
            "{label}     {} running: {}",
            count,
            job_parts.join(&format!(" {sep} "))
        );

        if count > max_show {
            line.push_str(&format!(
                " {} {}",
                style(format!("+{} more", count - max_show)).dim(),
                style("→ xtask jobs list --active").cyan()
            ));
        }

        println!("{line}");
        for issue in &self.data.job_issues {
            let indent = " ".repeat(LABEL_COL);
            println!("{indent}{}", style(issue).yellow());
        }
    }

    // ─── Git Working Directory ──────────────────────────────────────────

    fn render_git(&self) {
        let git = &self.data.git;

        // Show when there's something notable
        let has_commit = git.last_commit_hash.is_some();
        let notable = git.probe_message.is_some()
            || git.dirty
            || git.ahead > 0
            || git.behind > 0
            || git.stash_count > 0
            || git.uncommitted_count > 0;

        if !has_commit && !notable {
            return;
        }

        let label = style("  git").dim();

        // First line: last commit (width-aware truncation)
        if let (Some(hash), Some(msg)) = (&git.last_commit_hash, &git.last_commit_message) {
            let age = git.last_commit_age_mins.map(format_age).unwrap_or_default();
            // Available space: width - label(LABEL_COL) - hash(7) - separators(6) - age
            let overhead = LABEL_COL + hash.len() + age.len() + 6;
            let max_msg = self.width.saturating_sub(overhead).max(10);
            let truncated_msg = if msg.len() > max_msg {
                format!("{}…", &msg[..max_msg - 1])
            } else {
                msg.clone()
            };
            println!(
                "{label}      {} {}   {}",
                style(hash).dim(),
                style(truncated_msg).dim(),
                style(age).dim()
            );
        }

        // Second line: stats (ahead/behind shown in header, not here)
        let mut stat_parts = Vec::new();
        if let Some(files) = &git.files_changed {
            stat_parts.push(files.clone());
        }
        // Only show uncommitted_count when files_changed is absent (untracked-only changes)
        if git.files_changed.is_none() && git.uncommitted_count > 0 {
            stat_parts.push(format!("{} uncommitted", git.uncommitted_count));
        }
        if git.stash_count > 0 {
            stat_parts.push(format!(
                "{} stash{}",
                git.stash_count,
                if git.stash_count == 1 { "" } else { "es" }
            ));
        }

        if !stat_parts.is_empty() {
            let indent = " ".repeat(LABEL_COL);
            let sep = style("·").dim();
            println!("{indent}{}", stat_parts.join(&format!(" {sep} ")));
        }

        if let Some(message) = &git.probe_message {
            let indent = " ".repeat(LABEL_COL);
            println!("{indent}{}", style(message).yellow());
        }
    }
}

/// Format an age in minutes to a human-readable relative time.
fn format_age(mins: i64) -> String {
    if mins < 1 {
        "just now".to_string()
    } else if mins < 60 {
        format!("{mins}m ago")
    } else if mins < 60 * 24 {
        format!("{}h ago", mins / 60)
    } else {
        format!("{}d ago", mins / (60 * 24))
    }
}

// ─── Full Status ────────────────────────────────────────────────────────────

/// Collect one round of workspace status data.
fn collect_status_data(
    ctx: &CommandContext,
) -> (
    PostgresProbe,
    NatsProbe,
    bool,
    Option<RuntimeMetrics>,
    Vec<ServiceStatus>,
    JobsSnapshot,
    HistorySnapshot,
) {
    let cfg = config();
    let runtime_db_url = resolve_runtime_metrics_database_url(cfg.database_url.as_deref());
    let runtime_configured = runtime_db_url
        .as_ref()
        .ok()
        .and_then(|value| value.as_ref())
        .is_some();

    let (pg_probe, nats_probe, runtime_metrics, services, jobs, history) =
        std::thread::scope(|s| {
            // Thread 1: Infrastructure + services (subprocesses)
            let infra_handle = s.spawn(move || {
                let pg = probe_postgres();
                let nats = probe_nats();

                let svcs = collect_core_service_statuses();

                (pg, nats, svcs)
            });

            let runtime_metrics_handle = s.spawn(move || collect_runtime_metrics(runtime_db_url));
            let history_handle = s.spawn(move || collect_history_snapshot(ctx, 10, false));

            // Main thread: local operations (jobs)
            let jobs = collect_jobs_snapshot(20);

            let (pg, nats, svcs) = match infra_handle.join() {
                Ok(result) => result,
                Err(payload) => {
                    let message = format!(
                        "infra probe thread panicked: {}",
                        describe_thread_panic(&*payload)
                    );
                    (
                        PostgresProbe {
                            running: false,
                            accepting_connections: false,
                            latency_ms: 0,
                            message: Some(message.clone()),
                        },
                        NatsProbe {
                            running: false,
                            reachable: false,
                            latency_ms: 0,
                            port: 4222,
                            message: Some(message),
                        },
                        vec![],
                    )
                }
            };
            let runtime_metrics = recover_runtime_metrics_thread(runtime_metrics_handle.join());
            let history = history_handle.join().unwrap_or_else(|payload| {
                HistorySnapshot::unavailable(format!(
                    "history collection thread panicked: {}",
                    describe_thread_panic(&*payload)
                ))
            });

            (pg, nats, runtime_metrics, svcs, jobs, history)
        });

    (
        pg_probe,
        nats_probe,
        runtime_configured,
        runtime_metrics,
        services,
        jobs,
        history,
    )
}

/// Render and optionally return one status snapshot.
fn render_status_tick(ctx: &CommandContext, watch: bool) -> Result<Option<CommandResult>> {
    let (pg_probe, nats_probe, runtime_configured, runtime_metrics, services, jobs, history) =
        collect_status_data(ctx);
    let runtime_assessment = runtime_metrics.as_ref().map(RuntimeMetrics::assessment);
    let unavailable_services: Vec<&str> = services
        .iter()
        .filter(|service| !matches!(service.status, ServiceRunStatus::Running))
        .map(|service| service.name.as_str())
        .collect();

    let recent_failures = jobs
        .recent
        .iter()
        .filter(|j| {
            matches!(
                j.job_status,
                crate::history::JobLifecycleStatus::Failed
                    | crate::history::JobLifecycleStatus::Orphaned
                    | crate::history::JobLifecycleStatus::Killed
            )
        })
        .count();

    let recent_activity: Vec<ActivityEntry> = history
        .recent
        .iter()
        .map(|inv| ActivityEntry {
            command: inv.command.clone(),
            status: match inv.status {
                InvocationStatus::Success => "success",
                InvocationStatus::Failed => "failed",
                InvocationStatus::Running => "running",
                InvocationStatus::Cancelled => "cancelled",
            }
            .to_string(),
            duration_secs: inv.duration_secs.unwrap_or(0.0),
            timestamp: inv
                .started_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
        })
        .collect();

    // Build warnings
    let mut warnings = Vec::new();
    if !pg_probe.ready() {
        warnings.push(
            pg_probe
                .message
                .clone()
                .unwrap_or_else(|| "Postgres is offline. Some commands will fail.".to_string()),
        );
    }
    if !nats_probe.ready() {
        warnings.push(
            nats_probe
                .message
                .clone()
                .unwrap_or_else(|| "NATS is offline. Real-time features won't work.".to_string()),
        );
    }
    if let Some(fail) = history
        .recent
        .iter()
        .find(|i| i.status == InvocationStatus::Failed)
    {
        warnings.push(format!("Last run of '{}' failed.", fail.command));
    }
    if jobs.active.len() > 5 {
        warnings.push(format!("{} background jobs running.", jobs.active.len()));
    }
    if !unavailable_services.is_empty() {
        warnings.push(format!(
            "Core services not healthy: {}",
            unavailable_services.join(", ")
        ));
    }
    if history.is_synthetic {
        warnings.push("History DB is seeded with synthetic data".to_string());
    }
    warnings.extend(history.issues.clone());
    warnings.extend(jobs.issues.clone());
    if let Some(runtime_assessment) = runtime_assessment.as_ref() {
        warnings.extend(runtime_assessment.warnings.clone());
    }

    // Human output
    if ctx.is_human() {
        println!(
            "{}",
            style("━━━━━━━━━━━━━━━━ WORKSPACE STATUS ━━━━━━━━━━━━━━━━").bold()
        );

        // Infrastructure
        println!("\n{}", style("Infrastructure:").bold());
        println!(
            "  {:<12} {} ({}ms)",
            "Postgres",
            if pg_probe.ready() {
                style("online").green()
            } else {
                style("offline").red()
            },
            pg_probe.latency_ms
        );
        if let Some(message) = &pg_probe.message {
            println!("  {:<12} {}", "", style(message).dim());
        }
        println!(
            "  {:<12} {} ({}ms, port {})",
            "NATS",
            if nats_probe.ready() {
                style("online").green()
            } else {
                style("offline").red()
            },
            nats_probe.latency_ms,
            nats_probe.port
        );
        if let Some(message) = &nats_probe.message {
            println!("  {:<12} {}", "", style(message).dim());
        }

        // Services
        println!("\n{}", style("Services:").bold());
        for svc in &services {
            let status_label = match svc.status {
                ServiceRunStatus::Running => "running",
                ServiceRunStatus::Stopped => "stopped",
                ServiceRunStatus::Unknown => "unknown",
            };
            let status_display = match svc.status {
                ServiceRunStatus::Running => style(status_label).green(),
                ServiceRunStatus::Stopped => style(status_label).dim(),
                ServiceRunStatus::Unknown => style(status_label).yellow(),
            };
            let pid_str = svc.pid.map(|p| format!(" (pid {p})")).unwrap_or_default();
            println!("  {:<20} {}{}", svc.name, status_display, pid_str);
            if let Some(message) = &svc.message {
                println!("  {:<20} {}", "", style(message).dim());
            }
        }

        println!("\n{}", style("Runtime:").bold());
        if let Some(metrics) = &runtime_metrics {
            let status_icon = match metrics.ingestd_status {
                IngestdStatus::Healthy => style("✓").green(),
                IngestdStatus::Stale => style("⚠").yellow(),
                IngestdStatus::Down => style("✗").red(),
                IngestdStatus::Unknown => style("?").dim(),
            };
            let heartbeat = metrics
                .last_heartbeat_age_secs
                .map(|age| format!(" (heartbeat {age}s ago)"))
                .unwrap_or_default();
            println!(
                "  {} {:<20}{}",
                status_icon,
                format!("ingestd: {}", metrics.ingestd_status),
                style(heartbeat).dim()
            );

            match metrics.fresh_consumer_lag_pending() {
                Some(lag) => println!(
                    "  {} Consumer lag:       {:.0} pending",
                    style("-").dim(),
                    lag
                ),
                None if metrics.consumer_lag_is_stale() => println!(
                    "  {} Consumer lag:       stale telemetry (last sample {}s ago)",
                    style("⚠").yellow(),
                    metrics.consumer_lag_age_secs.unwrap_or_default()
                ),
                None => {}
            }

            match metrics.fresh_batch_latency_ms() {
                Some(latency) => {
                    println!(
                        "  {} Batch latency:      {:.0}ms",
                        style("-").dim(),
                        latency
                    )
                }
                None if metrics.batch_latency_is_stale() => println!(
                    "  {} Batch latency:      stale telemetry (last sample {}s ago)",
                    style("⚠").yellow(),
                    metrics.last_batch_latency_age_secs.unwrap_or_default()
                ),
                None => {}
            }
            if let Some(message) = runtime_query_error_message(metrics) {
                println!("  {} {}", style("✗").red(), message);
            }
        } else if runtime_configured {
            println!(
                "  {} Runtime metrics unavailable despite DATABASE_URL being set",
                style("⚠").yellow()
            );
        } else {
            println!(
                "  {} Runtime metrics unavailable (DATABASE_URL not set)",
                style("·").dim()
            );
        }

        println!("\n{}", style("History:").bold());
        let history_status = match history.status() {
            "available" => style("available").green().to_string(),
            "synthetic" => style("synthetic").yellow().to_string(),
            "degraded" => style("degraded").yellow().to_string(),
            _ => style("unavailable").red().to_string(),
        };
        println!("  {:<12} {}", "Status", history_status);
        println!("  {:<12} {}", "Recent", history.recent.len());
        if history.diag_counts.total() > 0 || history.flaky_count > 0 {
            println!(
                "  {:<12} {} errors, {} warnings, {} fixable, {} flaky",
                "Diagnostics",
                history.diag_counts.errors,
                history.diag_counts.warnings,
                history.diag_counts.fixable,
                history.flaky_count
            );
        }
        if let Some(message) = history.message() {
            println!("  {:<12} {}", "", style(message).dim());
        }

        // Jobs
        println!("\n{}", style("Background Jobs:").bold());
        println!("  Active:    {}", jobs.active.len());
        println!(
            "  Failures:  {}",
            if recent_failures > 0 {
                style(recent_failures.to_string()).red()
            } else {
                style("0".to_string()).dim()
            }
        );
        for issue in &jobs.issues {
            println!("  {:<12} {}", "", style(issue).dim());
        }

        // Recent activity
        println!("\n{}", style("Recent Activity:").bold());
        if recent_activity.is_empty() {
            println!("  {}", style("No recent xtask invocations recorded.").dim());
        } else {
            for entry in recent_activity.iter().take(5) {
                let status_style = match entry.status.as_str() {
                    "success" => style(&entry.status).green(),
                    "failed" => style(&entry.status).red(),
                    "running" => style(&entry.status).yellow(),
                    _ => style(&entry.status).dim(),
                };
                println!(
                    "  {:<15} {:<10} ({:.1}s)",
                    entry.command, status_style, entry.duration_secs
                );
            }
        }

        // Warnings
        println!("\n{}", style("Warnings:").bold());
        if warnings.is_empty() {
            println!("  {} No issues detected.", style("✓").green());
        } else {
            for w in &warnings {
                println!("  {} {}", style("⚠").yellow(), w);
            }
        }
    }

    if !watch {
        if !ctx.is_human() {
            let output = StatusOutput {
                infrastructure: InfrastructureStatus {
                    postgres: ComponentStatus {
                        status: if pg_probe.ready() { "ready" } else { "offline" }.to_string(),
                        latency_ms: Some(pg_probe.latency_ms),
                        port: None,
                        message: pg_probe.message,
                    },
                    nats: ComponentStatus {
                        status: if nats_probe.ready() {
                            "reachable"
                        } else {
                            "offline"
                        }
                        .to_string(),
                        latency_ms: Some(nats_probe.latency_ms),
                        port: Some(nats_probe.port),
                        message: nats_probe.message,
                    },
                },
                services,
                runtime: runtime_metrics,
                runtime_assessment,
                history: history.output(),
                jobs: JobsStatus {
                    active: jobs.active.len(),
                    recent_failures,
                },
                recent_activity,
                warnings,
            };
            return Ok(Some(
                CommandResult::success()
                    .with_data(serde_json::to_value(&output)?)
                    .with_duration(ctx.elapsed()),
            ));
        }
        return Ok(Some(CommandResult::success().with_duration(ctx.elapsed())));
    }

    Ok(None)
}

/// Full status (default mode)
async fn execute_full_status(watch: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if !watch {
        if let Some(result) = render_status_tick(ctx, false)? {
            return Ok(result);
        }
    }

    let term = console::Term::stdout();
    WatchLoop::with_interval_secs(3)
        .run(|first| {
            let ctx = ctx;
            let term = &term;
            async move {
                if !first {
                    term.clear_screen()?;
                    term.move_cursor_to(0, 0)?;
                }
                render_status_tick(ctx, true)?;
                Ok(WatchAction::Continue)
            }
        })
        .await?;

    Ok(CommandResult::success().with_duration(ctx.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::os::unix::process::ExitStatusExt;
    use std::path::Path;
    use tempfile::tempdir;
    use xtask::sandbox::EnvGuard;

    fn run_git(args: &[&str], cwd: &Path) -> ::xtask::sandbox::TestResult<()> {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()?;
        assert!(
            output.status.success(),
            "git {} failed: stdout={} stderr={}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = StatusCommand {
            watch: false,
            summary: false,
            schemas: false,
        };
        assert_eq!(cmd.name(), "status");
        Ok(())
    }

    #[sinex_test]
    async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = StatusCommand {
            watch: false,
            summary: false,
            schemas: false,
        };
        let metadata = cmd.metadata();
        assert!(!metadata.modifies_state);
        assert!(metadata.track_in_history);
        Ok(())
    }

    #[sinex_test]
    async fn test_probe_service_status_reports_probe_failures_as_unknown()
    -> ::xtask::sandbox::TestResult<()> {
        let status = probe_service_status_with(
            "sinex-gateway",
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "pgrep unavailable",
            )),
        );

        assert_eq!(status.status, ServiceRunStatus::Unknown);
        assert!(status.pid.is_none());
        assert!(
            status
                .message
                .as_deref()
                .is_some_and(|message| message.contains("failed to run process probe"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_probe_service_status_reports_unreadable_pid_output_as_unknown()
    -> ::xtask::sandbox::TestResult<()> {
        let status = probe_service_status_with(
            "sinex-gateway",
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: b"not-a-pid\n".to_vec(),
                stderr: Vec::new(),
            }),
        );

        assert_eq!(status.status, ServiceRunStatus::Unknown);
        assert!(status.pid.is_none());
        assert!(
            status
                .message
                .as_deref()
                .is_some_and(|message| message.contains("unreadable pid output"))
        );
        Ok(())
    }

    // --- JSON shape tests: verify serialization contracts agents depend on ---

    #[sinex_test]
    async fn test_status_output_json_shape() -> ::xtask::sandbox::TestResult<()> {
        let output = StatusOutput {
            infrastructure: InfrastructureStatus {
                postgres: ComponentStatus {
                    status: "ready".into(),
                    latency_ms: Some(5),
                    port: None,
                    message: None,
                },
                nats: ComponentStatus {
                    status: "reachable".into(),
                    latency_ms: Some(2),
                    port: Some(4222),
                    message: None,
                },
            },
            services: vec![ServiceStatus {
                name: "sinex-gateway".into(),
                status: ServiceRunStatus::Running,
                probe: "process_exact_name",
                pid: Some(12345),
                message: None,
            }],
            runtime: Some(RuntimeMetrics {
                ingestd_status: IngestdStatus::Healthy,
                last_heartbeat_age_secs: Some(5),
                consumer_lag_pending: Some(7.0),
                consumer_lag_age_secs: Some(10),
                last_batch_latency_ms: Some(125.0),
                last_batch_latency_age_secs: Some(10),
                query_error: None,
            }),
            runtime_assessment: Some(RuntimeAssessment {
                status: crate::runtime_metrics::RuntimeHealthStatus::Healthy,
                warnings: Vec::new(),
            }),
            history: HistoryStatusOutput {
                status: "available".into(),
                synthetic: false,
                recent_invocations: 1,
                diagnostic_errors: 0,
                diagnostic_warnings: 0,
                fixable_diagnostics: 0,
                flaky_tests: 0,
                message: None,
            },
            jobs: JobsStatus {
                active: 2,
                recent_failures: 0,
            },
            recent_activity: vec![ActivityEntry {
                command: "check".into(),
                status: "success".into(),
                duration_secs: 3.5,
                timestamp: "2025-01-01T00:00:00Z".into(),
            }],
            warnings: vec!["Test warning".into()],
        };

        let json = serde_json::to_value(&output)?;

        // Infrastructure shape (agents use: .data.infrastructure.postgres.status)
        assert!(json["infrastructure"]["postgres"]["status"].is_string());
        assert!(json["infrastructure"]["postgres"]["latency_ms"].is_number());
        assert!(json["infrastructure"]["nats"]["status"].is_string());
        assert!(json["infrastructure"]["nats"]["port"].is_number());
        // port=None on postgres should be absent (skip_serializing_if)
        assert!(json["infrastructure"]["postgres"]["port"].is_null());

        // Services shape (agents use: .data.services[].name, .status)
        assert!(json["services"].is_array());
        assert_eq!(json["services"][0]["name"], "sinex-gateway");
        assert_eq!(json["services"][0]["status"], "running");
        assert_eq!(json["services"][0]["pid"], 12345);
        assert_eq!(json["runtime"]["ingestd_status"], "healthy");
        assert_eq!(json["runtime_assessment"]["status"], "healthy");
        assert_eq!(json["history"]["status"], "available");
        assert_eq!(json["history"]["recent_invocations"], 1);

        // Jobs shape (agents use: .data.jobs.active, .recent_failures)
        assert_eq!(json["jobs"]["active"], 2);
        assert_eq!(json["jobs"]["recent_failures"], 0);

        // Activity shape (agents use: .data.recent_activity[].command)
        assert!(json["recent_activity"].is_array());
        assert_eq!(json["recent_activity"][0]["command"], "check");
        assert_eq!(json["recent_activity"][0]["status"], "success");

        // Warnings
        assert!(json["warnings"].is_array());
        assert_eq!(json["warnings"][0], "Test warning");
        Ok(())
    }

    #[sinex_test]
    async fn test_summary_output_json_shape() -> ::xtask::sandbox::TestResult<()> {
        let output = SummaryOutput {
            health: "degraded".into(),
            health_indicator: "warn".into(),
            summary: "infra:ok jobs:1 tests:ok warns:2w fixes:1f git:dirty".into(),
            infrastructure: SummaryInfraHealth {
                postgres: true,
                nats: true,
            },
            last_commands: SummaryLastCommands {
                check: Some(SummaryCommandInfo {
                    status: InvocationStatus::Success,
                    duration_secs: 3.2,
                    age_mins: 15,
                }),
                test: None,
                build: None,
            },
            diagnostics: SummaryDiagnostics {
                errors: 0,
                warnings: 2,
                fixable: 1,
                flaky_tests: 0,
            },
            active_jobs: 1,
            git: SummaryGitState {
                branch: Some("feature/test".into()),
                dirty: true,
                ahead: 2,
                behind: 0,
                message: None,
            },
            warnings: vec!["Uncommitted changes".into()],
            history: HistoryStatusOutput {
                status: "degraded".into(),
                synthetic: false,
                recent_invocations: 3,
                diagnostic_errors: 0,
                diagnostic_warnings: 2,
                fixable_diagnostics: 1,
                flaky_tests: 0,
                message: Some("Failed to compute workspace recommendations".into()),
            },
            // Rich fields
            health_score: Some(85),
            velocity: Some(vec![VelocityTrendOutput {
                command: "check".into(),
                recent_avg_secs: Some(4.2),
                delta_pct: Some(-12.0),
                trend: "improving".into(),
                sample_count: 8,
            }]),
            recommendations: Some(vec![RecommendationOutput {
                severity: "warning".into(),
                category: "diagnostics".into(),
                description: "3 auto-fixable".into(),
                action: "xtask fix --smart".into(),
            }]),
            runtime: Some(RuntimeMetrics {
                ingestd_status: IngestdStatus::Down,
                last_heartbeat_age_secs: Some(240),
                consumer_lag_pending: Some(42.0),
                consumer_lag_age_secs: Some(240),
                last_batch_latency_ms: Some(125.0),
                last_batch_latency_age_secs: Some(240),
                query_error: None,
            }),
            services: Some(vec![ServiceStatus {
                name: "sinex-ingestd".into(),
                status: ServiceRunStatus::Stopped,
                probe: "process_exact_name",
                pid: None,
                message: Some("exact process-name probe found no matching process".into()),
            }]),
            last_commit: Some(CommitInfo {
                hash: "aafd524".into(),
                message: "fix(xtask): correct estimate_package_count".into(),
                age_mins: 32,
            }),
            stash_count: None,
            files_changed: Some("2 files changed".into()),
            uncommitted_count: Some(5),
        };

        let json = serde_json::to_value(&output)?;

        // Original fields preserved
        assert_eq!(json["health"], "degraded");
        assert_eq!(json["health_indicator"], "warn");
        assert!(json["summary"].as_str().unwrap().contains("infra:ok"));
        assert_eq!(json["infrastructure"]["postgres"], true);
        assert_eq!(json["infrastructure"]["nats"], true);
        assert_eq!(json["last_commands"]["check"]["status"], "success");
        assert!(json["last_commands"]["check"]["duration_secs"].is_number());
        assert!(json["last_commands"]["check"]["age_mins"].is_number());
        assert!(json["last_commands"]["test"].is_null());
        assert!(json["last_commands"]["build"].is_null());
        assert_eq!(json["git"]["branch"], "feature/test");
        assert_eq!(json["git"]["dirty"], true);
        assert_eq!(json["git"]["ahead"], 2);
        assert_eq!(json["git"]["behind"], 0);
        assert!(json["git"].get("message").is_none() || json["git"]["message"].is_null());
        assert_eq!(json["diagnostics"]["errors"], 0);
        assert_eq!(json["diagnostics"]["warnings"], 2);
        assert_eq!(json["diagnostics"]["fixable"], 1);
        assert_eq!(json["diagnostics"]["flaky_tests"], 0);
        assert_eq!(json["active_jobs"], 1);
        assert_eq!(json["history"]["status"], "degraded");
        assert_eq!(json["history"]["diagnostic_warnings"], 2);

        // New rich fields
        assert_eq!(json["health_score"], 85);
        assert!(json["velocity"].is_array());
        assert_eq!(json["velocity"][0]["command"], "check");
        assert_eq!(json["velocity"][0]["delta_pct"], -12.0);
        assert!(json["recommendations"].is_array());
        assert_eq!(json["recommendations"][0]["severity"], "warning");
        assert_eq!(json["recommendations"][0]["action"], "xtask fix --smart");
        assert_eq!(json["runtime"]["ingestd_status"], "down");
        assert_eq!(json["runtime"]["consumer_lag_age_secs"], 240);
        assert!(json["services"].is_array());
        assert_eq!(json["services"][0]["name"], "sinex-ingestd");
        assert_eq!(json["services"][0]["status"], "stopped");
        assert!(json["services"][0]["pid"].is_null());
        assert_eq!(json["last_commit"]["hash"], "aafd524");
        assert_eq!(json["last_commit"]["age_mins"], 32);
        assert_eq!(json["files_changed"], "2 files changed");
        assert_eq!(json["uncommitted_count"], 5);
        // stash_count=None should be absent
        assert!(json.get("stash_count").is_none() || json["stash_count"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_summary_health_promotes_history_errors_to_unhealthy()
    -> ::xtask::sandbox::TestResult<()> {
        let (health, indicator) = classify_summary_health(
            true,
            true,
            false,
            1,
            0,
            false,
            false,
            false,
            false,
            None,
            true,
        );
        assert_eq!(health, "unhealthy");
        assert_eq!(indicator, "error");
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_summary_health_marks_warning_only_state_degraded()
    -> ::xtask::sandbox::TestResult<()> {
        let (health, indicator) = classify_summary_health(
            true,
            true,
            true,
            0,
            1,
            false,
            false,
            false,
            false,
            None,
            true,
        );
        assert_eq!(health, "degraded");
        assert_eq!(indicator, "warn");
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_summary_health_promotes_runtime_failures_to_unhealthy()
    -> ::xtask::sandbox::TestResult<()> {
        let (health, indicator) = classify_summary_health(
            true,
            true,
            true,
            0,
            0,
            false,
            false,
            false,
            false,
            Some(SummaryRuntimeImpact::Unhealthy),
            true,
        );
        assert_eq!(health, "unhealthy");
        assert_eq!(indicator, "error");
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_runtime_summary_impact_treats_ingestd_down_as_unhealthy()
    -> ::xtask::sandbox::TestResult<()> {
        let metrics = RuntimeMetrics {
            ingestd_status: IngestdStatus::Down,
            last_heartbeat_age_secs: None,
            consumer_lag_pending: None,
            consumer_lag_age_secs: None,
            last_batch_latency_ms: None,
            last_batch_latency_age_secs: None,
            query_error: None,
        };
        assert_eq!(
            classify_runtime_summary_impact(&metrics),
            SummaryRuntimeImpact::Unhealthy
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_describe_thread_panic_handles_string_payload()
    -> ::xtask::sandbox::TestResult<()> {
        let payload: Box<dyn Any + Send> = Box::new(String::from("boom"));
        assert_eq!(describe_thread_panic(&*payload), "boom");
        Ok(())
    }

    #[sinex_test]
    async fn test_recover_runtime_metrics_thread_surfaces_panic_detail()
    -> ::xtask::sandbox::TestResult<()> {
        let metrics = recover_runtime_metrics_thread(Err(Box::new("boom")))
            .unwrap_or_else(|| panic!("expected runtime metrics error payload"));
        assert_eq!(
            runtime_query_error_message(&metrics).as_deref(),
            Some("Runtime metrics query failed: runtime metrics collection thread panicked: boom")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_runtime_metrics_database_url_skips_descriptor_load_when_explicit_url_present()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempdir()?;
        let descriptor_path = temp.path().join("deployment-readiness.json");
        std::fs::write(&descriptor_path, "{ definitely-not-json")?;

        let mut env = EnvGuard::new();
        env.set(
            "SINEX_DEPLOYMENT_READINESS_CONFIG",
            descriptor_path.display().to_string(),
        );

        let url = resolve_runtime_metrics_database_url(Some(
            "postgresql:///sinex_dev?host=/tmp/sinex-test-run",
        ))?;

        assert_eq!(
            url.as_deref(),
            Some("postgresql:///sinex_dev?host=/tmp/sinex-test-run")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_runtime_metrics_database_url_reports_descriptor_parse_failure_without_explicit_url()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempdir()?;
        let descriptor_path = temp.path().join("deployment-readiness.json");
        std::fs::write(&descriptor_path, "{ definitely-not-json")?;

        let mut env = EnvGuard::new();
        env.set(
            "SINEX_DEPLOYMENT_READINESS_CONFIG",
            descriptor_path.display().to_string(),
        );

        let error = resolve_runtime_metrics_database_url(None)
            .expect_err("invalid descriptor should still fail when no DATABASE_URL is provided");

        let error_text = format!("{error:?}");
        assert!(error_text.contains("failed to load deployment readiness descriptor"));
        assert!(error_text.contains("failed to parse deployment readiness descriptor"));
        Ok(())
    }

    #[sinex_test]
    async fn test_runtime_query_error_message_surfaces_full_detail()
    -> ::xtask::sandbox::TestResult<()> {
        let metrics = RuntimeMetrics {
            ingestd_status: IngestdStatus::Unknown,
            last_heartbeat_age_secs: None,
            consumer_lag_pending: None,
            consumer_lag_age_secs: None,
            last_batch_latency_ms: None,
            last_batch_latency_age_secs: None,
            query_error: Some("permission denied".to_string()),
        };
        assert_eq!(
            runtime_query_error_message(&metrics).as_deref(),
            Some("Runtime metrics query failed: permission denied")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_component_status_skip_serializing_none() -> ::xtask::sandbox::TestResult<()> {
        let status = ComponentStatus {
            status: "offline".into(),
            latency_ms: None,
            port: None,
            message: None,
        };
        let json = serde_json::to_value(&status)?;
        assert!(json.get("latency_ms").is_none());
        assert!(json.get("port").is_none());
        assert_eq!(json["status"], "offline");
        Ok(())
    }

    #[sinex_test]
    async fn test_probe_git_state_reports_missing_upstream() -> ::xtask::sandbox::TestResult<()> {
        let repo = tempdir()?;
        run_git(&["init", "-q"], repo.path())?;
        run_git(&["config", "user.name", "Sinex Test"], repo.path())?;
        run_git(&["config", "user.email", "sinex@example.test"], repo.path())?;
        std::fs::write(repo.path().join("README.md"), "hello\n")?;
        run_git(&["add", "README.md"], repo.path())?;
        run_git(&["commit", "-qm", "init"], repo.path())?;

        let git = probe_git_state(repo.path());

        assert_eq!(git.ahead, 0);
        assert_eq!(git.behind, 0);
        assert!(git.last_commit_hash.is_some());
        assert!(
            git.probe_message
                .as_deref()
                .is_some_and(|message| message.contains("git rev-list --left-right --count HEAD...@{u} failed"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_probe_git_state_reports_non_repo_failures() -> ::xtask::sandbox::TestResult<()> {
        let dir = tempdir()?;

        let git = probe_git_state(dir.path());

        assert!(!git.dirty);
        assert!(git.last_commit_hash.is_none());
        let probe_message = git
            .probe_message
            .as_deref()
            .unwrap_or_else(|| panic!("expected git probe failure message"));
        assert!(probe_message.contains("git branch --show-current failed"));
        assert!(probe_message.contains("git status --porcelain failed"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_git_upstream_counts_accepts_valid_counts(
    ) -> ::xtask::sandbox::TestResult<()> {
        let mut probe_issues = Vec::new();

        assert_eq!(parse_git_upstream_counts("2\t7\n", &mut probe_issues), (2, 7));
        assert!(probe_issues.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_git_upstream_counts_reports_invalid_numbers(
    ) -> ::xtask::sandbox::TestResult<()> {
        let mut probe_issues = Vec::new();

        assert_eq!(parse_git_upstream_counts("2\tnope", &mut probe_issues), (0, 0));
        let message = probe_issues.join("; ");
        assert!(message.contains("git rev-list --left-right --count HEAD...@{u} failed"));
        assert!(message.contains("invalid behind count `nope`"));
        Ok(())
    }

    #[sinex_test]
    async fn test_service_status_skip_serializing_none_pid() -> ::xtask::sandbox::TestResult<()> {
        let stopped = ServiceStatus {
            name: "sinex-ingestd".into(),
            status: ServiceRunStatus::Stopped,
            probe: "process_exact_name",
            pid: None,
            message: None,
        };
        let json = serde_json::to_value(&stopped)?;
        assert!(
            json.get("pid").is_none(),
            "pid=None should be absent from JSON"
        );
        assert_eq!(json["name"], "sinex-ingestd");

        let running = ServiceStatus {
            name: "sinex-gateway".into(),
            status: ServiceRunStatus::Running,
            probe: "process_exact_name",
            pid: Some(42),
            message: None,
        };
        let json = serde_json::to_value(&running)?;
        assert_eq!(json["pid"], 42);
        Ok(())
    }

    #[sinex_test]
    async fn test_format_age() -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(format_age(0), "just now");
        assert_eq!(format_age(3), "3m ago");
        assert_eq!(format_age(59), "59m ago");
        assert_eq!(format_age(60), "1h ago");
        assert_eq!(format_age(120), "2h ago");
        assert_eq!(format_age(60 * 24), "1d ago");
        assert_eq!(format_age(60 * 24 * 3), "3d ago");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_git_commit_age_mins() -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(parse_git_commit_age_mins("100", 100), Some(0));
        assert_eq!(parse_git_commit_age_mins("40", 100), Some(1));
        assert_eq!(
            parse_git_commit_age_mins("0", 60 * 60 * 24 * 3),
            Some(60 * 24 * 3)
        );
        assert_eq!(parse_git_commit_age_mins("200", 100), Some(0));
        assert_eq!(parse_git_commit_age_mins("", 100), None);
        assert_eq!(parse_git_commit_age_mins("garbage", 100), None);
        Ok(())
    }
}
