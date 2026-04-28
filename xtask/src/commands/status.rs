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
use crate::infra::stack::StackConfig;
use crate::runtime_metrics::{IngestdStatus, RuntimeAssessment, RuntimeMetrics};
use crate::runtime_target::{
    RuntimeTargetSummary, checkout_runtime_target, checkout_status_snapshot, signal, warning,
};
use crate::session::{WatchAction, WatchLoop};
use color_eyre::eyre::{Result, WrapErr};
use console::style;
use serde::Serialize;
use sinex_primitives::{
    RuntimeStatusSignalStatus, RuntimeStatusSnapshot, RuntimeTargetDescriptor, RuntimeTargetKind,
};
use std::any::Any;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Inspect workspace status, service health, and recent activity.
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
    runtime_target: RuntimeTargetSummary,
    runtime_snapshot: RuntimeStatusSnapshot,
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
    Skipped,
    Unknown,
}

fn service_status_from_active_job(service_name: &str, job: &crate::jobs::Job) -> ServiceStatus {
    ServiceStatus {
        name: service_name.to_string(),
        status: ServiceRunStatus::Running,
        probe: "background_job",
        pid: job.pid,
        message: None,
    }
}

fn active_job_for_service<'a>(
    service_name: &str,
    active_jobs: &'a [crate::jobs::Job],
) -> Option<&'a crate::jobs::Job> {
    active_jobs.iter().find(|job| {
        job.is_alive()
            && std::path::Path::new(&job.command)
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|binary| binary == service_name)
    })
}

fn gateway_service_status_from_readiness(
    readiness: crate::commands::doctor::DeploymentReadinessItem,
    pid: Option<u32>,
    force_probe: bool,
) -> ServiceStatus {
    let (status, message) = match readiness.status.as_str() {
        "pass" => (ServiceRunStatus::Running, None),
        "fail" => {
            if force_probe {
                (
                    ServiceRunStatus::Unknown,
                    Some(format!(
                        "gateway process is alive but readiness probe failed: {}",
                        readiness.description
                    )),
                )
            } else {
                (ServiceRunStatus::Stopped, Some(readiness.description))
            }
        }
        "skip" => {
            if force_probe {
                (
                    ServiceRunStatus::Unknown,
                    Some(format!(
                        "gateway process is alive but readiness probe skipped unexpectedly: {}",
                        readiness.description
                    )),
                )
            } else {
                (ServiceRunStatus::Skipped, Some(readiness.description))
            }
        }
        other => (
            ServiceRunStatus::Unknown,
            Some(format!(
                "gateway readiness probe returned unexpected status `{other}`: {}",
                readiness.description
            )),
        ),
    };

    ServiceStatus {
        name: "sinex-gateway".to_string(),
        status,
        probe: "gateway_ready_http",
        pid,
        message,
    }
}

async fn probe_gateway_service_status(
    gateway_url: Option<&str>,
    force_probe: bool,
    pid: Option<u32>,
) -> ServiceStatus {
    let readiness = crate::commands::doctor::check_gateway_ready(gateway_url, None).await;
    gateway_service_status_from_readiness(readiness, pid, force_probe)
}

fn status_profile_enabled() -> bool {
    std::env::var("SINEX_STATUS_PROFILE").is_ok_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn emit_status_profile(label: &str, started_at: Instant) {
    if status_profile_enabled() {
        eprintln!(
            "[status-profile] {label}: {:.3}s",
            started_at.elapsed().as_secs_f64()
        );
    }
}

fn emit_status_profile_duration(label: &str, duration: Duration) {
    if status_profile_enabled() {
        eprintln!("[status-profile] {label}: {:.3}s", duration.as_secs_f64());
    }
}

async fn collect_core_service_statuses(
    gateway_url: Option<&str>,
    runtime_metrics: Option<&RuntimeMetrics>,
    active_jobs: &[crate::jobs::Job],
) -> Vec<ServiceStatus> {
    let ingestd = active_job_for_service("sinex-ingestd", active_jobs).map_or_else(
        || ingestd_service_status_from_runtime_metrics(runtime_metrics),
        |job| service_status_from_active_job("sinex-ingestd", job),
    );
    let gateway_process = active_job_for_service("sinex-gateway", active_jobs).map_or_else(
        || ServiceStatus {
            name: "sinex-gateway".to_string(),
            status: ServiceRunStatus::Stopped,
            probe: "checkout_local",
            pid: None,
            message: Some("no active checkout-local gateway job is tracked".to_string()),
        },
        |job| service_status_from_active_job("sinex-gateway", job),
    );
    let gateway_force_probe = matches!(gateway_process.status, ServiceRunStatus::Running);

    vec![
        probe_gateway_service_status(gateway_url, gateway_force_probe, gateway_process.pid).await,
        ingestd,
    ]
}

fn resolve_runtime_metrics_database_url(database_url: Option<&str>) -> Result<Option<String>> {
    if let Some(url) = database_url {
        return Ok(Some(url.to_string()));
    }

    let stack_config = StackConfig::for_current_checkout()
        .wrap_err("failed to load checkout stack config for runtime metrics")?;
    Ok(Some(stack_config.database_url()))
}

fn ingestd_service_status_from_runtime_metrics(
    runtime_metrics: Option<&RuntimeMetrics>,
) -> ServiceStatus {
    let (status, message) = match runtime_metrics {
        Some(metrics) => match metrics.ingestd_status {
            IngestdStatus::Healthy => (ServiceRunStatus::Running, None),
            IngestdStatus::Down => (
                ServiceRunStatus::Stopped,
                Some(
                    "no checkout-local ingestd heartbeat found in the local runtime database"
                        .to_string(),
                ),
            ),
            IngestdStatus::Stale => (
                ServiceRunStatus::Unknown,
                Some(
                    "checkout-local ingestd heartbeat is stale in the local runtime database"
                        .to_string(),
                ),
            ),
            IngestdStatus::Unknown => (
                ServiceRunStatus::Unknown,
                metrics
                    .query_error
                    .clone()
                    .or_else(|| Some("checkout-local ingestd status is unavailable".to_string())),
            ),
        },
        None => (
            ServiceRunStatus::Unknown,
            Some("checkout-local runtime database target is unavailable".to_string()),
        ),
    };

    ServiceStatus {
        name: "sinex-ingestd".to_string(),
        status,
        probe: "runtime_metrics",
        pid: None,
        message,
    }
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

fn collect_runtime_metrics_if_postgres_ready(
    pg_probe: &PostgresProbe,
    runtime_db_url: Result<Option<String>>,
    target_kind: &RuntimeTargetKind,
) -> Option<RuntimeMetrics> {
    // Only gate on the local dev-stack probe when the runtime target IS that
    // local stack.  For deployed or VM targets the runtime database is a
    // separate system; skipping it because the local dev Postgres is not
    // running would silently suppress valid runtime telemetry.
    if *target_kind == RuntimeTargetKind::DevCheckout && !pg_probe.ready() {
        return Some(RuntimeMetrics::unavailable());
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

#[cfg(test)]
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
    duration_secs: Option<f64>,
    timestamp: String,
}

/// Summary (MOTD) output structure
#[derive(Debug, Serialize)]
struct SummaryOutput {
    runtime_target: RuntimeTargetSummary,
    runtime_snapshot: RuntimeStatusSnapshot,
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
    baseline_velocity: Option<Vec<VelocityTrendOutput>>,
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

#[allow(
    clippy::fn_params_excessive_bools,
    reason = "Each bool names a distinct health signal sourced from a different probe; bundling into a struct would only indirect the call sites"
)]
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

fn fallback_checkout_runtime_target(error: impl std::fmt::Display) -> RuntimeTargetDescriptor {
    RuntimeTargetDescriptor {
        version: 1,
        name: "checkout-local".to_string(),
        kind: RuntimeTargetKind::DevCheckout,
        source: Some("xtask checkout config".to_string()),
        notes: vec![format!("failed to derive checkout runtime target: {error}")],
        ..RuntimeTargetDescriptor::default()
    }
}

fn service_signal_status(status: ServiceRunStatus) -> RuntimeStatusSignalStatus {
    match status {
        ServiceRunStatus::Running => RuntimeStatusSignalStatus::Healthy,
        ServiceRunStatus::Stopped => RuntimeStatusSignalStatus::Unhealthy,
        ServiceRunStatus::Skipped => RuntimeStatusSignalStatus::Skipped,
        ServiceRunStatus::Unknown => RuntimeStatusSignalStatus::Unknown,
    }
}

fn build_runtime_status_snapshot(
    target: &RuntimeTargetDescriptor,
    pg_probe: &PostgresProbe,
    nats_probe: &NatsProbe,
    services: &[ServiceStatus],
    runtime_metrics: Option<&RuntimeMetrics>,
    warnings: &[String],
) -> RuntimeStatusSnapshot {
    let mut signals = vec![
        signal(
            "postgres",
            if pg_probe.ready() {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unhealthy
            },
            "checkout-local postgres probe",
            pg_probe.message.clone(),
        ),
        signal(
            "nats",
            if nats_probe.ready() {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unhealthy
            },
            "checkout-local nats probe",
            nats_probe.message.clone(),
        ),
    ];

    for service in services {
        signals.push(signal(
            service.name.clone(),
            service_signal_status(service.status),
            service.probe,
            service.message.clone(),
        ));
    }

    if let Some(metrics) = runtime_metrics {
        signals.push(signal(
            "ingestd_heartbeat",
            match metrics.ingestd_status {
                IngestdStatus::Healthy => RuntimeStatusSignalStatus::Healthy,
                IngestdStatus::Stale => RuntimeStatusSignalStatus::Stale,
                IngestdStatus::Down => RuntimeStatusSignalStatus::Unhealthy,
                IngestdStatus::Unknown => RuntimeStatusSignalStatus::Unknown,
            },
            "checkout-local runtime database telemetry",
            metrics
                .last_heartbeat_age_secs
                .map(|age| format!("heartbeat {age}s ago"))
                .or_else(|| metrics.query_error.clone()),
        ));

        signals.push(signal(
            "consumer_lag",
            if metrics.consumer_lag_is_stale() {
                RuntimeStatusSignalStatus::Stale
            } else if metrics.fresh_consumer_lag_pending().is_some() {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unknown
            },
            "checkout-local runtime database telemetry",
            metrics
                .fresh_consumer_lag_pending()
                .map(|pending| format!("{pending:.0} pending"))
                .or_else(|| metrics.consumer_lag_stale_note()),
        ));

        signals.push(signal(
            "batch_latency",
            if metrics.batch_latency_is_stale() {
                RuntimeStatusSignalStatus::Stale
            } else if metrics.fresh_batch_latency_ms().is_some() {
                RuntimeStatusSignalStatus::Healthy
            } else {
                RuntimeStatusSignalStatus::Unknown
            },
            "checkout-local runtime database telemetry",
            metrics
                .fresh_batch_latency_ms()
                .map(|latency| format!("{latency:.0}ms"))
                .or_else(|| metrics.batch_latency_stale_note()),
        ));
    }

    let attributed_warnings = warnings
        .iter()
        .map(|message| warning("xtask status", message.clone()))
        .collect();

    checkout_status_snapshot(target.clone(), signals, attributed_warnings)
}

#[derive(Debug, Serialize)]
struct VelocityTrendOutput {
    command: String,
    scope_label: Option<String>,
    recent_avg_secs: Option<f64>,
    delta_pct: Option<f64>,
    trend: String,
    sample_count: usize,
}

impl From<&VelocityTrend> for VelocityTrendOutput {
    fn from(v: &VelocityTrend) -> Self {
        Self {
            command: v.command.clone(),
            scope_label: v.scope_label.clone(),
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
    age_mins: Option<i64>,
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
    duration_secs: Option<f64>,
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
            execute_summary(ctx).await
        } else {
            execute_full_status(self.watch, ctx).await
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
            .with_history_tracking(false)
            .with_history_access(crate::command::HistoryAccessMode::Query)
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
    stash_count: Option<usize>,
    files_changed: Option<String>,
    uncommitted_count: Option<usize>,
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

fn record_git_probe_issue(
    probe_issues: &mut Vec<String>,
    args: &[&str],
    detail: impl Into<String>,
) {
    probe_issues.push(format!("git {} failed: {}", args.join(" "), detail.into()));
}

fn run_git_output(
    cwd: &Path,
    probe_issues: &mut Vec<String>,
    args: &[&str],
) -> Option<std::process::Output> {
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

    let (branch, dirty, uncommitted_count, ahead, behind) = run_git_output(
        cwd,
        &mut probe_issues,
        &["status", "--porcelain=v2", "--branch"],
    )
    .map_or((None, false, None, 0, 0), |output| {
        parse_git_status_branch_porcelain(
            &String::from_utf8_lossy(&output.stdout),
            &mut probe_issues,
        )
    });

    let commit = run_git_output(
        cwd,
        &mut probe_issues,
        &["log", "-1", "--format=%h\t%s\t%ct"],
    )
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

    let stash_count = run_git_output(cwd, &mut probe_issues, &["stash", "list"]).map(|output| {
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !line.is_empty())
            .count()
    });

    let files_changed = run_git_output(cwd, &mut probe_issues, &["diff", "--shortstat", "HEAD"])
        .and_then(|output| {
            let shortstat = String::from_utf8_lossy(&output.stdout).trim().to_string();
            (!shortstat.is_empty()).then_some(shortstat)
        });

    let now_unix_ts = current_unix_timestamp_secs();
    let last_age = commit.as_ref().and_then(|(_, _, commit_unix_ts)| {
        if let Some(now_unix_ts) = now_unix_ts {
            parse_git_commit_age_mins(commit_unix_ts, now_unix_ts).or_else(|| {
                record_git_probe_issue(
                    &mut probe_issues,
                    &["log", "-1", "--format=%h\t%s\t%ct"],
                    format!("unexpected commit timestamp: {commit_unix_ts}"),
                );
                None
            })
        } else {
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
    baseline_velocity: Vec<VelocityTrend>,
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
    runtime_target: RuntimeTargetDescriptor,
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

fn collect_history_snapshot_from_db(
    db: &HistoryDb,
    recent_limit: usize,
    include_analytics: bool,
) -> HistorySnapshot {
    use crate::history::DiagnosticQuery;

    let total_started_at = Instant::now();
    let mut snapshot = HistorySnapshot {
        available: true,
        is_synthetic: db.is_synthetic,
        ..HistorySnapshot::default()
    };

    let recent_started_at = Instant::now();
    match db.get_recent(recent_limit, None) {
        Ok(recent) => snapshot.recent = recent,
        Err(error) => snapshot
            .issues
            .push(format!("Failed to read recent command history: {error}")),
    }
    emit_status_profile("history.get_recent", recent_started_at);

    let flaky_started_at = Instant::now();
    match db.get_flaky_test_count(50) {
        Ok(flaky_count) => snapshot.flaky_count = flaky_count,
        Err(error) => snapshot
            .issues
            .push(format!("Failed to read flaky-test history: {error}")),
    }
    emit_status_profile("history.get_flaky_test_count", flaky_started_at);

    if include_analytics {
        let analysis = HistoryAnalysis::new(db);
        let analytics_started_at = Instant::now();
        match analysis.status_summary_snapshot() {
            Ok(analytics) => {
                snapshot.diag_counts = DiagnosticCounts {
                    errors: analytics.health.error_count,
                    warnings: analytics.health.warning_count,
                    fixable: analytics.health.fixable_count,
                };
                snapshot.health_report = Some(analytics.health);
                snapshot.velocity = analytics.loop_velocity;
                snapshot.baseline_velocity = analytics.baseline_velocity;
                snapshot.recommendations = analytics.recommendations;
            }
            Err(error) => snapshot.issues.push(format!(
                "Failed to compute workspace analytics snapshot: {error}"
            )),
        }
        emit_status_profile("history.analytics_snapshot", analytics_started_at);
    } else {
        let diagnostics_started_at = Instant::now();
        match db.get_current_diagnostic_counts() {
            Ok(counts) => snapshot.diag_counts = counts,
            Err(error) => snapshot
                .issues
                .push(format!("Failed to read current diagnostics: {error}")),
        }
        emit_status_profile(
            "history.get_current_diagnostic_counts",
            diagnostics_started_at,
        );
    }

    if snapshot.diag_counts.errors > 0 {
        let error_packages_started_at = Instant::now();
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
        emit_status_profile("history.error_packages", error_packages_started_at);
    }

    emit_status_profile("history.total", total_started_at);
    snapshot
}

fn collect_history_and_jobs_snapshot(
    ctx: &CommandContext,
    history_recent_limit: usize,
    include_analytics: bool,
    jobs_recent_limit: usize,
) -> (HistorySnapshot, JobsSnapshot) {
    let jobs_dir = config().jobs_dir();
    let Some(result) = ctx.try_with_history_db_query(|db| {
        let history = collect_history_snapshot_from_db(db, history_recent_limit, include_analytics);
        let jobs_started_at = Instant::now();
        let jobs = crate::jobs::snapshot_recent_and_active_from_history_db(
            db,
            &jobs_dir,
            jobs_recent_limit,
        )
        .map_or_else(
            |error| JobsSnapshot {
                active: Vec::new(),
                recent: Vec::new(),
                issues: vec![format!(
                    "Failed to read background jobs from {}: {error}",
                    ctx.history_db_path().display()
                )],
            },
            |(active, recent)| JobsSnapshot {
                active,
                recent,
                issues: Vec::new(),
            },
        );
        emit_status_profile("jobs.read_snapshot", jobs_started_at);
        Ok((history, jobs))
    }) else {
        return (
            HistorySnapshot::unavailable(explain_history_db_open_failure(ctx)),
            JobsSnapshot {
                active: Vec::new(),
                recent: Vec::new(),
                issues: vec![format!(
                    "Jobs state unavailable at {}",
                    ctx.history_db_path().display()
                )],
            },
        );
    };

    match result {
        Ok((history, jobs)) => (history, jobs),
        Err(error) => (
            HistorySnapshot::unavailable(format!(
                "History DB query failed at {}: {error}",
                ctx.history_db_path().display()
            )),
            JobsSnapshot {
                active: Vec::new(),
                recent: Vec::new(),
                issues: vec![format!(
                    "Failed to read background jobs from {}: {error}",
                    ctx.history_db_path().display()
                )],
            },
        ),
    }
}

/// Collect all data for --summary in parallel threads.
async fn collect_summary_data(ctx: &CommandContext) -> SummaryData {
    let total_started_at = Instant::now();
    let cfg = config();
    let runtime_target =
        checkout_runtime_target(&cfg).unwrap_or_else(fallback_checkout_runtime_target);
    let gateway_url = runtime_target.gateway.base_url.clone();
    let runtime_db_url =
        resolve_runtime_metrics_database_url(runtime_target.database.url.as_deref());
    let runtime_target_kind = runtime_target.kind.clone();
    let runtime_target_kind_for_thread = runtime_target_kind.clone();

    let threaded_stage_started_at = Instant::now();
    let (
        pg_probe,
        nats_probe,
        git,
        active_job_details,
        active_job_count,
        history,
        job_issues,
        active_jobs_list,
        runtime_metrics,
        gateway_url,
    ) = std::thread::scope(|s| {
        // Thread 1: Infrastructure
        let infra_handle = s.spawn(move || {
            let started_at = Instant::now();
            let pg = probe_postgres();
            let nats = probe_nats();
            let infra_duration = started_at.elapsed();

            let runtime_metrics_started_at = Instant::now();
            let runtime_metrics = collect_runtime_metrics_if_postgres_ready(&pg, runtime_db_url, &runtime_target_kind_for_thread);
            let runtime_duration = runtime_metrics_started_at.elapsed();

            (
                (pg, nats, runtime_metrics),
                infra_duration,
                runtime_duration,
            )
        });

        // Thread 2: History + jobs snapshot (single SQLite handle)
        let history_jobs_handle = s.spawn(move || {
            let started_at = Instant::now();
            (
                collect_history_and_jobs_snapshot(ctx, 50, true, 20),
                started_at.elapsed(),
            )
        });

        // Thread 3: Git state (expanded for rich mode)
        let git_handle = s.spawn(move || match std::env::current_dir() {
            Ok(cwd) => {
                let started_at = Instant::now();
                (probe_git_state(&cwd), started_at.elapsed())
            }
            Err(error) => (
                GitState {
                    branch: None,
                    dirty: false,
                    ahead: 0,
                    behind: 0,
                    probe_message: Some(format!(
                        "failed to determine current directory for git probe: {error}"
                    )),
                    last_commit_hash: None,
                    last_commit_message: None,
                    last_commit_age_mins: None,
                    stash_count: None,
                    files_changed: None,
                    uncommitted_count: None,
                },
                Duration::ZERO,
            ),
        });

        // Collect thread results
        let ((pg_probe, nats_probe, runtime_metrics), infra_duration, runtime_duration) =
            match infra_handle.join() {
                Ok(result) => result,
                Err(payload) => {
                    let message = format!(
                        "infra probe thread panicked: {}",
                        describe_thread_panic(&*payload)
                    );
                    (
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
                        Some(RuntimeMetrics::query_failure(
                            "runtime metrics collection skipped because infra probe thread panicked"
                                .to_string(),
                        )),
                    ),
                    Duration::ZERO,
                    Duration::ZERO,
                )
                }
            };
        let (git, git_duration) = git_handle.join().unwrap_or_else(|payload| {
            (
                GitState {
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
                    stash_count: None,
                    files_changed: None,
                    uncommitted_count: None,
                },
                Duration::ZERO,
            )
        });
        let ((history, jobs), history_jobs_duration) =
            history_jobs_handle.join().unwrap_or_else(|payload| {
                (
                    (
                        HistorySnapshot::unavailable(format!(
                            "history collection thread panicked: {}",
                            describe_thread_panic(&*payload)
                        )),
                        JobsSnapshot {
                            active: Vec::new(),
                            recent: Vec::new(),
                            issues: vec![format!(
                                "background job collection thread panicked: {}",
                                describe_thread_panic(&*payload)
                            )],
                        },
                    ),
                    Duration::ZERO,
                )
            });
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
        emit_status_profile_duration("summary.infra_probe", infra_duration);
        emit_status_profile_duration("summary.runtime_metrics", runtime_duration);
        emit_status_profile_duration("summary.history_jobs", history_jobs_duration);
        emit_status_profile_duration("summary.git", git_duration);

        (
            pg_probe,
            nats_probe,
            git,
            active_job_details,
            active_job_count,
            history,
            jobs.issues,
            active_jobs_list,
            runtime_metrics,
            gateway_url,
        )
    });
    emit_status_profile("summary.threaded_stage", threaded_stage_started_at);

    let service_stage_started_at = Instant::now();
    let services = collect_core_service_statuses(
        gateway_url.as_deref(),
        runtime_metrics.as_ref(),
        &active_jobs_list,
    )
    .await;
    emit_status_profile("summary.service_stage", service_stage_started_at);
    emit_status_profile("summary.total_collection", total_started_at);

    SummaryData {
        runtime_target,
        pg_probe,
        nats_probe,
        services,
        git,
        active_job_details,
        active_job_count,
        history,
        job_issues,
        runtime_metrics,
    }
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

fn parse_git_status_branch_porcelain(
    output: &str,
    probe_issues: &mut Vec<String>,
) -> (Option<String>, bool, Option<usize>, u32, u32) {
    let mut branch = None;
    let mut ahead = 0u32;
    let mut behind = 0u32;
    let mut entry_count = 0usize;

    for line in output.lines() {
        if let Some(head) = line.strip_prefix("# branch.head ") {
            let head = head.trim();
            if !head.is_empty() && head != "(detached)" {
                branch = Some(head.to_string());
            }
            continue;
        }

        if let Some(ab) = line.strip_prefix("# branch.ab ") {
            let parts: Vec<&str> = ab.split_whitespace().collect();
            if parts.len() != 2 {
                record_git_probe_issue(
                    probe_issues,
                    &["status", "--porcelain=v2", "--branch"],
                    format!("unexpected branch.ab payload: {ab}"),
                );
                continue;
            }

            let parsed_ahead = parts[0]
                .strip_prefix('+')
                .and_then(|value| value.parse::<u32>().ok());
            let parsed_behind = parts[1]
                .strip_prefix('-')
                .and_then(|value| value.parse::<u32>().ok());

            match (parsed_ahead, parsed_behind) {
                (Some(parsed_ahead), Some(parsed_behind)) => {
                    ahead = parsed_ahead;
                    behind = parsed_behind;
                }
                _ => record_git_probe_issue(
                    probe_issues,
                    &["status", "--porcelain=v2", "--branch"],
                    format!("invalid branch.ab payload: {ab}"),
                ),
            }
            continue;
        }

        if line.starts_with('#') {
            continue;
        }

        if !line.trim().is_empty() {
            entry_count += 1;
        }
    }

    (branch, entry_count > 0, Some(entry_count), ahead, behind)
}

// ─── Summary / Compact execution ────────────────────────────────────────────

/// Execute --summary (rich multi-section MOTD)
async fn execute_summary(ctx: &CommandContext) -> Result<CommandResult> {
    let data = collect_summary_data(ctx).await;

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
                    duration_secs: i.duration_secs,
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
        .filter(|service| {
            !matches!(
                service.status,
                ServiceRunStatus::Running | ServiceRunStatus::Skipped
            )
        })
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
    if data.pg_probe.ready()
        && let Some(runtime_metrics) = data.runtime_metrics.as_ref()
    {
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
        runtime_target: RuntimeTargetSummary::from(&data.runtime_target),
        runtime_snapshot: build_runtime_status_snapshot(
            &data.runtime_target,
            &data.pg_probe,
            &data.nats_probe,
            &data.services,
            data.runtime_metrics.as_ref(),
            &warnings,
        ),
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
        velocity: if data.history.velocity.is_empty() {
            None
        } else {
            Some(
                data.history
                    .velocity
                    .iter()
                    .map(VelocityTrendOutput::from)
                    .collect(),
            )
        },
        baseline_velocity: if data.history.baseline_velocity.is_empty() {
            None
        } else {
            Some(
                data.history
                    .baseline_velocity
                    .iter()
                    .map(VelocityTrendOutput::from)
                    .collect(),
            )
        },
        recommendations: if data.history.recommendations.is_empty() {
            None
        } else {
            Some(
                data.history
                    .recommendations
                    .iter()
                    .map(RecommendationOutput::from)
                    .collect(),
            )
        },
        runtime: data.runtime_metrics.clone(),
        services: (!data.services.is_empty()).then(|| data.services.clone()),
        last_commit: data.git.last_commit_hash.as_ref().map(|hash| CommitInfo {
            hash: hash.clone(),
            message: data.git.last_commit_message.clone().unwrap_or_default(),
            age_mins: data.git.last_commit_age_mins,
        }),
        stash_count: data.git.stash_count.filter(|count| *count > 0),
        files_changed: data.git.files_changed.clone(),
        uncommitted_count: data.git.uncommitted_count.filter(|count| *count > 0),
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

        // Runtime target (always)
        self.render_target();

        // Infra + services (always)
        self.render_infra();

        // Build status (when any history exists)
        self.render_build();

        // Velocity trends (when meaningful data exists)
        self.render_loop_velocity();
        self.render_baseline_velocity();

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

        let right = format!("{score_part}   {branch_part}{ab_part}  ");
        let right_vis = console::measure_text_width(&right);

        let padding = inner.saturating_sub(left_vis + right_vis);
        println!("│{}{}{}│", left, " ".repeat(padding), right);

        // Bottom border
        println!("└{}┘", "─".repeat(inner));
    }

    fn render_target(&self) {
        let label = style("  target").dim();
        let target = &self.output.runtime_target;
        let kind = runtime_target_kind_label(&target.kind);
        let source = target
            .source
            .as_deref()
            .map(|source| format!(" source {source}"))
            .unwrap_or_default();
        let db = target
            .database_url
            .as_deref()
            .map_or_else(|| "db unset".to_string(), redact_runtime_target_url);
        let gateway = target.gateway_url.as_deref().unwrap_or("gateway unset");

        println!(
            "{label}   {} {} {} {}",
            style(format!("{} ({kind})", target.name)).cyan(),
            style(source).dim(),
            style("·").dim(),
            style(format!("{db} · {gateway}")).dim()
        );
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
                        ServiceRunStatus::Skipped => {
                            style(format!("{short}:skip")).dim().to_string()
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
                let dur = info
                    .duration_secs
                    .map_or_else(|| "?".to_string(), |duration| format!("{duration:.1}s"));
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

    fn render_velocity_line(&self, label_text: &str, trends: &[VelocityTrend]) {
        let meaningful: Vec<_> = trends
            .iter()
            .filter(|v| v.sample_count >= 4 && v.recent_avg_secs.is_some())
            .collect();

        if meaningful.is_empty() {
            return;
        }

        let label = style(label_text).dim();
        let parts: Vec<String> = meaningful
            .iter()
            .map(|v| {
                let avg = format!("~{:.1}s", v.recent_avg_secs.unwrap_or(0.0));
                let delta = match v.delta_pct {
                    Some(d) if d < -5.0 => style(format!("↓{:.0}%", d.abs())).green().to_string(),
                    Some(d) if d > 5.0 => style(format!("↑{d:.0}%")).red().to_string(),
                    _ => style("→").dim().to_string(),
                };
                let label = match v.scope_label.as_deref() {
                    Some(scope) if !scope.is_empty() => format!("{} [{}]", v.command, scope),
                    _ => v.command.clone(),
                };
                format!("{label} {avg} {delta}")
            })
            .collect();

        println!("{label}    {}", parts.join("   "));
    }

    fn render_loop_velocity(&self) {
        self.render_velocity_line("  loop", &self.data.history.velocity);
    }

    fn render_baseline_velocity(&self) {
        self.render_velocity_line("  repo", &self.data.history.baseline_velocity);
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
                " ".repeat(label_text.len())
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
                let s = format!("heartbeat {secs}s ago");
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
                style("→ xtask jobs active").cyan()
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
            || git.stash_count.is_some_and(|count| count > 0)
            || git.uncommitted_count.is_some_and(|count| count > 0);

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
        if git.files_changed.is_none() && git.uncommitted_count.is_some_and(|count| count > 0) {
            stat_parts.push(format!(
                "{} uncommitted",
                git.uncommitted_count.unwrap_or_default()
            ));
        }
        if git.stash_count.is_some_and(|count| count > 0) {
            stat_parts.push(format!(
                "{} stash{}",
                git.stash_count.unwrap_or_default(),
                if git.stash_count == Some(1) { "" } else { "es" }
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

fn redact_runtime_target_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let authority_and_path = rest
        .rsplit_once('@')
        .map_or(rest, |(_, after_auth)| after_auth);
    format!("{scheme}://{authority_and_path}")
}

fn runtime_target_kind_label(kind: &RuntimeTargetKind) -> &'static str {
    match kind {
        RuntimeTargetKind::Unknown => "unknown",
        RuntimeTargetKind::DevCheckout => "dev_checkout",
        RuntimeTargetKind::DeployedHost => "deployed_host",
        RuntimeTargetKind::Vm => "vm",
        RuntimeTargetKind::Test => "test",
    }
}

// ─── Full Status ────────────────────────────────────────────────────────────

/// Collect one round of workspace status data.
async fn collect_status_data(
    ctx: &CommandContext,
) -> (
    RuntimeTargetDescriptor,
    PostgresProbe,
    NatsProbe,
    bool,
    Option<RuntimeMetrics>,
    Vec<ServiceStatus>,
    JobsSnapshot,
    HistorySnapshot,
) {
    let total_started_at = Instant::now();
    let cfg = config();
    let runtime_target =
        checkout_runtime_target(&cfg).unwrap_or_else(fallback_checkout_runtime_target);
    let gateway_url = runtime_target.gateway.base_url.clone();
    let runtime_db_url =
        resolve_runtime_metrics_database_url(runtime_target.database.url.as_deref());
    let runtime_configured = runtime_db_url
        .as_ref()
        .ok()
        .and_then(|value| value.as_ref())
        .is_some();
    let runtime_target_kind = runtime_target.kind.clone();
    let runtime_target_kind_for_thread = runtime_target_kind.clone();

    let threaded_stage_started_at = Instant::now();
    let (pg_probe, nats_probe, runtime_metrics, jobs, history) = std::thread::scope(|s| {
        // Thread 1: Infrastructure
        let infra_handle = s.spawn(move || {
            let started_at = Instant::now();
            let pg = probe_postgres();
            let nats = probe_nats();
            let infra_duration = started_at.elapsed();

            let runtime_metrics_started_at = Instant::now();
            let runtime_metrics = collect_runtime_metrics_if_postgres_ready(&pg, runtime_db_url, &runtime_target_kind_for_thread);
            let runtime_duration = runtime_metrics_started_at.elapsed();

            (
                (pg, nats, runtime_metrics),
                infra_duration,
                runtime_duration,
            )
        });

        let history_jobs_handle = s.spawn(move || {
            let started_at = Instant::now();
            (
                collect_history_and_jobs_snapshot(ctx, 10, false, 20),
                started_at.elapsed(),
            )
        });

        let ((pg, nats, runtime_metrics), infra_duration, runtime_duration) = match infra_handle
            .join()
        {
            Ok(result) => result,
            Err(payload) => {
                let message = format!(
                    "infra probe thread panicked: {}",
                    describe_thread_panic(&*payload)
                );
                (
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
                            Some(RuntimeMetrics::query_failure(
                                "runtime metrics collection skipped because infra probe thread panicked"
                                    .to_string(),
                            )),
                        ),
                        Duration::ZERO,
                        Duration::ZERO,
                    )
            }
        };
        let ((history, jobs), history_jobs_duration) =
            history_jobs_handle.join().unwrap_or_else(|payload| {
                (
                    (
                        HistorySnapshot::unavailable(format!(
                            "history collection thread panicked: {}",
                            describe_thread_panic(&*payload)
                        )),
                        JobsSnapshot {
                            active: Vec::new(),
                            recent: Vec::new(),
                            issues: vec![format!(
                                "background job collection thread panicked: {}",
                                describe_thread_panic(&*payload)
                            )],
                        },
                    ),
                    Duration::ZERO,
                )
            });
        emit_status_profile_duration("full.infra_probe", infra_duration);
        emit_status_profile_duration("full.runtime_metrics", runtime_duration);
        emit_status_profile_duration("full.history_jobs", history_jobs_duration);

        (pg, nats, runtime_metrics, jobs, history)
    });
    emit_status_profile("full.threaded_stage", threaded_stage_started_at);
    let service_stage_started_at = Instant::now();
    let services = collect_core_service_statuses(
        gateway_url.as_deref(),
        runtime_metrics.as_ref(),
        &jobs.active,
    )
    .await;
    emit_status_profile("full.service_stage", service_stage_started_at);
    emit_status_profile("full.total_collection", total_started_at);

    (
        runtime_target,
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
async fn render_status_tick(ctx: &CommandContext, watch: bool) -> Result<Option<CommandResult>> {
    let (
        runtime_target,
        pg_probe,
        nats_probe,
        runtime_configured,
        runtime_metrics,
        services,
        jobs,
        history,
    ) = collect_status_data(ctx).await;
    let runtime_assessment = runtime_metrics.as_ref().map(RuntimeMetrics::assessment);
    let unavailable_services: Vec<&str> = services
        .iter()
        .filter(|service| {
            !matches!(
                service.status,
                ServiceRunStatus::Running | ServiceRunStatus::Skipped
            )
        })
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
            duration_secs: inv.duration_secs,
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
    if pg_probe.ready()
        && let Some(runtime_assessment) = runtime_assessment.as_ref()
    {
        warnings.extend(runtime_assessment.warnings.clone());
    }
    let runtime_snapshot = build_runtime_status_snapshot(
        &runtime_target,
        &pg_probe,
        &nats_probe,
        &services,
        runtime_metrics.as_ref(),
        &warnings,
    );

    // Human output
    if ctx.is_human() {
        println!(
            "{}",
            style("━━━━━━━━━━━━━━━━ WORKSPACE STATUS ━━━━━━━━━━━━━━━━").bold()
        );

        println!("\n{}", style("Runtime Target:").bold());
        let target_summary = RuntimeTargetSummary::from(&runtime_target);
        println!("  {:<12} {}", "Name", target_summary.name);
        println!(
            "  {:<12} {}",
            "Kind",
            runtime_target_kind_label(&target_summary.kind)
        );
        if let Some(source) = &target_summary.source {
            println!("  {:<12} {}", "Source", source);
        }
        if let Some(source_path) = &target_summary.source_path {
            println!("  {:<12} {}", "Source path", source_path.display());
        }
        if let Some(database_url) = &target_summary.database_url {
            println!(
                "  {:<12} {}",
                "Database",
                redact_runtime_target_url(database_url)
            );
        }
        if let Some(gateway_url) = &target_summary.gateway_url {
            println!("  {:<12} {}", "Gateway", gateway_url);
        }

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
                ServiceRunStatus::Skipped => "skipped",
                ServiceRunStatus::Unknown => "unknown",
            };
            let status_display = match svc.status {
                ServiceRunStatus::Running => style(status_label).green(),
                ServiceRunStatus::Stopped => style(status_label).dim(),
                ServiceRunStatus::Skipped => style(status_label).dim(),
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

            if let Some(lag) = metrics.fresh_consumer_lag_pending() {
                println!(
                    "  {} Consumer lag:       {:.0} pending",
                    style("-").dim(),
                    lag
                );
            } else if let Some(note) = metrics.consumer_lag_stale_note() {
                println!(
                    "  {} Consumer lag:       stale telemetry ({})",
                    style("⚠").yellow(),
                    note
                );
            }

            if let Some(latency) = metrics.fresh_batch_latency_ms() {
                println!(
                    "  {} Batch latency:      {:.0}ms",
                    style("-").dim(),
                    latency
                );
            } else if let Some(note) = metrics.batch_latency_stale_note() {
                println!(
                    "  {} Batch latency:      stale telemetry ({})",
                    style("⚠").yellow(),
                    note
                );
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
                    "  {:<15} {:<10} ({})",
                    entry.command,
                    status_style,
                    entry.duration_secs.map_or_else(
                        || "unknown".to_string(),
                        |duration| format!("{duration:.1}s")
                    )
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
                runtime_target: RuntimeTargetSummary::from(&runtime_target),
                runtime_snapshot,
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
    if !watch && let Some(result) = render_status_tick(ctx, false).await? {
        return Ok(result);
    }

    let term = console::Term::stdout();
    WatchLoop::with_interval_secs(3)
        .run(|first| {
            let term = &term;
            async move {
                if !first {
                    term.clear_screen()?;
                    term.move_cursor_to(0, 0)?;
                }
                render_status_tick(ctx, true).await?;
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
        assert!(!metadata.track_in_history);
        assert_eq!(
            metadata.history_access,
            crate::command::HistoryAccessMode::Query
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_ingestd_service_status_maps_healthy_runtime_to_running()
    -> ::xtask::sandbox::TestResult<()> {
        let metrics = RuntimeMetrics {
            ingestd_status: IngestdStatus::Healthy,
            last_heartbeat_age_secs: Some(2),
            consumer_lag_pending: None,
            consumer_lag_age_secs: None,
            last_batch_latency_ms: None,
            last_batch_latency_age_secs: None,
            query_error: None,
        };
        let status = ingestd_service_status_from_runtime_metrics(Some(&metrics));

        assert_eq!(status.status, ServiceRunStatus::Running);
        assert_eq!(status.probe, "runtime_metrics");
        assert!(status.pid.is_none());
        assert!(status.message.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_ingestd_service_status_reports_query_error_as_unknown()
    -> ::xtask::sandbox::TestResult<()> {
        let metrics = RuntimeMetrics {
            ingestd_status: IngestdStatus::Unknown,
            last_heartbeat_age_secs: None,
            consumer_lag_pending: None,
            consumer_lag_age_secs: None,
            last_batch_latency_ms: None,
            last_batch_latency_age_secs: None,
            query_error: Some("database unavailable".to_string()),
        };
        let status = ingestd_service_status_from_runtime_metrics(Some(&metrics));

        assert_eq!(status.status, ServiceRunStatus::Unknown);
        assert_eq!(status.probe, "runtime_metrics");
        assert!(status.pid.is_none());
        assert!(
            status
                .message
                .as_deref()
                .is_some_and(|message| message.contains("database unavailable"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_gateway_service_status_maps_ready_probe_pass_to_running()
    -> ::xtask::sandbox::TestResult<()> {
        let status = gateway_service_status_from_readiness(
            crate::commands::doctor::DeploymentReadinessItem {
                name: "gateway-ready".into(),
                status: "pass".into(),
                description: "gateway is serving".into(),
                blocking: true,
            },
            Some(42),
            true,
        );

        assert_eq!(status.name, "sinex-gateway");
        assert_eq!(status.status, ServiceRunStatus::Running);
        assert_eq!(status.probe, "gateway_ready_http");
        assert_eq!(status.pid, Some(42));
        assert!(status.message.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_gateway_service_status_maps_ready_probe_fail_to_stopped()
    -> ::xtask::sandbox::TestResult<()> {
        let status = gateway_service_status_from_readiness(
            crate::commands::doctor::DeploymentReadinessItem {
                name: "gateway-ready".into(),
                status: "fail".into(),
                description: "https://127.0.0.1:9999/ready returned HTTP 503".into(),
                blocking: true,
            },
            None,
            false,
        );

        assert_eq!(status.status, ServiceRunStatus::Stopped);
        assert_eq!(status.probe, "gateway_ready_http");
        assert!(
            status
                .message
                .as_deref()
                .is_some_and(|message| message.contains("HTTP 503"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_gateway_service_status_marks_live_process_as_unknown_when_not_ready()
    -> ::xtask::sandbox::TestResult<()> {
        let status = gateway_service_status_from_readiness(
            crate::commands::doctor::DeploymentReadinessItem {
                name: "gateway-ready".into(),
                status: "fail".into(),
                description: "https://127.0.0.1:9999/ready returned HTTP 503".into(),
                blocking: true,
            },
            Some(123),
            true,
        );

        assert_eq!(status.status, ServiceRunStatus::Unknown);
        assert_eq!(status.pid, Some(123));
        assert!(
            status
                .message
                .as_deref()
                .is_some_and(|message| message.contains("process is alive"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_gateway_service_status_maps_ready_probe_skip_to_skipped()
    -> ::xtask::sandbox::TestResult<()> {
        let status = gateway_service_status_from_readiness(
            crate::commands::doctor::DeploymentReadinessItem {
                name: "gateway-ready".into(),
                status: "skip".into(),
                description: "Gateway runtime is not expected".into(),
                blocking: false,
            },
            None,
            false,
        );

        assert_eq!(status.status, ServiceRunStatus::Skipped);
        assert_eq!(status.probe, "gateway_ready_http");
        assert!(
            status
                .message
                .as_deref()
                .is_some_and(|message| message.contains("not expected"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_active_job_for_service_matches_command_basename()
    -> ::xtask::sandbox::TestResult<()> {
        use crate::history::JobLifecycleStatus;

        let jobs = vec![crate::jobs::Job {
            id: 7,
            invocation_id: None,
            command: "/realm/project/sinex/.sinex/target/debug/sinex-ingestd".into(),
            args: vec![],
            started_at: time::OffsetDateTime::now_utc(),
            pid: Some(std::process::id()),
            job_status: JobLifecycleStatus::Running,
            stdout_path: std::env::temp_dir().join("stdout.log"),
            stderr_path: std::env::temp_dir().join("stderr.log"),
            exit_code: None,
        }];

        let matched = active_job_for_service("sinex-ingestd", &jobs);
        assert!(
            matched.is_some(),
            "active job should match by binary basename"
        );
        assert_eq!(matched.and_then(|job| job.pid), Some(std::process::id()));
        Ok(())
    }

    // --- JSON shape tests: verify serialization contracts agents depend on ---

    fn test_runtime_target() -> RuntimeTargetDescriptor {
        RuntimeTargetDescriptor {
            version: 1,
            name: "checkout-local".into(),
            kind: RuntimeTargetKind::DevCheckout,
            source: Some("xtask checkout config".into()),
            database: sinex_primitives::RuntimeTargetDatabase {
                url: Some("postgresql:///sinex_dev?host=.sinex/run".into()),
                ..Default::default()
            },
            gateway: sinex_primitives::RuntimeTargetGateway {
                base_url: Some("https://127.0.0.1:9999".into()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn test_runtime_snapshot(target: &RuntimeTargetDescriptor) -> RuntimeStatusSnapshot {
        checkout_status_snapshot(
            target.clone(),
            vec![signal(
                "postgres",
                RuntimeStatusSignalStatus::Healthy,
                "checkout-local postgres probe",
                None,
            )],
            Vec::new(),
        )
    }

    #[sinex_test]
    async fn test_status_output_json_shape() -> ::xtask::sandbox::TestResult<()> {
        let target = test_runtime_target();
        let output = StatusOutput {
            runtime_target: RuntimeTargetSummary::from(&target),
            runtime_snapshot: test_runtime_snapshot(&target),
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
                duration_secs: Some(3.5),
                timestamp: "2025-01-01T00:00:00Z".into(),
            }],
            warnings: vec!["Test warning".into()],
        };

        let json = serde_json::to_value(&output)?;

        assert_eq!(json["runtime_target"]["name"], "checkout-local");
        assert_eq!(json["runtime_target"]["kind"], "dev_checkout");
        assert_eq!(json["runtime_snapshot"]["target"]["name"], "checkout-local");
        assert_eq!(
            json["runtime_snapshot"]["signals"][0]["source"],
            "checkout-local postgres probe"
        );

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
        let target = test_runtime_target();
        let output = SummaryOutput {
            runtime_target: RuntimeTargetSummary::from(&target),
            runtime_snapshot: test_runtime_snapshot(&target),
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
                    duration_secs: Some(3.2),
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
                scope_label: Some("-p sinex-db".into()),
                recent_avg_secs: Some(4.2),
                delta_pct: Some(-12.0),
                trend: "improving".into(),
                sample_count: 8,
            }]),
            baseline_velocity: Some(vec![VelocityTrendOutput {
                command: "test".into(),
                scope_label: Some("workspace".into()),
                recent_avg_secs: Some(61.0),
                delta_pct: Some(9.0),
                trend: "slower".into(),
                sample_count: 6,
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
                age_mins: Some(32),
            }),
            stash_count: None,
            files_changed: Some("2 files changed".into()),
            uncommitted_count: Some(5),
        };

        let json = serde_json::to_value(&output)?;

        assert_eq!(json["runtime_target"]["name"], "checkout-local");
        assert_eq!(json["runtime_snapshot"]["signals"][0]["status"], "healthy");

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
        assert!(json["baseline_velocity"].is_array());
        assert_eq!(json["baseline_velocity"][0]["command"], "test");
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
    async fn test_summary_output_preserves_missing_commit_age() -> ::xtask::sandbox::TestResult<()>
    {
        let target = test_runtime_target();
        let output = SummaryOutput {
            runtime_target: RuntimeTargetSummary::from(&target),
            runtime_snapshot: test_runtime_snapshot(&target),
            health: "healthy".into(),
            health_indicator: "ok".into(),
            summary: "infra:ok".into(),
            infrastructure: SummaryInfraHealth {
                postgres: true,
                nats: true,
            },
            last_commands: SummaryLastCommands {
                check: None,
                test: None,
                build: None,
            },
            diagnostics: SummaryDiagnostics {
                errors: 0,
                warnings: 0,
                fixable: 0,
                flaky_tests: 0,
            },
            active_jobs: 0,
            git: SummaryGitState {
                branch: Some("master".into()),
                dirty: false,
                ahead: 0,
                behind: 0,
                message: None,
            },
            warnings: Vec::new(),
            history: HistoryStatusOutput {
                status: "healthy".into(),
                synthetic: false,
                recent_invocations: 0,
                diagnostic_errors: 0,
                diagnostic_warnings: 0,
                fixable_diagnostics: 0,
                flaky_tests: 0,
                message: None,
            },
            health_score: None,
            velocity: None,
            baseline_velocity: None,
            recommendations: None,
            runtime: None,
            services: None,
            last_commit: Some(CommitInfo {
                hash: "abc1234".into(),
                message: "status contract test".into(),
                age_mins: None,
            }),
            stash_count: None,
            files_changed: None,
            uncommitted_count: None,
        };

        let json = serde_json::to_value(&output)?;
        assert!(json["last_commit"]["age_mins"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn test_summary_output_preserves_missing_command_duration()
    -> ::xtask::sandbox::TestResult<()> {
        let target = test_runtime_target();
        let output = SummaryOutput {
            runtime_target: RuntimeTargetSummary::from(&target),
            runtime_snapshot: test_runtime_snapshot(&target),
            health: "healthy".into(),
            health_indicator: "ok".into(),
            summary: "infra:ok".into(),
            infrastructure: SummaryInfraHealth {
                postgres: true,
                nats: true,
            },
            last_commands: SummaryLastCommands {
                check: Some(SummaryCommandInfo {
                    status: InvocationStatus::Success,
                    duration_secs: None,
                    age_mins: 5,
                }),
                test: None,
                build: None,
            },
            diagnostics: SummaryDiagnostics {
                errors: 0,
                warnings: 0,
                fixable: 0,
                flaky_tests: 0,
            },
            active_jobs: 0,
            git: SummaryGitState {
                branch: Some("master".into()),
                dirty: false,
                ahead: 0,
                behind: 0,
                message: None,
            },
            warnings: Vec::new(),
            history: HistoryStatusOutput {
                status: "healthy".into(),
                synthetic: false,
                recent_invocations: 0,
                diagnostic_errors: 0,
                diagnostic_warnings: 0,
                fixable_diagnostics: 0,
                flaky_tests: 0,
                message: None,
            },
            health_score: None,
            velocity: None,
            baseline_velocity: None,
            recommendations: None,
            runtime: None,
            services: None,
            last_commit: None,
            stash_count: None,
            files_changed: None,
            uncommitted_count: None,
        };

        let json = serde_json::to_value(&output)?;
        assert!(json["last_commands"]["check"]["duration_secs"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn test_status_output_preserves_missing_recent_activity_duration()
    -> ::xtask::sandbox::TestResult<()> {
        let target = test_runtime_target();
        let output = StatusOutput {
            runtime_target: RuntimeTargetSummary::from(&target),
            runtime_snapshot: test_runtime_snapshot(&target),
            infrastructure: InfrastructureStatus {
                postgres: ComponentStatus {
                    status: "online".into(),
                    latency_ms: Some(1),
                    port: Some(5432),
                    message: None,
                },
                nats: ComponentStatus {
                    status: "online".into(),
                    latency_ms: Some(1),
                    port: Some(4222),
                    message: None,
                },
            },
            services: Vec::new(),
            runtime: Some(RuntimeMetrics::unavailable()),
            runtime_assessment: None,
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
                active: 0,
                recent_failures: 0,
            },
            recent_activity: vec![ActivityEntry {
                command: "test".into(),
                status: "success".into(),
                duration_secs: None,
                timestamp: "2025-01-01T00:00:00Z".into(),
            }],
            warnings: Vec::new(),
        };

        let json = serde_json::to_value(&output)?;
        assert!(json["recent_activity"][0]["duration_secs"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_summary_health_promotes_history_errors_to_unhealthy()
    -> ::xtask::sandbox::TestResult<()> {
        let (health, indicator) = classify_summary_health(
            true, true, false, 1, 0, false, false, false, false, None, true,
        );
        assert_eq!(health, "unhealthy");
        assert_eq!(indicator, "error");
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_summary_health_marks_warning_only_state_degraded()
    -> ::xtask::sandbox::TestResult<()> {
        let (health, indicator) = classify_summary_health(
            true, true, true, 0, 1, false, false, false, false, None, true,
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
    async fn test_describe_thread_panic_handles_string_payload() -> ::xtask::sandbox::TestResult<()>
    {
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
    async fn test_collect_runtime_metrics_skips_offline_postgres_probe()
    -> ::xtask::sandbox::TestResult<()> {
        let pg_probe = PostgresProbe {
            running: false,
            accepting_connections: false,
            latency_ms: 0,
            message: Some("postgres is offline".to_string()),
        };

        let metrics = collect_runtime_metrics_if_postgres_ready(
            &pg_probe,
            Ok(Some(
                "postgresql:///sinex_dev?host=/tmp/never-used".to_string(),
            )),
            &RuntimeTargetKind::DevCheckout,
        )
        .unwrap_or_else(|| panic!("expected runtime metrics placeholder"));

        assert_eq!(metrics.ingestd_status, IngestdStatus::Unknown);
        assert!(metrics.query_error.is_none());
        assert!(runtime_query_error_message(&metrics).is_none());
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
    async fn test_resolve_runtime_metrics_database_url_uses_checkout_stack_without_descriptor_load()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempdir()?;
        let descriptor_path = temp.path().join("deployment-readiness.json");
        std::fs::write(&descriptor_path, "{ definitely-not-json")?;

        let mut env = EnvGuard::new();
        env.set(
            "SINEX_DEPLOYMENT_READINESS_CONFIG",
            descriptor_path.display().to_string(),
        );

        let expected = StackConfig::for_current_checkout()?.database_url();
        let url = resolve_runtime_metrics_database_url(None)?
            .expect("checkout stack config should provide a runtime metrics database URL");

        assert_eq!(url, expected);
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
    async fn test_probe_git_state_handles_missing_upstream_without_probe_error()
    -> ::xtask::sandbox::TestResult<()> {
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
        assert_eq!(git.stash_count, Some(0));
        assert_eq!(git.uncommitted_count, Some(0));
        assert!(git.probe_message.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_probe_git_state_reports_non_repo_failures() -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::Builder::new()
            .prefix("xtask-nongit-")
            .tempdir_in("/tmp")?;

        let git = probe_git_state(dir.path());

        assert!(!git.dirty);
        assert!(git.last_commit_hash.is_none());
        let probe_message = git
            .probe_message
            .as_deref()
            .unwrap_or_else(|| panic!("expected git probe failure message"));
        assert!(probe_message.contains("git status --porcelain=v2 --branch failed"));
        assert_eq!(git.stash_count, None);
        assert_eq!(git.uncommitted_count, None);
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_git_status_branch_porcelain_extracts_branch_and_upstream_counts()
    -> ::xtask::sandbox::TestResult<()> {
        let mut probe_issues = Vec::new();

        assert_eq!(
            parse_git_status_branch_porcelain(
                "# branch.oid abcdef\n# branch.head master\n# branch.upstream origin/master\n# branch.ab +2 -7\n1 .M N... 100644 100644 100644 abcdef abcdef file.txt\n",
                &mut probe_issues,
            ),
            (Some("master".to_string()), true, Some(1), 2, 7)
        );
        assert!(probe_issues.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_git_status_branch_porcelain_reports_invalid_branch_ab_payload()
    -> ::xtask::sandbox::TestResult<()> {
        let mut probe_issues = Vec::new();

        assert_eq!(
            parse_git_status_branch_porcelain(
                "# branch.head master\n# branch.ab +2 nope\n",
                &mut probe_issues,
            ),
            (Some("master".to_string()), false, Some(0), 0, 0)
        );
        let message = probe_issues.join("; ");
        assert!(message.contains("git status --porcelain=v2 --branch failed"));
        assert!(message.contains("invalid branch.ab payload: +2 nope"));
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
