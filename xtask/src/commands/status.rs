//! Status command - workspace health and recent activity
//!
//! Unified command for workspace status with options:
//! - Default: Full status (infra + services + jobs + recent activity)
//! - `--summary`: Rich multi-section MOTD
//! - `--watch`: Live updates

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
#[cfg(test)]
use crate::history::InvocationStatus;
use crate::history::{DiagnosticCounts, HistoryAnalysis, HistoryDb};
#[cfg(test)]
use crate::infra::probe::PostgresProbe;
#[cfg(test)]
use crate::infra::stack::StackConfig;
#[cfg(test)]
use crate::runtime_metrics::RuntimeAssessment;
#[cfg(test)]
use crate::runtime_metrics::{IngestdStatus, RuntimeMetrics};
#[cfg(test)]
use crate::runtime_target::RuntimeTargetSummary;
#[cfg(test)]
use crate::runtime_target::{checkout_status_snapshot, signal};
use color_eyre::eyre::Result;
#[cfg(test)]
use sinex_primitives::RuntimeStatusSignalStatus;
#[cfg(test)]
use sinex_primitives::RuntimeStatusSnapshot;
use sinex_primitives::{
    RuntimeTargetDescriptor, RuntimeTargetKind, utils::redact_url_credentials_for_display,
};
#[cfg(test)]
use std::any::Any;
use std::time::{Duration, Instant};

mod full;
mod git;
mod motd;
mod output;
mod services;
mod summary;

#[cfg(test)]
use output::HistoryStatusOutput;
#[cfg(test)]
use output::{
    ActivityEntry, CommitInfo, ComponentStatus, InfrastructureStatus, JobsStatus,
    RecommendationOutput, ServiceRunStatus, ServiceStatus, StatusOutput, SummaryCommandInfo,
    SummaryDiagnostics, SummaryGitState, SummaryInfraHealth, SummaryLastCommands, SummaryOutput,
    VelocityTrendOutput,
};
use output::{HistorySnapshot, JobsSnapshot};
#[cfg(test)]
use services::runtime_query_error_message;
#[cfg(test)]
use services::{
    active_job_for_service, gateway_service_status_from_readiness,
    ingestd_service_status_from_runtime_metrics, recover_runtime_metrics_thread,
};
#[cfg(test)]
use services::{
    collect_runtime_metrics_if_postgres_ready, describe_thread_panic,
    resolve_runtime_metrics_database_url,
};
#[cfg(test)]
use summary::{SummaryRuntimeImpact, classify_runtime_summary_impact, classify_summary_health};

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

impl XtaskCommand for StatusCommand {
    fn name(&self) -> &'static str {
        "status"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        if self.schemas {
            Ok(execute_schemas(ctx))
        } else if self.summary {
            summary::execute(ctx).await
        } else {
            full::execute(self.watch, ctx).await
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

/// Format an age in minutes to a human-readable relative time.
pub(super) fn format_age(mins: i64) -> String {
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

pub(super) fn redact_runtime_target_url(url: &str) -> String {
    redact_url_credentials_for_display(url)
}

pub(super) fn runtime_target_kind_label(kind: &RuntimeTargetKind) -> &'static str {
    match kind {
        RuntimeTargetKind::Unknown => "unknown",
        RuntimeTargetKind::DevCheckout => "dev_checkout",
        RuntimeTargetKind::DeployedHost => "deployed_host",
        RuntimeTargetKind::Vm => "vm",
        RuntimeTargetKind::Test => "test",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use tempfile::tempdir;
    use xtask::sandbox::EnvGuard;

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
}
