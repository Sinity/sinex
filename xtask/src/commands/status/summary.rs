use super::git::{GitState, probe_git_state};
use super::output::{
    ActiveJobDetail, CommitInfo, HistorySnapshot, JobsSnapshot, RecommendationOutput,
    ServiceRunStatus, SummaryCommandInfo, SummaryData, SummaryDiagnostics, SummaryGitState,
    SummaryInfraHealth, SummaryLastCommands, SummaryOutput, VelocityTrendOutput,
};
use super::services::{
    build_runtime_status_snapshot, collect_core_service_statuses,
    collect_runtime_metrics_if_postgres_ready, describe_thread_panic,
    resolve_runtime_metrics_database_url,
};
use super::{
    collect_history_and_jobs_snapshot, emit_status_profile, emit_status_profile_duration,
    fallback_checkout_runtime_target,
};
use crate::command::{CommandContext, CommandResult};
use crate::config::config;
use crate::history::InvocationStatus;
use crate::infra::probe::{NatsProbe, PostgresProbe, probe_nats, probe_postgres};
use crate::runtime_metrics::{IngestdStatus, RuntimeMetrics};
use crate::runtime_target::{RuntimeTargetSummary, checkout_runtime_target};
use color_eyre::eyre::Result;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SummaryRuntimeImpact {
    Healthy,
    Degraded,
    Unhealthy,
}

pub(super) fn classify_runtime_summary_impact(metrics: &RuntimeMetrics) -> SummaryRuntimeImpact {
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
pub(super) fn classify_summary_health(
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
            let runtime_metrics = collect_runtime_metrics_if_postgres_ready(
                &pg,
                runtime_db_url,
                &runtime_target_kind_for_thread,
            );
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

// ─── Summary / Compact execution ────────────────────────────────────────────

/// Execute --summary (rich multi-section MOTD)
pub(super) async fn execute(ctx: &CommandContext) -> Result<CommandResult> {
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
        super::motd::render(&output, &data);
        Ok(CommandResult::success().with_duration(ctx.elapsed()))
    } else {
        Ok(CommandResult::success()
            .with_data(serde_json::to_value(&output)?)
            .with_duration(ctx.elapsed()))
    }
}
