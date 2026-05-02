use super::output::{
    ActivityEntry, ComponentStatus, HistorySnapshot, InfrastructureStatus, JobsSnapshot,
    JobsStatus, ServiceRunStatus, ServiceStatus, StatusOutput,
};
use super::services::{
    build_runtime_status_snapshot, collect_core_service_statuses,
    collect_runtime_metrics_if_postgres_ready, describe_thread_panic,
    resolve_runtime_metrics_database_url, runtime_query_error_message,
};
use super::{
    collect_history_and_jobs_snapshot, emit_status_profile, emit_status_profile_duration,
    fallback_checkout_runtime_target, redact_runtime_target_url, runtime_target_kind_label,
};
use crate::command::{CommandContext, CommandResult};
use crate::config::config;
use crate::history::InvocationStatus;
use crate::infra::probe::{NatsProbe, PostgresProbe, probe_nats, probe_postgres};
use crate::runtime_metrics::{IngestdStatus, RuntimeMetrics};
use crate::runtime_target::{RuntimeTargetSummary, checkout_runtime_target};
use crate::session::{WatchAction, WatchLoop};
use color_eyre::eyre::Result;
use console::style;
use sinex_primitives::RuntimeTargetDescriptor;
use std::time::{Duration, Instant};

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
pub(super) async fn execute(watch: bool, ctx: &CommandContext) -> Result<CommandResult> {
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
