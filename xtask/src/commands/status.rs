//! Status command - workspace health and recent activity
//!
//! Unified command for workspace status with options:
//! - Default: Full status (infra + services + jobs + recent activity)
//! - `--summary`: Rich multi-section MOTD
//! - `--watch`: Live updates

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{DiagnosticCounts, HistoryAnalysis, HistoryDb};
use color_eyre::eyre::Result;
use sinex_primitives::{
    RuntimeTargetDescriptor, RuntimeTargetKind, utils::redact_url_credentials_for_display,
};
use std::time::{Duration, Instant};

mod full;
mod git;
mod motd;
mod output;
mod services;
mod summary;

use output::{HistorySnapshot, JobsSnapshot};

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

    /// Show recommended next actions (planner)
    #[arg(long)]
    pub next: bool,
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
        } else if self.next {
            execute_next(ctx)
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

/// Show recommended next actions from the planner (#1144).
fn execute_next(ctx: &CommandContext) -> Result<CommandResult> {
    let actions = crate::planner::plan_next_actions()?;

    if ctx.is_json() {
        let payload = serde_json::json!({
            "actions": actions,
        });
        return Ok(CommandResult::success()
            .with_data(payload)
            .with_message(format!("{} recommended action(s)", actions.len())));
    }

    if actions.is_empty() {
        println!("No recommended actions — workspace is idle.");
        return Ok(CommandResult::success().with_message("idle"));
    }

    println!("Recommended next actions:\n");
    for action in &actions {
        let priority_marker = match action.priority {
            crate::planner::Priority::Now => "●",
            crate::planner::Priority::Soon => "○",
            crate::planner::Priority::Idle => "·",
        };
        println!(
            "  {priority_marker} {}  [confidence: {:.0}%]",
            action.command,
            action.confidence * 100.0
        );
        println!("    {}\n", action.reason);
    }

    Ok(CommandResult::success().with_message(format!("{} recommended action(s)", actions.len())))
}

/// Show event payload schema information (formerly `contracts info describe-schemas`)
fn execute_schemas(ctx: &CommandContext) -> CommandResult {
    use sinex_db::schema::registry::SINEX_SCHEMAS;

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

    let bypass_started_at = Instant::now();
    match db.get_drift_guard_bypass_count(30) {
        Ok(count) => snapshot.drift_guard_bypass_count = count,
        Err(error) => snapshot.issues.push(format!(
            "Failed to read drift guard bypass history: {error}"
        )),
    }
    match db.get_drift_guard_bypass_latest() {
        Ok(latest) => snapshot.drift_guard_bypass_latest = latest,
        Err(error) => snapshot
            .issues
            .push(format!("Failed to read latest drift guard bypass: {error}")),
    }
    emit_status_profile("history.drift_guard_bypass", bypass_started_at);

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
#[path = "status_test.rs"]
mod tests;
