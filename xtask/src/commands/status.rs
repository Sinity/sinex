//! Status command - workspace health and recent activity
//!
//! Unified command for workspace status with options:
//! - Default: Full status (infra + services + jobs + recent activity)
//! - `--summary`: Rich multi-section MOTD
//! - `--watch`: Live updates

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{
    HistoryAnalysis, HistoryDb, InvocationStatus, Recommendation, VelocityTrend,
    WorkspaceHealthReport,
};
use crate::jobs::JobManager;
use crate::runtime_metrics::{IngestdStatus, RuntimeMetrics};
use crate::session::{WatchAction, WatchLoop};
use color_eyre::eyre::Result;
use console::style;
use serde::Serialize;

#[derive(Debug, Clone, clap::Args)]
pub struct StatusCommand {
    /// Service to check (default: all)
    pub service: Option<String>,

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
}

#[derive(Debug, Clone, Serialize)]
struct ServiceStatus {
    name: String,
    status: ServiceRunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ServiceRunStatus {
    Running,
    Stopped,
}

#[derive(Debug, Serialize)]
struct JobsStatus {
    active: usize,
    recent_failures: usize,
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
    last_commit_hash: Option<String>,
    last_commit_message: Option<String>,
    last_commit_age_mins: Option<i64>,
    stash_count: usize,
    files_changed: Option<String>,
    uncommitted_count: usize,
}

/// Active job detail for rich MOTD
struct ActiveJobDetail {
    command: String,
    elapsed_secs: f64,
}

/// All collected summary data
struct SummaryData {
    pg_ready: bool,
    nats_ready: bool,
    services: Vec<ServiceStatus>,
    git: GitState,
    active_job_details: Vec<ActiveJobDetail>,
    active_job_count: usize,
    recent: Vec<crate::history::Invocation>,
    diag_counts: crate::history::DiagnosticCounts,
    /// Package names that have errors (for contextual display)
    error_packages: Vec<String>,
    flaky_count: usize,
    is_synthetic_history: bool,
    runtime_metrics: Option<RuntimeMetrics>,
    health_report: Option<WorkspaceHealthReport>,
    velocity: Vec<VelocityTrend>,
    recommendations: Vec<Recommendation>,
}

/// Collect all data for --summary in parallel threads.
fn collect_summary_data() -> SummaryData {
    let nats_port = current_nats_port();
    let cfg = config();

    std::thread::scope(|s| {
        // Thread 1: Infrastructure + services
        let infra_handle = s.spawn(move || {
            let pg = std::process::Command::new("pg_isready")
                .arg("-q")
                .status()
                .is_ok_and(|s| s.success());
            let nats = std::net::TcpStream::connect(format!("127.0.0.1:{nats_port}")).is_ok();

            let services = ["sinex-gateway", "sinex-ingestd"]
                .iter()
                .map(|svc| {
                    let output = std::process::Command::new("pgrep")
                        .arg("-f")
                        .arg(svc)
                        .output();
                    let (status, pid) = match output {
                        Ok(o) if !o.stdout.is_empty() => {
                            let pid_str = String::from_utf8_lossy(&o.stdout);
                            let pid = pid_str.lines().next().and_then(|l| l.trim().parse().ok());
                            (ServiceRunStatus::Running, pid)
                        }
                        _ => (ServiceRunStatus::Stopped, None),
                    };
                    ServiceStatus {
                        name: svc.to_string(),
                        status,
                        pid,
                    }
                })
                .collect();

            (pg, nats, services)
        });

        // Thread 2: Runtime metrics from Postgres
        let db_url_for_metrics = cfg.database_url.clone();
        let runtime_metrics_handle = s.spawn(move || {
            db_url_for_metrics.and_then(|url| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .ok()
                    .map(|rt| rt.block_on(crate::runtime_metrics::query_runtime_metrics(&url)))
            })
        });

        // Thread 3: Git state (expanded for rich mode)
        let git_handle = s.spawn(move || {
            let branch = std::process::Command::new("git")
                .args(["branch", "--show-current"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());

            let porcelain_output = std::process::Command::new("git")
                .args(["status", "--porcelain"])
                .output()
                .ok();
            let dirty = porcelain_output
                .as_ref()
                .is_some_and(|o| !o.stdout.is_empty());
            let uncommitted_count = porcelain_output
                .as_ref()
                .map(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .lines()
                        .filter(|l| !l.is_empty())
                        .count()
                })
                .unwrap_or(0);

            let (ahead, behind) = std::process::Command::new("git")
                .args(["rev-list", "--left-right", "--count", "HEAD...@{u}"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map_or((0, 0), |o| {
                    let text = String::from_utf8_lossy(&o.stdout);
                    let parts: Vec<&str> = text.trim().split('\t').collect();
                    if parts.len() == 2 {
                        (parts[0].parse().unwrap_or(0), parts[1].parse().unwrap_or(0))
                    } else {
                        (0, 0)
                    }
                });

            // Last commit, stash count, diff stat
            let commit = std::process::Command::new("git")
                .args(["log", "-1", "--format=%h\t%s\t%cr"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| {
                    let text = String::from_utf8_lossy(&o.stdout).trim().to_string();
                    let parts: Vec<&str> = text.splitn(3, '\t').collect();
                    if parts.len() == 3 {
                        Some((
                            parts[0].to_string(),
                            parts[1].to_string(),
                            parts[2].to_string(),
                        ))
                    } else {
                        None
                    }
                });

            let stash_count = std::process::Command::new("git")
                .args(["stash", "list"])
                .output()
                .ok()
                .map(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .lines()
                        .filter(|l| !l.is_empty())
                        .count()
                })
                .unwrap_or(0);

            let files_changed = std::process::Command::new("git")
                .args(["diff", "--shortstat", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success() && !o.stdout.is_empty())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());

            let last_age = commit.as_ref().and_then(|(_, _, age)| parse_git_age(age));
            let last_hash = commit.as_ref().map(|(h, _, _)| h.clone());
            let last_msg = commit.as_ref().map(|(_, m, _)| m.clone());

            GitState {
                branch,
                dirty,
                ahead,
                behind,
                last_commit_hash: last_hash,
                last_commit_message: last_msg,
                last_commit_age_mins: last_age,
                stash_count,
                files_changed,
                uncommitted_count,
            }
        });

        // Main thread: jobs + history + analytics
        let job_manager = JobManager::new(cfg.jobs_dir()).ok();
        let active_jobs_list = job_manager
            .as_ref()
            .and_then(|jm| jm.list_active().ok())
            .unwrap_or_default();
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

        let (
            recent,
            diag_counts,
            error_packages,
            flaky_count,
            is_synthetic_history,
            health_report,
            velocity,
            recommendations,
        ) = HistoryDb::open(&cfg.history_db_path())
            .ok()
            .map(|h| {
                let r = h.get_recent(50, None).unwrap_or_default();
                let d = h.get_current_diagnostic_counts().unwrap_or_default();
                let flaky = h.get_flaky_tests(50).map(|v| v.len()).unwrap_or(0);
                let synthetic = h.is_synthetic;

                // Error package names for contextual display
                use crate::history::DiagnosticQuery;
                let err_pkgs: Vec<String> = DiagnosticQuery::new()
                    .level("error")
                    .limit(50)
                    .run(&h)
                    .ok()
                    .map(|diags| {
                        let mut pkgs: Vec<String> =
                            diags.iter().filter_map(|d| d.package.clone()).collect();
                        pkgs.sort();
                        pkgs.dedup();
                        pkgs
                    })
                    .unwrap_or_default();

                // Analytics (SQLite-local, fast)
                let analysis = HistoryAnalysis::new(&h);
                let hr = analysis.workspace_health_report().ok();
                let vel = analysis.velocity_trends().ok().unwrap_or_default();
                let recs = analysis.recommendations().ok().unwrap_or_default();

                (r, d, err_pkgs, flaky, synthetic, hr, vel, recs)
            })
            .unwrap_or_default();

        // Collect thread results
        let (pg_ready, nats_ready, services) =
            infra_handle.join().unwrap_or((false, false, vec![]));
        let git = git_handle.join().unwrap_or_else(|_| GitState {
            branch: None,
            dirty: false,
            ahead: 0,
            behind: 0,
            last_commit_hash: None,
            last_commit_message: None,
            last_commit_age_mins: None,
            stash_count: 0,
            files_changed: None,
            uncommitted_count: 0,
        });
        let runtime_metrics = runtime_metrics_handle.join().unwrap_or(None);

        SummaryData {
            pg_ready,
            nats_ready,
            services,
            git,
            active_job_details,
            active_job_count,
            recent,
            diag_counts,
            error_packages,
            flaky_count,
            is_synthetic_history,
            runtime_metrics,
            health_report,
            velocity,
            recommendations,
        }
    })
}

/// Parse git's relative age string (e.g. "32 minutes ago", "2 hours ago") into minutes.
fn parse_git_age(age: &str) -> Option<i64> {
    let parts: Vec<&str> = age.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let n: i64 = parts[0].parse().ok()?;
    let unit = parts[1];
    Some(if unit.starts_with("second") {
        0
    } else if unit.starts_with("minute") {
        n
    } else if unit.starts_with("hour") {
        n * 60
    } else if unit.starts_with("day") {
        n * 60 * 24
    } else if unit.starts_with("week") {
        n * 60 * 24 * 7
    } else if unit.starts_with("month") {
        n * 60 * 24 * 30
    } else if unit.starts_with("year") {
        n * 60 * 24 * 365
    } else {
        return None;
    })
}

// ─── Summary / Compact execution ────────────────────────────────────────────

/// Execute --summary (rich multi-section MOTD)
fn execute_summary(ctx: &CommandContext) -> Result<CommandResult> {
    let data = collect_summary_data();

    let now = time::OffsetDateTime::now_utc();
    let get_last_command = |cmd: &str| -> Option<SummaryCommandInfo> {
        data.recent
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

    // Build warnings
    let mut warnings = Vec::new();
    if !data.pg_ready {
        warnings.push("Postgres offline".to_string());
    }
    if !data.nats_ready {
        warnings.push("NATS offline".to_string());
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

    // Health
    let health = if !data.pg_ready
        || !data.nats_ready
        || last_test
            .as_ref()
            .is_some_and(|t| matches!(t.status, InvocationStatus::Failed))
        || last_check
            .as_ref()
            .is_some_and(|c| matches!(c.status, InvocationStatus::Failed))
    {
        "unhealthy"
    } else if !warnings.is_empty() {
        "degraded"
    } else {
        "healthy"
    };

    let health_indicator = if !data.pg_ready || !data.nats_ready {
        "infra"
    } else if data.diag_counts.errors > 0 {
        "error"
    } else if data.diag_counts.warnings > 0 || !warnings.is_empty() {
        "warn"
    } else {
        "ok"
    };

    // Summary line (always computed for JSON)
    let warns_str = if data.diag_counts.errors > 0 {
        format!(
            "{}e+{}w",
            data.diag_counts.errors, data.diag_counts.warnings
        )
    } else if data.diag_counts.warnings > 0 {
        format!("{}w", data.diag_counts.warnings)
    } else {
        "0".to_string()
    };
    let fixes_str = format!("{}f", data.diag_counts.fixable);
    let rt_fragment = data
        .runtime_metrics
        .as_ref()
        .map(|m| format!(" {}", m.summary_fragment()))
        .unwrap_or_default();
    let summary = format!(
        "infra:{} jobs:{} tests:{} warns:{} fixes:{}{} git:{}{}",
        if data.pg_ready && data.nats_ready {
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
        if data.is_synthetic_history {
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
            postgres: data.pg_ready,
            nats: data.nats_ready,
        },
        last_commands: SummaryLastCommands {
            check: last_check,
            test: last_test,
            build: last_build,
        },
        diagnostics: SummaryDiagnostics {
            errors: data.diag_counts.errors,
            warnings: data.diag_counts.warnings,
            fixable: data.diag_counts.fixable,
            flaky_tests: data.flaky_count,
        },
        active_jobs: data.active_job_count,
        git: SummaryGitState {
            branch: data.git.branch.clone(),
            dirty: data.git.dirty,
            ahead: data.git.ahead,
            behind: data.git.behind,
        },
        warnings: warnings.clone(),
        // Rich fields
        health_score: data.health_report.as_ref().map(|r| r.score),
        velocity: if !data.velocity.is_empty() {
            Some(
                data.velocity
                    .iter()
                    .map(VelocityTrendOutput::from)
                    .collect(),
            )
        } else {
            None
        },
        recommendations: if !data.recommendations.is_empty() {
            Some(
                data.recommendations
                    .iter()
                    .map(RecommendationOutput::from)
                    .collect(),
            )
        } else {
            None
        },
        runtime: data.runtime_metrics.as_ref().and_then(|m| {
            if matches!(
                m.ingestd_status,
                IngestdStatus::Healthy | IngestdStatus::Stale
            ) {
                Some(m.clone())
            } else {
                None
            }
        }),
        services: {
            let running: Vec<_> = data
                .services
                .iter()
                .filter(|s| matches!(s.status, ServiceRunStatus::Running))
                .cloned()
                .collect();
            if running.is_empty() {
                None
            } else {
                Some(running)
            }
        },
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

        let score_part = match self.data.health_report.as_ref() {
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

        let pg = if self.data.pg_ready {
            style("pg:ok").green().to_string()
        } else {
            style("pg:offline").red().bold().to_string()
        };

        let nats = if self.data.nats_ready {
            style("nats:ok").green().to_string()
        } else {
            style("nats:offline").red().bold().to_string()
        };

        let running_services: Vec<_> = self
            .data
            .services
            .iter()
            .filter(|s| matches!(s.status, ServiceRunStatus::Running))
            .collect();

        if running_services.is_empty() {
            println!("{label}    {pg}  {nats}");
        } else {
            let svc_parts: Vec<String> = running_services
                .iter()
                .map(|s| {
                    let short = s.name.strip_prefix("sinex-").unwrap_or(&s.name);
                    style(format!("{short}:up")).green().to_string()
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
        if !has_any {
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

        println!("{label}    {}", parts.join("   "));

        // Diagnostics sub-line — show what's wrong and where
        let d = &self.output.diagnostics;
        if d.errors > 0 || d.warnings > 0 {
            let mut diag_parts = Vec::new();

            if d.errors > 0 {
                // Include package names for context
                let err_label = if self.data.error_packages.len() == 1 {
                    format!("{} error in {}", d.errors, self.data.error_packages[0])
                } else if self.data.error_packages.len() <= 3
                    && !self.data.error_packages.is_empty()
                {
                    format!(
                        "{} error{} in {}",
                        d.errors,
                        if d.errors == 1 { "" } else { "s" },
                        self.data.error_packages.join(", ")
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
    }

    // ─── Velocity ───────────────────────────────────────────────────────

    fn render_velocity(&self) {
        let meaningful: Vec<_> = self
            .data
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
        let metrics = match &self.data.runtime_metrics {
            Some(m)
                if matches!(
                    m.ingestd_status,
                    IngestdStatus::Healthy | IngestdStatus::Stale
                ) =>
            {
                m
            }
            _ => return,
        };

        let label = style("  runtime").dim();

        let lag = metrics
            .consumer_lag_pending
            .map(|v| {
                let s = format!("{v:.0}");
                let colored = if v < 10.0 {
                    style(s).green().to_string()
                } else if v < 100.0 {
                    style(s).yellow().to_string()
                } else {
                    style(s).red().to_string()
                };
                format!("lag {colored}")
            })
            .unwrap_or_default();

        let batch = metrics
            .last_batch_latency_ms
            .map(|v| format!("batch {}ms", v as u64))
            .unwrap_or_default();

        let heartbeat = metrics
            .last_heartbeat_age_secs
            .map(|secs| {
                let s = format!("heartbeat {}s ago", secs);
                if secs < 30 {
                    style(s).green().to_string()
                } else if secs < 120 {
                    style(s).yellow().to_string()
                } else {
                    style(s).red().to_string()
                }
            })
            .unwrap_or_default();

        let sep = style("·").dim();
        let parts: Vec<&str> = [lag.as_str(), batch.as_str(), heartbeat.as_str()]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect();

        println!("{label}  ingestd: {}", parts.join(&format!(" {sep} ")));
    }

    // ─── Active Jobs ────────────────────────────────────────────────────

    fn render_jobs(&self) {
        if self.data.active_job_details.is_empty() {
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
    }

    // ─── Git Working Directory ──────────────────────────────────────────

    fn render_git(&self) {
        let git = &self.data.git;

        // Show when there's something notable
        let has_commit = git.last_commit_hash.is_some();
        let notable = git.dirty
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

fn current_nats_port() -> u16 {
    crate::infra::stack::StackConfig::for_current_checkout()
        .map(|config| config.nats.port)
        .unwrap_or(4222)
}

/// Collect one round of workspace status data.
fn collect_status_data() -> (
    bool,
    u64,
    bool,
    u64,
    u16,
    Vec<ServiceStatus>,
    Vec<crate::jobs::Job>,
    Vec<crate::jobs::Job>,
    Vec<crate::history::Invocation>,
) {
    let nats_port = current_nats_port();
    let cfg = config();

    let (pg_ready, pg_latency, nats_ready, nats_latency, services, active_jobs, all_jobs, recent) =
        std::thread::scope(|s| {
            // Thread 1: Infrastructure + services (subprocesses)
            let infra_handle = s.spawn(move || {
                let pg_start = std::time::Instant::now();
                let pg = std::process::Command::new("pg_isready")
                    .arg("-q")
                    .status()
                    .is_ok_and(|s| s.success());
                let pg_lat = pg_start.elapsed().as_millis() as u64;

                let nats_start = std::time::Instant::now();
                let nats = std::net::TcpStream::connect(format!("127.0.0.1:{nats_port}")).is_ok();
                let nats_lat = nats_start.elapsed().as_millis() as u64;

                let service_names = ["sinex-gateway", "sinex-ingestd"];
                let svcs: Vec<ServiceStatus> = service_names
                    .iter()
                    .map(|svc| {
                        let output = std::process::Command::new("pgrep")
                            .arg("-f")
                            .arg(svc)
                            .output();

                        let (status, pid) = match output {
                            Ok(o) if !o.stdout.is_empty() => {
                                let pid_str = String::from_utf8_lossy(&o.stdout);
                                let pid =
                                    pid_str.lines().next().and_then(|s| s.trim().parse().ok());
                                (ServiceRunStatus::Running, pid)
                            }
                            _ => (ServiceRunStatus::Stopped, None),
                        };

                        ServiceStatus {
                            name: svc.to_string(),
                            status,
                            pid,
                        }
                    })
                    .collect();

                (pg, pg_lat, nats, nats_lat, svcs)
            });

            // Main thread: local operations (jobs + history)
            let job_manager = JobManager::new(cfg.jobs_dir()).ok();
            let active = job_manager
                .as_ref()
                .and_then(|jm| jm.list_active().ok())
                .unwrap_or_default();
            let all = job_manager
                .as_ref()
                .and_then(|jm| jm.list_recent(20).ok())
                .unwrap_or_default();

            let recent = HistoryDb::open(&cfg.history_db_path())
                .ok()
                .and_then(|db| db.get_recent(10, None).ok())
                .unwrap_or_default();

            let (pg, pg_lat, nats, nats_lat, svcs) =
                infra_handle.join().unwrap_or((false, 0, false, 0, vec![]));

            (pg, pg_lat, nats, nats_lat, svcs, active, all, recent)
        });

    (
        pg_ready,
        pg_latency,
        nats_ready,
        nats_latency,
        nats_port,
        services,
        active_jobs,
        all_jobs,
        recent,
    )
}

/// Render and optionally return one status snapshot.
fn render_status_tick(ctx: &CommandContext, watch: bool) -> Result<Option<CommandResult>> {
    let (
        pg_ready,
        pg_latency,
        nats_ready,
        nats_latency,
        nats_port,
        services,
        active_jobs,
        all_jobs,
        recent,
    ) = collect_status_data();

    let recent_failures = all_jobs
        .iter()
        .filter(|j| {
            matches!(
                j.job_status,
                crate::history::JobLifecycleStatus::Orphaned
                    | crate::history::JobLifecycleStatus::Killed
            )
        })
        .count();

    let recent_activity: Vec<ActivityEntry> = recent
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
    if !pg_ready {
        warnings.push("Postgres is offline. Some commands will fail.".to_string());
    }
    if !nats_ready {
        warnings.push("NATS is offline. Real-time features won't work.".to_string());
    }
    if let Some(fail) = recent.iter().find(|i| i.status == InvocationStatus::Failed) {
        warnings.push(format!("Last run of '{}' failed.", fail.command));
    }
    if active_jobs.len() > 5 {
        warnings.push(format!("{} background jobs running.", active_jobs.len()));
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
            if pg_ready {
                style("online").green()
            } else {
                style("offline").red()
            },
            pg_latency
        );
        println!(
            "  {:<12} {} ({}ms, port {})",
            "NATS",
            if nats_ready {
                style("online").green()
            } else {
                style("offline").red()
            },
            nats_latency,
            nats_port
        );

        // Services
        println!("\n{}", style("Services:").bold());
        for svc in &services {
            let status_label = match svc.status {
                ServiceRunStatus::Running => "running",
                ServiceRunStatus::Stopped => "stopped",
            };
            let status_display = if matches!(svc.status, ServiceRunStatus::Running) {
                style(status_label).green()
            } else {
                style(status_label).dim()
            };
            let pid_str = svc.pid.map(|p| format!(" (pid {p})")).unwrap_or_default();
            println!("  {:<20} {}{}", svc.name, status_display, pid_str);
        }

        // Jobs
        println!("\n{}", style("Background Jobs:").bold());
        println!("  Active:    {}", active_jobs.len());
        println!(
            "  Failures:  {}",
            if recent_failures > 0 {
                style(recent_failures.to_string()).red()
            } else {
                style("0".to_string()).dim()
            }
        );

        // Recent activity
        println!("\n{}", style("Recent Activity:").bold());
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
                        status: if pg_ready { "healthy" } else { "offline" }.to_string(),
                        latency_ms: Some(pg_latency),
                        port: None,
                    },
                    nats: ComponentStatus {
                        status: if nats_ready { "healthy" } else { "offline" }.to_string(),
                        latency_ms: Some(nats_latency),
                        port: Some(nats_port),
                    },
                },
                services,
                jobs: JobsStatus {
                    active: active_jobs.len(),
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

    #[sinex_test]
    async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = StatusCommand {
            service: None,
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
            service: None,
            watch: false,
            summary: false,
            schemas: false,
        };
        let metadata = cmd.metadata();
        assert!(!metadata.modifies_state);
        assert!(metadata.track_in_history);
        Ok(())
    }

    // --- JSON shape tests: verify serialization contracts agents depend on ---

    #[sinex_test]
    async fn test_status_output_json_shape() -> ::xtask::sandbox::TestResult<()> {
        let output = StatusOutput {
            infrastructure: InfrastructureStatus {
                postgres: ComponentStatus {
                    status: "healthy".into(),
                    latency_ms: Some(5),
                    port: None,
                },
                nats: ComponentStatus {
                    status: "healthy".into(),
                    latency_ms: Some(2),
                    port: Some(4222),
                },
            },
            services: vec![ServiceStatus {
                name: "sinex-gateway".into(),
                status: ServiceRunStatus::Running,
                pid: Some(12345),
            }],
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
            },
            warnings: vec!["Uncommitted changes".into()],
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
            runtime: None,
            services: Some(vec![ServiceStatus {
                name: "sinex-ingestd".into(),
                status: ServiceRunStatus::Running,
                pid: Some(9999),
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
        assert_eq!(json["diagnostics"]["errors"], 0);
        assert_eq!(json["diagnostics"]["warnings"], 2);
        assert_eq!(json["diagnostics"]["fixable"], 1);
        assert_eq!(json["diagnostics"]["flaky_tests"], 0);
        assert_eq!(json["active_jobs"], 1);

        // New rich fields
        assert_eq!(json["health_score"], 85);
        assert!(json["velocity"].is_array());
        assert_eq!(json["velocity"][0]["command"], "check");
        assert_eq!(json["velocity"][0]["delta_pct"], -12.0);
        assert!(json["recommendations"].is_array());
        assert_eq!(json["recommendations"][0]["severity"], "warning");
        assert_eq!(json["recommendations"][0]["action"], "xtask fix --smart");
        assert!(json["services"].is_array());
        assert_eq!(json["services"][0]["name"], "sinex-ingestd");
        assert_eq!(json["last_commit"]["hash"], "aafd524");
        assert_eq!(json["last_commit"]["age_mins"], 32);
        assert_eq!(json["files_changed"], "2 files changed");
        assert_eq!(json["uncommitted_count"], 5);
        // stash_count=None should be absent
        assert!(json.get("stash_count").is_none() || json["stash_count"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn test_component_status_skip_serializing_none() -> ::xtask::sandbox::TestResult<()> {
        let status = ComponentStatus {
            status: "offline".into(),
            latency_ms: None,
            port: None,
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
            pid: None,
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
            pid: Some(42),
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
    async fn test_parse_git_age() -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(parse_git_age("5 seconds ago"), Some(0));
        assert_eq!(parse_git_age("32 minutes ago"), Some(32));
        assert_eq!(parse_git_age("2 hours ago"), Some(120));
        assert_eq!(parse_git_age("1 day ago"), Some(60 * 24));
        assert_eq!(parse_git_age("3 weeks ago"), Some(60 * 24 * 7 * 3));
        assert_eq!(parse_git_age(""), None);
        assert_eq!(parse_git_age("garbage"), None);
        Ok(())
    }
}
