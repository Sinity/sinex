//! Status command - workspace health and recent activity
//!
//! Unified command for workspace status with options:
//! - Default: Full status (infra + services + jobs + recent activity)
//! - `--summary`: Quick one-liner (replaces motd command)
//! - `--watch`: Live updates

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{HistoryDb, InvocationStatus};
use crate::jobs::JobManager;
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

    /// Quick one-liner summary (replaces 'motd' command)
    #[arg(long, alias = "compact")]
    pub summary: bool,
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

#[derive(Debug, Serialize)]
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
    summary: String,
    infrastructure: SummaryInfraHealth,
    last_commands: SummaryLastCommands,
    diagnostics: SummaryDiagnostics,
    active_jobs: usize,
    git: SummaryGitState,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SummaryDiagnostics {
    errors: usize,
    warnings: usize,
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
        if self.summary {
            execute_summary(ctx)
        } else {
            execute_full_status(self.watch, ctx).await
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
    }
}

/// Quick one-liner summary (replaces 'motd' command)
fn execute_summary(ctx: &CommandContext) -> Result<CommandResult> {
    // Run infrastructure checks, git checks, and local ops in parallel.
    // Uses std::thread::scope to parallelize subprocess spawning:
    //   Thread 1: pg_isready + NATS TCP connect
    //   Thread 2: git branch + status + rev-list (3 subprocesses)
    //   Main thread: jobs + history DB queries (no subprocess, fast)
    let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(4222);
    let cfg = config();

    let (pg_ready, nats_ready, git_state, active_jobs, recent, diag_counts) =
        std::thread::scope(|s| {
            // Thread 1: Infrastructure checks
            let infra_handle = s.spawn(move || {
                let pg = std::process::Command::new("pg_isready")
                    .arg("-q")
                    .status()
                    .is_ok_and(|s| s.success());
                let nats = std::net::TcpStream::connect(format!("127.0.0.1:{nats_port}")).is_ok();
                (pg, nats)
            });

            // Thread 2: Git state (3 subprocesses)
            let git_handle = s.spawn(|| {
                let branch = std::process::Command::new("git")
                    .args(["branch", "--show-current"])
                    .output()
                    .ok()
                    .filter(|o| o.status.success())
                    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());

                let dirty = std::process::Command::new("git")
                    .args(["status", "--porcelain"])
                    .output()
                    .ok()
                    .is_some_and(|o| !o.stdout.is_empty());

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

                (branch, dirty, ahead, behind)
            });

            // Main thread: local operations (jobs + history, no subprocess)
            let job_manager = JobManager::new(cfg.jobs_dir()).ok();
            let active = job_manager
                .as_ref()
                .and_then(|jm| jm.list_active().ok())
                .unwrap_or_default()
                .len();

            let (recent, diag) = HistoryDb::open(&cfg.history_db_path())
                .ok()
                .map(|h| {
                    let r = h.get_recent(50, None).unwrap_or_default();
                    let d = h.get_current_diagnostic_counts().unwrap_or_default();
                    (r, d)
                })
                .unwrap_or_default();

            // Collect thread results
            let (pg, nats) = infra_handle.join().unwrap_or((false, false));
            let git = git_handle.join().unwrap_or((None, false, 0, 0));

            (pg, nats, git, active, recent, diag)
        });

    let (git_branch, git_dirty, ahead, behind) = git_state;

    // Derive last command info from history
    let now = time::OffsetDateTime::now_utc();
    let get_last_command = |cmd: &str| -> Option<SummaryCommandInfo> {
        recent
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

    if !pg_ready {
        warnings.push("Postgres offline".to_string());
    }
    if !nats_ready {
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

    if active_jobs > 3 {
        warnings.push(format!("{active_jobs} jobs running"));
    }

    if git_dirty {
        warnings.push("Uncommitted changes".to_string());
    }

    // Determine overall health
    let health = if !pg_ready
        || !nats_ready
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

    // Build summary line
    let warns_str = if diag_counts.errors > 0 {
        format!("{}e+{}w", diag_counts.errors, diag_counts.warnings)
    } else if diag_counts.warnings > 0 {
        format!("{}w", diag_counts.warnings)
    } else {
        "0".to_string()
    };
    let summary = format!(
        "infra:{} jobs:{} tests:{} warns:{} git:{}",
        if pg_ready && nats_ready { "ok" } else { "x" },
        active_jobs,
        last_test.as_ref().map_or("?", |t| {
            if matches!(t.status, InvocationStatus::Success) {
                "ok"
            } else {
                "x"
            }
        }),
        warns_str,
        if git_dirty { "dirty" } else { "clean" }
    );

    let output = SummaryOutput {
        health: health.to_string(),
        summary: summary.clone(),
        infrastructure: SummaryInfraHealth {
            postgres: pg_ready,
            nats: nats_ready,
        },
        last_commands: SummaryLastCommands {
            check: last_check,
            test: last_test,
            build: last_build,
        },
        diagnostics: SummaryDiagnostics {
            errors: diag_counts.errors,
            warnings: diag_counts.warnings,
        },
        active_jobs,
        git: SummaryGitState {
            branch: git_branch.clone(),
            dirty: git_dirty,
            ahead,
            behind,
        },
        warnings: warnings.clone(),
    };

    if ctx.is_human() {
        // Compact, colorful output
        let health_color = match health {
            "healthy" => style(health).green().bold(),
            "degraded" => style(health).yellow().bold(),
            _ => style(health).red().bold(),
        };

        println!("+----- sinex workspace ----------------------+");
        println!(
            "| Health: {:<10} Branch: {:<12} |",
            health_color,
            git_branch.as_deref().unwrap_or("-")
        );
        println!("| {summary:<40} |");

        if !warnings.is_empty() {
            println!("+--------------------------------------------+");
            for w in &warnings {
                println!("| ! {w:<38} |");
            }
        }

        println!("+--------------------------------------------+");

        Ok(CommandResult::success().with_duration(ctx.elapsed()))
    } else {
        Ok(CommandResult::success()
            .with_data(serde_json::to_value(&output)?)
            .with_duration(ctx.elapsed()))
    }
}

/// Full status (default mode)
async fn execute_full_status(watch: bool, ctx: &CommandContext) -> Result<CommandResult> {
    let term = console::Term::stdout();

    loop {
        if watch {
            term.clear_screen()?;
            term.move_cursor_to(0, 0)?;
        }

        // Collect status data in parallel.
        // Thread 1: Infrastructure (pg_isready + NATS + service pgrep)
        // Thread 2: Jobs + History (local filesystem/SQLite)
        // Main thread waits for both.
        let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(4222);
        let cfg = config();

        let (
            pg_ready,
            pg_latency,
            nats_ready,
            nats_latency,
            services,
            active_jobs,
            all_jobs,
            recent,
        ) = std::thread::scope(|s| {
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

            let recent = open_history_db()
                .ok()
                .and_then(|h| h.get_recent(10, None).ok())
                .unwrap_or_default();

            let (pg, pg_lat, nats, nats_lat, svcs) =
                infra_handle.join().unwrap_or((false, 0, false, 0, vec![]));

            (pg, pg_lat, nats, nats_lat, svcs, active, all, recent)
        });

        let recent_failures = all_jobs
            .iter()
            .filter(|j| matches!(j.status, crate::history::InvocationStatus::Failed))
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

        // Output based on format
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
            // JSON output (non-watch mode)
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

                return Ok(CommandResult::success()
                    .with_data(serde_json::to_value(&output)?)
                    .with_duration(ctx.elapsed()));
            }

            return Ok(CommandResult::success().with_duration(ctx.elapsed()));
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
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
        };
        let metadata = cmd.metadata();
        // Diagnostics commands don't modify state and are tracked in history
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
            summary: "infra:ok jobs:1 tests:ok git:dirty".into(),
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
            },
            active_jobs: 1,
            git: SummaryGitState {
                branch: Some("feature/test".into()),
                dirty: true,
                ahead: 2,
                behind: 0,
            },
            warnings: vec!["Uncommitted changes".into()],
        };

        let json = serde_json::to_value(&output)?;

        // Health (agents use: .data.health)
        assert_eq!(json["health"], "degraded");

        // Summary line (agents use: .data.summary)
        assert!(json["summary"].as_str().unwrap().contains("infra:ok"));

        // Infrastructure (agents use: .data.infrastructure.postgres, .nats)
        assert_eq!(json["infrastructure"]["postgres"], true);
        assert_eq!(json["infrastructure"]["nats"], true);

        // Last commands (agents use: .data.last_commands.check.status)
        assert_eq!(json["last_commands"]["check"]["status"], "success");
        assert!(json["last_commands"]["check"]["duration_secs"].is_number());
        assert!(json["last_commands"]["check"]["age_mins"].is_number());
        assert!(json["last_commands"]["test"].is_null());
        assert!(json["last_commands"]["build"].is_null());

        // Git (agents use: .data.git.branch, .dirty, .ahead, .behind)
        assert_eq!(json["git"]["branch"], "feature/test");
        assert_eq!(json["git"]["dirty"], true);
        assert_eq!(json["git"]["ahead"], 2);
        assert_eq!(json["git"]["behind"], 0);

        // Active jobs
        assert_eq!(json["active_jobs"], 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_component_status_skip_serializing_none() -> ::xtask::sandbox::TestResult<()> {
        // When latency_ms and port are None, they should be absent from JSON
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
        // pid=None should be absent from JSON (skip_serializing_if)
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

        // pid=Some should be present
        let running = ServiceStatus {
            name: "sinex-gateway".into(),
            status: ServiceRunStatus::Running,
            pid: Some(42),
        };
        let json = serde_json::to_value(&running)?;
        assert_eq!(json["pid"], 42);
        Ok(())
    }
}
