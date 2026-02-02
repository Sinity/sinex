//! Status command - workspace health and recent activity
//!
//! Unified command for workspace status with options:
//! - Default: Full status (infra + services + jobs + recent activity)
//! - `--summary`: Quick one-liner (replaces motd command)
//! - `--doctor`: Run diagnostics (replaces stack doctor)
//! - `--watch`: Live updates

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{HistoryDb, InvocationStatus};
use crate::jobs::JobManager;
use crate::tools::ToolManager;
use anyhow::Result;
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
    #[arg(long)]
    pub summary: bool,

    /// Run diagnostics (replaces 'stack doctor')
    #[arg(long)]
    pub doctor: bool,

    /// Include pipeline smoke tests (with --doctor)
    #[arg(long)]
    pub pipelines: bool,
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
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
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
    active_jobs: usize,
    git: SummaryGitState,
    warnings: Vec<String>,
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
    status: String,
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

/// Doctor report structures
#[derive(Debug, Serialize)]
struct DoctorReport {
    postgres: DoctorServiceCheck,
    nats: DoctorServiceCheck,
    tools: Vec<ToolCheck>,
    tls: Option<TlsCheck>,
    overall: bool,
}

#[derive(Debug, Serialize)]
struct DoctorServiceCheck {
    available: bool,
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct ToolCheck {
    name: String,
    available: bool,
    version: Option<String>,
}

#[derive(Debug, Serialize)]
struct TlsCheck {
    ca_exists: bool,
    server_cert_exists: bool,
    client_cert_exists: bool,
}

impl XtaskCommand for StatusCommand {
    fn name(&self) -> &'static str {
        "status"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Dispatch based on mode
        if self.summary {
            return execute_summary(ctx);
        }

        if self.doctor {
            return execute_doctor(self.pipelines, ctx);
        }

        // Default: full status
        execute_full_status(self.watch, ctx)
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
    }
}

/// Quick one-liner summary (replaces 'motd' command)
fn execute_summary(ctx: &CommandContext) -> Result<CommandResult> {
    // Quick infrastructure checks
    let pg_ready = std::process::Command::new("pg_isready")
        .arg("-q")
        .status()
        .is_ok_and(|s| s.success());

    let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(4222);
    let nats_ready = std::net::TcpStream::connect(format!("127.0.0.1:{nats_port}")).is_ok();

    // Jobs
    let cfg = config();
    let job_manager = JobManager::new(cfg.jobs_dir())?;
    let active_jobs = job_manager.list_active().unwrap_or_default().len();

    // History - last commands
    let history = HistoryDb::open(&cfg.history_db_path())?;
    let recent = history.get_recent(50, None)?;

    let now = time::OffsetDateTime::now_utc();
    let get_last_command = |cmd: &str| -> Option<SummaryCommandInfo> {
        recent
            .iter()
            .find(|i| i.command == cmd && i.status != InvocationStatus::Running)
            .map(|i| {
                let age = now - i.started_at;
                SummaryCommandInfo {
                    status: match i.status {
                        InvocationStatus::Success => "success",
                        InvocationStatus::Failed => "failed",
                        InvocationStatus::Running => "running",
                        InvocationStatus::Cancelled => "cancelled",
                    }
                    .to_string(),
                    duration_secs: i.duration_secs.unwrap_or(0.0),
                    age_mins: age.whole_minutes(),
                }
            })
    };

    let last_check = get_last_command("check");
    let last_test = get_last_command("test");
    let last_build = get_last_command("build");

    // Git state
    let git_branch = std::process::Command::new("git")
        .args(["branch", "--show-current"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());

    let git_dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .is_some_and(|o| !o.stdout.is_empty());

    // Get ahead/behind counts
    let (ahead, behind) = std::process::Command::new("git")
        .args(["rev-list", "--left-right", "--count", "HEAD...@{u}"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or((0, 0), |o| {
            let s = String::from_utf8_lossy(&o.stdout);
            let parts: Vec<&str> = s.trim().split('\t').collect();
            if parts.len() == 2 {
                (parts[0].parse().unwrap_or(0), parts[1].parse().unwrap_or(0))
            } else {
                (0, 0)
            }
        });

    // Build warnings
    let mut warnings = Vec::new();

    if !pg_ready {
        warnings.push("Postgres offline".to_string());
    }
    if !nats_ready {
        warnings.push("NATS offline".to_string());
    }

    if let Some(ref test) = last_test {
        if test.status == "failed" {
            warnings.push("Tests failing".to_string());
        }
        if test.age_mins > 60 {
            warnings.push(format!("Tests not run in {}h", test.age_mins / 60));
        }
    } else {
        warnings.push("No test runs recorded".to_string());
    }

    if let Some(ref check) = last_check {
        if check.status == "failed" {
            warnings.push("Check failing".to_string());
        }
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
        || last_test.as_ref().is_some_and(|t| t.status == "failed")
        || last_check.as_ref().is_some_and(|c| c.status == "failed")
    {
        "unhealthy"
    } else if !warnings.is_empty() {
        "degraded"
    } else {
        "healthy"
    };

    // Build summary line
    let summary = format!(
        "infra:{} jobs:{} tests:{} git:{}",
        if pg_ready && nats_ready { "ok" } else { "x" },
        active_jobs,
        last_test
            .as_ref()
            .map_or("?", |t| if t.status == "success" { "ok" } else { "x" }),
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
        println!("| {:<40} |", summary);

        if !warnings.is_empty() {
            println!("+--------------------------------------------+");
            for w in &warnings {
                println!("| ! {:<38} |", w);
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

/// Run diagnostics (replaces 'stack doctor')
fn execute_doctor(pipelines: bool, ctx: &CommandContext) -> Result<CommandResult> {
    use crate::process::ProcessBuilder;

    let mut all_ok = true;

    // Check Postgres
    let pg_ready = std::process::Command::new("pg_isready")
        .arg("-q")
        .status()
        .is_ok_and(|s| s.success());
    let pg_msg = if pg_ready {
        None
    } else {
        all_ok = false;
        Some("pg_isready failed - is Postgres running?".to_string())
    };

    // Check NATS
    let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(4222);
    let nats_ready = std::net::TcpStream::connect(format!("127.0.0.1:{nats_port}")).is_ok();
    let nats_msg = if nats_ready {
        None
    } else {
        all_ok = false;
        Some(format!("Cannot connect to NATS on port {nats_port}"))
    };

    // Check required tools
    let tools_to_check = ["ast-grep", "repomix", "cargo-machete", "cargo-nextest"];
    let mut tool_checks = Vec::new();
    for tool in tools_to_check {
        let check_result = ToolManager::check_tool(tool);
        let (available, version) = if let Ok(info) = check_result {
            (true, Some(info.version))
        } else {
            all_ok = false;
            (false, None)
        };
        tool_checks.push(ToolCheck {
            name: tool.to_string(),
            available,
            version,
        });
    }

    // Check TLS certificates
    let tls_dir = std::path::Path::new("certs");
    let tls_check = if tls_dir.exists() {
        Some(TlsCheck {
            ca_exists: tls_dir.join("ca.crt").exists(),
            server_cert_exists: tls_dir.join("server.crt").exists(),
            client_cert_exists: tls_dir.join("client.crt").exists(),
        })
    } else {
        None
    };

    let report = DoctorReport {
        postgres: DoctorServiceCheck {
            available: pg_ready,
            message: pg_msg,
        },
        nats: DoctorServiceCheck {
            available: nats_ready,
            message: nats_msg,
        },
        tools: tool_checks,
        tls: tls_check,
        overall: all_ok,
    };

    if ctx.is_human() {
        println!("{}", style("━━━━━━━━━━ DOCTOR ━━━━━━━━━━").bold());
        println!();

        // Infrastructure
        println!("{}", style("Infrastructure:").bold());
        print_check(
            "Postgres",
            report.postgres.available,
            report.postgres.message.as_deref(),
        );
        print_check(
            "NATS",
            report.nats.available,
            report.nats.message.as_deref(),
        );

        // Tools
        println!("\n{}", style("Required Tools:").bold());
        for tool in &report.tools {
            let version_str = tool.version.as_deref().unwrap_or("");
            print_check(&tool.name, tool.available, Some(version_str));
        }

        // TLS
        if let Some(tls) = &report.tls {
            println!("\n{}", style("TLS Certificates:").bold());
            print_check("CA certificate", tls.ca_exists, None);
            print_check("Server certificate", tls.server_cert_exists, None);
            print_check("Client certificate", tls.client_cert_exists, None);
        }

        // Pipeline smoke tests
        if pipelines {
            println!("\n{}", style("Pipeline Smoke Test:").bold());
            let result = ProcessBuilder::cargo()
                .args(["run", "-p", "sinex-test-utils"])
                .run();
            match result {
                Ok(_) => println!("  {} Pipeline test passed", style("✓").green()),
                Err(e) => println!("  {} Pipeline test failed: {}", style("✗").red(), e),
            }
        }

        // Summary
        println!();
        if all_ok {
            println!("{}", style("✓ All checks passed").green().bold());
        } else {
            println!("{}", style("✗ Some checks failed").red().bold());
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&report)?);
    }

    Ok(CommandResult::success()
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed()))
}

fn print_check(name: &str, ok: bool, detail: Option<&str>) {
    let status = if ok {
        style("✓").green()
    } else {
        style("✗").red()
    };
    let detail_str = detail.map(|d| format!(" ({d})")).unwrap_or_default();
    println!("  {} {:<20}{}", status, name, style(detail_str).dim());
}

/// Full status (default mode)
fn execute_full_status(watch: bool, ctx: &CommandContext) -> Result<CommandResult> {
    loop {
        if watch {
            print!("\x1B[2J\x1B[H"); // Clear screen
        }

        // Collect status data
        let pg_start = std::time::Instant::now();
        let pg_ready = std::process::Command::new("pg_isready")
            .arg("-q")
            .status()
            .is_ok_and(|s| s.success());
        let pg_latency = pg_start.elapsed().as_millis() as u64;

        let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(4222);
        let nats_start = std::time::Instant::now();
        let nats_ready = std::net::TcpStream::connect(format!("127.0.0.1:{nats_port}")).is_ok();
        let nats_latency = nats_start.elapsed().as_millis() as u64;

        // Check services
        let service_names = ["sinex-gateway", "sinex-ingestd"];
        let services: Vec<ServiceStatus> = service_names
            .iter()
            .map(|svc| {
                let output = std::process::Command::new("pgrep")
                    .arg("-f")
                    .arg(svc)
                    .output();

                let (status, pid) = match output {
                    Ok(o) if !o.stdout.is_empty() => {
                        let pid_str = String::from_utf8_lossy(&o.stdout);
                        let pid = pid_str.lines().next().and_then(|s| s.trim().parse().ok());
                        ("running".to_string(), pid)
                    }
                    _ => ("stopped".to_string(), None),
                };

                ServiceStatus {
                    name: svc.to_string(),
                    status,
                    pid,
                }
            })
            .collect();

        // Check jobs
        let cfg = config();
        let job_manager = JobManager::new(cfg.jobs_dir())?;
        let active_jobs = job_manager.list_active().unwrap_or_default();
        let all_jobs = job_manager.list_recent(20).unwrap_or_default();
        let recent_failures = all_jobs
            .iter()
            .filter(|j| matches!(j.status, crate::history::InvocationStatus::Failed))
            .count();

        // Check history
        let history = open_history_db()?;
        let recent = history.get_recent(10, None)?;

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
                let status_display = if svc.status == "running" {
                    style(&svc.status).green()
                } else {
                    style(&svc.status).dim()
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

        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}

fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}
