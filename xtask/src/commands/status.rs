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
use crate::tools::{ToolInfo, ToolManager};
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
    environment: Option<serde_json::Value>,
    tls: Option<TlsCheck>,
    postgres_extensions: Option<Vec<String>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}

#[derive(Debug, Serialize)]
struct TlsCheck {
    ca_exists: bool,
    server_cert_exists: bool,
    client_cert_exists: bool,
}

#[async_trait::async_trait]
impl XtaskCommand for StatusCommand {
    fn name(&self) -> &'static str {
        "status"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        if self.summary {
            execute_summary(ctx)
        } else if self.doctor {
            execute_doctor(self.pipelines, ctx)
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

    if let Some(ref check) = last_check
        && check.status == "failed" {
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
    let tools_to_check = [
        "rustc",
        "ast-grep",
        "repomix",
        "cargo-machete",
        "cargo-nextest",
    ];
    let mut tool_checks = Vec::new();
    for tool in tools_to_check {
        let check_result = ToolManager::check_tool(tool);
        let info = check_result.unwrap_or_else(|_| {
            all_ok = false;
            ToolInfo::unavailable(tool)
        });
        let available = info.is_available;
        let version = if info.is_available {
            Some(info.version)
        } else {
            None
        };
        let path = if info.is_available {
            Some(info.path.display().to_string())
        } else {
            None
        };
        tool_checks.push(ToolCheck {
            name: tool.to_string(),
            available,
            version,
            path,
        });
    }

    // Batch validation summary for missing tools
    let missing = ToolManager::check_required_tools(&tools_to_check);

    // Check Postgres extensions
    let mut pg_extensions = None;
    if pg_ready {
        let config = crate::infra::stack::StackConfig::for_current_checkout().ok();
        if let Some(cfg) = config {
            let output = std::process::Command::new("psql")
                .env("PGHOST", cfg.run_dir())
                .env("PGPORT", cfg.postgres.port.to_string())
                .args(["-tAc", "SELECT extname FROM pg_extension"])
                .output();

            if let Ok(o) = output {
                let exts: Vec<String> = String::from_utf8_lossy(&o.stdout)
                    .lines()
                    .map(ToString::to_string)
                    .collect();
                pg_extensions = Some(exts);
            }
        }
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

    // Collect environment configuration
    let cfg = config();
    let environment = Some(serde_json::json!({
        "hostname": cfg.hostname,
        "state_dir": cfg.state_dir.display().to_string(),
        "cache_dir": cfg.cache_dir.display().to_string(),
        "database_url": cfg.database_url,
        "nats_url": cfg.nats_url,
        "test_results_dir": cfg.test_results_dir.as_ref().map(|p| p.display().to_string()),
        "toolchain": cfg.toolchain,
        "in_devenv": cfg.in_devenv,
    }));

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
        environment,
        tls: tls_check,
        postgres_extensions: pg_extensions,
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

        // Installation guidance for missing tools
        if !missing.is_empty() {
            println!("\n{}", style("Installation Guidance:").bold().yellow());
            for (tool_name, guidance) in &missing {
                println!("  {} {tool_name}:", style("→").yellow());
                for line in guidance.lines() {
                    println!("    {line}");
                }
            }
        }

        // Environment
        if let Some(env_data) = &report.environment {
            println!("\n{}", style("Environment:").bold());
            print_env_field(env_data, "hostname", "Hostname:");
            print_env_field(env_data, "state_dir", "State dir:");
            print_env_field(env_data, "cache_dir", "Cache dir:");
            print_env_field(env_data, "database_url", "Database URL:");
            print_env_field(env_data, "nats_url", "NATS URL:");
            print_env_field(env_data, "test_results_dir", "Test results:");
            print_env_field(env_data, "toolchain", "Toolchain:");
            if let Some(in_devenv) = env_data
                .get("in_devenv")
                .and_then(serde_json::Value::as_bool)
            {
                println!(
                    "  {:<20} {}",
                    "In devenv:",
                    if in_devenv { "yes" } else { "no" }
                );
            }
        }

        // TLS
        if let Some(tls) = &report.tls {
            println!("\n{}", style("TLS Certificates:").bold());
            print_check("CA certificate", tls.ca_exists, None);
            print_check("Server certificate", tls.server_cert_exists, None);
            print_check("Client certificate", tls.client_cert_exists, None);
        }

        // Extensions
        if let Some(exts) = &report.postgres_extensions {
            println!("\n{}", style("Postgres Extensions:").bold());
            println!("  {}", exts.join(", "));
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
    }

    Ok(CommandResult::success()
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed()))
}

fn print_env_field(env_data: &serde_json::Value, key: &str, label: &str) {
    if let Some(val) = env_data.get(key) {
        let display = if val.is_null() {
            "(not set)"
        } else {
            val.as_str().unwrap_or("(not set)")
        };
        println!("  {label:<20} {display}");
    }
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
async fn execute_full_status(watch: bool, ctx: &CommandContext) -> Result<CommandResult> {
    let term = console::Term::stdout();

    loop {
        if watch {
            term.clear_screen()?;
            term.move_cursor_to(0, 0)?;
        }

        // Collect status data
        let pg_start = std::time::Instant::now();
        // Use pg_isready if available
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
    fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = StatusCommand {
            service: None,
            watch: false,
            summary: false,
            doctor: false,
            pipelines: false,
        };
        assert_eq!(cmd.name(), "status");
        Ok(())
    }

    #[sinex_test]
    fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = StatusCommand {
            service: None,
            watch: false,
            summary: false,
            doctor: false,
            pipelines: false,
        };
        let metadata = cmd.metadata();
        // Diagnostics commands don't modify state and are tracked in history
        assert!(!metadata.modifies_state);
        assert!(metadata.track_in_history);
        Ok(())
    }

    // --- JSON shape tests: verify serialization contracts agents depend on ---

    #[sinex_test]
    fn test_status_output_json_shape() -> ::xtask::sandbox::TestResult<()> {
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
                status: "running".into(),
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
    fn test_doctor_report_json_shape() -> ::xtask::sandbox::TestResult<()> {
        let report = DoctorReport {
            postgres: DoctorServiceCheck {
                available: true,
                message: None,
            },
            nats: DoctorServiceCheck {
                available: false,
                message: Some("Cannot connect to NATS on port 4222".into()),
            },
            tools: vec![
                ToolCheck {
                    name: "rustc".into(),
                    available: true,
                    version: Some("1.95.0-nightly".into()),
                    path: Some("/nix/store/.../rustc".into()),
                },
                ToolCheck {
                    name: "ast-grep".into(),
                    available: false,
                    version: None,
                    path: None,
                },
            ],
            environment: Some(serde_json::json!({
                "hostname": "testhost",
                "in_devenv": true,
            })),
            tls: Some(TlsCheck {
                ca_exists: true,
                server_cert_exists: true,
                client_cert_exists: false,
            }),
            postgres_extensions: Some(vec!["pgvector".into(), "timescaledb".into()]),
            overall: false,
        };

        let json = serde_json::to_value(&report)?;

        // Postgres/NATS (agents use: .data.postgres.available, .data.nats.available)
        assert_eq!(json["postgres"]["available"], true);
        assert!(json["postgres"]["message"].is_null());
        assert_eq!(json["nats"]["available"], false);
        assert!(json["nats"]["message"].is_string());

        // Tools (agents use: .data.tools[].name, .available, .version)
        assert!(json["tools"].is_array());
        assert_eq!(json["tools"][0]["name"], "rustc");
        assert_eq!(json["tools"][0]["available"], true);
        assert!(json["tools"][0]["version"].is_string());
        assert_eq!(json["tools"][1]["available"], false);
        // Unavailable tool should have null version and no path
        assert!(json["tools"][1]["version"].is_null());
        assert!(json["tools"][1].get("path").is_none() || json["tools"][1]["path"].is_null());

        // Overall (agents use: .data.overall)
        assert_eq!(json["overall"], false);

        // TLS (agents use: .data.tls.ca_exists, etc.)
        assert_eq!(json["tls"]["ca_exists"], true);
        assert_eq!(json["tls"]["client_cert_exists"], false);

        // Extensions (agents use: .data.postgres_extensions[])
        assert!(json["postgres_extensions"].is_array());
        assert_eq!(json["postgres_extensions"][0], "pgvector");
        Ok(())
    }

    #[sinex_test]
    fn test_summary_output_json_shape() -> ::xtask::sandbox::TestResult<()> {
        let output = SummaryOutput {
            health: "degraded".into(),
            summary: "infra:ok jobs:1 tests:ok git:dirty".into(),
            infrastructure: SummaryInfraHealth {
                postgres: true,
                nats: true,
            },
            last_commands: SummaryLastCommands {
                check: Some(SummaryCommandInfo {
                    status: "success".into(),
                    duration_secs: 3.2,
                    age_mins: 15,
                }),
                test: None,
                build: None,
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
    fn test_component_status_skip_serializing_none() -> ::xtask::sandbox::TestResult<()> {
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
    fn test_service_status_skip_serializing_none_pid() -> ::xtask::sandbox::TestResult<()> {
        // pid=None should be absent from JSON (skip_serializing_if)
        let stopped = ServiceStatus {
            name: "sinex-ingestd".into(),
            status: "stopped".into(),
            pid: None,
        };
        let json = serde_json::to_value(&stopped)?;
        assert!(json.get("pid").is_none(), "pid=None should be absent from JSON");
        assert_eq!(json["name"], "sinex-ingestd");

        // pid=Some should be present
        let running = ServiceStatus {
            name: "sinex-gateway".into(),
            status: "running".into(),
            pid: Some(42),
        };
        let json = serde_json::to_value(&running)?;
        assert_eq!(json["pid"], 42);
        Ok(())
    }

    #[sinex_test]
    fn test_doctor_service_check_serialization() -> ::xtask::sandbox::TestResult<()> {
        let check = DoctorServiceCheck {
            available: false,
            message: Some("Connection refused".into()),
        };
        let json = serde_json::to_value(&check)?;
        assert_eq!(json["available"], false);
        assert_eq!(json["message"], "Connection refused");

        // When available, message is typically None
        let check_ok = DoctorServiceCheck {
            available: true,
            message: None,
        };
        let json_ok = serde_json::to_value(&check_ok)?;
        assert_eq!(json_ok["available"], true);
        assert!(json_ok["message"].is_null());
        Ok(())
    }

    #[sinex_test]
    fn test_tls_check_serialization() -> ::xtask::sandbox::TestResult<()> {
        let check = TlsCheck {
            ca_exists: true,
            server_cert_exists: false,
            client_cert_exists: false,
        };
        let json = serde_json::to_value(&check)?;
        assert_eq!(json["ca_exists"], true);
        assert_eq!(json["server_cert_exists"], false);
        assert_eq!(json["client_cert_exists"], false);
        Ok(())
    }
}
