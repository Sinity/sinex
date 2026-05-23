//! Analytics command — composite workspace health, hotspots, reliability, velocity, recommendations.

use color_eyre::eyre::Result;
use console::style;
use serde::Serialize;
use std::process::Command;
use std::thread;
use std::time::Duration;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::history::{HistoryAnalysis, HistoryDb, WorkspaceHealthReport};

/// `xtask analytics` — developer intelligence analytics.
#[derive(Debug, Clone, clap::Args)]
pub struct AnalyticsCommand {
    #[command(subcommand)]
    pub subcommand: AnalyticsSubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum AnalyticsSubcommand {
    /// Composite workspace health score (0-100) across build, test, and velocity dimensions (J1)
    WorkspaceHealth {
        /// Show per-package breakdown
        #[arg(long)]
        breakdown: bool,
    },
    /// Diagnostic churn analysis — most active recurring and chronic issues (J2)
    Hotspots {
        #[arg(long, default_value = "15")]
        limit: usize,
    },
    /// Test reliability per package — pass rates and flakiness (J3)
    Reliability {
        #[arg(long, default_value = "15")]
        limit: usize,
    },
    /// Build and test time trends (J4)
    Velocity,
    /// Actionable heuristic recommendations with exact commands to run (J5)
    Recommend,
    /// CPU and memory usage trends across invocations (J6)
    Resources {
        /// Filter by command (e.g., "check", "test")
        #[arg(long)]
        command: Option<String>,
        /// Number of recent invocations to show
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Include watchdog/stale-pid cancellation rows normally hidden as zombie noise
        #[arg(long)]
        include_zombies: bool,
    },
    /// Current host pressure snapshot, with Sinnix observability join when available
    Pressure {
        /// Also run sinnix-observe for a host-level attribution report when available.
        #[arg(long)]
        observe: bool,
        /// Sample /proc/PID/io and show the processes doing the most physical IO.
        #[arg(long)]
        top_io: bool,
        /// Sampling window for --top-io, in milliseconds.
        #[arg(long, default_value_t = 1_000)]
        sample_ms: u64,
        /// Time window passed to sinnix-observe --since.
        #[arg(long, default_value = "2 min ago")]
        since: String,
        /// Duration passed to sinnix-observe --duration.
        #[arg(long, default_value = "60 sec")]
        duration: String,
        /// Row limit passed to sinnix-observe --limit.
        #[arg(long, default_value_t = 8)]
        limit: usize,
    },
    /// Stage-level timing breakdowns aggregated across invocations (J7)
    Stages {
        /// Filter by command
        #[arg(long)]
        command: Option<String>,
        /// Number of slowest stages to show
        #[arg(long, default_value = "15")]
        limit: usize,
    },
}

impl XtaskCommand for AnalyticsCommand {
    fn name(&self) -> &'static str {
        "analytics"
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::analysis()
            .with_history_tracking(false)
            .with_history_access(crate::command::HistoryAccessMode::Query)
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        use color_eyre::eyre::eyre;
        let sub = &self.subcommand;
        if let AnalyticsSubcommand::Pressure {
            observe,
            top_io,
            sample_ms,
            since,
            duration,
            limit,
        } = sub
        {
            return execute_pressure(*observe, *top_io, *sample_ms, since, duration, *limit, ctx);
        }
        ctx.try_with_history_db_query(|db| {
            let analysis = HistoryAnalysis::new(db);
            match sub {
                AnalyticsSubcommand::WorkspaceHealth { breakdown } => {
                    execute_workspace_health(&analysis, *breakdown, ctx)
                }
                AnalyticsSubcommand::Hotspots { limit } => execute_hotspots(&analysis, *limit, ctx),
                AnalyticsSubcommand::Reliability { limit } => {
                    execute_reliability(&analysis, *limit, ctx)
                }
                AnalyticsSubcommand::Velocity => execute_velocity(&analysis, ctx),
                AnalyticsSubcommand::Recommend => execute_recommend(&analysis, ctx),
                AnalyticsSubcommand::Resources {
                    command,
                    limit,
                    include_zombies,
                } => execute_resources(db, command.as_deref(), *limit, *include_zombies, ctx),
                AnalyticsSubcommand::Pressure { .. } => unreachable!("handled before DB open"),
                AnalyticsSubcommand::Stages { command, limit } => {
                    execute_stages(db, command.as_deref(), *limit, ctx)
                }
            }
        })
        .ok_or_else(|| eyre!("history DB unavailable"))?
    }
}

// ── J1: workspace-health ──────────────────────────────────────────────────────

fn execute_workspace_health(
    analysis: &HistoryAnalysis<'_>,
    breakdown: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let report = analysis.workspace_health_report()?;

    if ctx.is_json() {
        return Ok(CommandResult::success()
            .with_message("workspace health computed")
            .with_data(serde_json::to_value(&report)?)
            .with_duration(ctx.elapsed()));
    }

    render_health_report(&report, breakdown);

    Ok(CommandResult::success()
        .with_message(format!("workspace health score: {}/100", report.score))
        .with_duration(ctx.elapsed()))
}

fn render_health_report(report: &WorkspaceHealthReport, breakdown: bool) {
    let score_color = |s: u32| {
        if s >= 80 {
            style(s.to_string()).green()
        } else if s >= 60 {
            style(s.to_string()).yellow()
        } else {
            style(s.to_string()).red()
        }
    };

    println!(
        "\n{} Workspace Health Score: {}/100",
        style("■").bold(),
        score_color(report.score)
    );
    println!(
        "  Build:    {}/100  ({} errors, {} warnings)",
        score_color(report.build_score),
        report.error_count,
        report.warning_count
    );
    println!(
        "  Tests:    {}/100  ({} packages with test data)",
        score_color(report.test_score),
        report.test_packages
    );
    if let Some(avg) = report.avg_test_pass_rate {
        println!("             avg pass rate: {:.1}%", avg * 100.0);
    }
    println!("  Velocity: {}/100", score_color(report.velocity_score));

    if breakdown && !report.packages.is_empty() {
        println!("\n{}", style("Package Breakdown:").bold());
        let mut builder = Builder::new();
        builder.push_record(["PACKAGE", "ERRORS", "FIXABLE", "PASS RATE", "AVG BUILD"]);
        for pkg in &report.packages {
            builder.push_record([
                pkg.package.as_str(),
                &pkg.diagnostic_count.to_string(),
                &pkg.fixable_count.to_string(),
                &pkg.test_pass_rate
                    .map_or_else(|| "-".into(), |r| format!("{:.0}%", r * 100.0)),
                &pkg.avg_build_time_secs
                    .map_or_else(|| "-".into(), |s| format!("{s:.1}s")),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::sharp());
        println!("{table}");
    }
    println!();
}

// ── J2: hotspots ─────────────────────────────────────────────────────────────

fn execute_hotspots(
    analysis: &HistoryAnalysis<'_>,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let hotspots = analysis.diagnostic_hotspots(limit)?;

    if ctx.is_json() {
        return Ok(CommandResult::success()
            .with_message(format!("{} hotspots", hotspots.len()))
            .with_data(serde_json::to_value(&hotspots)?)
            .with_duration(ctx.elapsed()));
    }

    if hotspots.is_empty() {
        println!("No diagnostic hotspots found.");
        return Ok(CommandResult::success()
            .with_message("no hotspots")
            .with_duration(ctx.elapsed()));
    }

    println!("\n{}", style("Diagnostic Hotspots (highest churn):").bold());
    let mut builder = Builder::new();
    builder.push_record(["STATUS", "RUNS", "PKG", "LEVEL", "CODE", "MESSAGE"]);
    for h in &hotspots {
        let status_cell = match h.status.as_str() {
            "chronic" => style("chronic").red().to_string(),
            "recurring" => style("recurring").yellow().to_string(),
            "new" => style("new").green().to_string(),
            _ => h.status.clone(),
        };
        let msg = truncate_str(&h.message, 60);
        builder.push_record([
            &status_cell,
            &h.occurrences.to_string(),
            h.package.as_deref().unwrap_or("-"),
            &h.level,
            h.code.as_deref().unwrap_or("-"),
            &msg,
        ]);
    }
    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
    println!();

    Ok(CommandResult::success()
        .with_message(format!("{} hotspots", hotspots.len()))
        .with_duration(ctx.elapsed()))
}

// ── J3: reliability ───────────────────────────────────────────────────────────

fn execute_reliability(
    analysis: &HistoryAnalysis<'_>,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let reliability = analysis.package_reliability(limit)?;

    if ctx.is_json() {
        return Ok(CommandResult::success()
            .with_message(format!("{} packages", reliability.len()))
            .with_data(serde_json::to_value(&reliability)?)
            .with_duration(ctx.elapsed()));
    }

    if reliability.is_empty() {
        println!("No test reliability data found.");
        return Ok(CommandResult::success()
            .with_message("no reliability data")
            .with_duration(ctx.elapsed()));
    }

    println!("\n{}", style("Test Reliability (last 7 days):").bold());
    let mut builder = Builder::new();
    builder.push_record(["PACKAGE", "PASS RATE", "RUNS", "FLAKY", "TREND"]);
    for pkg in &reliability {
        let rate_str = format!("{:.1}%", pkg.pass_rate * 100.0);
        let rate_colored = if pkg.pass_rate >= 0.95 {
            style(rate_str).green().to_string()
        } else if pkg.pass_rate >= 0.80 {
            style(rate_str).yellow().to_string()
        } else {
            style(rate_str).red().to_string()
        };
        let trend_colored = match pkg.trend.as_str() {
            "improving" => style("↑ improving").green().to_string(),
            "degrading" => style("↓ degrading").red().to_string(),
            _ => style("→ stable").dim().to_string(),
        };
        builder.push_record([
            &pkg.package,
            &rate_colored,
            &pkg.total_runs.to_string(),
            &pkg.flaky_count.to_string(),
            &trend_colored,
        ]);
    }
    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
    println!();

    Ok(CommandResult::success()
        .with_message(format!("{} packages", reliability.len()))
        .with_duration(ctx.elapsed()))
}

// ── J4: velocity ──────────────────────────────────────────────────────────────

fn execute_velocity(analysis: &HistoryAnalysis<'_>, ctx: &CommandContext) -> Result<CommandResult> {
    let loop_trends = analysis.loop_velocity_trends()?;
    let baseline_trends = analysis.workspace_baseline_velocity_trends()?;

    if ctx.is_json() {
        return Ok(CommandResult::success()
            .with_message("velocity trends computed")
            .with_data(serde_json::json!({
                "loop": loop_trends,
                "baseline": baseline_trends,
            }))
            .with_duration(ctx.elapsed()));
    }

    for (heading, trends) in [
        ("Current Loop Velocity", &loop_trends),
        ("Canonical Workspace Baselines", &baseline_trends),
    ] {
        println!(
            "\n{}",
            style(format!("{heading} (recent 7d vs prior 7d):")).bold()
        );
        let mut builder = Builder::new();
        builder.push_record([
            "TARGET",
            "RECENT AVG",
            "PRIOR AVG",
            "DELTA",
            "TREND",
            "SAMPLES",
        ]);
        for t in trends {
            let target = t.display_label();
            let recent = t
                .recent_avg_secs
                .map_or_else(|| "-".into(), |s| format!("{s:.1}s"));
            let older = t
                .older_avg_secs
                .map_or_else(|| "-".into(), |s| format!("{s:.1}s"));
            let delta = t
                .delta_pct
                .map_or_else(|| "-".into(), |d| format!("{d:+.1}%"));
            let trend_colored = match t.trend.as_str() {
                "faster" => style("↓ faster").green().to_string(),
                "slower" => style("↑ slower").red().to_string(),
                "stable" => style("→ stable").dim().to_string(),
                _ => style("no data").dim().to_string(),
            };
            builder.push_record([
                &target,
                &recent,
                &older,
                &delta,
                &trend_colored,
                &t.sample_count.to_string(),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::sharp());
        println!("{table}");
    }
    println!();

    Ok(CommandResult::success()
        .with_message("velocity trends computed")
        .with_duration(ctx.elapsed()))
}

// ── J5: recommend ─────────────────────────────────────────────────────────────

fn execute_recommend(
    analysis: &HistoryAnalysis<'_>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let recs = analysis.recommendations()?;

    if ctx.is_json() {
        return Ok(CommandResult::success()
            .with_message(format!("{} recommendations", recs.len()))
            .with_data(serde_json::to_value(&recs)?)
            .with_duration(ctx.elapsed()));
    }

    println!("\n{}", style("Recommendations:").bold());
    for rec in &recs {
        let prefix = match rec.severity.as_str() {
            "critical" => style("✗ CRITICAL").red().bold().to_string(),
            "warning" => style("⚠ WARNING ").yellow().to_string(),
            _ => style("ℹ INFO    ").dim().to_string(),
        };
        println!(
            "  {} [{}] {}",
            prefix,
            style(&rec.category).dim(),
            rec.description
        );
        println!("           → {}", style(&rec.action).cyan());
    }
    println!();

    Ok(CommandResult::success()
        .with_message(format!("{} recommendations", recs.len()))
        .with_duration(ctx.elapsed()))
}

// ── J6: resources ────────────────────────────────────────────────────────────

fn execute_resources(
    db: &HistoryDb,
    command: Option<&str>,
    limit: usize,
    include_zombies: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let rows = db.get_resource_usage_with_zombies(command, limit, include_zombies)?;

    if ctx.is_json() {
        return Ok(CommandResult::success()
            .with_message(format!("{} resource records", rows.len()))
            .with_data(serde_json::to_value(&rows)?)
            .with_duration(ctx.elapsed()));
    }

    if rows.is_empty() {
        println!(
            "No command-local resource usage data found. Metrics are recorded only once an invocation captures process-tree samples."
        );
        return Ok(CommandResult::success()
            .with_message("no resource data")
            .with_duration(ctx.elapsed()));
    }

    println!(
        "\n{}",
        style("Command Resource Usage (recent invocations):").bold()
    );
    println!(
        "{}",
        style("  TREE columns cover xtask plus spawned descendants; shared-slice columns reflect concurrent Nix/background cgroups; PSI and /dev/shm are host-level context observed during the invocation window")
            .dim()
    );
    let mut builder = Builder::new();
    builder.push_record([
        "COMMAND",
        "STATUS",
        "STARTED",
        "DURATION",
        "TREE CPU AVG %",
        "TREE MEM MAX MB",
        "NIXD CPU AVG %",
        "NIXD MEM MAX MB",
        "NIX-BUILD CPU AVG %",
        "NIX-BUILD MEM MAX MB",
        "BG CPU AVG %",
        "BG MEM MAX MB",
        "XTASK CPU AVG %",
        "XTASK MEM MAX MB",
        "IO FULL MAX %",
        "MEM FULL MAX %",
        "SHM USED MAX MB",
        "SHM FREE MIN MB",
        "MAX PROCS",
        "SAMPLES",
    ]);
    let mut has_legacy_host_fallback = false;
    for r in &rows {
        let cpu_cell = if let Some(cpu) = r.process_cpu_usage_avg {
            format!("{cpu:.1}")
        } else if let Some(cpu) = r.host_cpu_usage_avg {
            has_legacy_host_fallback = true;
            format!("{cpu:.1}h")
        } else {
            "-".to_string()
        };
        let mem_cell = if let Some(mem) = r.process_memory_usage_max_mb {
            format!("{mem:.0}")
        } else if let Some(mem) = r.host_memory_usage_max_mb {
            has_legacy_host_fallback = true;
            format!("{mem:.0}h")
        } else {
            "-".to_string()
        };
        builder.push_record([
            &r.command,
            &r.status,
            &r.started_at,
            &r.duration_secs
                .map_or_else(|| "-".into(), |d| format!("{d:.1}s")),
            &cpu_cell,
            &mem_cell,
            &r.shared_nix_daemon_cpu_usage_avg
                .map_or_else(|| "-".into(), |cpu| format!("{cpu:.1}")),
            &r.shared_nix_daemon_memory_usage_max_mb
                .map_or_else(|| "-".into(), |mem| format!("{mem:.0}")),
            &r.shared_nix_build_slice_cpu_usage_avg
                .map_or_else(|| "-".into(), |cpu| format!("{cpu:.1}")),
            &r.shared_nix_build_slice_memory_usage_max_mb
                .map_or_else(|| "-".into(), |mem| format!("{mem:.0}")),
            &r.shared_background_slice_cpu_usage_avg
                .map_or_else(|| "-".into(), |cpu| format!("{cpu:.1}")),
            &r.shared_background_slice_memory_usage_max_mb
                .map_or_else(|| "-".into(), |mem| format!("{mem:.0}")),
            &r.root_process_cpu_usage_avg
                .map_or_else(|| "-".into(), |cpu| format!("{cpu:.1}")),
            &r.root_process_memory_usage_max_mb
                .map_or_else(|| "-".into(), |mem| format!("{mem:.0}")),
            &r.host_io_pressure_full_avg10_max
                .map_or_else(|| "-".into(), |psi| format!("{psi:.1}")),
            &r.host_memory_pressure_full_avg10_max
                .map_or_else(|| "-".into(), |psi| format!("{psi:.1}")),
            &r.shm_used_max_mb
                .map_or_else(|| "-".into(), |mb| format!("{mb:.0}")),
            &r.shm_free_min_mb
                .map_or_else(|| "-".into(), |mb| format!("{mb:.0}")),
            &r.process_count_max
                .map_or_else(|| "-".into(), |count| count.to_string()),
            &r.sample_count
                .map_or_else(|| "-".into(), |count| count.to_string()),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
    if has_legacy_host_fallback {
        println!(
            "{}",
            style("  values suffixed with 'h' come from legacy host-wide samples recorded before command-local tracking").dim()
        );
    }
    println!();

    Ok(CommandResult::success()
        .with_message(format!("{} resource records", rows.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_pressure(
    observe: bool,
    top_io: bool,
    sample_ms: u64,
    since: &str,
    duration: &str,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let cpu = crate::process::read_pressure_snapshot("cpu");
    let io = crate::process::read_pressure_snapshot("io");
    let memory = crate::process::read_pressure_snapshot("memory");
    let shm = crate::process::shm_usage_mb();
    let pressure = crate::resources::PressureRecommendation::from_snapshots(
        cpu.clone(),
        io.clone(),
        memory.clone(),
        shm,
    );
    let observe_output = if observe {
        run_sinnix_observe(since, duration, limit)
    } else {
        None
    };
    let sample_ms = sample_ms.clamp(100, 30_000);
    let top_io_processes = if top_io {
        sample_top_io(Duration::from_millis(sample_ms), limit)
    } else {
        Vec::new()
    };

    if ctx.is_json() {
        return Ok(CommandResult::success()
            .with_message("pressure snapshot")
            .with_data(serde_json::json!({
                "cpu": cpu,
                "io": io,
                "memory": memory,
                "dev_shm": shm.map(|(used_mb, free_mb)| serde_json::json!({
                    "used_mb": used_mb,
                    "free_mb": free_mb,
                })),
                "level": pressure_level_name(pressure.level),
                "summary": pressure.summary(),
                "recommendation": pressure.recommendation(),
                "broad_start_blocked": pressure.broad_start_error("check/test").is_some(),
                "top_io": top_io_processes,
                "sinnix_observe": observe_output.as_ref().map(|output| serde_json::json!({
                    "command": output.command,
                    "status": output.status,
                    "stdout": output.stdout,
                    "stderr": output.stderr,
                })),
            }))
            .with_duration(ctx.elapsed()));
    }

    println!("\n{}", style("Current Host Pressure:").bold());
    println!("  cpu.some avg10: {}", format_pressure_cell(cpu.some_avg10));
    println!(
        "  io.some/full avg10: {}/{}",
        format_pressure_cell(io.some_avg10),
        format_pressure_cell(io.full_avg10)
    );
    println!(
        "  memory.some/full avg10: {}/{}",
        format_pressure_cell(memory.some_avg10),
        format_pressure_cell(memory.full_avg10)
    );
    if let Some((used_mb, free_mb)) = shm {
        println!("  /dev/shm: {used_mb:.0} MB used, {free_mb:.0} MB free");
    } else {
        println!("  /dev/shm: unavailable");
    }
    println!(
        "  level: {}",
        style(pressure_level_name(pressure.level)).bold()
    );
    println!("  recommendation: {}", pressure.recommendation());
    if !top_io_processes.is_empty() {
        println!();
        println!(
            "{}",
            style(format!("Top physical IO over {sample_ms} ms:")).bold()
        );
        let mut builder = Builder::new();
        builder.push_record([
            "PID",
            "READ",
            "WRITE",
            "READ CALLS",
            "WRITE CALLS",
            "COMMAND",
        ]);
        for row in &top_io_processes {
            builder.push_record([
                &row.pid.to_string(),
                &format_bytes(row.read_bytes_delta),
                &format_bytes(row.write_bytes_delta),
                &row.read_syscalls_delta.to_string(),
                &row.write_syscalls_delta.to_string(),
                &truncate_for_table(&row.command, 88),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::sharp());
        println!("{table}");
    } else if top_io {
        println!();
        println!("{}", style("No process IO deltas observed.").dim());
    }

    if let Some(output) = observe_output {
        println!();
        println!("{}", style("sinnix-observe:").bold());
        println!("  {}", output.command);
        if output.status {
            print!("{}", output.stdout);
        } else {
            println!("  failed");
            if !output.stderr.trim().is_empty() {
                println!("{}", output.stderr.trim());
            }
        }
    } else if observe {
        println!();
        println!(
            "{}",
            style("sinnix-observe unavailable; raw PSI snapshot shown only").dim()
        );
    }
    println!();

    Ok(CommandResult::success()
        .with_message("pressure snapshot")
        .with_duration(ctx.elapsed()))
}

#[derive(Debug, Clone, Serialize)]
struct ProcessIoDelta {
    pid: u32,
    command: String,
    read_bytes_delta: u64,
    write_bytes_delta: u64,
    read_syscalls_delta: u64,
    write_syscalls_delta: u64,
}

#[derive(Debug, Clone)]
struct ProcessIoSample {
    command: String,
    read_bytes: u64,
    write_bytes: u64,
    read_syscalls: u64,
    write_syscalls: u64,
}

fn sample_top_io(sample_window: Duration, limit: usize) -> Vec<ProcessIoDelta> {
    let before = read_process_io_samples();
    thread::sleep(sample_window);
    let after = read_process_io_samples();
    let mut rows = after
        .into_iter()
        .filter_map(|(pid, after)| {
            let before = before.get(&pid)?;
            let read_bytes_delta = after.read_bytes.saturating_sub(before.read_bytes);
            let write_bytes_delta = after.write_bytes.saturating_sub(before.write_bytes);
            let read_syscalls_delta = after.read_syscalls.saturating_sub(before.read_syscalls);
            let write_syscalls_delta = after.write_syscalls.saturating_sub(before.write_syscalls);
            if read_bytes_delta == 0
                && write_bytes_delta == 0
                && read_syscalls_delta == 0
                && write_syscalls_delta == 0
            {
                return None;
            }
            Some(ProcessIoDelta {
                pid,
                command: after.command,
                read_bytes_delta,
                write_bytes_delta,
                read_syscalls_delta,
                write_syscalls_delta,
            })
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        let left_bytes = left.read_bytes_delta.saturating_add(left.write_bytes_delta);
        let right_bytes = right
            .read_bytes_delta
            .saturating_add(right.write_bytes_delta);
        right_bytes
            .cmp(&left_bytes)
            .then_with(|| right.read_syscalls_delta.cmp(&left.read_syscalls_delta))
            .then_with(|| right.write_syscalls_delta.cmp(&left.write_syscalls_delta))
    });
    rows.truncate(limit);
    rows
}

fn read_process_io_samples() -> std::collections::HashMap<u32, ProcessIoSample> {
    let mut samples = std::collections::HashMap::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return samples;
    };

    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let proc_dir = entry.path();
        let Some((read_bytes, write_bytes, read_syscalls, write_syscalls)) =
            read_proc_io_file(&proc_dir.join("io"))
        else {
            continue;
        };
        samples.insert(
            pid,
            ProcessIoSample {
                command: read_proc_command(&proc_dir),
                read_bytes,
                write_bytes,
                read_syscalls,
                write_syscalls,
            },
        );
    }

    samples
}

fn read_proc_io_file(path: &std::path::Path) -> Option<(u64, u64, u64, u64)> {
    let contents = std::fs::read_to_string(path).ok()?;
    let mut read_bytes = None;
    let mut write_bytes = None;
    let mut read_syscalls = None;
    let mut write_syscalls = None;
    for line in contents.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let Ok(value) = value.trim().parse::<u64>() else {
            continue;
        };
        match key {
            "read_bytes" => read_bytes = Some(value),
            "write_bytes" => write_bytes = Some(value),
            "syscr" => read_syscalls = Some(value),
            "syscw" => write_syscalls = Some(value),
            _ => {}
        }
    }
    Some((read_bytes?, write_bytes?, read_syscalls?, write_syscalls?))
}

fn read_proc_command(proc_dir: &std::path::Path) -> String {
    if let Ok(cmdline) = std::fs::read(proc_dir.join("cmdline")) {
        let rendered = cmdline
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part))
            .collect::<Vec<_>>()
            .join(" ");
        if !rendered.is_empty() {
            return rendered;
        }
    }
    std::fs::read_to_string(proc_dir.join("comm"))
        .map_or_else(|_| "<unknown>".to_string(), |comm| comm.trim().to_string())
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes_f = bytes as f64;
    if bytes_f >= GIB {
        format!("{:.1} GiB", bytes_f / GIB)
    } else if bytes_f >= MIB {
        format!("{:.1} MiB", bytes_f / MIB)
    } else if bytes_f >= KIB {
        format!("{:.1} KiB", bytes_f / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn truncate_for_table(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn format_pressure_cell(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.2}%"))
}

fn pressure_level_name(level: crate::resources::PressureLevel) -> &'static str {
    match level {
        crate::resources::PressureLevel::Clear => "clear",
        crate::resources::PressureLevel::Elevated => "elevated",
        crate::resources::PressureLevel::Severe => "severe",
    }
}

struct SinnixObserveOutput {
    command: String,
    status: bool,
    stdout: String,
    stderr: String,
}

fn run_sinnix_observe(since: &str, duration: &str, limit: usize) -> Option<SinnixObserveOutput> {
    let exe = find_sinnix_observe()?;
    let command = format!(
        "{} --format human --since {:?} --duration {:?} --limit {}",
        exe.display(),
        since,
        duration,
        limit
    );
    let output = Command::new(&exe)
        .args([
            "--format",
            "human",
            "--since",
            since,
            "--duration",
            duration,
            "--limit",
            &limit.to_string(),
        ])
        .output()
        .ok()?;
    Some(SinnixObserveOutput {
        command,
        status: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn find_sinnix_observe() -> Option<std::path::PathBuf> {
    if let Some(path) = std::env::var_os("SINEX_OBSERVE_COMMAND") {
        let path = std::path::PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    if let Some(path) = std::env::var_os("SINNIX_OBSERVE_COMMAND") {
        let path = std::path::PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join("sinnix-observe"))
            .find(|candidate| candidate.is_file())
    })
}

// ── J7: stages ───────────────────────────────────────────────────────────────

fn execute_stages(
    db: &HistoryDb,
    command: Option<&str>,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let stages = db.get_slowest_stages(command, limit)?;

    if ctx.is_json() {
        return Ok(CommandResult::success()
            .with_message(format!("{} stage stats", stages.len()))
            .with_data(serde_json::to_value(&stages)?)
            .with_duration(ctx.elapsed()));
    }

    if stages.is_empty() {
        println!("No stage timing data found.");
        return Ok(CommandResult::success()
            .with_message("no stage data")
            .with_duration(ctx.elapsed()));
    }

    println!(
        "\n{}",
        style("Slowest Pipeline Stages (aggregated):").bold()
    );
    let mut builder = Builder::new();
    builder.push_record(["STAGE", "AVG DURATION", "MAX DURATION", "RUNS"]);
    for s in &stages {
        builder.push_record([
            &s.stage_name,
            &format!("{:.1}s", s.avg_duration_secs),
            &format!("{:.1}s", s.max_duration_secs),
            &s.run_count.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
    println!();

    Ok(CommandResult::success()
        .with_message(format!("{} stage stats", stages.len()))
        .with_duration(ctx.elapsed()))
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}
