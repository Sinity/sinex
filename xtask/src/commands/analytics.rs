//! Analytics command — composite workspace health, hotspots, reliability, velocity, recommendations.

use color_eyre::eyre::Result;
use console::style;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::query::HistoryAnalysis;
use crate::history::{HistoryDb, PackageHealth, WorkspaceHealthReport};

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
}

impl XtaskCommand for AnalyticsCommand {
    fn name(&self) -> &str {
        "analytics"
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::analysis()
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let db = open_history_db()?;
        let analysis = HistoryAnalysis::new(&db);

        match &self.subcommand {
            AnalyticsSubcommand::WorkspaceHealth { breakdown } => {
                execute_workspace_health(&analysis, *breakdown, ctx)
            }
            AnalyticsSubcommand::Hotspots { limit } => execute_hotspots(&analysis, *limit, ctx),
            AnalyticsSubcommand::Reliability { limit } => {
                execute_reliability(&analysis, *limit, ctx)
            }
            AnalyticsSubcommand::Velocity => execute_velocity(&analysis, ctx),
            AnalyticsSubcommand::Recommend => execute_recommend(&analysis, ctx),
        }
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
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(CommandResult::success()
            .with_message("workspace health computed")
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
                    .map(|r| format!("{:.0}%", r * 100.0))
                    .unwrap_or_else(|| "-".into()),
                &pkg.avg_build_time_secs
                    .map(|s| format!("{s:.1}s"))
                    .unwrap_or_else(|| "-".into()),
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
        println!("{}", serde_json::to_string_pretty(&hotspots)?);
        return Ok(CommandResult::success()
            .with_message(format!("{} hotspots", hotspots.len()))
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
        println!("{}", serde_json::to_string_pretty(&reliability)?);
        return Ok(CommandResult::success()
            .with_message(format!("{} packages", reliability.len()))
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
    let trends = analysis.velocity_trends()?;

    if ctx.is_json() {
        println!("{}", serde_json::to_string_pretty(&trends)?);
        return Ok(CommandResult::success()
            .with_message("velocity trends computed")
            .with_duration(ctx.elapsed()));
    }

    println!(
        "\n{}",
        style("Build/Test Velocity (recent 7d vs prior 7d):").bold()
    );
    let mut builder = Builder::new();
    builder.push_record([
        "COMMAND",
        "RECENT AVG",
        "PRIOR AVG",
        "DELTA",
        "TREND",
        "SAMPLES",
    ]);
    for t in &trends {
        let recent = t
            .recent_avg_secs
            .map(|s| format!("{s:.1}s"))
            .unwrap_or_else(|| "-".into());
        let older = t
            .older_avg_secs
            .map(|s| format!("{s:.1}s"))
            .unwrap_or_else(|| "-".into());
        let delta = t
            .delta_pct
            .map(|d| format!("{:+.1}%", d))
            .unwrap_or_else(|| "-".into());
        let trend_colored = match t.trend.as_str() {
            "faster" => style("↓ faster").green().to_string(),
            "slower" => style("↑ slower").red().to_string(),
            "stable" => style("→ stable").dim().to_string(),
            _ => style("no data").dim().to_string(),
        };
        builder.push_record([
            t.command.as_str(),
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
        println!("{}", serde_json::to_string_pretty(&recs)?);
        return Ok(CommandResult::success()
            .with_message(format!("{} recommendations", recs.len()))
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

// ── helpers ───────────────────────────────────────────────────────────────────

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

fn open_history_db() -> Result<HistoryDb> {
    HistoryDb::open(&config().history_db_path())
}

#[allow(dead_code)]
fn render_package_health_row(pkg: &PackageHealth) -> [String; 5] {
    [
        pkg.package.clone(),
        pkg.diagnostic_count.to_string(),
        pkg.fixable_count.to_string(),
        pkg.test_pass_rate
            .map(|r| format!("{:.0}%", r * 100.0))
            .unwrap_or_else(|| "-".into()),
        pkg.avg_build_time_secs
            .map(|s| format!("{s:.1}s"))
            .unwrap_or_else(|| "-".into()),
    ]
}
