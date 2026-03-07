//! History command - query build/test execution history

use color_eyre::eyre::Result;
use console::style;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::query::HistoryAnalysis;
use crate::history::{HistoryDb, InvocationStatus};

/// History command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum HistorySubcommand {
    /// List recent invocations
    List {
        #[arg(long, default_value = "20")]
        limit: usize,
        #[arg(long)]
        command: Option<String>,
        /// Show only the most recent invocation (like the old `last` subcommand)
        #[arg(long, conflicts_with = "no_limit")]
        first: bool,
        /// Export all history as JSON without limit
        #[arg(long, conflicts_with = "first")]
        no_limit: bool,
        /// Skip N entries (pagination)
        #[arg(long, default_value = "0")]
        offset: usize,
        /// Sort field: started (default), duration, status
        #[arg(long, default_value = "started")]
        sort_by: String,
        /// Only show invocations started after this duration ago (e.g. "1h", "30m", "1d")
        #[arg(long)]
        since: Option<String>,
        /// Include diagnostic error/warning counts for each invocation
        #[arg(long)]
        with_diagnostics: bool,
        /// Include stage timing summary for each invocation
        #[arg(long)]
        with_stages: bool,
        /// Include test pass/fail counts for each invocation
        #[arg(long)]
        with_tests: bool,
    },
    /// Show statistics for a command (or all commands / all packages)
    Stats {
        /// Command to analyse (required unless --all-commands is set)
        #[arg(long, required_unless_present_any = ["all_commands", "all_packages"])]
        command: Option<String>,
        #[arg(long, default_value = "30")]
        days: u32,
        /// Narrow to a specific package (uses invocation_packages join)
        #[arg(long)]
        package: Option<String>,
        /// Show stats for all packages that have appeared in diagnostics (G4)
        #[arg(long, conflicts_with_all = ["command", "package"])]
        all_packages: bool,
        /// Show stats for every command in the history (G4)
        #[arg(long, conflicts_with = "all_packages")]
        all_commands: bool,
    },
    /// Prune old history entries
    Prune {
        #[arg(long, default_value = "90")]
        older_than: u32,
    },
    /// Query test result history
    Tests {
        #[command(subcommand)]
        tests_cmd: HistoryTestsSubcommand,
    },
    /// Query build diagnostics (warnings/errors)
    ///
    /// Default: shows package-scoped current diagnostics (each package's most recent invocation).
    /// Use --scope all for raw accumulated view, --scope <id|latest> for a specific run.
    Diagnostics {
        /// Filter by level (error, warning)
        #[arg(long)]
        level: Option<String>,
        /// Filter by file path pattern
        #[arg(long)]
        file: Option<String>,
        /// Filter by command (check, build, test)
        #[arg(long)]
        command: Option<String>,
        /// Filter by package name
        #[arg(long)]
        package: Option<String>,
        /// Diagnostic scope: "all" (accumulated), "latest" or an invocation ID (single run),
        /// or omit for package-scoped supersession (default).
        #[arg(long)]
        scope: Option<String>,
        /// Maximum number of diagnostics (with --scope all)
        #[arg(long, default_value = "50")]
        limit: usize,
        /// Show only auto-fixable diagnostics (MachineApplicable)
        #[arg(long)]
        fixable: bool,
        /// Show diagnostic count trend over recent invocations
        #[arg(long, conflicts_with_all = ["scope", "fixable"])]
        trend: bool,
        /// Number of invocations to include in trend (with --trend)
        #[arg(long, default_value = "20", requires = "trend")]
        window: usize,
        /// Output format: table (default) or gcc (file:line:col: level: message)
        #[arg(long, default_value = "table")]
        emit: DiagnosticsFormat,
        // G1: Delta analytics flags
        /// Show diagnostic delta between two invocations (new/resolved/persistent)
        #[arg(long, conflicts_with_all = ["scope", "trend"])]
        delta: bool,
        /// Base invocation ID for delta comparison (defaults to previous check invocation)
        #[arg(long, requires = "delta")]
        delta_from: Option<i64>,
        /// Target invocation ID for delta comparison (defaults to latest)
        #[arg(long, requires = "delta")]
        delta_to: Option<i64>,
        /// Filter diagnostics by error code (e.g. E0308)
        #[arg(long)]
        code: Option<String>,
        /// Group and summarise diagnostics by error code
        #[arg(long, conflicts_with_all = ["scope", "delta"])]
        by_code: bool,
    },
    /// Query pipeline stage timing data (G2)
    Stages {
        /// Filter by command (check, build, test, etc.)
        #[arg(long)]
        command: Option<String>,
        /// Show timings for a specific invocation ID
        #[arg(long)]
        invocation: Option<i64>,
        /// Show the N slowest stages by average duration
        #[arg(long, default_value = "10")]
        slowest: usize,
        /// Show duration trend for a specific stage name
        #[arg(long)]
        trend: Option<String>,
        /// Number of invocations to include in trend (with --trend)
        #[arg(long, default_value = "20")]
        window: usize,
    },
    /// Query fix session history (G3)
    Fix {
        /// Show the N most recent fix sessions
        #[arg(long, default_value = "10")]
        sessions: usize,
        /// Analyse fix effectiveness (before/after diagnostic counts)
        #[arg(long)]
        effectiveness: bool,
    },
}

/// History tests subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum HistoryTestsSubcommand {
    Slowest {
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    Flaky {
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    GettingSlower {
        #[arg(long, default_value = "20.0")]
        threshold_pct: f64,
        #[arg(long, default_value = "10")]
        window: usize,
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    Trends {
        #[arg(long)]
        pattern: Option<String>,
        #[arg(long)]
        package: Option<String>,
        #[arg(long, default_value = "30")]
        runs: usize,
    },
    /// Show failing tests from the most recent test run
    Failures {
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Show captured failure output (can be verbose)
        #[arg(long)]
        output: bool,
    },
    /// Comprehensive analysis of the most recent test run
    ///
    /// Shows duration distribution, probable timeouts, and per-package failure summaries.
    Analyze,
    /// Show captured output for a test (pass or fail)
    Output {
        /// Test name pattern to search for
        pattern: String,
    },
    Eta,
    /// Full-text search across stored test output (G7)
    Grep {
        /// Text to search for in captured test output
        text: String,
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Per-package pass rate, test count, avg duration, and flaky count (G7)
    ByPackage,
    /// P95 duration per test over recent runs (G7)
    DurationP95 {
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Tests newly failing in the last N runs that previously passed (G7)
    Regression {
        /// Number of recent invocations to search for regressions
        #[arg(long, default_value = "5")]
        runs: usize,
    },
}

/// History management command
#[derive(Debug, Clone, clap::Args)]
pub struct HistoryCommand {
    #[command(subcommand)]
    pub subcommand: HistorySubcommand,
}

impl XtaskCommand for HistoryCommand {
    fn name(&self) -> &'static str {
        "history"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let db = open_history_db()?;

        match &self.subcommand {
            HistorySubcommand::List {
                limit,
                command,
                first,
                no_limit,
                offset,
                sort_by,
                since,
                with_diagnostics,
                with_stages,
                with_tests,
            } => {
                if *first {
                    execute_last(&db, command.as_deref().unwrap_or(""), ctx)
                } else if *no_limit {
                    execute_export(&db, usize::MAX, ctx)
                } else {
                    execute_list(
                        &db,
                        *limit,
                        *offset,
                        command.as_deref(),
                        since.as_deref(),
                        sort_by.as_str(),
                        *with_diagnostics,
                        *with_stages,
                        *with_tests,
                        ctx,
                    )
                }
            }
            HistorySubcommand::Stats {
                command,
                days,
                package,
                all_packages,
                all_commands,
            } => {
                if *all_packages {
                    execute_stats_all_packages(&db, ctx)
                } else if *all_commands {
                    execute_stats_all_commands(&db, *days, ctx)
                } else {
                    execute_stats(
                        &db,
                        command.as_deref().unwrap_or(""),
                        *days,
                        package.as_deref(),
                        ctx,
                    )
                }
            }
            HistorySubcommand::Prune { older_than } => execute_prune(&db, *older_than, ctx),
            HistorySubcommand::Tests { tests_cmd } => execute_tests(tests_cmd, &db, ctx),
            HistorySubcommand::Diagnostics {
                level,
                file,
                command,
                package,
                scope,
                limit,
                fixable,
                trend,
                window,
                emit,
                delta,
                delta_from,
                delta_to,
                code,
                by_code,
            } => {
                if *trend {
                    return execute_diagnostics_trend(&db, *window, ctx);
                }
                if *delta {
                    return execute_diagnostics_delta(
                        &db,
                        *delta_from,
                        *delta_to,
                        level.as_deref(),
                        file.as_deref(),
                        command.as_deref(),
                        package.as_deref(),
                        *fixable,
                        code.as_deref(),
                        emit,
                        ctx,
                    );
                }
                if *by_code {
                    return execute_diagnostics_by_code(
                        &db,
                        level.as_deref(),
                        file.as_deref(),
                        command.as_deref(),
                        package.as_deref(),
                        *fixable,
                        code.as_deref(),
                        ctx,
                    );
                }

                match scope.as_deref() {
                    Some("all") => execute_diagnostics_all(
                        &db,
                        *limit,
                        level.as_deref(),
                        file.as_deref(),
                        command.as_deref(),
                        package.as_deref(),
                        *fixable,
                        code.as_deref(),
                        emit,
                        ctx,
                    ),
                    Some(inv) => execute_diagnostics_invocation(
                        &db,
                        inv,
                        command.as_deref(),
                        level.as_deref(),
                        file.as_deref(),
                        package.as_deref(),
                        *fixable,
                        code.as_deref(),
                        emit,
                        ctx,
                    ),
                    None => execute_diagnostics_current(
                        &db,
                        level.as_deref(),
                        file.as_deref(),
                        command.as_deref(),
                        package.as_deref(),
                        *fixable,
                        code.as_deref(),
                        emit,
                        ctx,
                    ),
                }
            }
            HistorySubcommand::Stages {
                command,
                invocation,
                slowest,
                trend,
                window,
            } => execute_stages(
                &db,
                command.as_deref(),
                *invocation,
                *slowest,
                trend.as_deref(),
                *window,
                ctx,
            ),
            HistorySubcommand::Fix {
                sessions,
                effectiveness,
            } => execute_fix_sessions(&db, *sessions, *effectiveness, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
    }
}

/// Open the history database
fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}

/// Parse a human-readable duration string into seconds (G5 --since).
/// Accepts: "30m", "2h", "1d", "90s". Returns None on parse failure.
fn parse_duration_secs(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('s') {
        n.parse::<i64>().ok()
    } else if let Some(n) = s.strip_suffix('m') {
        n.parse::<i64>().ok().map(|n| n * 60)
    } else if let Some(n) = s.strip_suffix('h') {
        n.parse::<i64>().ok().map(|n| n * 3600)
    } else if let Some(n) = s.strip_suffix('d') {
        n.parse::<i64>().ok().map(|n| n * 86400)
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_list(
    db: &HistoryDb,
    limit: usize,
    offset: usize,
    command: Option<&str>,
    since: Option<&str>,
    sort_by: &str,
    with_diagnostics: bool,
    with_stages: bool,
    with_tests: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Parse --since into an RFC3339 cutoff timestamp
    let since_ts: Option<String> = since.and_then(|s| {
        parse_duration_secs(s).map(|secs| {
            let cutoff = time::OffsetDateTime::now_utc() - time::Duration::seconds(secs);
            cutoff
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default()
        })
    });

    let invocations =
        db.get_recent_filtered(limit, offset, command, since_ts.as_deref(), sort_by)?;

    if ctx.is_human() {
        if invocations.is_empty() {
            println!("No history entries found.");
        } else {
            let enriched = with_diagnostics || with_stages || with_tests;
            if enriched {
                println!(
                    "{:<6} {:<12} {:<10} {:>8}  STARTED             ENRICHMENT",
                    "ID", "COMMAND", "STATUS", "DURATION"
                );
            } else {
                println!(
                    "{:<6} {:<12} {:<10} {:<10} {:>8}  STARTED",
                    "ID", "COMMAND", "PROFILE", "STATUS", "DURATION"
                );
            }
            for inv in &invocations {
                let duration = inv
                    .duration_secs
                    .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
                let status = format!("{:?}", inv.status).to_lowercase();

                if enriched {
                    let mut parts = Vec::new();
                    if with_diagnostics {
                        let counts = db
                            .get_diagnostic_counts_for_invocation(inv.id)
                            .unwrap_or_default();
                        if counts.errors > 0 || counts.warnings > 0 {
                            parts.push(format!("diag:{}E{}W", counts.errors, counts.warnings));
                        } else {
                            parts.push("diag:ok".to_string());
                        }
                    }
                    if with_stages {
                        let timings = db
                            .get_stage_timings_for_invocation(inv.id)
                            .unwrap_or_default();
                        if timings.is_empty() {
                            parts.push("stages:-".to_string());
                        } else {
                            let total: f64 = timings.iter().map(|t| t.duration_secs).sum();
                            parts.push(format!("stages:{:.1}s", total));
                        }
                    }
                    if with_tests {
                        let (passed, failed, _) = db
                            .get_test_counts_for_invocation(inv.id)
                            .unwrap_or((0, 0, 0));
                        if passed > 0 || failed > 0 {
                            parts.push(format!("tests:{}p{}f", passed, failed));
                        } else {
                            parts.push("tests:-".to_string());
                        }
                    }
                    println!(
                        "{:<6} {:<12} {:<10} {:>8}  {}  {}",
                        inv.id,
                        inv.command,
                        status,
                        duration,
                        super::format_display_time(&inv.started_at),
                        parts.join(" "),
                    );
                } else {
                    let profile = inv.profile.as_deref().unwrap_or("-");
                    println!(
                        "{:<6} {:<12} {:<10} {:<10} {:>8}  {}",
                        inv.id,
                        inv.command,
                        profile,
                        status,
                        duration,
                        super::format_display_time(&inv.started_at)
                    );
                }
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&invocations)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} history entries", invocations.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_last(db: &HistoryDb, command: &str, ctx: &CommandContext) -> Result<CommandResult> {
    let inv = db.get_last(command)?;

    if ctx.is_human() {
        match &inv {
            Some(inv) => {
                println!("Last {command} invocation:");
                println!("  ID:       {}", inv.id);
                println!("  Status:   {:?}", inv.status);
                println!("  Started:  {}", inv.started_at);
                if let Some(d) = inv.duration_secs {
                    println!("  Duration: {d:.2}s");
                }
                if let Some(c) = &inv.git_commit {
                    println!(
                        "  Commit:   {}{}",
                        c,
                        if inv.git_dirty { " (dirty)" } else { "" }
                    );
                }
            }
            None => println!("No history for command: {command}"),
        }
    } else {
        let json = serde_json::to_string_pretty(&inv)?;
        println!("{json}");
    }

    let message = if inv.is_some() {
        format!("Last invocation for '{command}'")
    } else {
        format!("No history for command '{command}'")
    };

    Ok(CommandResult::success()
        .with_message(message)
        .with_duration(ctx.elapsed()))
}

fn execute_stats(
    db: &HistoryDb,
    command: &str,
    days: u32,
    package: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let stats = db.get_stats(command, days)?;

    if ctx.is_human() {
        let pkg_note = package
            .map(|p| format!(" (package: {p})"))
            .unwrap_or_default();
        println!("Statistics for '{command}'{pkg_note} (last {days} days):");
        println!("  Total:     {}", stats.total);
        println!("  Successes: {}", stats.successes);
        println!("  Failures:  {}", stats.failures);
        if let Some(avg) = stats.avg_duration_secs {
            println!("  Avg time:  {avg:.2}s");
        }
        if stats.total > 0 {
            let rate = (stats.successes as f64 / stats.total as f64) * 100.0;
            println!("  Success:   {rate:.1}%");
        }
    } else {
        let json = serde_json::to_string_pretty(&stats)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Statistics for '{command}' over {days} days"))
        .with_duration(ctx.elapsed()))
}

fn execute_stats_all_packages(db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let analysis = HistoryAnalysis::new(db);
    let health = analysis.all_packages_health()?;

    if ctx.is_human() {
        if health.is_empty() {
            println!("No package diagnostic data found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record([
                "PACKAGE",
                "DIAGNOSTICS",
                "FIXABLE",
                "TEST RATE",
                "AVG BUILD",
            ]);
            for h in &health {
                let test_rate = h
                    .test_pass_rate
                    .map_or_else(|| "-".into(), |r| format!("{:.0}%", r * 100.0));
                let avg_build = h
                    .avg_build_time_secs
                    .map_or_else(|| "-".into(), |s| format!("{s:.1}s"));
                builder.push_record([
                    h.package.clone(),
                    h.diagnostic_count.to_string(),
                    h.fixable_count.to_string(),
                    test_rate,
                    avg_build,
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&health)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Health for {} packages", health.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_stats_all_commands(
    db: &HistoryDb,
    days: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Collect unique commands from history, then get stats for each
    let invocations = db.get_recent(500, None)?;
    let mut commands: Vec<String> = invocations
        .iter()
        .map(|i| i.command.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    commands.sort();

    let mut all_stats = Vec::new();
    for cmd in &commands {
        let stats = db.get_stats(cmd, days)?;
        all_stats.push((cmd.clone(), stats));
    }

    if ctx.is_human() {
        if all_stats.is_empty() {
            println!("No history found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record([
                "COMMAND",
                "TOTAL",
                "SUCCESS",
                "FAILED",
                "SUCCESS %",
                "AVG TIME",
            ]);
            for (cmd, s) in &all_stats {
                let rate = if s.total > 0 {
                    format!("{:.1}%", (s.successes as f64 / s.total as f64) * 100.0)
                } else {
                    "-".into()
                };
                let avg = s
                    .avg_duration_secs
                    .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
                builder.push_record([
                    cmd.clone(),
                    s.total.to_string(),
                    s.successes.to_string(),
                    s.failures.to_string(),
                    rate,
                    avg,
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(
            &all_stats
                .iter()
                .map(|(cmd, s)| serde_json::json!({"command": cmd, "stats": s}))
                .collect::<Vec<_>>(),
        )?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Stats for {} commands", all_stats.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_prune(db: &HistoryDb, older_than: u32, ctx: &CommandContext) -> Result<CommandResult> {
    let count = db.prune(older_than)?;

    if ctx.is_human() {
        println!("Pruned {count} entries older than {older_than} days");
    } else {
        println!(r#"{{"pruned": {count}, "older_than_days": {older_than}}}"#);
    }

    Ok(CommandResult::success()
        .with_message(format!("Pruned {count} old entries"))
        .with_duration(ctx.elapsed()))
}

fn execute_export(db: &HistoryDb, limit: usize, ctx: &CommandContext) -> Result<CommandResult> {
    let invocations = db.get_recent(limit, None)?;
    let json = serde_json::to_string_pretty(&invocations)?;
    println!("{json}");

    Ok(CommandResult::success()
        .with_message(format!("Exported {} entries", invocations.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests(
    tests_cmd: &HistoryTestsSubcommand,
    db: &HistoryDb,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    match tests_cmd {
        HistoryTestsSubcommand::Slowest { limit } => execute_tests_slowest(db, *limit, ctx),
        HistoryTestsSubcommand::Flaky { limit } => execute_tests_flaky(db, *limit, ctx),
        HistoryTestsSubcommand::GettingSlower {
            threshold_pct,
            window,
            limit,
        } => execute_tests_getting_slower(db, *threshold_pct, *window, *limit, ctx),
        HistoryTestsSubcommand::Trends {
            pattern,
            package,
            runs,
        } => execute_tests_trends(db, pattern.as_deref(), package.as_deref(), *runs, ctx),
        HistoryTestsSubcommand::Failures { limit, output } => {
            execute_tests_failures(db, *limit, *output, ctx)
        }
        HistoryTestsSubcommand::Analyze => execute_tests_analyze(db, ctx),
        HistoryTestsSubcommand::Output { pattern } => execute_tests_output(db, pattern, ctx),
        HistoryTestsSubcommand::Eta => execute_tests_eta(db, ctx),
        HistoryTestsSubcommand::Grep { text, limit } => execute_tests_grep(db, text, *limit, ctx),
        HistoryTestsSubcommand::ByPackage => execute_tests_by_package(db, ctx),
        HistoryTestsSubcommand::DurationP95 { limit } => {
            execute_tests_duration_p95(db, *limit, ctx)
        }
        HistoryTestsSubcommand::Regression { runs } => execute_tests_regression(db, *runs, ctx),
    }
}

fn execute_tests_slowest(
    db: &HistoryDb,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_slowest_tests(limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No test timing data found.");
        } else {
            println!(
                "{:<50} {:<20} {:>10} {:>6}",
                "TEST", "PACKAGE", "AVG (s)", "RUNS"
            );
            for (name, package, avg, runs) in &tests {
                let display_name = if name.len() > 48 {
                    format!("...{}", &name[name.len() - 45..])
                } else {
                    name.clone()
                };
                println!("{display_name:<50} {package:<20} {avg:>10.3} {runs:>6}");
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} slowest tests", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_flaky(
    db: &HistoryDb,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_flaky_tests(limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No flaky tests found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "INVOCATION"]);
            for (name, package, inv_id) in &tests {
                let display_name = if name.len() > 48 {
                    format!("...{}", &name[name.len() - 45..])
                } else {
                    name.clone()
                };
                builder.push_record([display_name, package.clone(), inv_id.to_string()]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} flaky tests", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_getting_slower(
    db: &HistoryDb,
    threshold_pct: f64,
    window: usize,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_tests_getting_slower(window, threshold_pct, limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No tests found slowing >{threshold_pct}% over {window} runs.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "OLD (s)", "NEW (s)", "CHANGE"]);
            for test in &tests {
                let display_name = if test.test_name.len() > 43 {
                    format!("...{}", &test.test_name[test.test_name.len() - 40..])
                } else {
                    test.test_name.clone()
                };
                builder.push_record([
                    display_name,
                    test.package.clone(),
                    format!("{:.3}", test.older_avg_secs),
                    format!("{:.3}", test.recent_avg_secs),
                    format!("{:+.1}%", test.pct_change),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} tests getting slower", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_trends(
    db: &HistoryDb,
    pattern: Option<&str>,
    package: Option<&str>,
    runs: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_test_trends(pattern, package, runs)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No matching tests found.");
        } else {
            for test in &tests {
                println!(
                    "{}::{} (avg: {:.3}s)",
                    test.package, test.test_name, test.avg_duration_secs
                );
                for (i, duration) in test.durations.iter().enumerate() {
                    let timestamp = test.timestamps.get(i).map_or("-", |s| s.as_str());
                    println!("  {timestamp}: {duration:.3}s");
                }
                println!();
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} test trends", tests.len()))
        .with_duration(ctx.elapsed()))
}

/// Output format for diagnostics.
#[derive(Debug, Clone, Default, clap::ValueEnum)]
pub enum DiagnosticsFormat {
    /// Human-readable table (default)
    #[default]
    Table,
    /// GCC-compatible format: file:line:col: level: message [code]
    ///
    /// Consumed by VS Code problem matchers, Vim :make, Emacs compile-mode.
    Gcc,
}

/// Display mode controls which columns are shown in the diagnostics table.
enum DiagnosticsDisplayMode {
    /// Default: shows PACKAGE and SOURCE columns
    Current,
    /// --all: raw accumulated, no package/source
    All,
    /// --invocation: single invocation, no source
    Invocation,
    /// --fixable: shows FIX column
    Fixable,
}

#[derive(Clone, Copy)]
struct DiagnosticFilter<'a> {
    level: Option<&'a str>,
    file: Option<&'a str>,
    command: Option<&'a str>,
    package: Option<&'a str>,
    code: Option<&'a str>,
    fixable: bool,
}

impl<'a> DiagnosticFilter<'a> {
    const fn new(
        level: Option<&'a str>,
        file: Option<&'a str>,
        command: Option<&'a str>,
        package: Option<&'a str>,
        code: Option<&'a str>,
        fixable: bool,
    ) -> Self {
        Self {
            level,
            file,
            command,
            package,
            code,
            fixable,
        }
    }
}

fn apply_diagnostic_filters(
    diagnostics: &mut Vec<crate::history::StoredDiagnostic>,
    filter: DiagnosticFilter<'_>,
) {
    diagnostics.retain(|diagnostic| {
        if let Some(level) = filter.level
            && diagnostic.level != level
        {
            return false;
        }

        if let Some(pattern) = filter.file
            && !diagnostic
                .file_path
                .as_ref()
                .is_some_and(|path| path.contains(pattern))
        {
            return false;
        }

        if let Some(command) = filter.command
            && diagnostic.source_command.as_deref() != Some(command)
        {
            return false;
        }

        if let Some(package) = filter.package
            && diagnostic.package.as_deref() != Some(package)
        {
            return false;
        }

        if let Some(code) = filter.code
            && diagnostic.code.as_deref() != Some(code)
        {
            return false;
        }

        if filter.fixable && diagnostic.fix_applicability.as_deref() != Some("MachineApplicable") {
            return false;
        }

        true
    });
}

/// Format a file path + line for display (truncates long paths).
fn format_file_loc(path: &Option<String>, line: Option<u32>) -> String {
    match (path, line) {
        (Some(path), Some(line)) => {
            let short_path = if path.len() > 45 {
                format!("...{}", &path[path.len() - 42..])
            } else {
                path.clone()
            };
            format!("{short_path}:{line}")
        }
        (Some(path), None) => {
            if path.len() > 48 {
                format!("...{}", &path[path.len() - 45..])
            } else {
                path.clone()
            }
        }
        _ => "-".to_string(),
    }
}

/// Format a source_time string to short "HH:MM" display.
fn format_source_short(command: &Option<String>, time: &Option<String>) -> String {
    let cmd = command.as_deref().unwrap_or("-");
    let time_short = time
        .as_ref()
        .and_then(|t| {
            // Parse ISO timestamp and extract HH:MM
            t.get(11..16)
        })
        .unwrap_or("-");
    format!("{cmd} @ {time_short}")
}

/// Render diagnostics table with mode-specific columns.
fn render_diagnostics_table(
    diagnostics: &[crate::history::StoredDiagnostic],
    mode: DiagnosticsDisplayMode,
) {
    let mut builder = Builder::new();

    match mode {
        DiagnosticsDisplayMode::Current => {
            builder.push_record(["LEVEL", "PACKAGE", "CODE", "FILE", "MESSAGE", "SOURCE"]);
            for diag in diagnostics {
                let code = diag.code.as_deref().unwrap_or("-");
                let file_loc = format_file_loc(&diag.file_path, diag.line);
                let package = diag.package.as_deref().unwrap_or("-");
                let message = truncate_message(&diag.message, 50);
                let source = format_source_short(&diag.source_command, &diag.source_time);
                builder.push_record([
                    diag.level.clone(),
                    package.to_string(),
                    code.to_string(),
                    file_loc,
                    message,
                    source,
                ]);
            }
        }
        DiagnosticsDisplayMode::All | DiagnosticsDisplayMode::Invocation => {
            builder.push_record(["LEVEL", "PACKAGE", "CODE", "FILE", "MESSAGE"]);
            for diag in diagnostics {
                let code = diag.code.as_deref().unwrap_or("-");
                let file_loc = format_file_loc(&diag.file_path, diag.line);
                let package = diag.package.as_deref().unwrap_or("-");
                let message = truncate_message(&diag.message, 55);
                builder.push_record([
                    diag.level.clone(),
                    package.to_string(),
                    code.to_string(),
                    file_loc,
                    message,
                ]);
            }
        }
        DiagnosticsDisplayMode::Fixable => {
            builder.push_record(["FILE", "CODE", "FIX", "MESSAGE"]);
            for diag in diagnostics {
                let code = diag.code.as_deref().unwrap_or("-");
                let file_loc = format_file_loc(&diag.file_path, diag.line);
                let fix = diag
                    .fix_replacement
                    .as_deref()
                    .map_or_else(|| "-".to_string(), |r| truncate_message(r, 40));
                let message = truncate_message(&diag.message, 45);
                builder.push_record([file_loc, code.to_string(), fix, message]);
            }
        }
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");
}

/// Render diagnostics in GCC-compatible format: `file:line:col: level: message [code]`
fn render_diagnostics_gcc(diagnostics: &[crate::history::StoredDiagnostic]) {
    for diag in diagnostics {
        let file = diag.file_path.as_deref().unwrap_or("<unknown>");
        let line = diag.line.unwrap_or(1);
        let col = diag.col.unwrap_or(1);
        let level = &diag.level;
        let msg = &diag.message;
        if let Some(code) = &diag.code {
            println!("{file}:{line}:{col}: {level}: {msg} [{code}]");
        } else {
            println!("{file}:{line}:{col}: {level}: {msg}");
        }
    }
}

fn truncate_message(msg: &str, max_len: usize) -> String {
    if msg.len() > max_len {
        format!("{}...", &msg[..max_len.saturating_sub(3)])
    } else {
        msg.to_string()
    }
}

/// Default mode: package-scoped current diagnostics.
fn execute_diagnostics_current(
    db: &HistoryDb,
    level: Option<&str>,
    file: Option<&str>,
    command: Option<&str>,
    package: Option<&str>,
    fixable: bool,
    code: Option<&str>,
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_current_diagnostics(level, file, package, command, fixable)?;
    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(level, file, command, package, code, fixable),
    );

    if matches!(format, DiagnosticsFormat::Gcc) {
        render_diagnostics_gcc(&diagnostics);
    } else if ctx.is_human() {
        if diagnostics.is_empty() {
            println!("No current diagnostics.");
            println!(
                "  {}",
                style("(Run `xtask check` to populate diagnostic data)").dim()
            );
        } else {
            let mode = if fixable {
                DiagnosticsDisplayMode::Fixable
            } else {
                DiagnosticsDisplayMode::Current
            };
            println!(
                "Current diagnostics ({} total):",
                style(diagnostics.len()).bold()
            );
            render_diagnostics_table(&diagnostics, mode);
        }
    } else {
        let json = serde_json::to_string_pretty(&diagnostics)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} current diagnostics", diagnostics.len()))
        .with_duration(ctx.elapsed()))
}

/// --all mode: raw accumulated diagnostics.
fn execute_diagnostics_all(
    db: &HistoryDb,
    limit: usize,
    level: Option<&str>,
    file: Option<&str>,
    command: Option<&str>,
    package: Option<&str>,
    fixable: bool,
    code: Option<&str>,
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_recent_diagnostics_all(limit, level, file, command, package)?;
    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(level, file, command, package, code, fixable),
    );

    if matches!(format, DiagnosticsFormat::Gcc) {
        render_diagnostics_gcc(&diagnostics);
    } else if ctx.is_human() {
        if diagnostics.is_empty() {
            println!("No diagnostics found.");
        } else {
            println!(
                "All diagnostics (limit {}, {} shown):",
                limit,
                diagnostics.len()
            );
            render_diagnostics_table(&diagnostics, DiagnosticsDisplayMode::All);
        }
    } else {
        let json = serde_json::to_string_pretty(&diagnostics)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} diagnostics", diagnostics.len()))
        .with_duration(ctx.elapsed()))
}

/// --invocation mode: diagnostics from a specific invocation.
fn execute_diagnostics_invocation(
    db: &HistoryDb,
    invocation: &str,
    command: Option<&str>,
    level_filter: Option<&str>,
    file_filter: Option<&str>,
    package_filter: Option<&str>,
    fixable_only: bool,
    code_filter: Option<&str>,
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_diagnostics_for_invocation(invocation, command)?;
    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(
            level_filter,
            file_filter,
            command,
            package_filter,
            code_filter,
            fixable_only,
        ),
    );

    if matches!(format, DiagnosticsFormat::Gcc) {
        render_diagnostics_gcc(&diagnostics);
    } else if ctx.is_human() {
        let scope = if invocation == "latest" {
            format!("latest {}", command.unwrap_or("any"))
        } else {
            format!("invocation #{invocation}")
        };
        if diagnostics.is_empty() {
            println!("No diagnostics found for {scope}.");
        } else {
            println!("Diagnostics from {scope} ({} total):", diagnostics.len());
            render_diagnostics_table(&diagnostics, DiagnosticsDisplayMode::Invocation);
        }
    } else {
        let json = serde_json::to_string_pretty(&diagnostics)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Found {} diagnostics from invocation",
            diagnostics.len()
        ))
        .with_duration(ctx.elapsed()))
}

/// --trend mode: show diagnostic count trend over recent invocations.
fn execute_diagnostics_trend(
    db: &HistoryDb,
    window: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let points = db.get_diagnostic_trend(window)?;

    if ctx.is_human() {
        if points.is_empty() {
            println!("No check/build invocations found for trend analysis.");
            println!(
                "  {}",
                style("(Run `xtask check` a few times to build trend data)").dim()
            );
        } else {
            // Compute trend direction
            let (trend_label, trend_dir) = compute_trend_direction(&points);

            println!(
                "Diagnostic trend ({} invocations, {}):",
                style(points.len()).bold(),
                trend_label,
            );
            println!();

            // Header
            println!(
                "  {:>5}  {:>6}  {:>7}  {:>5}  {:>6}  {:>6}  TIME",
                "ID", "CMD", "STATUS", "ERRS", "WARNS", "TOTAL"
            );
            println!("  {}", "─".repeat(60));

            for pt in &points {
                let time_short = pt.started_at.get(11..16).unwrap_or("??:??");
                let date_short = pt.started_at.get(5..10).unwrap_or("??-??");
                let status_label = match pt.status {
                    InvocationStatus::Success => "success",
                    InvocationStatus::Failed => "failed",
                    InvocationStatus::Running => "running",
                    InvocationStatus::Cancelled => "cancelled",
                };
                let status_styled = if matches!(pt.status, InvocationStatus::Success) {
                    style(status_label).green()
                } else {
                    style(status_label).red()
                };
                let errors_styled = if pt.errors > 0 {
                    style(pt.errors.to_string()).red().bold()
                } else {
                    style("0".to_string()).dim()
                };
                let warns_styled = if pt.warnings > 0 {
                    style(pt.warnings.to_string()).yellow()
                } else {
                    style("0".to_string()).dim()
                };

                println!(
                    "  {:>5}  {:>6}  {:>7}  {:>5}  {:>6}  {:>6}  {} {}",
                    pt.invocation_id,
                    pt.command,
                    status_styled,
                    errors_styled,
                    warns_styled,
                    pt.total,
                    date_short,
                    time_short,
                );
            }

            println!();

            // Summary
            if let Some(latest) = points.last() {
                let trend_symbol = match trend_dir {
                    TrendDirection::Improving => style("↓ improving").green(),
                    TrendDirection::Worsening => style("↑ worsening").red(),
                    TrendDirection::Stable => style("→ stable").dim(),
                    TrendDirection::Insufficient => style("? insufficient data").dim(),
                };
                println!(
                    "  Latest: {} errors, {} warnings | Trend: {}",
                    latest.errors, latest.warnings, trend_symbol
                );
            }
        }
    } else {
        // JSON output
        let json_output = serde_json::json!({
            "points": points,
            "count": points.len(),
            "trend": compute_trend_direction(&points).0,
        });
        println!("{}", serde_json::to_string_pretty(&json_output)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("Showed trend for {} invocations", points.len()))
        .with_duration(ctx.elapsed()))
}

enum TrendDirection {
    Improving,
    Worsening,
    Stable,
    Insufficient,
}

/// Compute trend direction by comparing older half vs recent half of invocations.
fn compute_trend_direction(
    points: &[crate::history::DiagnosticTrendPoint],
) -> (String, TrendDirection) {
    if points.len() < 4 {
        return (
            "insufficient data".to_string(),
            TrendDirection::Insufficient,
        );
    }

    let mid = points.len() / 2;
    let older = &points[..mid];
    let recent = &points[mid..];

    let older_avg = older.iter().map(|p| p.total).sum::<usize>() as f64 / older.len() as f64;
    let recent_avg = recent.iter().map(|p| p.total).sum::<usize>() as f64 / recent.len() as f64;

    if older_avg == 0.0 && recent_avg == 0.0 {
        return ("stable (clean)".to_string(), TrendDirection::Stable);
    }

    let pct_change = if older_avg > 0.0 {
        ((recent_avg - older_avg) / older_avg) * 100.0
    } else {
        100.0 // went from 0 to something
    };

    if pct_change > 15.0 {
        (
            format!("worsening (+{pct_change:.0}%)"),
            TrendDirection::Worsening,
        )
    } else if pct_change < -15.0 {
        (
            format!("improving ({pct_change:.0}%)"),
            TrendDirection::Improving,
        )
    } else {
        ("stable".to_string(), TrendDirection::Stable)
    }
}

fn execute_tests_failures(
    db: &HistoryDb,
    limit: usize,
    show_output: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_failing_tests_with_output(limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No failing tests in the most recent run.");
        } else {
            let mut builder = Builder::new();
            let has_failure_msgs = tests.iter().any(|t| t.failure_message.is_some());
            if has_failure_msgs {
                builder.push_record(["TEST", "PACKAGE", "DURATION", "FAILURE"]);
            } else {
                builder.push_record(["TEST", "PACKAGE", "DURATION"]);
            }
            for test in &tests {
                let display_name = if test.test_name.len() > 48 {
                    format!("...{}", &test.test_name[test.test_name.len() - 45..])
                } else {
                    test.test_name.clone()
                };
                if has_failure_msgs {
                    let msg = test
                        .failure_message
                        .as_deref()
                        .unwrap_or("-")
                        .chars()
                        .take(60)
                        .collect::<String>();
                    builder.push_record([
                        display_name,
                        test.package.clone(),
                        format!("{:.3}s", test.duration_secs),
                        msg,
                    ]);
                } else {
                    builder.push_record([
                        display_name,
                        test.package.clone(),
                        format!("{:.3}s", test.duration_secs),
                    ]);
                }
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");

            if show_output {
                println!();
                for test in &tests {
                    if let Some(output) = &test.output {
                        println!("── {} ({}) ──", test.test_name, test.package);
                        println!("{output}");
                        println!();
                    }
                }
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} failing tests", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_analyze(db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let analysis = db.analyze_last_run()?;

    match analysis {
        None => {
            if ctx.is_human() {
                println!("No test run data found.");
            }
            Ok(CommandResult::success()
                .with_message("No test run data")
                .with_duration(ctx.elapsed()))
        }
        Some(analysis) => {
            if ctx.is_human() {
                println!("{}", style("━━━ Test Suite Analysis ━━━").bold());
                println!(
                    "Invocation #{}, started {}",
                    analysis.invocation_id, analysis.started_at
                );
                println!(
                    "  {} passed, {} failed, {} ignored",
                    style(analysis.total_passed).green(),
                    if analysis.total_failed > 0 {
                        style(analysis.total_failed).red().to_string()
                    } else {
                        style(analysis.total_failed).to_string()
                    },
                    analysis.total_ignored
                );
                println!("  Total duration: {:.1}s", analysis.total_duration_secs);

                // Duration distribution
                println!("\n{}", style("Duration Distribution:").bold());
                for bucket in &analysis.duration_buckets {
                    if bucket.count > 0 {
                        let bar = "█".repeat(std::cmp::min(bucket.count, 50));
                        println!("  {:>8} │ {:>4} │ {}", bucket.label, bucket.count, bar);
                    }
                }

                // Probable timeouts
                if !analysis.probable_timeouts.is_empty() {
                    println!("\n{}", style("⚠ Probable Timeouts:").yellow().bold());
                    for t in &analysis.probable_timeouts {
                        println!(
                            "  {}::{} ({:.1}s, {})",
                            t.package, t.test_name, t.duration_secs, t.status
                        );
                    }
                }

                // Per-package failure summary
                if !analysis.failure_summary.is_empty() {
                    println!("\n{}", style("Failures by Package:").red().bold());
                    let mut builder = Builder::new();
                    builder.push_record(["PACKAGE", "FAILED", "PASSED", "RATE", "TESTS"]);
                    for pkg in &analysis.failure_summary {
                        let tests_display = if pkg.failed_tests.len() <= 3 {
                            pkg.failed_tests.join(", ")
                        } else {
                            let first_three = &pkg.failed_tests[..3];
                            format!(
                                "{}, +{} more",
                                first_three.join(", "),
                                pkg.failed_tests.len() - 3
                            )
                        };
                        builder.push_record([
                            pkg.package.clone(),
                            pkg.failed_count.to_string(),
                            pkg.passed_count.to_string(),
                            format!("{:.1}%", pkg.failure_rate_pct),
                            tests_display,
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }

                // Infrastructure timing (from sandbox slog metadata)
                if let Ok(Some(infra)) = db.get_infra_timing_summary() {
                    println!("\n{}", style("Infrastructure Timing:").cyan().bold());
                    println!(
                        "  Slot acquisition: avg {:.0}ms, max {}ms ({} tests with data)",
                        infra.avg_slot_wait_ms, infra.max_slot_wait_ms, infra.tests_with_metadata,
                    );
                    if infra.dirty_slot_count > 0 {
                        println!(
                            "  Dirty slot cleanup: avg {:.0}ms ({} of {} slots were dirty)",
                            infra.avg_cleanup_ms, infra.dirty_slot_count, infra.tests_with_metadata,
                        );
                    }
                    if infra.slot_usage.len() > 1 {
                        let top_slots: Vec<String> = infra
                            .slot_usage
                            .iter()
                            .take(5)
                            .map(|(name, count)| format!("{name}:{count}"))
                            .collect();
                        println!(
                            "  Slot distribution: {} slots used (top: {})",
                            infra.slot_usage.len(),
                            top_slots.join(", ")
                        );
                    }
                }
            } else {
                let json = serde_json::to_string_pretty(&analysis)?;
                println!("{json}");
            }

            Ok(CommandResult::success()
                .with_message(format!(
                    "Analysis: {} passed, {} failed",
                    analysis.total_passed, analysis.total_failed
                ))
                .with_data(serde_json::to_value(&analysis)?)
                .with_duration(ctx.elapsed()))
        }
    }
}

fn execute_tests_output(
    db: &HistoryDb,
    pattern: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let entries = db.get_test_output(pattern)?;

    if ctx.is_human() {
        if entries.is_empty() {
            println!("No tests matching '{pattern}' found in the most recent run.");
        } else {
            for entry in &entries {
                println!(
                    "── {} ({}, {}, {:.3}s) ──",
                    entry.test_name, entry.package, entry.status, entry.duration_secs
                );
                if let Some(output) = &entry.output {
                    println!("{output}");
                } else {
                    println!("  (no captured output)");
                }
                println!();
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&entries)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} matching tests", entries.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_eta(db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let estimate = db.estimate_runtime()?;

    if ctx.is_human() {
        if estimate.test_count == 0 {
            println!("No test history available for estimation.");
        } else {
            println!(
                "Estimated runtime: {:.0}s ({} tests, {} confidence)",
                estimate.estimated_secs, estimate.test_count, estimate.confidence
            );
            if !estimate.breakdown.is_empty() && estimate.breakdown.len() <= 10 {
                println!("\nBreakdown by package:");
                for (pkg, secs) in &estimate.breakdown {
                    println!("  {pkg:<30} {secs:>6.1}s");
                }
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&estimate)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Estimated runtime: {:.0}s",
            estimate.estimated_secs
        ))
        .with_duration(ctx.elapsed()))
}

// ─── G7: Test Analytics Extensions ──────────────────────────────────────────

/// Search stored test output for text (G7 --grep).
fn execute_tests_grep(
    db: &HistoryDb,
    text: &str,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let results = db.search_test_output(text, limit)?;

    if ctx.is_human() {
        if results.is_empty() {
            println!("No test output matching '{text}' found in the most recent run.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "STATUS", "DURATION"]);
            for entry in &results {
                let display_name = if entry.test_name.len() > 48 {
                    format!("...{}", &entry.test_name[entry.test_name.len() - 45..])
                } else {
                    entry.test_name.clone()
                };
                builder.push_record([
                    display_name,
                    entry.package.clone(),
                    entry.status.clone(),
                    format!("{:.3}s", entry.duration_secs),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
            println!();
            for entry in &results {
                if let Some(output) = &entry.output {
                    // Highlight matching text in output (simple prefix/suffix)
                    let excerpt: String = output
                        .lines()
                        .filter(|l| l.to_lowercase().contains(&text.to_lowercase()))
                        .take(3)
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !excerpt.is_empty() {
                        println!("  {} → {}", style(&entry.test_name).dim(), excerpt);
                    }
                }
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&results)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} matching tests", results.len()))
        .with_duration(ctx.elapsed()))
}

/// Per-package test stats (G7 --by-package).
fn execute_tests_by_package(db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let stats = db.get_tests_by_package()?;

    if ctx.is_human() {
        if stats.is_empty() {
            println!("No test run data found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["PACKAGE", "TOTAL", "PASSED", "FAILED", "AVG (s)", "FLAKY"]);
            for s in &stats {
                let pass_rate = if s.total > 0 {
                    format!("{:.1}%", (s.passed as f64 / s.total as f64) * 100.0)
                } else {
                    "-".into()
                };
                builder.push_record([
                    s.package.clone(),
                    s.total.to_string(),
                    format!("{} ({})", s.passed, pass_rate),
                    s.failed.to_string(),
                    format!("{:.3}", s.avg_duration_secs),
                    s.flaky_count.to_string(),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&stats)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Stats for {} packages", stats.len()))
        .with_duration(ctx.elapsed()))
}

/// P95 duration per test (G7 --duration-p95).
fn execute_tests_duration_p95(
    db: &HistoryDb,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let results = db.get_test_duration_p95(limit)?;

    if ctx.is_human() {
        if results.is_empty() {
            println!("No test duration data found.");
        } else {
            println!("P95 test durations (slowest {limit}):");
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "P95 (s)"]);
            for (name, pkg, p95) in &results {
                let display_name = if name.len() > 48 {
                    format!("...{}", &name[name.len() - 45..])
                } else {
                    name.clone()
                };
                builder.push_record([display_name, pkg.clone(), format!("{p95:.3}")]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(
            &results
                .iter()
                .map(|(n, p, d)| serde_json::json!({"test_name": n, "package": p, "p95_secs": d}))
                .collect::<Vec<_>>(),
        )?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("{} tests with P95 data", results.len()))
        .with_duration(ctx.elapsed()))
}

/// Tests newly failing in recent runs that previously passed (G7 --regression).
fn execute_tests_regression(
    db: &HistoryDb,
    runs: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let regressions = db.get_tests_regressing(runs)?;

    if ctx.is_human() {
        if regressions.is_empty() {
            println!("No test regressions found in the last {runs} runs.");
        } else {
            println!(
                "{} test{} newly failing in the last {runs} runs:",
                style(regressions.len()).red().bold(),
                if regressions.len() == 1 { "" } else { "s" }
            );
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "DURATION"]);
            for r in &regressions {
                let display_name = if r.test_name.len() > 48 {
                    format!("...{}", &r.test_name[r.test_name.len() - 45..])
                } else {
                    r.test_name.clone()
                };
                builder.push_record([
                    display_name,
                    r.package.clone(),
                    format!("{:.3}s", r.duration_secs),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&regressions)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("{} regressions found", regressions.len()))
        .with_duration(ctx.elapsed()))
}

// ─── G1: Diagnostic Delta ────────────────────────────────────────────────────

/// Show new/resolved/persistent diagnostics between two invocations (G1).
fn execute_diagnostics_delta(
    db: &HistoryDb,
    delta_from: Option<i64>,
    delta_to: Option<i64>,
    level: Option<&str>,
    file: Option<&str>,
    command: Option<&str>,
    package: Option<&str>,
    fixable: bool,
    code: Option<&str>,
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Resolve to/from invocation IDs
    let to_id: i64 = if let Some(id) = delta_to {
        id
    } else if let Some(cmd) = command {
        db.get_last(cmd)?
            .map(|inv| inv.id)
            .ok_or_else(|| color_eyre::eyre::eyre!("No recent {cmd} invocation found"))?
    } else {
        db.get_last("check")?
            .or_else(|| db.get_last("build").ok().flatten())
            .map(|inv| inv.id)
            .ok_or_else(|| color_eyre::eyre::eyre!("No recent check/build invocation found"))?
    };

    let from_id: i64 = if let Some(id) = delta_from {
        id
    } else {
        // Find the invocation before `to_id` for the same command
        let inv = db.get_recent(50, None)?;
        inv.into_iter()
            .filter(|i| {
                i.id < to_id
                    && command.is_none_or(|cmd| i.command == cmd)
                    && matches!(
                        i.status,
                        InvocationStatus::Success | InvocationStatus::Failed
                    )
            })
            .next()
            .map(|i| i.id)
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("No previous invocation found to compare against")
            })?
    };

    let mut delta = db.get_diagnostic_delta(from_id, to_id)?;
    let filter = DiagnosticFilter::new(level, file, command, package, code, fixable);
    apply_diagnostic_filters(&mut delta.new, filter);
    apply_diagnostic_filters(&mut delta.resolved, filter);
    apply_diagnostic_filters(&mut delta.persistent, filter);

    if matches!(format, DiagnosticsFormat::Gcc) {
        // GCC mode: prefix new/resolved
        for d in &delta.new {
            if let (Some(path), Some(line)) = (&d.file_path, d.line) {
                println!(
                    "{}:{}:{}:NEW {} {}",
                    path,
                    line,
                    d.col.unwrap_or(0),
                    d.level,
                    d.message
                );
            }
        }
        for d in &delta.resolved {
            if let (Some(path), Some(line)) = (&d.file_path, d.line) {
                println!(
                    "{}:{}:{}:RESOLVED {} {}",
                    path,
                    line,
                    d.col.unwrap_or(0),
                    d.level,
                    d.message
                );
            }
        }
    } else if ctx.is_human() {
        println!(
            "Diagnostic delta: invocation {} → {} ({} new, {} resolved, {} persistent)",
            from_id,
            to_id,
            style(delta.new.len()).green().bold(),
            style(delta.resolved.len()).red().bold(),
            delta.persistent.len(),
        );

        if !delta.new.is_empty() {
            println!("\n{}", style("NEW (appeared):").green().bold());
            render_diagnostics_table(&delta.new, DiagnosticsDisplayMode::Current);
        }
        if !delta.resolved.is_empty() {
            println!("\n{}", style("RESOLVED (fixed):").cyan().bold());
            render_diagnostics_table(&delta.resolved, DiagnosticsDisplayMode::Current);
        }
        if delta.new.is_empty() && delta.resolved.is_empty() {
            println!("\n{}", style("No changes — diagnostics are stable.").dim());
        }
    } else {
        let json = serde_json::to_string_pretty(&delta)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Delta: {} new, {} resolved, {} persistent",
            delta.new.len(),
            delta.resolved.len(),
            delta.persistent.len()
        ))
        .with_duration(ctx.elapsed()))
}

/// Group current diagnostics by error code (G1 --by-code).
fn execute_diagnostics_by_code(
    db: &HistoryDb,
    level: Option<&str>,
    file: Option<&str>,
    command: Option<&str>,
    package: Option<&str>,
    fixable: bool,
    code: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_current_diagnostics(level, file, package, command, fixable)?;
    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(level, file, command, package, code, fixable),
    );

    // Group by code
    let mut by_code: std::collections::BTreeMap<String, Vec<&crate::history::StoredDiagnostic>> =
        std::collections::BTreeMap::new();
    for d in &diagnostics {
        let key = d.code.clone().unwrap_or_else(|| "(no code)".into());
        by_code.entry(key).or_default().push(d);
    }

    if ctx.is_human() {
        if by_code.is_empty() {
            println!("No current diagnostics.");
        } else {
            for (code, diags) in &by_code {
                println!(
                    "{} — {} occurrence{}",
                    style(code).yellow().bold(),
                    diags.len(),
                    if diags.len() == 1 { "" } else { "s" }
                );
                for d in diags.iter().take(3) {
                    let loc = d
                        .file_path
                        .as_deref()
                        .map(|p| {
                            if let Some(line) = d.line {
                                format!(" @ {p}:{line}")
                            } else {
                                format!(" @ {p}")
                            }
                        })
                        .unwrap_or_default();
                    println!(
                        "  {} {}{}",
                        style(&d.level).dim(),
                        d.message,
                        style(loc).dim()
                    );
                }
                if diags.len() > 3 {
                    println!("  {} …and {} more", style("").dim(), diags.len() - 3);
                }
            }
        }
    } else {
        let grouped: Vec<serde_json::Value> = by_code
            .iter()
            .map(|(code, diags)| {
                serde_json::json!({
                    "code": code,
                    "count": diags.len(),
                    "diagnostics": diags,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&grouped)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("{} unique codes", by_code.len()))
        .with_duration(ctx.elapsed()))
}

// ─── G2: Stage Analytics ────────────────────────────────────────────────────

/// Show pipeline stage timing data (G2).
#[allow(clippy::too_many_arguments)]
fn execute_stages(
    db: &HistoryDb,
    command: Option<&str>,
    invocation: Option<i64>,
    slowest: usize,
    trend: Option<&str>,
    window: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if let Some(stage_name) = trend {
        // Trend view: per-invocation timing for one stage
        let points = db.get_stage_trend(stage_name, command, window)?;
        if ctx.is_human() {
            if points.is_empty() {
                println!("No timing data for stage '{stage_name}'.");
            } else {
                println!(
                    "Stage '{}' trend (last {} invocations):",
                    style(stage_name).bold(),
                    points.len()
                );
                for pt in &points {
                    let status_icon = if pt.success { "✓" } else { "✗" };
                    println!(
                        "  [{}] {} {:.3}s  {}",
                        status_icon,
                        super::format_display_time_str(&pt.started_at),
                        pt.duration_secs,
                        style(format!("(inv {})", pt.invocation_id)).dim()
                    );
                }
            }
        } else {
            println!("{}", serde_json::to_string_pretty(&points)?);
        }
        return Ok(CommandResult::success()
            .with_message(format!(
                "{} data points for stage '{stage_name}'",
                points.len()
            ))
            .with_duration(ctx.elapsed()));
    }

    if let Some(inv_id) = invocation {
        // Per-invocation timings
        let timings = db.get_stage_timings_for_invocation(inv_id)?;
        if ctx.is_human() {
            if timings.is_empty() {
                println!("No stage timings for invocation {inv_id}.");
            } else {
                println!("Stage timings for invocation {inv_id}:");
                let mut builder = Builder::new();
                builder.push_record(["STAGE", "STARTED", "DURATION", "STATUS"]);
                for t in &timings {
                    let status = if t.success { "ok" } else { "fail" };
                    builder.push_record([
                        t.stage_name.clone(),
                        super::format_display_time_str(&t.started_at),
                        format!("{:.3}s", t.duration_secs),
                        status.to_string(),
                    ]);
                }
                let mut table = builder.build();
                table.with(Style::rounded());
                println!("{table}");
            }
        } else {
            println!("{}", serde_json::to_string_pretty(&timings)?);
        }
        return Ok(CommandResult::success()
            .with_message(format!("{} stage timings for inv {inv_id}", timings.len()))
            .with_duration(ctx.elapsed()));
    }

    // Default: slowest N stages by avg duration
    let stats = db.get_slowest_stages(command, slowest)?;
    if ctx.is_human() {
        if stats.is_empty() {
            println!("No stage timing data found.");
            if command.is_some() {
                println!(
                    "  {}",
                    style("(Try without --command to see all stages)").dim()
                );
            }
        } else {
            let cmd_note = command
                .map(|c| format!(" (command: {c})"))
                .unwrap_or_default();
            println!("Slowest stages{cmd_note} (avg):");
            let mut builder = Builder::new();
            builder.push_record(["STAGE", "AVG (s)", "MAX (s)", "RUNS"]);
            for s in &stats {
                builder.push_record([
                    s.stage_name.clone(),
                    format!("{:.3}", s.avg_duration_secs),
                    format!("{:.3}", s.max_duration_secs),
                    s.run_count.to_string(),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&stats)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("{} stages", stats.len()))
        .with_duration(ctx.elapsed()))
}

// ─── G3: Fix Session Analytics ──────────────────────────────────────────────

/// Show fix session history (G3).
fn execute_fix_sessions(
    db: &HistoryDb,
    sessions: usize,
    effectiveness: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let fix_sessions = db.get_fix_sessions(sessions)?;

    if ctx.is_human() {
        if fix_sessions.is_empty() {
            println!("No fix session history found.");
            println!(
                "  {}",
                style("(Fix sessions are recorded when you run `xtask fix`)").dim()
            );
        } else if effectiveness {
            println!(
                "Fix effectiveness ({} session{}):",
                fix_sessions.len(),
                if fix_sessions.len() == 1 { "" } else { "s" }
            );
            let mut builder = Builder::new();
            builder.push_record([
                "STARTED",
                "DURATION",
                "PRE-ERRORS",
                "PRE-WARNINGS",
                "PRE-FIXABLE",
            ]);
            for s in &fix_sessions {
                let duration = s
                    .duration_secs
                    .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
                builder.push_record([
                    super::format_display_time_str(&s.started_at),
                    duration,
                    s.pre_fix_errors
                        .map_or_else(|| "-".into(), |v| v.to_string()),
                    s.pre_fix_warnings
                        .map_or_else(|| "-".into(), |v| v.to_string()),
                    s.pre_fix_fixable
                        .map_or_else(|| "-".into(), |v| v.to_string()),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        } else {
            println!("Fix sessions (last {}):", fix_sessions.len());
            for s in &fix_sessions {
                let duration = s
                    .duration_secs
                    .map_or_else(|| "running".into(), |d| format!("{d:.1}s"));
                let pre = if let (Some(e), Some(w)) = (s.pre_fix_errors, s.pre_fix_warnings) {
                    format!(" [pre-fix: {e}E {w}W]")
                } else {
                    String::new()
                };
                println!(
                    "  {} — {}{}",
                    super::format_display_time_str(&s.started_at),
                    duration,
                    style(pre).dim()
                );
            }
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&fix_sessions)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("{} fix sessions", fix_sessions.len()))
        .with_duration(ctx.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cargo_diagnostics::CompilerDiagnostic;
    use crate::history::HistoryDb;
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::sinex_test;
    use std::collections::HashSet;
    use tempfile::tempdir;

    fn silent_ctx() -> CommandContext {
        CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, None)
    }

    fn sample_diagnostic(
        level: &str,
        file_path: Option<&str>,
        package: Option<&str>,
        code: Option<&str>,
        fixable: bool,
        command: Option<&str>,
    ) -> crate::history::StoredDiagnostic {
        crate::history::StoredDiagnostic {
            id: 1,
            level: level.to_string(),
            code: code.map(str::to_string),
            message: "sample".to_string(),
            file_path: file_path.map(str::to_string),
            line: Some(1),
            col: Some(1),
            rendered: None,
            package: package.map(str::to_string),
            fix_replacement: None,
            fix_applicability: fixable.then(|| "MachineApplicable".to_string()),
            fix_byte_start: None,
            fix_byte_end: None,
            source_command: command.map(str::to_string),
            source_time: None,
        }
    }

    fn seeded_history_db(name: &str) -> Result<HistoryDb> {
        let dir = tempdir()?;
        let db_path = dir.path().join(name);
        HistoryDb::open(&db_path)
    }

    #[sinex_test]
    async fn test_history_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = HistoryCommand {
            subcommand: HistorySubcommand::List {
                limit: 10,
                command: None,
                first: false,
                no_limit: false,
                offset: 0,
                sort_by: "started".to_string(),
                since: None,
                with_diagnostics: false,
                with_stages: false,
                with_tests: false,
            },
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("diagnostics"));
        assert!(metadata.timeout.is_some());
        assert!(!metadata.modifies_state); // History commands are read-only
        Ok(())
    }

    #[sinex_test]
    async fn test_history_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = HistoryCommand {
            subcommand: HistorySubcommand::Stats {
                command: Some("test".to_string()),
                days: 7,
                package: None,
                all_packages: false,
                all_commands: false,
            },
        };

        assert_eq!(cmd.name(), "history");
        Ok(())
    }

    #[sinex_test]
    async fn test_apply_diagnostic_filters_honors_all_fields() -> ::xtask::sandbox::TestResult<()> {
        let mut diagnostics = vec![
            sample_diagnostic(
                "warning",
                Some("crate/lib/sinex-db/src/lib.rs"),
                Some("sinex-db"),
                Some("W001"),
                true,
                Some("check"),
            ),
            sample_diagnostic(
                "warning",
                Some("crate/cli/src/main.rs"),
                Some("sinexctl"),
                Some("W001"),
                true,
                Some("check"),
            ),
            sample_diagnostic(
                "error",
                Some("crate/lib/sinex-db/src/lib.rs"),
                Some("sinex-db"),
                Some("E001"),
                false,
                Some("build"),
            ),
        ];

        apply_diagnostic_filters(
            &mut diagnostics,
            DiagnosticFilter::new(
                Some("warning"),
                Some("sinex-db/src"),
                Some("check"),
                Some("sinex-db"),
                Some("W001"),
                true,
            ),
        );

        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics[0];
        assert_eq!(diagnostic.package.as_deref(), Some("sinex-db"));
        assert_eq!(diagnostic.code.as_deref(), Some("W001"));
        assert_eq!(diagnostic.source_command.as_deref(), Some("check"));
        assert_eq!(
            diagnostic.fix_applicability.as_deref(),
            Some("MachineApplicable")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_diagnostics_invocation_applies_package_code_and_fixable_filters()
    -> ::xtask::sandbox::TestResult<()> {
        let db = seeded_history_db("diag-invocation.db")?;
        let ctx = silent_ctx();

        let inv_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W001".into()),
                message: "target".into(),
                package: Some("sinex-db".into()),
                file_path: Some("crate/lib/sinex-db/src/lib.rs".into()),
                fix_applicability: Some("MachineApplicable".into()),
                ..Default::default()
            },
        )?;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W001".into()),
                message: "other package".into(),
                package: Some("sinexctl".into()),
                file_path: Some("crate/cli/src/main.rs".into()),
                fix_applicability: Some("MachineApplicable".into()),
                ..Default::default()
            },
        )?;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W001".into()),
                message: "not fixable".into(),
                package: Some("sinex-db".into()),
                file_path: Some("crate/lib/sinex-db/src/state.rs".into()),
                ..Default::default()
            },
        )?;

        let result = execute_diagnostics_invocation(
            &db,
            "latest",
            Some("check"),
            Some("warning"),
            Some("sinex-db/src"),
            Some("sinex-db"),
            true,
            Some("W001"),
            &DiagnosticsFormat::Table,
            &ctx,
        )?;

        assert_eq!(
            result.message.as_deref(),
            Some("Found 1 diagnostics from invocation")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_diagnostics_delta_respects_command_and_filters()
    -> ::xtask::sandbox::TestResult<()> {
        let db = seeded_history_db("diag-delta.db")?;
        let ctx = silent_ctx();

        let build_old = db.start_invocation("build", None, None, None)?;
        db.finish_invocation(build_old, InvocationStatus::Success, Some(0), 1.0)?;
        db.record_diagnostic(
            build_old,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W001".into()),
                message: "persistent".into(),
                package: Some("sinex-db".into()),
                file_path: Some("crate/lib/sinex-db/src/lib.rs".into()),
                fix_applicability: Some("MachineApplicable".into()),
                ..Default::default()
            },
        )?;

        let build_new = db.start_invocation("build", None, None, None)?;
        db.finish_invocation(build_new, InvocationStatus::Failed, Some(1), 1.0)?;
        db.record_diagnostic(
            build_new,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W001".into()),
                message: "persistent".into(),
                package: Some("sinex-db".into()),
                file_path: Some("crate/lib/sinex-db/src/lib.rs".into()),
                fix_applicability: Some("MachineApplicable".into()),
                ..Default::default()
            },
        )?;
        db.record_diagnostic(
            build_new,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W002".into()),
                message: "new build-only".into(),
                package: Some("sinex-db".into()),
                file_path: Some("crate/lib/sinex-db/src/state.rs".into()),
                fix_applicability: Some("MachineApplicable".into()),
                ..Default::default()
            },
        )?;

        let check_new = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(check_new, InvocationStatus::Failed, Some(1), 1.0)?;
        db.record_diagnostic(
            check_new,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W999".into()),
                message: "check-only".into(),
                package: Some("sinexctl".into()),
                file_path: Some("crate/cli/src/lib.rs".into()),
                fix_applicability: Some("MachineApplicable".into()),
                ..Default::default()
            },
        )?;

        let result = execute_diagnostics_delta(
            &db,
            None,
            None,
            Some("warning"),
            Some("sinex-db/src"),
            Some("build"),
            Some("sinex-db"),
            true,
            Some("W002"),
            &DiagnosticsFormat::Table,
            &ctx,
        )?;

        assert_eq!(
            result.message.as_deref(),
            Some("Delta: 1 new, 0 resolved, 0 persistent")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_diagnostics_by_code_respects_file_and_fixable()
    -> ::xtask::sandbox::TestResult<()> {
        let db = seeded_history_db("diag-by-code.db")?;
        let ctx = silent_ctx();

        let inv_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;
        db.record_compiled_packages(
            inv_id,
            &HashSet::from(["sinex-db".to_string(), "sinexctl".to_string()]),
        )?;

        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W001".into()),
                message: "target".into(),
                package: Some("sinex-db".into()),
                file_path: Some("crate/lib/sinex-db/src/lib.rs".into()),
                fix_applicability: Some("MachineApplicable".into()),
                ..Default::default()
            },
        )?;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W002".into()),
                message: "other path".into(),
                package: Some("sinex-db".into()),
                file_path: Some("crate/lib/sinex-db/tests/lib.rs".into()),
                fix_applicability: Some("MachineApplicable".into()),
                ..Default::default()
            },
        )?;
        db.record_diagnostic(
            inv_id,
            &CompilerDiagnostic {
                level: "warning".into(),
                code: Some("W001".into()),
                message: "not fixable".into(),
                package: Some("sinexctl".into()),
                file_path: Some("crate/cli/src/lib.rs".into()),
                ..Default::default()
            },
        )?;

        let result = execute_diagnostics_by_code(
            &db,
            Some("warning"),
            Some("sinex-db/src"),
            Some("check"),
            Some("sinex-db"),
            true,
            Some("W001"),
            &ctx,
        )?;

        assert_eq!(result.message.as_deref(), Some("1 unique codes"));
        Ok(())
    }
}
