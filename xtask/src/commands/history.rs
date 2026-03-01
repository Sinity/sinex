//! History command - query build/test execution history

use color_eyre::eyre::Result;
use console::style;
use tabled::{builder::Builder, settings::Style};

use std::sync::LazyLock as Lazy;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::HistoryDb;

static DISPLAY_TIME_FORMAT: Lazy<Vec<time::format_description::BorrowedFormatItem<'static>>> =
    Lazy::new(|| {
        time::format_description::parse("[year]-[month]-[day] [hour]:[minute]")
            .expect("static format string is valid")
    });

/// History command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum HistorySubcommand {
    /// List recent invocations
    List {
        #[arg(long, default_value = "20")]
        limit: usize,
        #[arg(long)]
        command: Option<String>,
    },
    /// Show the last invocation for a command
    Last {
        #[arg(long)]
        command: String,
    },
    /// Show statistics for a command
    Stats {
        #[arg(long)]
        command: String,
        #[arg(long, default_value = "30")]
        days: u32,
    },
    /// Prune old history entries
    Prune {
        #[arg(long, default_value = "90")]
        older_than: u32,
    },
    /// Export history as JSON
    Export {
        #[arg(long)]
        limit: usize,
    },
    /// Query test result history
    Tests {
        #[command(subcommand)]
        tests_cmd: HistoryTestsSubcommand,
    },
    /// Query build diagnostics (warnings/errors)
    ///
    /// Default: shows package-scoped current diagnostics (each package's most recent invocation).
    /// Use --all for raw accumulated view, --invocation for a specific run.
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
        /// Show raw accumulated diagnostics from all invocations (no dedup)
        #[arg(long)]
        all: bool,
        /// Maximum number of diagnostics (only with --all)
        #[arg(long, default_value = "50", requires = "all")]
        limit: usize,
        /// Show diagnostics from a specific invocation ID (or 'latest')
        #[arg(long, conflicts_with = "all")]
        invocation: Option<String>,
        /// Show only auto-fixable diagnostics (MachineApplicable)
        #[arg(long)]
        fixable: bool,
        /// Show diagnostic count trend over recent invocations
        #[arg(long, conflicts_with_all = ["all", "invocation", "fixable"])]
        trend: bool,
        /// Number of invocations to include in trend (with --trend)
        #[arg(long, default_value = "20", requires = "trend")]
        window: usize,
        /// Output format: table (default) or gcc (file:line:col: level: message)
        #[arg(long, default_value = "table")]
        emit: DiagnosticsFormat,
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
}

/// History management command
#[derive(Debug, Clone, clap::Args)]
pub struct HistoryCommand {
    #[command(subcommand)]
    pub subcommand: HistorySubcommand,
}

#[async_trait::async_trait]
impl XtaskCommand for HistoryCommand {
    fn name(&self) -> &'static str {
        "history"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let db = open_history_db()?;

        match &self.subcommand {
            HistorySubcommand::List { limit, command } => {
                execute_list(&db, *limit, command.as_deref(), ctx)
            }
            HistorySubcommand::Last { command } => execute_last(&db, command, ctx),
            HistorySubcommand::Stats { command, days } => execute_stats(&db, command, *days, ctx),
            HistorySubcommand::Prune { older_than } => execute_prune(&db, *older_than, ctx),
            HistorySubcommand::Export { limit } => execute_export(&db, *limit, ctx),
            HistorySubcommand::Tests { tests_cmd } => execute_tests(tests_cmd, &db, ctx),
            HistorySubcommand::Diagnostics {
                level,
                file,
                command,
                package,
                all,
                limit,
                invocation,
                fixable,
                trend,
                window,
                emit,
            } => {
                // Ensure schema migration for older DBs
                let _ = db.ensure_diagnostic_columns();

                if *trend {
                    return execute_diagnostics_trend(&db, *window, ctx);
                }

                match (all, invocation) {
                    (true, _) => execute_diagnostics_all(
                        &db,
                        *limit,
                        level.as_deref(),
                        file.as_deref(),
                        command.as_deref(),
                        package.as_deref(),
                        emit,
                        ctx,
                    ),
                    (_, Some(inv)) => execute_diagnostics_invocation(
                        &db,
                        inv,
                        command.as_deref(),
                        level.as_deref(),
                        file.as_deref(),
                        emit,
                        ctx,
                    ),
                    _ => execute_diagnostics_current(
                        &db,
                        level.as_deref(),
                        file.as_deref(),
                        command.as_deref(),
                        package.as_deref(),
                        *fixable,
                        emit,
                        ctx,
                    ),
                }
            }
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

fn execute_list(
    db: &HistoryDb,
    limit: usize,
    command: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let invocations = db.get_recent(limit, command)?;

    if ctx.is_human() {
        if invocations.is_empty() {
            println!("No history entries found.");
        } else {
            println!(
                "{:<6} {:<12} {:<10} {:<10} {:>8}  STARTED",
                "ID", "COMMAND", "PROFILE", "STATUS", "DURATION"
            );
            for inv in &invocations {
                let profile = inv.profile.as_deref().unwrap_or("-");
                let duration = inv
                    .duration_secs
                    .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
                let status = format!("{:?}", inv.status).to_lowercase();
                println!(
                    "{:<6} {:<12} {:<10} {:<10} {:>8}  {}",
                    inv.id,
                    inv.command,
                    profile,
                    status,
                    duration,
                    inv.started_at
                        .format(&*DISPLAY_TIME_FORMAT)
                        .unwrap_or_else(|_| "-".into())
                );
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
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let stats = db.get_stats(command, days)?;

    if ctx.is_human() {
        println!("Statistics for '{command}' (last {days} days):");
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
                    .map(|r| truncate_message(r, 40))
                    .unwrap_or_else(|| "-".to_string());
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
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let diagnostics = db.get_current_diagnostics(level, file, package, command, fixable)?;

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
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let diagnostics = db.get_recent_diagnostics_all(limit, level, file, command, package)?;

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
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_diagnostics_for_invocation(invocation, command)?;

    // Apply level and file filters in-memory
    if let Some(level) = level_filter {
        diagnostics.retain(|d| d.level == level);
    }
    if let Some(pattern) = file_filter {
        diagnostics.retain(|d| d.file_path.as_ref().is_some_and(|p| p.contains(pattern)));
    }

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
                "  {:>5}  {:>6}  {:>7}  {:>5}  {:>6}  {:>6}  {}",
                "ID", "CMD", "STATUS", "ERRS", "WARNS", "TOTAL", "TIME"
            );
            println!("  {}", "─".repeat(60));

            for pt in &points {
                let time_short = pt.started_at.get(11..16).unwrap_or("??:??");
                let date_short = pt.started_at.get(5..10).unwrap_or("??-??");
                let status_styled = if pt.status == "success" {
                    style(&pt.status).green()
                } else {
                    style(&pt.status).red()
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
            format!("worsening (+{:.0}%)", pct_change),
            TrendDirection::Worsening,
        )
    } else if pct_change < -15.0 {
        (
            format!("improving ({:.0}%)", pct_change),
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
            builder.push_record(["TEST", "PACKAGE", "DURATION"]);
            for test in &tests {
                let display_name = if test.test_name.len() > 48 {
                    format!("...{}", &test.test_name[test.test_name.len() - 45..])
                } else {
                    test.test_name.clone()
                };
                builder.push_record([
                    display_name,
                    test.package.clone(),
                    format!("{:.3}s", test.duration_secs),
                ]);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_history_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = HistoryCommand {
            subcommand: HistorySubcommand::List {
                limit: 10,
                command: None,
            },
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("diagnostics".to_string()));
        assert!(metadata.timeout.is_some());
        assert!(!metadata.modifies_state); // History commands are read-only
        Ok(())
    }

    #[sinex_test]
    async fn test_history_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = HistoryCommand {
            subcommand: HistorySubcommand::Stats {
                command: "test".to_string(),
                days: 7,
            },
        };

        assert_eq!(cmd.name(), "history");
        Ok(())
    }
}
