//! History command - query build/test execution history

use color_eyre::eyre::Result;
use console::style;
use tabled::{builder::Builder, settings::Style};

use color_eyre::eyre::WrapErr;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::history::query::HistoryAnalysis;
use crate::history::{
    DiagnosticQuery, ExerciseResultRow, HistoryDb, InvocationQuery, InvocationStatus,
    InvocationTimelineEntry, LifecycleStatus,
};

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
        /// Only show invocations with IDs greater than this selector
        /// (`latest`, `previous`, `current`, numeric, `inv:<id>`, `job:<id>`)
        #[arg(long)]
        after_invocation: Option<String>,
        /// Only show invocations with IDs less than this selector
        /// (`latest`, `previous`, `current`, numeric, `inv:<id>`, `job:<id>`)
        #[arg(long)]
        before_invocation: Option<String>,
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
        /// Show diagnostic lifecycle: New, Chronic, Recurring, Resolved (I3)
        #[arg(long, conflicts_with_all = ["scope", "trend", "delta", "by_code"])]
        lifecycle: bool,
        /// Filter --lifecycle results by status (new, chronic, recurring, resolved)
        #[arg(long, requires = "lifecycle")]
        lifecycle_status: Option<String>,
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
    /// Semantic named views — shortcuts to common queries (I1)
    ///
    /// Examples: fixable-now, chronic-diagnostics, new-diagnostics, resolved-last-run,
    /// flaky-tests, slow-stages, hot-packages, fix-history, recent-regressions,
    /// workspace-timeline, build-bottlenecks
    View {
        /// View name to display. Omit to list all available views.
        name: Option<String>,
    },
    /// Execute a read-only SQL query against the history database (I2)
    Query {
        /// SQL SELECT statement to execute (read-only enforced via PRAGMA)
        #[arg(long)]
        sql: String,
    },
    /// Open an interactive SQLite shell on the history database (I2)
    Shell,
    /// Dump CREATE TABLE statements for the history database schema (I2)
    Schema,
    /// Cross-invocation chronological view of recent activity (I4)
    Timeline {
        /// Restrict to a specific command (check, test, build, …)
        #[arg(long)]
        command: Option<String>,
        /// How many days back to show (default: 7)
        #[arg(long, default_value = "7")]
        days: u32,
        /// Maximum number of entries (default: 20)
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Compare two invocations: diagnostic delta, duration delta, stage delta (I5)
    Diff {
        /// Base invocation ID (default: previous invocation of same command)
        #[arg(long)]
        from: Option<i64>,
        /// Target invocation ID (default: most recent completed invocation)
        #[arg(long)]
        to: Option<i64>,
        /// Restrict diff to a specific command
        #[arg(long)]
        command: Option<String>,
    },
    /// Group invocations into working sessions separated by inactivity gaps (I6)
    Sessions {
        /// Number of sessions to show (default: 10)
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Inactivity gap in minutes that separates sessions (default: 30)
        #[arg(long, default_value = "30")]
        gap_minutes: u32,
    },
    /// Show complete details for a single invocation (I7)
    Invocation {
        /// Invocation selector: `latest`, `previous`, `current`, invocation ID,
        /// `inv:<id>`, or `job:<id>`. Bare numeric IDs resolve as invocation IDs
        /// first; use `job:<id>` for unambiguous background-job lookup.
        id: String,
        /// Include full stage timing and diagnostic details
        #[arg(long)]
        full: bool,
        /// Filter selector resolution to a specific command
        #[arg(long)]
        command: Option<String>,
    },
    /// Seed the history database with synthetic data for exploration (T2)
    ///
    /// Writes realistic-looking invocation history so that `xtask history`
    /// commands show rich output without requiring a real project run history.
    /// The real history DB is written; existing data is preserved.
    ///
    /// The database is marked synthetic — a warning is shown on subsequent reads.
    /// Real runs automatically clear the marker. Use `xtask reset --yes --history`
    /// to wipe and start fresh.
    Seed {
        /// Calendar days of history to generate (default: 30)
        #[arg(long, default_value = "30")]
        days: u32,
        /// Total number of invocations to generate (default: 100)
        #[arg(long, default_value = "100")]
        invocations: u32,
    },
    /// Show live or final progress for an invocation
    Progress {
        /// Invocation selector: `current` (default), `latest`, `previous`,
        /// invocation ID, `inv:<id>`, or `job:<id>`. Bare numeric IDs resolve as
        /// invocation IDs first; use `job:<id>` for unambiguous background-job lookup.
        #[arg(long)]
        invocation: Option<String>,
    },
    /// Show ETA estimates for a command based on recorded phase timings
    Eta {
        /// Command name (e.g. "check", "test", "build")
        command: String,
        /// Phase name (e.g. "compile", "tests"). If omitted, shows all phases.
        #[arg(long)]
        phase: Option<String>,
        /// Number of recent samples to use for the median estimate (default: 20)
        #[arg(long, default_value = "20")]
        window: usize,
    },
    /// Show exercise run history with pass/fail counts and regression detection
    Exercise {
        /// Number of recent exercise runs to show (default: 10)
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Show individual exercise-level results (verbose)
        #[arg(long)]
        verbose: bool,
    },
}

/// History tests subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum HistoryTestsSubcommand {
    Slowest {
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long)]
        invocation: Option<String>,
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
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    /// Comprehensive analysis of the most recent test run
    ///
    /// Shows duration distribution, probable timeouts, and per-package failure summaries.
    Analyze {
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    /// Show captured output for a test (pass or fail)
    Output {
        /// Test name pattern to search for
        pattern: String,
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    Eta,
    /// Full-text search across stored test output (G7)
    Grep {
        /// Text to search for in captured test output
        text: String,
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    /// Per-package pass rate, test count, avg duration, and flaky count (G7)
    ByPackage {
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
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

/// Query build, test, and runtime history recorded by xtask.
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
        use color_eyre::eyre::eyre;
        ctx.try_with_history_db_query(|db| {
            db.warn_if_synthetic(ctx.history_db_path());
            match &self.subcommand {
                HistorySubcommand::List {
                    limit,
                    command,
                    first,
                    no_limit,
                    offset,
                    after_invocation,
                    before_invocation,
                    sort_by,
                    since,
                    with_diagnostics,
                    with_stages,
                    with_tests,
                } => {
                    if *first {
                        execute_last(db, command.as_deref().unwrap_or(""), ctx)
                    } else if *no_limit {
                        execute_export(db, usize::MAX, ctx)
                    } else {
                        execute_list(
                            db,
                            *limit,
                            *offset,
                            command.as_deref(),
                            after_invocation.as_deref(),
                            before_invocation.as_deref(),
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
                        execute_stats_all_packages(db, ctx)
                    } else if *all_commands {
                        execute_stats_all_commands(db, *days, ctx)
                    } else {
                        execute_stats(
                            db,
                            command.as_deref().unwrap_or(""),
                            *days,
                            package.as_deref(),
                            ctx,
                        )
                    }
                }
                HistorySubcommand::Prune { older_than } => execute_prune(db, *older_than, ctx),
                HistorySubcommand::Tests { tests_cmd } => execute_tests(tests_cmd, db, ctx),
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
                    lifecycle,
                    lifecycle_status,
                } => {
                    if *trend {
                        return execute_diagnostics_trend(db, *window, ctx);
                    }
                    if *lifecycle {
                        return execute_diagnostics_lifecycle(
                            db,
                            package.as_deref(),
                            code.as_deref(),
                            level.as_deref(),
                            lifecycle_status.as_deref(),
                            ctx,
                        );
                    }
                    if *delta {
                        return execute_diagnostics_delta(
                            db,
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
                            db,
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
                            db,
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
                            db,
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
                            db,
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
                    db,
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
                } => execute_fix_sessions(db, *sessions, *effectiveness, ctx),
                HistorySubcommand::View { name } => execute_view(db, name.as_deref(), ctx),
                HistorySubcommand::Query { sql } => execute_query(db, sql, ctx),
                HistorySubcommand::Shell => execute_shell(db, ctx),
                HistorySubcommand::Schema => execute_schema(db, ctx),
                HistorySubcommand::Timeline {
                    command,
                    days,
                    limit,
                } => execute_timeline(db, command.as_deref(), *days, *limit, ctx),
                HistorySubcommand::Diff { from, to, command } => {
                    execute_diff(db, *from, *to, command.as_deref(), ctx)
                }
                HistorySubcommand::Sessions { limit, gap_minutes } => {
                    execute_sessions(db, *limit, *gap_minutes, ctx)
                }
                HistorySubcommand::Invocation { id, full, command } => {
                    execute_invocation(db, id, *full, command.as_deref(), ctx)
                }
                HistorySubcommand::Seed { days, invocations } => {
                    execute_seed(db, *days, *invocations, ctx)
                }
                HistorySubcommand::Progress { invocation } => {
                    execute_progress(db, invocation.as_deref(), ctx)
                }
                HistorySubcommand::Eta {
                    command,
                    phase,
                    window,
                } => execute_eta(db, command, phase.as_deref(), *window, ctx),
                HistorySubcommand::Exercise { limit, verbose } => {
                    execute_exercise_history(db, *limit, *verbose, ctx)
                }
            }
        })
        .ok_or_else(|| eyre!("history DB unavailable"))?
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
            .with_history_tracking(false)
            .with_history_access(crate::command::HistoryAccessMode::Query)
    }
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

fn format_history_cutoff_timestamp(
    cutoff: time::OffsetDateTime,
    context: &'static str,
) -> Result<String> {
    cutoff
        .format(&time::format_description::well_known::Rfc3339)
        .wrap_err_with(|| format!("failed to format {context} as RFC3339"))
}

#[allow(clippy::too_many_arguments)]
fn execute_list(
    db: &HistoryDb,
    limit: usize,
    offset: usize,
    command: Option<&str>,
    after_invocation: Option<&str>,
    before_invocation: Option<&str>,
    since: Option<&str>,
    sort_by: &str,
    with_diagnostics: bool,
    with_stages: bool,
    with_tests: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut warnings = Vec::new();

    // Parse --since into an RFC3339 cutoff timestamp
    let since_ts = since
        .and_then(parse_duration_secs)
        .map(|secs| {
            format_history_cutoff_timestamp(
                time::OffsetDateTime::now_utc() - time::Duration::seconds(secs),
                "history --since cutoff",
            )
        })
        .transpose()?;

    let after_id = after_invocation
        .map(|value| db.resolve_invocation_id(value, command))
        .transpose()?
        .flatten();
    let before_id = before_invocation
        .map(|value| db.resolve_invocation_id(value, command))
        .transpose()?
        .flatten();

    let mut query = InvocationQuery::new().limit(limit).offset(offset);
    if let Some(command) = command {
        query = query.command(command);
    }
    if let Some(after_id) = after_id {
        query = query.after_invocation(after_id);
    }
    if let Some(before_id) = before_id {
        query = query.before_invocation(before_id);
    }
    if let Some(since_ts) = since_ts {
        query = query.since_rfc3339(since_ts);
    }
    query = match sort_by {
        "duration" => query.sort_duration(),
        "status" => query.sort_status(),
        _ => query.sort_started(),
    };

    let invocations = query.run(db)?;

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
                        let probe = diagnostic_summary_probe_from_result(
                            inv.id,
                            db.get_diagnostic_counts_for_invocation(inv.id),
                        );
                        if let Some(issue) = probe.issue {
                            warnings.push(issue);
                        }
                        parts.push(probe.fragment);
                    }
                    if with_stages {
                        let probe = stage_summary_probe_from_result(
                            inv.id,
                            db.get_stage_timings_for_invocation(inv.id),
                        );
                        if let Some(issue) = probe.issue {
                            warnings.push(issue);
                        }
                        parts.push(probe.fragment);
                    }
                    if with_tests {
                        let probe = test_summary_probe_from_result(
                            inv.id,
                            db.get_test_counts_for_invocation(inv.id),
                        );
                        if let Some(issue) = probe.issue {
                            warnings.push(issue);
                        }
                        parts.push(probe.fragment);
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

    let mut result = CommandResult::success()
        .with_message(format!("Found {} history entries", invocations.len()))
        .with_duration(ctx.elapsed());
    for warning in warnings {
        result = result.with_warning(warning);
    }
    Ok(result)
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
        HistoryTestsSubcommand::Slowest { limit, invocation } => {
            execute_tests_slowest(db, invocation.as_deref(), *limit, ctx)
        }
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
        HistoryTestsSubcommand::Failures {
            limit,
            output,
            invocation,
        } => execute_tests_failures(db, invocation, *limit, *output, ctx),
        HistoryTestsSubcommand::Analyze { invocation } => {
            execute_tests_analyze(db, invocation, ctx)
        }
        HistoryTestsSubcommand::Output {
            pattern,
            invocation,
        } => execute_tests_output(db, invocation, pattern, ctx),
        HistoryTestsSubcommand::Eta => execute_tests_eta(db, ctx),
        HistoryTestsSubcommand::Grep {
            text,
            limit,
            invocation,
        } => execute_tests_grep(db, invocation, text, *limit, ctx),
        HistoryTestsSubcommand::ByPackage { invocation } => {
            execute_tests_by_package(db, invocation, ctx)
        }
        HistoryTestsSubcommand::DurationP95 { limit } => {
            execute_tests_duration_p95(db, *limit, ctx)
        }
        HistoryTestsSubcommand::Regression { runs } => execute_tests_regression(db, *runs, ctx),
    }
}

fn resolve_selected_test_run(
    db: &HistoryDb,
    invocation: &str,
) -> Result<Option<crate::history::ResolvedTestRun>> {
    db.resolve_test_run(Some(invocation))
}

fn describe_test_run(run: &crate::history::ResolvedTestRun) -> String {
    match run.job_id {
        Some(job_id) => format!("invocation #{} (job #{job_id})", run.invocation_id),
        None => format!("invocation #{}", run.invocation_id),
    }
}

fn execute_tests_slowest(
    db: &HistoryDb,
    invocation: Option<&str>,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if let Some(invocation) = invocation {
        let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
            if ctx.is_human() {
                println!("No test run data found.");
            }
            return Ok(CommandResult::success()
                .with_message("No test run data")
                .with_duration(ctx.elapsed()));
        };
        let tests = db.get_slowest_tests_for_invocation(test_run.invocation_id, limit)?;

        if ctx.is_human() {
            if tests.is_empty() {
                println!(
                    "No test timing rows found for {}.",
                    describe_test_run(&test_run)
                );
            } else {
                println!(
                    "{}, started {}",
                    describe_test_run(&test_run),
                    test_run.started_at
                );
                println!(
                    "{:<50} {:<20} {:<10} {:>10}",
                    "TEST", "PACKAGE", "STATUS", "DURATION"
                );
                for test in &tests {
                    let display_name = if test.test_name.len() > 48 {
                        format!("...{}", &test.test_name[test.test_name.len() - 45..])
                    } else {
                        test.test_name.clone()
                    };
                    println!(
                        "{display_name:<50} {:<20} {:<10} {:>10.3}",
                        test.package, test.status, test.duration_secs
                    );
                }
            }
        } else {
            let json = serde_json::to_string_pretty(&tests)?;
            println!("{json}");
        }

        return Ok(CommandResult::success()
            .with_message(format!(
                "Found {} slowest tests for {}",
                tests.len(),
                describe_test_run(&test_run)
            ))
            .with_data(serde_json::to_value(&tests)?)
            .with_duration(ctx.elapsed()));
    }

    let tests = db.get_slowest_tests(limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No test timing data found.");
        } else {
            println!(
                "{:<50} {:<20} {:>10} {:>6}",
                "TEST", "PACKAGE", "AVG (s)", "RUNS"
            );
            for test in &tests {
                let display_name = if test.test_name.len() > 48 {
                    format!("...{}", &test.test_name[test.test_name.len() - 45..])
                } else {
                    test.test_name.clone()
                };
                println!(
                    "{display_name:<50} {:<20} {:>10.3} {:>6}",
                    test.package, test.avg_duration_secs, test.passing_runs
                );
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} slowest tests", tests.len()))
        .with_data(serde_json::to_value(&tests)?)
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
    invocation: &str,
    limit: usize,
    show_output: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let tests = db.get_failing_tests_with_output(test_run.invocation_id, limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No failing tests in {}.", describe_test_run(&test_run));
        } else {
            println!("{}", describe_test_run(&test_run));
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
                    println!("── {} ({}) ──", test.test_name, test.package);
                    if let Some(output) = &test.output {
                        println!("{output}");
                    }
                    if let Some(nats_ctx) = &test.nats_context {
                        // Pretty-print NATS context if it's valid JSON, else raw
                        let rendered = serde_json::from_str::<serde_json::Value>(nats_ctx)
                            .ok()
                            .and_then(|v| serde_json::to_string_pretty(&v).ok())
                            .unwrap_or_else(|| nats_ctx.clone());
                        println!("  NATS consumer context:");
                        for line in rendered.lines() {
                            println!("    {line}");
                        }
                    }
                    println!();
                }
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Found {} failing tests in {}",
            tests.len(),
            describe_test_run(&test_run)
        ))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_analyze(
    db: &HistoryDb,
    invocation: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let analysis = db.analyze_test_run(test_run.invocation_id)?;

    match analysis {
        None => {
            if ctx.is_human() {
                println!(
                    "No test result rows found for {}.",
                    describe_test_run(&test_run)
                );
            }
            Ok(CommandResult::success()
                .with_message(format!(
                    "No test result rows for {}",
                    describe_test_run(&test_run)
                ))
                .with_duration(ctx.elapsed()))
        }
        Some(analysis) => {
            let infra_probe =
                infra_timing_probe_from_result(db.get_infra_timing_summary(test_run.invocation_id));
            if ctx.is_human() {
                println!("{}", style("━━━ Test Suite Analysis ━━━").bold());
                println!(
                    "{}, started {}",
                    describe_test_run(&test_run),
                    analysis.started_at
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

                if !analysis.slowest_tests.is_empty() {
                    println!("\n{}", style("Slowest Tests:").bold());
                    let mut builder = Builder::new();
                    builder.push_record(["TEST", "PACKAGE", "STATUS", "DURATION"]);
                    for test in &analysis.slowest_tests {
                        let display_name = if test.test_name.len() > 48 {
                            format!("...{}", &test.test_name[test.test_name.len() - 45..])
                        } else {
                            test.test_name.clone()
                        };
                        builder.push_record([
                            display_name,
                            test.package.clone(),
                            test.status.clone(),
                            format!("{:.3}s", test.duration_secs),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
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
                if let Some(infra) = infra_probe.value.as_ref() {
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
                } else if let Some(issue) = infra_probe.issue.as_ref() {
                    println!("\n{}", style("Infrastructure Timing:").cyan().bold());
                    println!("  {}", style(issue).yellow());
                }
            } else {
                let json = serde_json::to_string_pretty(&analysis)?;
                println!("{json}");
            }

            let mut result = CommandResult::success()
                .with_message(format!(
                    "Analysis for {}: {} passed, {} failed",
                    describe_test_run(&test_run),
                    analysis.total_passed,
                    analysis.total_failed
                ))
                .with_data(serde_json::to_value(&analysis)?)
                .with_duration(ctx.elapsed());
            if let Some(issue) = infra_probe.issue {
                result = result.with_warning(issue);
            }
            Ok(result)
        }
    }
}

fn execute_tests_output(
    db: &HistoryDb,
    invocation: &str,
    pattern: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let entries = db.get_test_output(test_run.invocation_id, pattern)?;

    if ctx.is_human() {
        if entries.is_empty() {
            println!(
                "No tests matching '{pattern}' found in {}.",
                describe_test_run(&test_run)
            );
        } else {
            println!("{}", describe_test_run(&test_run));
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
        .with_message(format!(
            "Found {} matching tests in {}",
            entries.len(),
            describe_test_run(&test_run)
        ))
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
    invocation: &str,
    text: &str,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let results = db.search_test_output(test_run.invocation_id, text, limit)?;

    if ctx.is_human() {
        if results.is_empty() {
            println!(
                "No test output matching '{text}' found in {}.",
                describe_test_run(&test_run)
            );
        } else {
            println!("{}", describe_test_run(&test_run));
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
        .with_message(format!(
            "Found {} matching tests in {}",
            results.len(),
            describe_test_run(&test_run)
        ))
        .with_duration(ctx.elapsed()))
}

/// Per-package test stats (G7 --by-package).
fn execute_tests_by_package(
    db: &HistoryDb,
    invocation: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let stats = db.get_tests_by_package(test_run.invocation_id)?;

    if ctx.is_human() {
        if stats.is_empty() {
            println!(
                "No per-package test data found in {}.",
                describe_test_run(&test_run)
            );
        } else {
            println!("{}", describe_test_run(&test_run));
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
        .with_message(format!(
            "Stats for {} packages in {}",
            stats.len(),
            describe_test_run(&test_run)
        ))
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
        let check_last = db.get_last("check")?.map(|inv| inv.id);
        resolve_default_diagnostics_delta_target(
            check_last,
            db.get_last("build").map(|inv| inv.map(|inv| inv.id)),
        )?
    };

    let from_id: i64 = if let Some(id) = delta_from {
        id
    } else {
        // Find the invocation before `to_id` for the same command
        let inv = db.get_recent(50, None)?;
        inv.into_iter()
            .find(|i| {
                i.id < to_id
                    && command.is_none_or(|cmd| i.command == cmd)
                    && matches!(
                        i.status,
                        InvocationStatus::Success | InvocationStatus::Failed
                    )
            })
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

fn resolve_default_diagnostics_delta_target(
    check_last: Option<i64>,
    build_last: Result<Option<i64>>,
) -> Result<i64> {
    if let Some(id) = check_last {
        return Ok(id);
    }

    if let Some(id) = build_last.wrap_err(
        "failed to read most recent build invocation while resolving diagnostics delta target",
    )? {
        return Ok(id);
    }

    Err(color_eyre::eyre::eyre!(
        "No recent check/build invocation found"
    ))
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

// ─── I: Semantic Query Intelligence execute functions ─────────────────────────

/// I3: Diagnostic lifecycle view.
fn execute_diagnostics_lifecycle(
    db: &HistoryDb,
    package: Option<&str>,
    code: Option<&str>,
    level: Option<&str>,
    lifecycle_status: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let entries = db.get_diagnostic_lifecycle(package, code, level, lifecycle_status, 200)?;

    if ctx.is_human() {
        if entries.is_empty() {
            println!("No diagnostic lifecycle data found.");
            println!(
                "  {}",
                style("(Run `xtask check` to populate diagnostic history)").dim()
            );
        } else {
            let mut builder = Builder::new();
            builder.push_record([
                "STATUS",
                "PACKAGE",
                "LEVEL",
                "CODE",
                "OCCURRENCES",
                "MESSAGE",
            ]);
            for e in &entries {
                let status = match e.status {
                    LifecycleStatus::New => style("new".to_string()).green().to_string(),
                    LifecycleStatus::Chronic => style("chronic".to_string()).red().to_string(),
                    LifecycleStatus::Recurring => {
                        style("recurring".to_string()).yellow().to_string()
                    }
                    LifecycleStatus::Resolved => style("resolved".to_string()).dim().to_string(),
                };
                let pkg = e.package.as_deref().unwrap_or("-");
                let code = e.code.as_deref().unwrap_or("-");
                let msg = truncate_message(&e.message, 55);
                builder.push_record([
                    status,
                    pkg.to_string(),
                    e.level.clone(),
                    code.to_string(),
                    e.occurrence_count.to_string(),
                    msg,
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&entries)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("Lifecycle: {} diagnostics", entries.len()))
        .with_duration(ctx.elapsed()))
}

/// I1: Named views dispatch.
fn execute_view(db: &HistoryDb, name: Option<&str>, ctx: &CommandContext) -> Result<CommandResult> {
    struct ViewDef {
        name: &'static str,
        description: &'static str,
    }
    let views = [
        ViewDef {
            name: "fixable-now",
            description: "Auto-fixable diagnostics in current workspace state",
        },
        ViewDef {
            name: "chronic-diagnostics",
            description: "Diagnostics present in 3+ recent invocations",
        },
        ViewDef {
            name: "new-diagnostics",
            description: "Diagnostics appearing for the first time",
        },
        ViewDef {
            name: "resolved-last-run",
            description: "Diagnostics that disappeared in the most recent run",
        },
        ViewDef {
            name: "flaky-tests",
            description: "Tests that have failed and passed across recent runs",
        },
        ViewDef {
            name: "slow-stages",
            description: "Slowest pipeline stages by average duration",
        },
        ViewDef {
            name: "hot-packages",
            description: "Packages with the most current diagnostics",
        },
        ViewDef {
            name: "fix-history",
            description: "Recent fix sessions with before/after counts",
        },
        ViewDef {
            name: "recent-regressions",
            description: "New errors correlated with test failures (last 7d)",
        },
        ViewDef {
            name: "workspace-timeline",
            description: "Chronological view of recent invocations",
        },
        ViewDef {
            name: "build-bottlenecks",
            description: "Pipeline stages contributing most to build time",
        },
    ];

    let Some(name) = name else {
        if ctx.is_human() {
            println!("Available views (use: xtask history view <name>):\n");
            for v in &views {
                println!("  {:30} {}", style(v.name).bold(), v.description);
            }
        } else {
            let json = serde_json::to_string_pretty(
                &views
                    .iter()
                    .map(|v| serde_json::json!({"name": v.name, "description": v.description}))
                    .collect::<Vec<_>>(),
            )?;
            println!("{json}");
        }
        return Ok(CommandResult::success()
            .with_message(format!("{} views available", views.len()))
            .with_duration(ctx.elapsed()));
    };

    match name {
        "fixable-now" => {
            let diags = DiagnosticQuery::new().fixable().current().run(db)?;
            if ctx.is_human() {
                if diags.is_empty() {
                    println!("No fixable diagnostics found.");
                } else {
                    println!("Fixable diagnostics ({}):", diags.len());
                    render_diagnostics_table(&diags, DiagnosticsDisplayMode::Fixable);
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&diags)?);
            }
            Ok(CommandResult::success()
                .with_message(format!("{} fixable diagnostics", diags.len()))
                .with_duration(ctx.elapsed()))
        }
        "chronic-diagnostics" | "new-diagnostics" | "resolved-last-run" => {
            let status = match name {
                "chronic-diagnostics" => "chronic",
                "new-diagnostics" => "new",
                _ => "resolved",
            };
            execute_diagnostics_lifecycle(db, None, None, None, Some(status), ctx)
        }
        "flaky-tests" => {
            let tests = db.get_flaky_tests(20)?;
            if ctx.is_human() {
                if tests.is_empty() {
                    println!("No flaky tests found.");
                } else {
                    let mut builder = Builder::new();
                    builder.push_record(["TEST", "PACKAGE", "INVOCATION"]);
                    for (name, pkg, inv) in &tests {
                        builder.push_record([
                            truncate_message(name, 48),
                            pkg.clone(),
                            inv.to_string(),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&tests)?);
            }
            Ok(CommandResult::success()
                .with_message(format!("{} flaky tests", tests.len()))
                .with_duration(ctx.elapsed()))
        }
        "slow-stages" | "build-bottlenecks" => {
            let stages = db.get_slowest_stages(None, 15)?;
            if ctx.is_human() {
                if stages.is_empty() {
                    println!("No stage timing data found.");
                } else {
                    println!("Slowest pipeline stages:");
                    let mut builder = Builder::new();
                    builder.push_record(["STAGE", "AVG (s)", "MAX (s)", "RUNS"]);
                    for s in &stages {
                        builder.push_record([
                            s.stage_name.clone(),
                            format!("{:.2}", s.avg_duration_secs),
                            format!("{:.2}", s.max_duration_secs),
                            s.run_count.to_string(),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&stages)?);
            }
            Ok(CommandResult::success()
                .with_message(format!("{} stages", stages.len()))
                .with_duration(ctx.elapsed()))
        }
        "hot-packages" => {
            let analysis = HistoryAnalysis::new(db);
            let health = analysis.all_packages_health()?;
            if ctx.is_human() {
                if health.is_empty() {
                    println!("No package diagnostic data found.");
                } else {
                    let mut builder = Builder::new();
                    builder.push_record(["PACKAGE", "DIAGNOSTICS", "FIXABLE", "TEST RATE"]);
                    for h in health.iter().take(20) {
                        let test_rate = h
                            .test_pass_rate
                            .map_or_else(|| "-".into(), |r| format!("{:.0}%", r * 100.0));
                        builder.push_record([
                            h.package.clone(),
                            h.diagnostic_count.to_string(),
                            h.fixable_count.to_string(),
                            test_rate,
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&health)?);
            }
            Ok(CommandResult::success()
                .with_message(format!("{} packages", health.len()))
                .with_duration(ctx.elapsed()))
        }
        "fix-history" => execute_fix_sessions(db, 10, true, ctx),
        "recent-regressions" => {
            let since = time::OffsetDateTime::now_utc() - time::Duration::days(7);
            let analysis = HistoryAnalysis::new(db);
            let regressions = analysis.regression_scan(since)?;
            if ctx.is_human() {
                if regressions.is_empty() {
                    println!("No recent regressions found (last 7 days).");
                } else {
                    println!("Recent regressions ({}):", regressions.len());
                    let mut builder = Builder::new();
                    builder.push_record([
                        "INVOCATION",
                        "PACKAGE",
                        "LEVEL",
                        "TEST FAILURES",
                        "MESSAGE",
                    ]);
                    for r in &regressions {
                        let pkg = r.package.as_deref().unwrap_or("-");
                        builder.push_record([
                            r.invocation_id.to_string(),
                            pkg.to_string(),
                            r.level.clone(),
                            r.test_failures.to_string(),
                            truncate_message(&r.message, 50),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&regressions)?);
            }
            Ok(CommandResult::success()
                .with_message(format!("{} regressions", regressions.len()))
                .with_duration(ctx.elapsed()))
        }
        "workspace-timeline" => execute_timeline(db, None, 7, 20, ctx),
        _ => {
            let names: Vec<&str> = views.iter().map(|v| v.name).collect();
            Err(color_eyre::eyre::eyre!(
                "Unknown view '{name}'. Available: {}",
                names.join(", ")
            ))
        }
    }
}

/// I2: Execute a read-only SQL query and display results.
fn execute_query(db: &HistoryDb, sql: &str, ctx: &CommandContext) -> Result<CommandResult> {
    let rows = db.run_readonly_query(sql).wrap_err("query failed")?;

    if ctx.is_human() {
        if rows.is_empty() {
            println!("(no rows)");
        } else {
            // Extract column names from first row
            let cols: Vec<&str> = rows[0].keys().map(String::as_str).collect();
            let mut builder = Builder::new();
            builder.push_record(cols.clone());
            for row in &rows {
                let record: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        row.get(*c)
                            .map(|v| match v {
                                serde_json::Value::Null => "-".to_string(),
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                builder.push_record(record);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&rows)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("{} rows", rows.len()))
        .with_duration(ctx.elapsed()))
}

/// I2: Open an interactive SQLite shell on the history database.
fn execute_shell(_db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let db_path = ctx.history_db_path();
    if !db_path.exists() {
        return Err(color_eyre::eyre::eyre!(
            "History database not found at {}. Run a command first.",
            db_path.display()
        ));
    }

    // Check sqlite3 is available
    ensure_sqlite3_available(std::process::Command::new("which").arg("sqlite3").output())?;

    if ctx.is_human() {
        println!("Opening history database: {}", db_path.display());
        println!("Type .tables to list tables, .schema <table> for schema, .quit to exit.");
    }

    let status = std::process::Command::new("sqlite3")
        .arg(&db_path)
        .status()
        .wrap_err("failed to launch sqlite3")?;

    let exit_code = status.code().unwrap_or(-1);
    Ok(CommandResult::success()
        .with_message(format!("sqlite3 exited with code {exit_code}"))
        .with_duration(ctx.elapsed()))
}

fn ensure_sqlite3_available(probe: std::io::Result<std::process::Output>) -> Result<()> {
    match probe {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let detail = stderr.trim();
            let suffix = if detail.is_empty() {
                String::new()
            } else {
                format!(" ({detail})")
            };
            Err(color_eyre::eyre::eyre!(
                "sqlite3 is not available on PATH{suffix}. Provide it via the devshell or system configuration"
            ))
        }
        Err(error) => Err(color_eyre::eyre::eyre!(
            "failed to probe sqlite3 availability: {error}"
        )),
    }
}

/// I2: Dump annotated schema CREATE TABLE statements.
fn execute_schema(db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let tables = db.get_schema_dump()?;

    if ctx.is_human() {
        if tables.is_empty() {
            println!("No tables found.");
        } else {
            for (name, sql) in &tables {
                println!("-- Table: {name}");
                println!("{sql};\n");
            }
        }
    } else {
        let json: Vec<_> = tables
            .iter()
            .map(|(n, s)| serde_json::json!({"name": n, "sql": s}))
            .collect();
        println!("{}", serde_json::to_string_pretty(&json)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("{} tables", tables.len()))
        .with_duration(ctx.elapsed()))
}

/// I4: Cross-invocation timeline.
fn execute_timeline(
    db: &HistoryDb,
    command: Option<&str>,
    days: u32,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let entries = db.get_invocation_timeline(command, days, limit)?;

    if ctx.is_human() {
        if entries.is_empty() {
            println!("No invocation history found for the last {days} days.");
        } else {
            render_timeline_table(&entries);
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&entries)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("{} timeline entries ({}d)", entries.len(), days))
        .with_duration(ctx.elapsed()))
}

fn render_timeline_table(entries: &[InvocationTimelineEntry]) {
    let mut builder = Builder::new();
    builder.push_record([
        "ID", "COMMAND", "STATUS", "STARTED", "DURATION", "STAGES", "ERRORS", "WARNS", "ΔDIAG",
    ]);
    for e in entries {
        let status = match e.status {
            InvocationStatus::Success => style("success".to_string()).green().to_string(),
            InvocationStatus::Failed => style("failed".to_string()).red().to_string(),
            InvocationStatus::Cancelled => style("cancelled".to_string()).dim().to_string(),
            InvocationStatus::Running => style("running".to_string()).yellow().to_string(),
        };
        let duration = e
            .duration_secs
            .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
        let delta = if e.diagnostic_delta == 0 {
            "—".to_string()
        } else if e.diagnostic_delta > 0 {
            style(format!("+{}", e.diagnostic_delta)).red().to_string()
        } else {
            style(format!("{}", e.diagnostic_delta)).green().to_string()
        };
        builder.push_record([
            e.id.to_string(),
            e.command.clone(),
            status,
            e.started_at[..16].to_string(), // trim to YYYY-MM-DDTHH:MM
            duration,
            e.stage_count.to_string(),
            e.error_count.to_string(),
            e.warning_count.to_string(),
            delta,
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");
}

/// I5: Compare two invocations.
fn execute_diff(
    db: &HistoryDb,
    from: Option<i64>,
    to: Option<i64>,
    command: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let to_id = match to {
        Some(id) => id,
        None => db
            .resolve_invocation_id("latest", command)?
            .ok_or_else(|| color_eyre::eyre::eyre!("No completed invocations found"))?,
    };
    let from_id = match from {
        Some(id) => id,
        None => db
            .get_previous_invocation_id(to_id, command)?
            .ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "No previous invocation found to diff against. Use --from <id>."
                )
            })?,
    };

    let from_full = db
        .get_invocation_full(from_id)?
        .ok_or_else(|| color_eyre::eyre::eyre!("Invocation #{from_id} not found"))?;
    let to_full = db
        .get_invocation_full(to_id)?
        .ok_or_else(|| color_eyre::eyre::eyre!("Invocation #{to_id} not found"))?;
    let delta = db.get_diagnostic_delta(from_id, to_id)?;

    let from_dur = from_full.invocation.duration_secs.unwrap_or(0.0);
    let to_dur = to_full.invocation.duration_secs.unwrap_or(0.0);

    if ctx.is_human() {
        println!(
            "Diff: #{from_id} ({}) → #{to_id} ({})",
            from_full.invocation.command, to_full.invocation.command
        );
        println!();
        let dur_delta = to_dur - from_dur;
        let dur_style = if dur_delta > 1.0 {
            style(format!("{dur_delta:+.1}s")).red().to_string()
        } else if dur_delta < -1.0 {
            style(format!("{dur_delta:+.1}s")).green().to_string()
        } else {
            format!("{dur_delta:+.1}s")
        };
        println!("  Duration: {from_dur:.1}s → {to_dur:.1}s ({dur_style})");
        println!(
            "  Stages:   {} → {}",
            from_full.stages.len(),
            to_full.stages.len()
        );
        println!(
            "  Diagnostics: {} → {} (new: {}, resolved: {}, persistent: {})",
            from_full.diagnostics.len(),
            to_full.diagnostics.len(),
            style(delta.new.len()).red(),
            style(delta.resolved.len()).green(),
            delta.persistent.len(),
        );

        if !delta.new.is_empty() {
            println!("\n  New diagnostics (+{}):", delta.new.len());
            render_diagnostics_table(&delta.new, DiagnosticsDisplayMode::All);
        }
        if !delta.resolved.is_empty() {
            println!("\n  Resolved diagnostics (-{}):", delta.resolved.len());
            render_diagnostics_table(&delta.resolved, DiagnosticsDisplayMode::All);
        }
    } else {
        let json = serde_json::json!({
            "from": { "id": from_id, "duration_secs": from_dur },
            "to": { "id": to_id, "duration_secs": to_dur },
            "duration_delta_secs": to_dur - from_dur,
            "stage_delta": to_full.stages.len() as i64 - from_full.stages.len() as i64,
            "new_diagnostics": delta.new,
            "resolved_diagnostics": delta.resolved,
            "persistent_diagnostics": delta.persistent,
        });
        println!("{}", serde_json::to_string_pretty(&json)?);
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Diff #{from_id}→#{to_id}: +{} -{}",
            delta.new.len(),
            delta.resolved.len()
        ))
        .with_duration(ctx.elapsed()))
}

/// I6: Working session grouping.
fn execute_sessions(
    db: &HistoryDb,
    limit: usize,
    gap_minutes: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let sessions = db.get_working_sessions(limit, gap_minutes)?;

    if ctx.is_human() {
        if sessions.is_empty() {
            println!("No working sessions found.");
        } else {
            println!(
                "Working sessions (gap > {gap_minutes}min, showing {}):",
                sessions.len()
            );
            let mut builder = Builder::new();
            builder.push_record([
                "#",
                "STARTED",
                "INVOCATIONS",
                "DURATION",
                "SUCCESS",
                "COMMANDS",
            ]);
            for s in &sessions {
                let duration = format!("{:.0}s", s.total_duration_secs);
                let rate = if s.invocation_count > 0 {
                    format!("{}/{} ok", s.success_count, s.invocation_count)
                } else {
                    "-".into()
                };
                let cmds = s.commands.join(", ");
                builder.push_record([
                    s.session_index.to_string(),
                    super::format_display_time_str(&s.first_started),
                    s.invocation_count.to_string(),
                    duration,
                    rate,
                    cmds,
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&sessions)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("{} sessions", sessions.len()))
        .with_duration(ctx.elapsed()))
}

/// I7: Full single-invocation details.
fn execute_invocation(
    db: &HistoryDb,
    id: &str,
    full: bool,
    command: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let inv_id = db
        .resolve_invocation_id(id, command)?
        .ok_or_else(|| color_eyre::eyre::eyre!("No invocation found for '{id}'"))?;

    let inv_full = db
        .get_invocation_full(inv_id)?
        .ok_or_else(|| color_eyre::eyre::eyre!("Invocation #{inv_id} not found"))?;

    if ctx.is_human() {
        let inv = &inv_full.invocation;
        let status_str = match inv.status {
            InvocationStatus::Success => style("success").green().to_string(),
            InvocationStatus::Failed => style("failed").red().to_string(),
            InvocationStatus::Cancelled => style("cancelled").dim().to_string(),
            InvocationStatus::Running => style("running").yellow().to_string(),
        };
        println!("Invocation #{}", inv.id);
        println!("  Command:  {}", inv.command);
        println!("  Status:   {status_str}");
        println!(
            "  Started:  {}",
            super::format_display_time(&inv.started_at)
        );
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
        println!(
            "  Diagnostics: {}E {}W",
            inv_full.error_count, inv_full.warning_count
        );
        println!("  Stages:   {}", inv_full.stages.len());

        if full {
            if !inv_full.stages.is_empty() {
                println!("\n  Stage timings:");
                let mut builder = Builder::new();
                builder.push_record(["STAGE", "DURATION", "OK"]);
                for s in &inv_full.stages {
                    builder.push_record([
                        s.stage_name.clone(),
                        format!("{:.2}s", s.duration_secs),
                        if s.success {
                            "✓".to_string()
                        } else {
                            "✗".to_string()
                        },
                    ]);
                }
                let mut table = builder.build();
                table.with(Style::rounded());
                println!("{table}");
            }

            if !inv_full.diagnostics.is_empty() {
                println!("\n  Diagnostics ({}):", inv_full.diagnostics.len());
                render_diagnostics_table(&inv_full.diagnostics, DiagnosticsDisplayMode::Invocation);
            }
        }
    } else if full {
        println!("{}", serde_json::to_string_pretty(&inv_full)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&inv_full.invocation)?);
    }

    Ok(CommandResult::success()
        .with_message(format!("Invocation #{inv_id}"))
        .with_duration(ctx.elapsed()))
}

fn execute_seed(
    db: &HistoryDb,
    days: u32,
    invocations: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    use crate::history::seed::{SeedOptions, seed_history};

    let opts = SeedOptions { days, invocations };

    if ctx.is_human() {
        println!(
            "Seeding history database with {invocations} synthetic invocations over {days} days…"
        );
    }

    seed_history(db, &opts)?;

    let db_path = ctx.history_db_path();
    if ctx.is_human() {
        println!("  ✓ Done. Database: {}", db_path.display());
        println!("  The database is now marked synthetic.");
        println!("  History commands will warn until real runs replace this data.");
        println!("  To clear: xtask reset --yes --history");
    }

    Ok(CommandResult::success()
        .with_message(format!("Seeded {invocations} invocations over {days} days"))
        .with_duration(ctx.elapsed())
        .with_data(serde_json::json!({
            "days": days,
            "invocations": invocations,
            "db_path": db_path.display().to_string(),
            "synthetic": true,
        })))
}

/// Show live/final progress for an invocation.
fn execute_progress(
    db: &HistoryDb,
    invocation: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let selector = invocation.unwrap_or("current");
    let inv_id = db
        .resolve_invocation_id(selector, None)?
        .ok_or_else(|| color_eyre::eyre::eyre!("No invocation found for selector '{selector}'"))?;

    let progress = db.get_progress(inv_id)?;

    if ctx.is_human() {
        match &progress {
            Some(p) => {
                println!("Progress for invocation #{inv_id}:");
                println!("  Phase:   {}", p.phase.as_deref().unwrap_or("(unknown)"));
                if let Some(step) = &p.step {
                    println!("  Step:    {step}");
                }
                if let Some(pct) = p.pct_done {
                    println!("  Done:    {pct:.1}%");
                }
                if let (Some(done), Some(total)) = (p.items_done, p.items_total) {
                    println!("  Items:   {done}/{total}");
                } else if let Some(done) = p.items_done {
                    println!("  Items:   {done} done");
                }
                println!("  Updated: {}", p.updated_at);
            }
            None => {
                println!("No progress data for invocation #{inv_id}.");
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&progress)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Progress for invocation #{inv_id}"))
        .with_duration(ctx.elapsed()))
}

/// Show ETA estimates for a command based on recorded phase timings.
fn execute_eta(
    db: &HistoryDb,
    command: &str,
    phase: Option<&str>,
    window: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if let Some(phase_name) = phase {
        // Single phase estimate
        let estimate = db.get_eta_estimate(command, phase_name, window)?;
        if ctx.is_human() {
            match estimate {
                Some(secs) => {
                    println!(
                        "ETA for '{command}' phase '{phase_name}': {secs:.1}s  (median of recent samples)"
                    );
                }
                None => {
                    println!(
                        "No ETA for '{command}' phase '{phase_name}' — fewer than 3 samples recorded."
                    );
                }
            }
        } else {
            let json = serde_json::json!({
                "command": command,
                "phase": phase_name,
                "median_secs": estimate,
                "window": window,
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        Ok(CommandResult::success()
            .with_message(format!(
                "ETA for '{command}' phase '{phase_name}': {}",
                estimate.map_or_else(|| "n/a".into(), |s| format!("{s:.1}s"))
            ))
            .with_duration(ctx.elapsed()))
    } else {
        // All phases for command
        let phases = db.get_eta_phases(command)?;
        if ctx.is_human() {
            if phases.is_empty() {
                println!("No ETA samples recorded for command '{command}'.");
                println!(
                    "  {}",
                    style("(ETA data is recorded as commands complete stages)").dim()
                );
            } else {
                println!("ETA estimates for '{command}':");
                let mut builder = Builder::new();
                builder.push_record(["PHASE", "MEDIAN (s)", "SAMPLES"]);
                for (phase_name, median, count) in &phases {
                    let median_str = median.map_or_else(|| "n/a".into(), |s| format!("{s:.1}"));
                    let count_str = if *count < 3 {
                        format!("{count} (need 3+)")
                    } else {
                        count.to_string()
                    };
                    builder.push_record([phase_name.clone(), median_str, count_str]);
                }
                let mut table = builder.build();
                table.with(Style::rounded());
                println!("{table}");
            }
        } else {
            let json: Vec<serde_json::Value> = phases
                .iter()
                .map(|(phase_name, median, count)| {
                    serde_json::json!({
                        "phase": phase_name,
                        "median_secs": median,
                        "sample_count": count,
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "command": command,
                    "phases": json,
                }))?
            );
        }
        Ok(CommandResult::success()
            .with_message(format!("ETA phases for '{command}': {}", phases.len()))
            .with_duration(ctx.elapsed()))
    }
}

fn execute_exercise_history(
    db: &HistoryDb,
    limit: usize,
    verbose: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let rows = db.get_exercise_runs(limit)?;
    let mut warnings = Vec::new();

    if ctx.is_json() {
        let mut json_runs = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut run = serde_json::json!({
                "run_id": row.run_id,
                "invocation_id": row.invocation_id,
                "tier": row.tier,
                "total": row.total,
                "passed": row.passed,
                "failed": row.failed,
                "skipped": row.skipped,
                "duration_secs": row.duration_secs,
                "recorded_at": row.recorded_at,
                "invocation_status": row.invocation_status,
                "git_commit": row.git_commit,
            });
            if verbose {
                let results_probe = exercise_results_probe_from_result(
                    row.run_id,
                    db.get_exercise_results_for_run(row.run_id),
                );
                if let Some(issue) = &results_probe.issue {
                    warnings.push(issue.clone());
                    run["results_issue"] = serde_json::Value::String(issue.clone());
                }
                let results = results_probe
                    .results
                    .into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "exercise_id": r.exercise_id,
                            "tier": r.exercise_tier,
                            "passed": r.passed,
                            "duration_secs": r.duration_secs,
                            "error": r.error,
                            "step_count": r.step_count,
                        })
                    })
                    .collect::<Vec<_>>();
                run["results"] = serde_json::Value::Array(results);
            }
            json_runs.push(run);
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "runs": json_runs }))?
        );
    } else {
        if rows.is_empty() {
            println!("No exercise runs recorded yet. Run `xtask exercise` first.");
            return Ok(CommandResult::success()
                .with_message("no exercise runs found")
                .with_duration(ctx.elapsed()));
        }

        let mut builder = Builder::new();
        builder.push_record(["WHEN", "TIER", "PASS", "FAIL", "SKIP", "DUR", "STATUS"]);

        let mut prev_passed_all = true;
        for (i, row) in rows.iter().enumerate() {
            let tier_str = row.tier.as_deref().unwrap_or("mixed");
            let regression = i > 0 && row.failed > 0 && prev_passed_all;
            let status = if row.failed == 0 {
                style("✓ green").green().to_string()
            } else if regression {
                style("↓ regressed").red().bold().to_string()
            } else {
                style("✗ failing").red().to_string()
            };
            let when: String = row.recorded_at.chars().take(16).collect();
            builder.push_record([
                when,
                tier_str.to_string(),
                row.passed.to_string(),
                row.failed.to_string(),
                row.skipped.to_string(),
                format!("{:.1}s", row.duration_secs),
                status,
            ]);
            prev_passed_all = row.total == row.passed;
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{table}");

        if verbose {
            for row in &rows {
                if row.failed > 0 {
                    println!("\nFailed exercises in run {}:", row.recorded_at);
                    let results_probe = exercise_results_probe_from_result(
                        row.run_id,
                        db.get_exercise_results_for_run(row.run_id),
                    );
                    if let Some(issue) = &results_probe.issue {
                        warnings.push(issue.clone());
                        println!("  {}", style(issue).yellow());
                        continue;
                    }
                    for r in results_probe.results.into_iter().filter(|r| !r.passed) {
                        let err_str = r.error.as_deref().unwrap_or("(no error)");
                        println!("  {} {}: {err_str}", style("✗").red(), r.exercise_id);
                    }
                }
            }
        }
    }

    let mut result = CommandResult::success()
        .with_message(format!("{} exercise run(s) shown", rows.len()))
        .with_duration(ctx.elapsed());
    for warning in warnings {
        result = result.with_warning(warning);
    }
    Ok(result)
}

struct OptionalProbe<T> {
    value: Option<T>,
    issue: Option<String>,
}

struct HistoryListEnrichmentProbe {
    fragment: String,
    issue: Option<String>,
}

fn diagnostic_summary_probe_from_result(
    invocation_id: i64,
    result: Result<crate::history::DiagnosticCounts>,
) -> HistoryListEnrichmentProbe {
    match result {
        Ok(counts) if counts.errors > 0 || counts.warnings > 0 => HistoryListEnrichmentProbe {
            fragment: format!("diag:{}E{}W", counts.errors, counts.warnings),
            issue: None,
        },
        Ok(_) => HistoryListEnrichmentProbe {
            fragment: "diag:ok".to_string(),
            issue: None,
        },
        Err(error) => HistoryListEnrichmentProbe {
            fragment: "diag:ERR".to_string(),
            issue: Some(format!(
                "failed to read diagnostic summary for invocation {invocation_id}: {error:#}"
            )),
        },
    }
}

fn stage_summary_probe_from_result(
    invocation_id: i64,
    result: Result<Vec<crate::history::StageTiming>>,
) -> HistoryListEnrichmentProbe {
    match result {
        Ok(timings) if timings.is_empty() => HistoryListEnrichmentProbe {
            fragment: "stages:-".to_string(),
            issue: None,
        },
        Ok(timings) => {
            let total: f64 = timings.iter().map(|t| t.duration_secs).sum();
            HistoryListEnrichmentProbe {
                fragment: format!("stages:{total:.1}s"),
                issue: None,
            }
        }
        Err(error) => HistoryListEnrichmentProbe {
            fragment: "stages:ERR".to_string(),
            issue: Some(format!(
                "failed to read stage timings for invocation {invocation_id}: {error:#}"
            )),
        },
    }
}

fn test_summary_probe_from_result(
    invocation_id: i64,
    result: Result<(i64, i64, i64)>,
) -> HistoryListEnrichmentProbe {
    match result {
        Ok((passed, failed, _)) if passed > 0 || failed > 0 => HistoryListEnrichmentProbe {
            fragment: format!("tests:{passed}p{failed}f"),
            issue: None,
        },
        Ok(_) => HistoryListEnrichmentProbe {
            fragment: "tests:-".to_string(),
            issue: None,
        },
        Err(error) => HistoryListEnrichmentProbe {
            fragment: "tests:ERR".to_string(),
            issue: Some(format!(
                "failed to read test counts for invocation {invocation_id}: {error:#}"
            )),
        },
    }
}

fn infra_timing_probe_from_result<T>(result: Result<Option<T>>) -> OptionalProbe<T> {
    match result {
        Ok(value) => OptionalProbe { value, issue: None },
        Err(error) => OptionalProbe {
            value: None,
            issue: Some(format!(
                "failed to read infrastructure timing summary: {error:#}"
            )),
        },
    }
}

struct ExerciseResultsProbe {
    results: Vec<ExerciseResultRow>,
    issue: Option<String>,
}

fn exercise_results_probe_from_result(
    run_id: i64,
    result: Result<Vec<ExerciseResultRow>>,
) -> ExerciseResultsProbe {
    match result {
        Ok(results) => ExerciseResultsProbe {
            results,
            issue: None,
        },
        Err(error) => ExerciseResultsProbe {
            results: Vec::new(),
            issue: Some(format!(
                "failed to read exercise results for run {run_id}: {error:#}"
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cargo_diagnostics::CompilerDiagnostic;
    use crate::history::{HistoryDb, TestResult as StoredTestResult, TestStatus};
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::sinex_test;
    use color_eyre::eyre::eyre;
    use std::collections::HashSet;
    use tempfile::tempdir;

    fn silent_ctx() -> CommandContext {
        CommandContext::new(
            OutputWriter::new(OutputFormat::Silent),
            false,
            None,
            "history",
        )
    }

    #[sinex_test]
    async fn test_infra_timing_probe_from_result_reports_errors() -> ::xtask::sandbox::TestResult<()>
    {
        let probe = infra_timing_probe_from_result::<()>(Err(eyre!("infra exploded")));
        assert!(probe.value.is_none());
        assert!(probe.issue.unwrap_or_default().contains("infra exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_exercise_results_probe_from_result_reports_errors()
    -> ::xtask::sandbox::TestResult<()> {
        let probe = exercise_results_probe_from_result(42, Err(eyre!("results exploded")));
        assert!(probe.results.is_empty());
        assert!(probe.issue.unwrap_or_default().contains("results exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_diagnostic_summary_probe_from_result_reports_errors()
    -> ::xtask::sandbox::TestResult<()> {
        let probe = diagnostic_summary_probe_from_result(7, Err(eyre!("diag exploded")));
        assert_eq!(probe.fragment, "diag:ERR");
        assert!(probe.issue.unwrap_or_default().contains("diag exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_stage_summary_probe_from_result_reports_errors()
    -> ::xtask::sandbox::TestResult<()> {
        let probe = stage_summary_probe_from_result(7, Err(eyre!("stages exploded")));
        assert_eq!(probe.fragment, "stages:ERR");
        assert!(probe.issue.unwrap_or_default().contains("stages exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_test_summary_probe_from_result_reports_errors() -> ::xtask::sandbox::TestResult<()>
    {
        let probe = test_summary_probe_from_result(7, Err(eyre!("tests exploded")));
        assert_eq!(probe.fragment, "tests:ERR");
        assert!(probe.issue.unwrap_or_default().contains("tests exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_default_diagnostics_delta_target_reports_build_lookup_errors()
    -> ::xtask::sandbox::TestResult<()> {
        let error = resolve_default_diagnostics_delta_target(None, Err(eyre!("build exploded")))
            .expect_err("build lookup failure should surface");
        let message = format!("{error:#}");
        assert!(message.contains("build exploded"));
        assert!(message.contains("diagnostics delta target"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_sqlite3_available_reports_probe_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let error = ensure_sqlite3_available(Err(std::io::Error::other("probe exploded")))
            .expect_err("probe failure should surface");
        assert!(
            error
                .to_string()
                .contains("failed to probe sqlite3 availability")
        );
        assert!(error.to_string().contains("probe exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_sqlite3_available_reports_missing_sqlite3_honestly()
    -> ::xtask::sandbox::TestResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            let output = std::process::Output {
                status: std::process::ExitStatus::from_raw(256),
                stdout: Vec::new(),
                stderr: b"which: no sqlite3 in PATH".to_vec(),
            };
            let error =
                ensure_sqlite3_available(Ok(output)).expect_err("missing sqlite3 should fail");
            let message = error.to_string();
            assert!(message.contains("sqlite3 is not available on PATH"));
            assert!(message.contains("which: no sqlite3 in PATH"));
        }
        Ok(())
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

    fn store_test_result(
        db: &HistoryDb,
        invocation_id: i64,
        test_name: &str,
        package: &str,
        status: TestStatus,
    ) -> Result<()> {
        db.store_test_results(
            invocation_id,
            &[StoredTestResult {
                test_name: test_name.to_string(),
                package: package.to_string(),
                status,
                duration_secs: Some(0.1),
                attempt: 1,
                output: None,
            }],
        )
        .map(|_| ())
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
                after_invocation: None,
                before_invocation: None,
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
    async fn test_execute_tests_analyze_honors_explicit_invocation()
    -> ::xtask::sandbox::TestResult<()> {
        let db = seeded_history_db("tests-analyze-explicit.db")?;
        let ctx = silent_ctx();

        let older = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(older, InvocationStatus::Success, Some(0), 1.0)?;
        store_test_result(&db, older, "older_pass", "pkg-a", TestStatus::Pass)?;

        let newer = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(newer, InvocationStatus::Success, Some(0), 2.0)?;
        store_test_result(&db, newer, "newer_fail", "pkg-b", TestStatus::Fail)?;

        let result = execute_tests_analyze(&db, &older.to_string(), &ctx)?;
        let data = result.data.expect("analysis data should be present");
        let invocation_id = data
            .get("invocation_id")
            .and_then(serde_json::Value::as_i64)
            .expect("analysis invocation id should be present");
        let passed = data
            .get("total_passed")
            .and_then(serde_json::Value::as_u64)
            .expect("analysis passed count should be present");
        let failed = data
            .get("total_failed")
            .and_then(serde_json::Value::as_u64)
            .expect("analysis failed count should be present");

        assert_eq!(invocation_id, older);
        assert_eq!(passed, 1);
        assert_eq!(failed, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_tests_analyze_accepts_background_job_selector()
    -> ::xtask::sandbox::TestResult<()> {
        let db = seeded_history_db("tests-analyze-job.db")?;
        let ctx = silent_ctx();

        let (invocation_id, job_id) = db.start_background_job(
            "test",
            &[],
            None,
            std::path::Path::new(""),
            std::path::Path::new(""),
        )?;
        db.finish_invocation(invocation_id, InvocationStatus::Success, Some(0), 1.0)?;
        store_test_result(&db, invocation_id, "job_pass", "pkg-job", TestStatus::Pass)?;

        let result = execute_tests_analyze(&db, &format!("job:{job_id}"), &ctx)?;
        let data = result.data.expect("analysis data should be present");
        let resolved_invocation_id = data
            .get("invocation_id")
            .and_then(serde_json::Value::as_i64)
            .expect("analysis invocation id should be present");
        let expected_message =
            format!("Analysis for invocation #{invocation_id} (job #{job_id}): 1 passed, 0 failed");

        assert_eq!(resolved_invocation_id, invocation_id);
        assert_eq!(result.message.as_deref(), Some(expected_message.as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_progress_defaults_to_current_selector() -> ::xtask::sandbox::TestResult<()>
    {
        let db = seeded_history_db("progress-current-selector.db")?;
        let ctx = silent_ctx();

        let older = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(older, InvocationStatus::Success, Some(0), 1.0)?;

        let stdout = std::path::Path::new("");
        let stderr = std::path::Path::new("");
        let (running_invocation, _job_id) =
            db.start_background_job("test", &[], None, stdout, stderr)?;
        db.write_progress(
            running_invocation,
            Some("tests"),
            Some("compiling targeted crates"),
            Some(12.5),
            Some(5),
            Some(40),
        )?;

        let result = execute_progress(&db, None, &ctx)?;
        let expected_message = format!("Progress for invocation #{running_invocation}");

        assert_eq!(result.message.as_deref(), Some(expected_message.as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_tests_slowest_accepts_explicit_invocation()
    -> ::xtask::sandbox::TestResult<()> {
        let db = seeded_history_db("tests-slowest-explicit.db")?;
        let ctx = silent_ctx();

        let older = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(older, InvocationStatus::Success, Some(0), 4.0)?;
        db.store_test_results(
            older,
            &[
                StoredTestResult {
                    test_name: "older_slowest".into(),
                    package: "pkg-a".into(),
                    status: TestStatus::Fail,
                    duration_secs: Some(4.0),
                    attempt: 1,
                    output: Some("boom".into()),
                },
                StoredTestResult {
                    test_name: "older_fast".into(),
                    package: "pkg-a".into(),
                    status: TestStatus::Pass,
                    duration_secs: Some(0.2),
                    attempt: 1,
                    output: None,
                },
            ],
        )?;

        let newer = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(newer, InvocationStatus::Success, Some(0), 20.0)?;
        db.store_test_results(
            newer,
            &[StoredTestResult {
                test_name: "newer_only".into(),
                package: "pkg-b".into(),
                status: TestStatus::Pass,
                duration_secs: Some(20.0),
                attempt: 1,
                output: None,
            }],
        )?;

        let result = execute_tests_slowest(&db, Some(&older.to_string()), 10, &ctx)?;
        let data = result.data.expect("slowest test data should be present");
        let tests = data
            .as_array()
            .expect("run-scoped slowest data should be an array");
        let expected_message = format!("Found 2 slowest tests for invocation #{older}");

        assert_eq!(tests.len(), 2);
        assert_eq!(
            tests[0]
                .get("test_name")
                .and_then(serde_json::Value::as_str),
            Some("older_slowest")
        );
        assert_eq!(
            tests[0].get("status").and_then(serde_json::Value::as_str),
            Some("fail")
        );
        assert_eq!(result.message.as_deref(), Some(expected_message.as_str()));
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

    // ────────────────────────────────────────────────────────────────────────
    // Property tests — parse_duration_secs and apply_diagnostic_filters
    // ────────────────────────────────────────────────────────────────────────

    use crate::sandbox::sinex_proptest;
    use proptest::prelude::*;

    sinex_proptest! {
        /// Larger numeric values with the same unit parse to larger durations (monotonicity).
        ///
        /// This verifies that `parse_duration_secs` acts as a monotone function
        /// within each unit: "30m" > "10m", "2h" > "1h", etc. Violated monotonicity
        /// would cause --since time windows to behave non-intuitively.
        ///
        /// Generates `a = base + delta` (so `a > b = base` by construction), avoiding
        /// prop_assume!-based rejection which causes Reject failures at high rates.
        fn prop_parse_duration_monotonic_within_unit(
            base  in 1i64..=5_000i64,
            delta in 1i64..=5_000i64,
            unit  in prop_oneof![Just('s'), Just('m'), Just('h'), Just('d')]
        ) -> TestResult<()> {
            let a = base + delta;   // a > base = b by construction, no prop_assume! needed
            let b = base;
            let da = parse_duration_secs(&format!("{a}{unit}"))
                .expect("valid format must parse");
            let db = parse_duration_secs(&format!("{b}{unit}"))
                .expect("valid format must parse");
            prop_assert!(da > db, "{a}{unit} must parse to more seconds than {b}{unit}");
            Ok(())
        }

        /// Larger units always produce longer durations for the same multiplier.
        ///
        /// For any positive n: n days > n hours, and n hours > n minutes, etc.
        /// Uses big/small unit partitions that are always ordered (d/h > m/s).
        fn prop_parse_duration_larger_unit_always_bigger(
            n in 1i64..=100i64,
            big_unit   in prop_oneof![Just('d'), Just('h')],
            small_unit in prop_oneof![Just('m'), Just('s')]
        ) -> TestResult<()> {
            let big   = parse_duration_secs(&format!("{n}{big_unit}"))
                .expect("big unit should parse");
            let small = parse_duration_secs(&format!("{n}{small_unit}"))
                .expect("small unit should parse");
            prop_assert!(
                big > small,
                "{n}{big_unit} ({big}s) must be longer than {n}{small_unit} ({small}s)"
            );
            Ok(())
        }

        /// Unknown suffixes return None — no silent misparse.
        ///
        /// The parser must return None for any suffix outside {s, m, h, d}.
        /// Silently parsing an unknown suffix (e.g. treating "100w" as 100)
        /// would corrupt --since time windows.
        fn prop_parse_duration_unknown_suffix_returns_none(
            n in 1i64..=1000i64,
            suffix in prop_oneof![
                Just('x'), Just('y'), Just('z'), Just('w'), Just('q'),
                Just('p'), Just('k'), Just('n'),
            ]
        ) -> TestResult<()> {
            let result = parse_duration_secs(&format!("{n}{suffix}"));
            prop_assert!(
                result.is_none(),
                "suffix '{}' must return None, got {:?}", suffix, result
            );
            Ok(())
        }

        /// All valid formats parse to a positive number of seconds.
        fn prop_parse_duration_valid_inputs_are_positive(
            n in 1i64..=1000i64,
            unit in prop_oneof![Just('s'), Just('m'), Just('h'), Just('d')]
        ) -> TestResult<()> {
            let result = parse_duration_secs(&format!("{n}{unit}"));
            prop_assert!(result.is_some(), "{n}{unit} must parse to Some");
            prop_assert!(result.unwrap() > 0, "parsed duration must be positive");
            Ok(())
        }

        /// Level filter retains exactly the matching diagnostics and drops the rest.
        ///
        /// This verifies AND semantics for the level predicate: every retained
        /// diagnostic must have the exact requested level, and all non-matching
        /// diagnostics are removed.
        fn prop_diagnostic_filter_level_and_semantics(
            matching_count   in 1usize..=8usize,
            unmatching_count in 0usize..=8usize
        ) -> TestResult<()> {
            let target  = "error";
            let other   = "warning";

            let mut diagnostics: Vec<crate::history::StoredDiagnostic> = Vec::new();
            for _ in 0..matching_count {
                diagnostics.push(sample_diagnostic(target, None, None, None, false, None));
            }
            for _ in 0..unmatching_count {
                diagnostics.push(sample_diagnostic(other, None, None, None, false, None));
            }

            apply_diagnostic_filters(
                &mut diagnostics,
                DiagnosticFilter::new(Some(target), None, None, None, None, false),
            );

            prop_assert_eq!(
                diagnostics.len(), matching_count,
                "should retain exactly {} matching-level entries", matching_count
            );
            for d in &diagnostics {
                prop_assert_eq!(&d.level, target, "all retained entries must match level");
            }
            Ok(())
        }

        /// Package filter retains exactly the matching diagnostics.
        fn prop_diagnostic_filter_package_and_semantics(
            count in 1usize..=8usize
        ) -> TestResult<()> {
            let target_pkg = "sinex-db";
            let other_pkg  = "sinex-primitives";

            let mut diagnostics: Vec<crate::history::StoredDiagnostic> = Vec::new();
            for _ in 0..count {
                diagnostics.push(sample_diagnostic("warning", None, Some(target_pkg), None, false, None));
                diagnostics.push(sample_diagnostic("warning", None, Some(other_pkg),  None, false, None));
            }

            apply_diagnostic_filters(
                &mut diagnostics,
                DiagnosticFilter::new(None, None, None, Some(target_pkg), None, false),
            );

            prop_assert_eq!(
                diagnostics.len(), count,
                "should retain exactly {} entries for package '{}'", count, target_pkg
            );
            for d in &diagnostics {
                prop_assert_eq!(
                    d.package.as_deref(), Some(target_pkg),
                    "all retained entries must match package"
                );
            }
            Ok(())
        }

        /// Combined level + package filters use AND logic, not OR.
        ///
        /// Only entries that satisfy BOTH predicates are retained. This rules out
        /// an accidental OR implementation where either match would be sufficient.
        fn prop_diagnostic_filter_combined_and_semantics(
            extra_matches in 0usize..=6usize
        ) -> TestResult<()> {
            let target_level = "error";
            let target_pkg   = "sinex-db";

            // Four categories: match both, match level only, match pkg only, match neither
            let mut diagnostics = vec![
                sample_diagnostic(target_level, None, Some(target_pkg), None, false, None), // MATCH BOTH
                sample_diagnostic(target_level, None, Some("sinex-primitives"), None, false, None), // level only
                sample_diagnostic("warning",    None, Some(target_pkg), None, false, None), // pkg only
                sample_diagnostic("warning",    None, Some("sinex-primitives"), None, false, None), // neither
            ];
            // Add extra_matches fully-matching entries to parameterize the expected count
            for _ in 0..extra_matches {
                diagnostics.push(sample_diagnostic(target_level, None, Some(target_pkg), None, false, None));
            }

            apply_diagnostic_filters(
                &mut diagnostics,
                DiagnosticFilter::new(Some(target_level), None, None, Some(target_pkg), None, false),
            );

            let expected = 1 + extra_matches;
            prop_assert_eq!(
                diagnostics.len(), expected,
                "combined AND filter must retain exactly {} entries", expected
            );
            for d in &diagnostics {
                prop_assert_eq!(&d.level, target_level, "retained entry must match level");
                prop_assert_eq!(
                    d.package.as_deref(), Some(target_pkg),
                    "retained entry must match package"
                );
            }
            Ok(())
        }
    }
}
