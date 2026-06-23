//! History command - query build/test execution history

use color_eyre::eyre::Result;
use console::style;
use std::collections::BTreeMap;
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::Path;
use std::time::Duration;
use tabled::{builder::Builder, settings::Style};

use color_eyre::eyre::WrapErr;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::history::{
    DiagnosticQuery, ExerciseResultRow, HistoryAnalysis, HistoryDb, InvocationQuery,
    InvocationStatus, InvocationTimelineEntry, LifecycleStatus, ResourceUsage,
};

mod analytics;
mod cost;
mod test_commands;
mod wrapper_events;
#[cfg(test)]
use analytics::resolve_history_day;
use analytics::{execute_compare_days, execute_explain, execute_overlap, execute_resources};
use cost::execute_cost;
use test_commands::{HistoryTestsSubcommand, execute_tests};
#[cfg(test)]
use test_commands::{execute_tests_analyze, execute_tests_slowest, infra_timing_probe_from_result};
use wrapper_events::execute_wrapper_events;
#[cfg(test)]
use wrapper_events::{
    WrapperEvent, WrapperRebuildTrigger, read_wrapper_events, wrapper_events_path,
    wrapper_stage_totals, wrapper_trigger_summary, wrapper_trigger_totals,
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
        /// Include watchdog/stale-pid cancellation rows normally hidden as zombie noise
        #[arg(long)]
        include_zombies: bool,
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
    /// Summarise dev-loop wallclock cost without double-counting wrappers
    Cost {
        /// Commands to include. Defaults to check+test.
        #[arg(long = "command")]
        commands: Vec<String>,
        /// How many days back to analyse
        #[arg(long, default_value = "7")]
        days: u32,
    },
    /// Show pre-exec devshell wrapper rebuild events
    WrapperEvents {
        /// How many days back to analyse.
        #[arg(long, default_value = "7")]
        days: u32,
        /// Maximum number of events to show.
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Compare command duration and pressure between two calendar days
    CompareDays {
        /// Day to inspect: YYYY-MM-DD, today, or yesterday. Defaults to today in UTC.
        #[arg(long)]
        day: Option<String>,
        /// Baseline day: YYYY-MM-DD, today, or yesterday. Defaults to the previous UTC day.
        #[arg(long)]
        against: Option<String>,
        /// Commands to include. Can be repeated or comma-separated.
        #[arg(long = "command", value_delimiter = ',')]
        commands: Vec<String>,
        /// Number of slowest invocations from the inspected day to include.
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Include failed invocations in addition to successful invocations.
        #[arg(long)]
        include_failures: bool,
    },
    /// Explain build/test runtime for a day using xtask history facts.
    Explain {
        /// Day to inspect: YYYY-MM-DD, today, or yesterday. Defaults to today in UTC.
        #[arg(long)]
        day: Option<String>,
        /// Baseline day: YYYY-MM-DD, today, or yesterday. Defaults to the previous UTC day.
        #[arg(long)]
        against: Option<String>,
        /// Commands to include. Defaults to check,test,build.
        #[arg(long = "command", value_delimiter = ',')]
        commands: Vec<String>,
        /// Number of slowest invocations/test-overhead rows to include.
        #[arg(long, default_value = "8")]
        limit: usize,
        /// Include failed invocations in addition to successful invocations.
        #[arg(long)]
        include_failures: bool,
    },
    /// Aggregate recorded resource pressure and block I/O by command/window.
    Resources {
        /// Exact UTC calendar day to inspect, in YYYY-MM-DD.
        #[arg(long, conflicts_with = "days")]
        day: Option<String>,
        /// Rolling window in days when --day is omitted.
        #[arg(long, default_value = "1")]
        days: u32,
        /// Commands to include. Can be repeated or comma-separated.
        #[arg(long = "command", value_delimiter = ',')]
        commands: Vec<String>,
        /// Number of slowest/high-pressure invocations to include.
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Include background invocations in addition to foreground work.
        #[arg(long)]
        include_background: bool,
        /// Restrict to successful invocations only.
        #[arg(long)]
        success_only: bool,
    },
    /// Explain what overlapped an invocation and what shared resources were recorded.
    Overlap {
        /// Invocation selector: `latest`, `previous`, `current`, invocation ID,
        /// `inv:<id>`, or `job:<id>`.
        #[arg(default_value = "latest")]
        invocation: String,
        /// Restrict selector resolution to this command.
        #[arg(long)]
        command: Option<String>,
        /// Number of overlapping invocations/background jobs to include.
        #[arg(long, default_value = "20")]
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
        /// Include watchdog/stale-pid cancellation rows normally hidden as zombie noise
        #[arg(long)]
        include_zombies: bool,
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
        /// Include watchdog/stale-pid cancellation rows normally hidden as zombie noise
        #[arg(long)]
        include_zombies: bool,
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
                    include_zombies,
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
                            ListFlags {
                                with_diagnostics: *with_diagnostics,
                                with_stages: *with_stages,
                                with_tests: *with_tests,
                                include_zombies: *include_zombies,
                            },
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
                HistorySubcommand::Cost { commands, days } => {
                    execute_cost(db, commands, *days, ctx)
                }
                HistorySubcommand::WrapperEvents { days, limit } => {
                    execute_wrapper_events(*days, *limit, ctx)
                }
                HistorySubcommand::CompareDays {
                    day,
                    against,
                    commands,
                    limit,
                    include_failures,
                } => execute_compare_days(
                    db,
                    day.as_deref(),
                    against.as_deref(),
                    commands,
                    *limit,
                    *include_failures,
                    ctx,
                ),
                HistorySubcommand::Explain {
                    day,
                    against,
                    commands,
                    limit,
                    include_failures,
                } => execute_explain(
                    db,
                    day.as_deref(),
                    against.as_deref(),
                    commands,
                    *limit,
                    *include_failures,
                    ctx,
                ),
                HistorySubcommand::Resources {
                    day,
                    days,
                    commands,
                    limit,
                    include_background,
                    success_only,
                } => execute_resources(
                    db,
                    day.as_deref(),
                    *days,
                    commands,
                    *limit,
                    *include_background,
                    *success_only,
                    ctx,
                ),
                HistorySubcommand::Overlap {
                    invocation,
                    command,
                    limit,
                } => execute_overlap(db, invocation, command.as_deref(), *limit, ctx),
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
                    include_zombies,
                } => execute_timeline(db, command.as_deref(), *days, *limit, *include_zombies, ctx),
                HistorySubcommand::Diff { from, to, command } => {
                    execute_diff(db, *from, *to, command.as_deref(), ctx)
                }
                HistorySubcommand::Sessions {
                    limit,
                    gap_minutes,
                    include_zombies,
                } => execute_sessions(db, *limit, *gap_minutes, *include_zombies, ctx),
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

#[derive(Clone, Copy)]
struct ListFlags {
    with_diagnostics: bool,
    with_stages: bool,
    with_tests: bool,
    include_zombies: bool,
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
    flags: ListFlags,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let ListFlags {
        with_diagnostics,
        with_stages,
        with_tests,
        include_zombies,
    } = flags;
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
        .map(|value| {
            db.resolve_invocation_id(value, command)?.ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "--after-invocation '{}' did not match any recorded invocation",
                    value
                )
            })
        })
        .transpose()?;
    let before_id = before_invocation
        .map(|value| {
            db.resolve_invocation_id(value, command)?.ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "--before-invocation '{}' did not match any recorded invocation",
                    value
                )
            })
        })
        .transpose()?;

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
    if include_zombies {
        query = query.include_zombies();
    }

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
        ctx.print_json(&invocations)?;
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
        ctx.print_json(&inv)?;
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
        ctx.print_json(&stats)?;
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
        ctx.print_json(&health)?;
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
        ctx.print_json(
            &all_stats
                .iter()
                .map(|(cmd, s)| serde_json::json!({"command": cmd, "stats": s}))
                .collect::<Vec<_>>(),
        )?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Stats for {} commands", all_stats.len()))
        .with_duration(ctx.elapsed()))
}

pub(super) fn parse_history_time(value: &str, field: &'static str) -> Result<time::OffsetDateTime> {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .wrap_err_with(|| format!("failed to parse history {field}: {value}"))
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, field: &'static str) -> Result<i64> {
    row.get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| color_eyre::eyre::eyre!("history cost row missing integer field {field}"))
}

fn json_string(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<String> {
    row.get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| color_eyre::eyre::eyre!("history cost row missing string field {field}"))
}

fn json_optional_string(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Option<String> {
    row.get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn json_optional_f64(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Option<f64> {
    row.get(field).and_then(serde_json::Value::as_f64)
}

fn json_optional_i64(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Option<i64> {
    row.get(field).and_then(serde_json::Value::as_i64)
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn secs_to_hours(secs: f64) -> f64 {
    let hours = secs / 3600.0;
    if hours.abs() < 0.000_001 { 0.0 } else { hours }
}

fn execute_export(db: &HistoryDb, limit: usize, ctx: &CommandContext) -> Result<CommandResult> {
    let invocations = db.get_recent(limit, None)?;
    ctx.print_json(&invocations)?;

    Ok(CommandResult::success()
        .with_message(format!("Exported {} entries", invocations.len()))
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

fn retain_existing_file_diagnostics(diagnostics: &mut Vec<crate::history::StoredDiagnostic>) {
    let workspace_root = crate::config::workspace_root();
    diagnostics.retain(|diagnostic| diagnostic.points_to_existing_file(&workspace_root));
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

fn format_source_with_authority(diagnostic: &crate::history::StoredDiagnostic) -> String {
    let source = format_source_short(&diagnostic.source_command, &diagnostic.source_time);
    if diagnostic.authority == "proof" {
        source
    } else {
        format!("{source}/{}", diagnostic.authority)
    }
}

fn diagnostic_source_command_counts(
    diagnostics: &[crate::history::StoredDiagnostic],
) -> Vec<(String, usize)> {
    let mut counts = BTreeMap::<String, usize>::new();
    for diagnostic in diagnostics {
        let command = diagnostic
            .source_command
            .as_deref()
            .unwrap_or("unknown")
            .to_string();
        *counts.entry(command).or_default() += 1;
    }

    let mut counts: Vec<_> = counts.into_iter().collect();
    counts.sort_by(|(left_command, left_count), (right_command, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_command.cmp(right_command))
    });
    counts
}

fn format_diagnostic_source_command_counts(
    diagnostics: &[crate::history::StoredDiagnostic],
) -> String {
    diagnostic_source_command_counts(diagnostics)
        .into_iter()
        .map(|(command, count)| format!("{command}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Render diagnostics table with mode-specific columns.
#[allow(
    clippy::needless_pass_by_value,
    reason = "DiagnosticsDisplayMode is Copy"
)]
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
                let source = format_source_with_authority(diag);
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
            builder.push_record(["LEVEL", "PACKAGE", "CODE", "FILE", "MESSAGE", "SOURCE"]);
            for diag in diagnostics {
                let code = diag.code.as_deref().unwrap_or("-");
                let file_loc = format_file_loc(&diag.file_path, diag.line);
                let package = diag.package.as_deref().unwrap_or("-");
                let message = truncate_message(&diag.message, 55);
                let source = format_source_with_authority(diag);
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
    retain_existing_file_diagnostics(&mut diagnostics);
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
            if command.is_none() && diagnostic_source_command_counts(&diagnostics).len() > 1 {
                println!(
                    "Sources: {}",
                    format_diagnostic_source_command_counts(&diagnostics)
                );
                println!(
                    "  {}",
                    style(
                        "(Use `xtask history diagnostics --command check` to isolate the normal check surface.)"
                    )
                    .dim()
                );
            }
        }
    } else {
        ctx.print_json(&diagnostics)?;
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
        ctx.print_json(&diagnostics)?;
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
        ctx.print_json(&diagnostics)?;
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
        ctx.print_json(&json_output)?;
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
        ctx.print_json(&delta)?;
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
    retain_existing_file_diagnostics(&mut diagnostics);
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
        ctx.print_json(&grouped)?;
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
            ctx.print_json(&points)?;
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
            ctx.print_json(&timings)?;
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
            builder.push_record(["STAGE", "AVG (s)", "TAIL (s)", "RUNS"]);
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
        ctx.print_json(&stats)?;
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
        ctx.print_json(&fix_sessions)?;
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
        ctx.print_json(&entries)?;
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
            name: "drift-guard-bypasses",
            description: "Recent pre-push drift-guard bypasses (security/hygiene audit trail)",
        },
        ViewDef {
            name: "impact-audit",
            description: "Recent impact-plan audit runs (skip-accuracy / false-negative evidence)",
        },
        ViewDef {
            name: "traces",
            description: "Most recent internal trace events",
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
            ctx.print_json(
                &views
                    .iter()
                    .map(|v| serde_json::json!({"name": v.name, "description": v.description}))
                    .collect::<Vec<_>>(),
            )?;
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
                ctx.print_json(&diags)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} fixable diagnostics", diags.len()))
                .with_duration(ctx.elapsed()))
        }
        "drift-guard-bypasses" => {
            let rows = db.get_drift_guard_bypasses(20)?;
            if ctx.is_human() {
                if rows.is_empty() {
                    println!("No drift-guard bypasses recorded.");
                } else {
                    println!("Drift-guard bypasses ({}):", rows.len());
                    let mut builder = Builder::new();
                    builder.push_record(["RECORDED", "BRANCH", "HEAD", "PUSH_OK"]);
                    for r in &rows {
                        builder.push_record([
                            r.recorded_at.clone(),
                            r.git_branch.clone().unwrap_or_else(|| "-".to_string()),
                            r.head_sha
                                .as_deref()
                                .map_or_else(|| "-".to_string(), |s| truncate_message(s, 12)),
                            r.push_succeeded
                                .map_or_else(|| "-".to_string(), |b| b.to_string()),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                ctx.print_json(&rows)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} drift-guard bypasses", rows.len()))
                .with_duration(ctx.elapsed()))
        }
        "impact-audit" => {
            let rows = db.get_impact_audit_runs(20)?;
            if ctx.is_human() {
                if rows.is_empty() {
                    println!("No impact-plan audit runs recorded.");
                } else {
                    println!("Impact-plan audit runs ({}):", rows.len());
                    let mut builder = Builder::new();
                    builder.push_record(["CREATED", "STATUS", "SAMPLE", "FALSE_NEG"]);
                    for r in &rows {
                        builder.push_record([
                            r.created_at.clone(),
                            r.status.clone(),
                            r.sample_size.to_string(),
                            r.false_negative_count.to_string(),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                ctx.print_json(&rows)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} impact audit runs", rows.len()))
                .with_duration(ctx.elapsed()))
        }
        "traces" => {
            let rows = db.get_recent_trace_events(50)?;
            if ctx.is_human() {
                if rows.is_empty() {
                    println!("No trace events recorded.");
                } else {
                    println!("Recent trace events ({}):", rows.len());
                    let mut builder = Builder::new();
                    builder.push_record(["TS", "LEVEL", "TARGET", "MESSAGE"]);
                    for r in &rows {
                        builder.push_record([
                            r.ts.clone(),
                            r.level.clone(),
                            truncate_message(&r.target, 28),
                            truncate_message(&r.message, 60),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                ctx.print_json(&rows)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} trace events", rows.len()))
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
                ctx.print_json(&tests)?;
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
                    builder.push_record(["STAGE", "AVG (s)", "TAIL (s)", "RUNS"]);
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
                ctx.print_json(&stages)?;
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
                ctx.print_json(&health)?;
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
                ctx.print_json(&regressions)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} regressions", regressions.len()))
                .with_duration(ctx.elapsed()))
        }
        "workspace-timeline" => execute_timeline(db, None, 7, 20, false, ctx),
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
        ctx.print_json(&rows)?;
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
        .arg(db_path)
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
        ctx.print_json(&json)?;
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
    include_zombies: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let entries = db.get_invocation_timeline_with_zombies(command, days, limit, include_zombies)?;

    if ctx.is_human() {
        if entries.is_empty() {
            println!("No invocation history found for the last {days} days.");
        } else {
            render_timeline_table(&entries);
        }
    } else {
        ctx.print_json(&entries)?;
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
        let delta = match e.diagnostic_delta.cmp(&0) {
            std::cmp::Ordering::Equal => "—".to_string(),
            std::cmp::Ordering::Greater => {
                style(format!("+{}", e.diagnostic_delta)).red().to_string()
            }
            std::cmp::Ordering::Less => {
                style(format!("{}", e.diagnostic_delta)).green().to_string()
            }
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
        ctx.print_json(&json)?;
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
    include_zombies: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let sessions = db.get_working_sessions_with_zombies(limit, gap_minutes, include_zombies)?;

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
        ctx.print_json(&sessions)?;
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
    let resource_usage = match db.get_resource_usage_for_invocation(inv_id)? {
        Some(usage) => Some(usage),
        None if matches!(inv_full.invocation.status, InvocationStatus::Running) => db
            .get_running_job_pid_for_invocation(inv_id)?
            .and_then(|pid| live_resource_usage_for_invocation(&inv_full.invocation, pid)),
        None => None,
    };

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
        if let Some(resources) = &resource_usage {
            println!("  Resources: {}", format_resource_usage(resources));
        }

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
        ctx.print_json(&serde_json::json!({
            "invocation": inv_full.invocation,
            "stages": inv_full.stages,
            "diagnostics": inv_full.diagnostics,
            "error_count": inv_full.error_count,
            "warning_count": inv_full.warning_count,
            "resource_usage": resource_usage,
        }))?;
    } else {
        ctx.print_json(&inv_full.invocation)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Invocation #{inv_id}"))
        .with_duration(ctx.elapsed()))
}

fn live_resource_usage_for_invocation(
    invocation: &crate::history::Invocation,
    pid: u32,
) -> Option<ResourceUsage> {
    let process_metrics =
        crate::process::probe_process_tree_metrics(pid, Duration::from_millis(120));
    let shared_build_metrics =
        crate::process::probe_shared_build_metrics(Duration::from_millis(120));

    match (process_metrics, shared_build_metrics) {
        (Some(metrics), shared_build_metrics) => Some(ResourceUsage {
            command: invocation.command.clone(),
            status: invocation.status.as_str().to_string(),
            started_at: invocation.started_at.to_string(),
            duration_secs: Some(
                (time::OffsetDateTime::now_utc() - invocation.started_at).as_seconds_f64(),
            ),
            process_cpu_usage_avg: metrics.cpu_usage_avg,
            process_memory_usage_max_mb: metrics.memory_usage_max_mb,
            root_process_cpu_usage_avg: metrics.root_cpu_usage_avg,
            root_process_memory_usage_max_mb: metrics.root_memory_usage_max_mb,
            shared_nix_daemon_cpu_usage_avg: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_nix_daemon_cpu_usage_avg),
            shared_nix_daemon_memory_usage_max_mb: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_nix_daemon_memory_usage_max_mb),
            shared_nix_build_slice_cpu_usage_avg: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_nix_build_slice_cpu_usage_avg),
            shared_nix_build_slice_memory_usage_max_mb: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_nix_build_slice_memory_usage_max_mb),
            shared_background_slice_cpu_usage_avg: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_background_slice_cpu_usage_avg),
            shared_background_slice_memory_usage_max_mb: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_background_slice_memory_usage_max_mb),
            process_count_max: metrics.process_count_max,
            sample_count: Some(metrics.sample_count),
            host_cpu_usage_avg: None,
            host_memory_usage_max_mb: None,
            host_cpu_pressure_some_avg10_max: None,
            host_io_pressure_some_avg10_max: None,
            host_io_pressure_full_avg10_max: None,
            host_memory_pressure_some_avg10_max: None,
            host_memory_pressure_full_avg10_max: None,
            host_block_read_mib_delta: None,
            host_block_write_mib_delta: None,
            host_block_read_iops_avg: None,
            host_block_write_iops_avg: None,
            host_block_busiest_device: None,
            host_block_busiest_device_total_mib_delta: None,
            host_block_busiest_device_read_iops_avg: None,
            host_block_busiest_device_write_iops_avg: None,
            host_block_busiest_device_weighted_io_ms_per_s: None,
            shm_free_min_mb: None,
            shm_used_max_mb: None,
        }),
        (None, Some(shared_build_metrics)) => Some(ResourceUsage {
            command: invocation.command.clone(),
            status: invocation.status.as_str().to_string(),
            started_at: invocation.started_at.to_string(),
            duration_secs: Some(
                (time::OffsetDateTime::now_utc() - invocation.started_at).as_seconds_f64(),
            ),
            process_cpu_usage_avg: None,
            process_memory_usage_max_mb: None,
            root_process_cpu_usage_avg: None,
            root_process_memory_usage_max_mb: None,
            shared_nix_daemon_cpu_usage_avg: shared_build_metrics.shared_nix_daemon_cpu_usage_avg,
            shared_nix_daemon_memory_usage_max_mb: shared_build_metrics
                .shared_nix_daemon_memory_usage_max_mb,
            shared_nix_build_slice_cpu_usage_avg: shared_build_metrics
                .shared_nix_build_slice_cpu_usage_avg,
            shared_nix_build_slice_memory_usage_max_mb: shared_build_metrics
                .shared_nix_build_slice_memory_usage_max_mb,
            shared_background_slice_cpu_usage_avg: shared_build_metrics
                .shared_background_slice_cpu_usage_avg,
            shared_background_slice_memory_usage_max_mb: shared_build_metrics
                .shared_background_slice_memory_usage_max_mb,
            process_count_max: None,
            sample_count: None,
            host_cpu_usage_avg: None,
            host_memory_usage_max_mb: None,
            host_cpu_pressure_some_avg10_max: None,
            host_io_pressure_some_avg10_max: None,
            host_io_pressure_full_avg10_max: None,
            host_memory_pressure_some_avg10_max: None,
            host_memory_pressure_full_avg10_max: None,
            host_block_read_mib_delta: None,
            host_block_write_mib_delta: None,
            host_block_read_iops_avg: None,
            host_block_write_iops_avg: None,
            host_block_busiest_device: None,
            host_block_busiest_device_total_mib_delta: None,
            host_block_busiest_device_read_iops_avg: None,
            host_block_busiest_device_write_iops_avg: None,
            host_block_busiest_device_weighted_io_ms_per_s: None,
            shm_free_min_mb: None,
            shm_used_max_mb: None,
        }),
        (None, None) => None,
    }
}

fn format_resource_usage(usage: &ResourceUsage) -> String {
    let cpu = if let Some(value) = usage.process_cpu_usage_avg {
        format!("{value:.1}% tree cpu")
    } else if let Some(value) = usage.host_cpu_usage_avg {
        format!("{value:.1}% host cpu (legacy)")
    } else {
        "cpu n/a".to_string()
    };
    let memory = if let Some(value) = usage.process_memory_usage_max_mb {
        format!("{value:.0} MB tree mem")
    } else if let Some(value) = usage.host_memory_usage_max_mb {
        format!("{value:.0} MB host mem (legacy)")
    } else {
        "mem n/a".to_string()
    };
    let process_count = usage.process_count_max.map_or_else(
        || "proc n/a".to_string(),
        |count| format!("max {count} proc"),
    );
    let root_cpu = usage.root_process_cpu_usage_avg.map_or_else(
        || "xtask cpu n/a".to_string(),
        |value| format!("{value:.1}% xtask cpu"),
    );
    let root_mem = usage.root_process_memory_usage_max_mb.map_or_else(
        || "xtask mem n/a".to_string(),
        |value| format!("{value:.0} MB xtask mem"),
    );
    let samples = usage.sample_count.map_or_else(
        || "samples n/a".to_string(),
        |count| format!("{count} samples"),
    );
    let mut parts = vec![cpu, memory];
    if let Some(cpu) = usage.shared_nix_daemon_cpu_usage_avg {
        parts.push(format!("{cpu:.1}% nix-daemon shared cpu"));
    }
    if let Some(memory) = usage.shared_nix_daemon_memory_usage_max_mb {
        parts.push(format!("{memory:.0} MB nix-daemon shared mem"));
    }
    if let Some(cpu) = usage.shared_nix_build_slice_cpu_usage_avg {
        parts.push(format!("{cpu:.1}% nix-build shared cpu"));
    }
    if let Some(memory) = usage.shared_nix_build_slice_memory_usage_max_mb {
        parts.push(format!("{memory:.0} MB nix-build shared mem"));
    }
    if let Some(cpu) = usage.shared_background_slice_cpu_usage_avg {
        parts.push(format!("{cpu:.1}% background shared cpu"));
    }
    if let Some(memory) = usage.shared_background_slice_memory_usage_max_mb {
        parts.push(format!("{memory:.0} MB background shared mem"));
    }
    parts.extend([process_count, root_cpu, root_mem, samples]);
    parts.join(", ")
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
        ctx.print_json(&progress)?;
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
            ctx.print_json(&json)?;
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
            ctx.print_json(&serde_json::json!({
                "command": command,
                "phases": json,
            }))?;
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
        ctx.print_json(&serde_json::json!({ "runs": json_runs }))?;
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
mod tests;
