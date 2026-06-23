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
use crate::commands::{format_display_time, format_display_time_str};
use crate::history::{
    DiagnosticQuery, ExerciseResultRow, HistoryAnalysis, HistoryDb, InvocationQuery,
    InvocationStatus, InvocationTimelineEntry, LifecycleStatus, ResourceUsage,
};

mod analytics;
mod cost;
mod diagnostics_command;
mod inspect;
mod listing;
mod test_commands;
mod views_command;
mod wrapper_events;
#[cfg(test)]
use analytics::resolve_history_day;
use analytics::{execute_compare_days, execute_explain, execute_overlap, execute_resources};
use cost::execute_cost;
#[cfg(test)]
use diagnostics_command::{
    DiagnosticFilter, apply_diagnostic_filters, diagnostic_source_command_counts,
    format_diagnostic_source_command_counts, resolve_default_diagnostics_delta_target,
};
use diagnostics_command::{
    DiagnosticsDisplayMode, execute_diagnostics_all, execute_diagnostics_by_code,
    execute_diagnostics_current, execute_diagnostics_delta, execute_diagnostics_invocation,
    execute_diagnostics_trend, render_diagnostics_table, truncate_message,
};
#[cfg(test)]
use inspect::ensure_sqlite3_available;
use inspect::{
    execute_diff, execute_eta, execute_exercise_history, execute_invocation, execute_progress,
    execute_query, execute_schema, execute_seed, execute_sessions, execute_shell, execute_timeline,
};
#[cfg(test)]
use listing::parse_duration_secs;
use listing::{
    DiagnosticsFormat, ListFlags, execute_export, execute_last, execute_list, execute_stats,
    execute_stats_all_commands, execute_stats_all_packages, format_history_cutoff_timestamp,
    json_i64, json_optional_f64, json_optional_i64, json_optional_string, json_string,
    parse_history_time, secs_to_hours, sql_string_literal,
};
use test_commands::{HistoryTestsSubcommand, execute_tests};
#[cfg(test)]
use test_commands::{execute_tests_analyze, execute_tests_slowest, infra_timing_probe_from_result};
use views_command::{
    execute_diagnostics_lifecycle, execute_fix_sessions, execute_stages, execute_view,
};
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
