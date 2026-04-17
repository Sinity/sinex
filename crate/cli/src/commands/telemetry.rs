use clap::Subcommand;
use console::style;
use sinex_primitives::rpc::telemetry::{
    CommandFrequencyEntry, FileActivityEntry, IngestdValidationSnapshot, RecentActivityEntry,
    SystemStateBucket, WindowFocusBucket,
};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

/// Telemetry data from event-time activity views and operator read models
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Window focus aggregates for the last 6 hours
    sinexctl telemetry window-focus --from 6h

    # Top commands by frequency over the past day
    sinexctl telemetry command-frequency --from 24h --limit 20

    # File activity for a specific date range (RFC3339)
    sinexctl telemetry file-activity --from 2026-03-01T00:00:00Z --to 2026-03-22T00:00:00Z

    # Recent activity summary (hardcoded lookback in view)
    sinexctl telemetry recent-activity

    # System state as JSON for piping
    sinexctl telemetry system-state --from 1h -f json

    # Latest ingestd validation snapshot
    sinexctl telemetry ingestd-validation
")]
pub enum TelemetryCommands {
    /// Window focus aggregates (5-minute buckets)
    WindowFocus {
        /// Start of time range: relative (1h, 6h, 2d) or RFC3339
        #[arg(long)]
        from: Option<String>,

        /// End of time range (default: now)
        #[arg(long)]
        to: Option<String>,

        /// Maximum number of buckets to return (default: 50)
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Command frequency aggregates (hourly buckets)
    CommandFrequency {
        /// Start of time range: relative (1h, 6h, 2d) or RFC3339
        #[arg(long)]
        from: Option<String>,

        /// End of time range (default: now)
        #[arg(long)]
        to: Option<String>,

        /// Maximum number of entries to return (default: 50)
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// File activity aggregates (hourly buckets, per directory)
    FileActivity {
        /// Start of time range: relative (1h, 6h, 2d) or RFC3339
        #[arg(long)]
        from: Option<String>,

        /// End of time range (default: now)
        #[arg(long)]
        to: Option<String>,

        /// Maximum number of entries to return (default: 50)
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Recent activity summary (hardcoded lookback window)
    RecentActivity {
        /// Maximum number of entries to return (default: 50)
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// System state aggregates (5-minute buckets: CPU, memory, disk I/O)
    SystemState {
        /// Start of time range: relative (1h, 6h, 2d) or RFC3339
        #[arg(long)]
        from: Option<String>,

        /// End of time range (default: now)
        #[arg(long)]
        to: Option<String>,

        /// Maximum number of buckets to return (default: 50)
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Latest ingestd validation and plausibility snapshot
    IngestdValidation {
        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },
}

impl TelemetryCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::WindowFocus {
                from,
                to,
                limit,
                format,
            } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let buckets = client
                    .telemetry_window_focus(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    buckets,
                    "No window-focus data found.",
                    format_window_focus_table,
                )
                .display(format)?;
            }

            Self::CommandFrequency {
                from,
                to,
                limit,
                format,
            } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let entries = client
                    .telemetry_command_frequency(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    entries,
                    "No command-frequency data found.",
                    format_command_frequency_table,
                )
                .display(format)?;
            }

            Self::FileActivity {
                from,
                to,
                limit,
                format,
            } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let entries = client
                    .telemetry_file_activity(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    entries,
                    "No file-activity data found.",
                    format_file_activity_table,
                )
                .display(format)?;
            }

            Self::RecentActivity { limit, format } => {
                let entries = client.telemetry_recent_activity(Some(*limit)).await?;
                CommandOutput::list(
                    entries,
                    "No recent activity found.",
                    format_recent_activity_table,
                )
                .display(format)?;
            }

            Self::SystemState {
                from,
                to,
                limit,
                format,
            } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let buckets = client
                    .telemetry_system_state(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    buckets,
                    "No system-state data found.",
                    format_system_state_table,
                )
                .display(format)?;
            }

            Self::IngestdValidation { format } => {
                match client.telemetry_ingestd_validation().await? {
                    Some(snapshot) => {
                        CommandOutput::single(snapshot, format_ingestd_validation_table)
                            .display(format)?;
                    }
                    None => CommandOutput::<IngestdValidationSnapshot>::empty(
                        "No ingestd-validation data found.",
                    )
                    .display(format)?,
                }
            }
        }
        Ok(())
    }
}

/// Resolve a time argument to an RFC3339 string.
///
/// Accepts:
/// - Relative: `1h`, `6h`, `2d`, `30m`
/// - Absolute RFC3339: `2026-03-17T00:00:00Z`
/// - Date-only: `2026-03-17`
fn resolve_time_arg(s: &str) -> Result<String> {
    use sinex_primitives::temporal::Timestamp;
    use sinex_primitives::utils::timestamp_helpers::parse_relative_duration;

    if let Some(dur) = parse_relative_duration(s) {
        let ts = Timestamp::now() - dur;
        return Ok(ts.format_rfc3339());
    }

    if Timestamp::parse_rfc3339(s).is_ok() {
        return Ok(s.to_string());
    }

    // Date-only: YYYY-MM-DD
    if let Ok(date) =
        time::Date::parse(s, time::macros::format_description!("[year]-[month]-[day]"))
    {
        #[allow(clippy::expect_used)]
        let ts = Timestamp::from(
            date.with_hms(0, 0, 0)
                .expect("midnight is always valid")
                .assume_utc(),
        );
        return Ok(ts.format_rfc3339());
    }

    Err(color_eyre::eyre::eyre!(
        "Invalid time format: '{}'\nSupported formats:\n  Relative: 1h, 6h, 2d, 30m\n  Absolute: 2026-03-17, 2026-03-17T00:00:00Z",
        s
    ))
}

// ─── Table formatters ────────────────────────────────────────────────────────

fn format_window_focus_table(buckets: &[WindowFocusBucket]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "BUCKET",
        "WORKSPACE",
        "WINDOW CLASS",
        "WINDOW TITLE",
        "FOCUS EVENTS",
        "LAST FOCUS",
    ]);
    for b in buckets {
        builder.push_record([
            style(b.bucket.as_str()).dim().to_string(),
            b.workspace.as_deref().unwrap_or("—").to_string(),
            b.window_class.as_deref().unwrap_or("—").to_string(),
            b.window_title.as_deref().unwrap_or("—").to_string(),
            b.focus_event_count.to_string(),
            b.last_focus_time.as_deref().unwrap_or("—").to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_command_frequency_table(entries: &[CommandFrequencyEntry]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "COMMAND",
        "SHELL",
        "TOTAL",
        "OK",
        "FAILED",
        "AVG DURATION (ms)",
    ]);
    for e in entries {
        builder.push_record([
            e.command.clone(),
            e.shell.as_deref().unwrap_or("—").to_string(),
            e.total_executions.to_string(),
            e.successful_executions.to_string(),
            e.failed_executions.to_string(),
            e.avg_duration_ms
                .map_or_else(|| "—".to_string(), |value| format!("{value:.1}")),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_file_activity_table(entries: &[FileActivityEntry]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["BUCKET", "DIRECTORY", "EVENT TYPE", "EVENTS", "FILES"]);
    for e in entries {
        builder.push_record([
            style(e.bucket.as_str()).dim().to_string(),
            e.directory.as_deref().unwrap_or("—").to_string(),
            e.event_type.clone(),
            e.total_events.to_string(),
            e.unique_files.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_recent_activity_table(entries: &[RecentActivityEntry]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["TYPE", "CONTEXT", "DETAIL", "TIMESTAMP"]);
    for e in entries {
        builder.push_record([
            e.activity_type.clone(),
            e.context.as_deref().unwrap_or("—").to_string(),
            e.detail.as_deref().unwrap_or("—").to_string(),
            e.timestamp.as_deref().unwrap_or("—").to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_system_state_table(buckets: &[SystemStateBucket]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "BUCKET",
        "AVG CPU %",
        "MAX CPU %",
        "AVG MEM %",
        "MAX MEM %",
        "AVG DISK %",
        "ACTIVE UNITS",
        "SAMPLES",
    ]);
    for b in buckets {
        builder.push_record([
            style(b.bucket.as_str()).dim().to_string(),
            b.avg_cpu_percent
                .map_or_else(|| "—".to_string(), |v| format!("{v:.1}")),
            b.max_cpu_percent
                .map_or_else(|| "—".to_string(), |v| format!("{v:.1}")),
            b.avg_memory_percent
                .map_or_else(|| "—".to_string(), |v| format!("{v:.1}")),
            b.max_memory_percent
                .map_or_else(|| "—".to_string(), |v| format!("{v:.1}")),
            b.avg_disk_percent
                .map_or_else(|| "—".to_string(), |v| format!("{v:.1}")),
            b.current_active_units
                .map_or_else(|| "—".to_string(), |value| value.to_string()),
            b.sample_count.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_ingestd_validation_table(snapshot: &IngestdValidationSnapshot) -> String {
    let mut builder = Builder::new();
    builder.push_record(["FIELD", "VALUE"]);
    builder.push_record(["Observed At", snapshot.observed_at.as_str()]);
    builder.push_record(["Batch Size", &snapshot.batch_size.to_string()]);
    builder.push_record(["Fetch→Ack ms", &snapshot.fetch_to_ack_ms.to_string()]);
    builder.push_record(["Deferred", &snapshot.events_deferred.to_string()]);
    builder.push_record(["Failed", &snapshot.events_failed.to_string()]);
    builder.push_record([
        "Had Synthesis",
        if snapshot.had_synthesis { "yes" } else { "no" },
    ]);
    builder.push_record(["Insert Path", snapshot.insert_path.as_str()]);
    builder.push_record(["Valid", &snapshot.validation_valid.to_string()]);
    builder.push_record(["Skipped", &snapshot.validation_skipped.to_string()]);
    builder.push_record(["No Schema", &snapshot.validation_no_schema.to_string()]);
    builder.push_record([
        "Schema Not Found",
        &snapshot.validation_schema_not_found.to_string(),
    ]);
    builder.push_record(["Invalid", &snapshot.validation_invalid.to_string()]);
    builder.push_record([
        "Coverage %",
        &format!("{:.2}", snapshot.validation_coverage_pct),
    ]);
    builder.push_record([
        "Suspicious Future ts_orig",
        &snapshot.suspicious_future_ts_orig.to_string(),
    ]);
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}
