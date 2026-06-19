use clap::Subcommand;
use console::style;
use sinex_primitives::otel_projection::{
    OtelMetricsProjectionView, gateway_stats_to_otel_metrics_projection,
};
use sinex_primitives::rpc::telemetry::{
    AssemblyStatsBucket, CommandFrequencyEntry, CurrentDeviceStateEntry, CurrentHealthEntry,
    EventEngineBatchStatsBucket, EventEngineValidationSnapshot, FileActivityEntry,
    GatewayStatsBucket, MetricCounterBucket, RecentActivityEntry, SourceStatsBucket,
    StreamStatsBucket, SystemStateBucket, WindowFocusBucket,
};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

/// Telemetry data from activity views and operator read models
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    sinexctl metrics telemetry current-health
    sinexctl metrics telemetry gateway-stats --from 24h
    sinexctl metrics telemetry metric-counters --from 6h --limit 20
    sinexctl metrics telemetry event-engine-batch-stats --from 12h -f json
    sinexctl metrics telemetry event-engine-validation
")]
pub enum TelemetryCommands {
    /// Latest component health-status rows
    CurrentHealth {
        /// Maximum number of rows to return (default: 50)
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Latest system/device state rows (materialized view)
    CurrentDeviceState {
        /// Maximum number of rows to return (default: 50)
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Window focus aggregates (5-minute buckets)
    WindowFocus {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Command frequency aggregates (hourly buckets)
    CommandFrequency {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// File activity aggregates (hourly buckets, per directory)
    FileActivity {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Recent activity summary (hardcoded lookback window)
    RecentActivity {
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// System state aggregates (5-minute buckets)
    SystemState {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Gateway hourly operator telemetry
    GatewayStats {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
        /// Render this read model as an OpenTelemetry-compatible metrics projection.
        #[arg(long)]
        otel: bool,
    },

    /// Stream hourly operator telemetry
    StreamStats {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Assembly hourly operator telemetry
    AssemblyStats {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// RuntimeModule hourly operator telemetry
    SourceStats {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Metric-counter hourly operator telemetry
    MetricCounters {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// EventEngine hourly batch-stat aggregates
    EventEngineBatchStats {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Latest event_engine validation and plausibility snapshot
    EventEngineValidation,
}

impl TelemetryCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::CurrentHealth { limit } => {
                let entries = client.telemetry_current_health(Some(*limit)).await?;
                CommandOutput::list(
                    entries,
                    "No current-health data found.",
                    format_current_health_table,
                )
                .display(&format)?;
            }

            Self::CurrentDeviceState { limit } => {
                let entries = client.telemetry_current_device_state(Some(*limit)).await?;
                CommandOutput::list(
                    entries,
                    "No current-device-state data found.",
                    format_current_device_state_table,
                )
                .display(&format)?;
            }

            Self::WindowFocus { from, to, limit } => {
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
                .display(&format)?;
            }

            Self::CommandFrequency { from, to, limit } => {
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
                .display(&format)?;
            }

            Self::FileActivity { from, to, limit } => {
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
                .display(&format)?;
            }

            Self::RecentActivity { limit } => {
                let entries = client.telemetry_recent_activity(Some(*limit)).await?;
                CommandOutput::list(
                    entries,
                    "No recent activity found.",
                    format_recent_activity_table,
                )
                .display(&format)?;
            }

            Self::SystemState { from, to, limit } => {
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
                .display(&format)?;
            }

            Self::GatewayStats {
                from,
                to,
                limit,
                otel,
            } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let buckets = client
                    .telemetry_gateway_stats(from_rfc, to_rfc, Some(*limit))
                    .await?;
                if *otel {
                    let projection = gateway_stats_to_otel_metrics_projection(buckets);
                    CommandOutput::single(projection, format_otel_metrics_projection_table)
                        .display(&format)?;
                } else {
                    CommandOutput::list(
                        buckets,
                        "No gateway-stats data found.",
                        format_gateway_stats_table,
                    )
                    .display(&format)?;
                }
            }

            Self::StreamStats { from, to, limit } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let buckets = client
                    .telemetry_stream_stats(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    buckets,
                    "No stream-stats data found.",
                    format_stream_stats_table,
                )
                .display(&format)?;
            }

            Self::AssemblyStats { from, to, limit } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let buckets = client
                    .telemetry_assembly_stats(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    buckets,
                    "No assembly-stats data found.",
                    format_assembly_stats_table,
                )
                .display(&format)?;
            }

            Self::SourceStats { from, to, limit } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let buckets = client
                    .telemetry_source_stats(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    buckets,
                    "No source-stats data found.",
                    format_source_stats_table,
                )
                .display(&format)?;
            }

            Self::MetricCounters { from, to, limit } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let buckets = client
                    .telemetry_metric_counters(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    buckets,
                    "No metric-counter data found.",
                    format_metric_counters_table,
                )
                .display(&format)?;
            }

            Self::EventEngineBatchStats { from, to, limit } => {
                let from_rfc = from.as_deref().map(resolve_time_arg).transpose()?;
                let to_rfc = to.as_deref().map(resolve_time_arg).transpose()?;
                let buckets = client
                    .telemetry_event_engine_batch_stats(from_rfc, to_rfc, Some(*limit))
                    .await?;
                CommandOutput::list(
                    buckets,
                    "No event-engine-batch-stats data found.",
                    format_event_engine_batch_stats_table,
                )
                .display(&format)?;
            }

            Self::EventEngineValidation => {
                match client.telemetry_event_engine_validation().await? {
                    Some(snapshot) => {
                        CommandOutput::single(snapshot, format_event_engine_validation_table)
                            .display(&format)?;
                    }
                    None => CommandOutput::<EventEngineValidationSnapshot>::empty(
                        "No event-engine-validation data found.",
                    )
                    .display(&format)?,
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

    if let Ok(date) =
        time::Date::parse(s, time::macros::format_description!("[year]-[month]-[day]"))
    {
        let midnight = date
            .with_hms(0, 0, 0)
            .map_err(|error| color_eyre::eyre::eyre!("invalid date {s}: {error}"))?;
        let ts = Timestamp::from(midnight.assume_utc());
        return Ok(ts.format_rfc3339());
    }

    Err(color_eyre::eyre::eyre!(
        "Invalid time format: '{}'\nSupported formats:\n  Relative: 1h, 6h, 2d, 30m\n  Absolute: 2026-03-17, 2026-03-17T00:00:00Z",
        s
    ))
}

fn format_current_health_table(entries: &[CurrentHealthEntry]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "SOURCE",
        "EVENT TYPE",
        "COMPONENT",
        "STATUS",
        "REASON",
        "LAST UPDATE",
    ]);
    for entry in entries {
        builder.push_record([
            entry.source.clone(),
            entry.event_type.clone(),
            entry.component.as_deref().unwrap_or("—").to_string(),
            entry.status.as_deref().unwrap_or("—").to_string(),
            entry.reason.as_deref().unwrap_or("—").to_string(),
            style(entry.last_update.as_str()).dim().to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_current_device_state_table(entries: &[CurrentDeviceStateEntry]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["UNIT", "TYPE", "STATE", "SUBSTATE", "LAST UPDATE"]);
    for entry in entries {
        builder.push_record([
            entry.unit_name.as_deref().unwrap_or("—").to_string(),
            entry.unit_type.as_deref().unwrap_or("—").to_string(),
            entry.state.as_deref().unwrap_or("—").to_string(),
            entry.sub_state.as_deref().unwrap_or("—").to_string(),
            style(entry.last_update.as_str()).dim().to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

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
    for bucket in buckets {
        builder.push_record([
            style(bucket.bucket.as_str()).dim().to_string(),
            bucket.workspace.as_deref().unwrap_or("—").to_string(),
            bucket.window_class.as_deref().unwrap_or("—").to_string(),
            bucket.window_title.as_deref().unwrap_or("—").to_string(),
            bucket.focus_event_count.to_string(),
            bucket.last_focus_time.as_deref().unwrap_or("—").to_string(),
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
    for entry in entries {
        builder.push_record([
            entry.command.clone(),
            entry.shell.as_deref().unwrap_or("—").to_string(),
            entry.total_executions.to_string(),
            entry.successful_executions.to_string(),
            entry.failed_executions.to_string(),
            entry
                .avg_duration_ms
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
    for entry in entries {
        builder.push_record([
            style(entry.bucket.as_str()).dim().to_string(),
            entry.directory.as_deref().unwrap_or("—").to_string(),
            entry.event_type.clone(),
            entry.total_events.to_string(),
            entry.unique_files.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_recent_activity_table(entries: &[RecentActivityEntry]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["TYPE", "CONTEXT", "DETAIL", "TIMESTAMP"]);
    for entry in entries {
        builder.push_record([
            entry.activity_type.clone(),
            entry.context.as_deref().unwrap_or("—").to_string(),
            entry.detail.as_deref().unwrap_or("—").to_string(),
            entry.timestamp.as_deref().unwrap_or("—").to_string(),
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
    for bucket in buckets {
        builder.push_record([
            style(bucket.bucket.as_str()).dim().to_string(),
            format_opt_f64(bucket.avg_cpu_percent),
            format_opt_f64(bucket.max_cpu_percent),
            format_opt_f64(bucket.avg_memory_percent),
            format_opt_f64(bucket.max_memory_percent),
            format_opt_f64(bucket.avg_disk_percent),
            format_opt_i64(bucket.current_active_units),
            bucket.sample_count.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_gateway_stats_table(buckets: &[GatewayStatsBucket]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "BUCKET",
        "SOURCE",
        "STAT EVENTS",
        "AVG REQS",
        "RATE LIMITED",
        "AVG LAT ms",
        "MAX P99 ms",
    ]);
    for bucket in buckets {
        builder.push_record([
            style(bucket.bucket.as_str()).dim().to_string(),
            bucket.source.clone(),
            bucket.stat_events.to_string(),
            format_opt_f64(bucket.avg_total_requests),
            format_opt_i64(bucket.total_rate_limited),
            format_opt_f64(bucket.avg_latency_ms),
            format_opt_f64(bucket.max_p99_latency_ms),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_otel_metrics_projection_table(view: &OtelMetricsProjectionView) -> String {
    let mut builder = Builder::new();
    builder.push_record(["FIELD", "VALUE"]);
    builder.push_record(["Schema", view.schema_version.as_str()]);
    builder.push_record(["Source Surface", view.source_surface.as_str()]);
    builder.push_record(["Source Response", view.source_response.as_str()]);
    let metric_count = view.metric_count().to_string();
    let point_count = view.point_count().to_string();
    builder.push_record(["Metrics", metric_count.as_str()]);
    builder.push_record(["Data Points", point_count.as_str()]);
    builder.push_record(["Disclosure Policy", view.disclosure.policy.as_str()]);
    let omitted = view.disclosure.omitted_attribute_families.join(", ");
    builder.push_record(["Omitted Families", omitted.as_str()]);
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_stream_stats_table(buckets: &[StreamStatsBucket]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "BUCKET",
        "STREAM",
        "AVG FILL %",
        "MAX FILL %",
        "MAX MSG %",
        "MAX BYTE %",
        "PRESSURE",
        "LIMIT",
        "AVG MSGS",
        "MAX MSGS",
        "SAMPLES",
    ]);
    for bucket in buckets {
        builder.push_record([
            style(bucket.bucket.as_str()).dim().to_string(),
            bucket.stream_name.as_deref().unwrap_or("—").to_string(),
            format_opt_f64(bucket.avg_fill_pct),
            format_opt_f64(bucket.max_fill_pct),
            format_opt_f64(bucket.max_message_fill_pct),
            format_opt_f64(bucket.max_byte_fill_pct),
            bucket
                .max_pressure_level
                .as_deref()
                .unwrap_or("—")
                .to_string(),
            bucket
                .limiting_dimension
                .as_deref()
                .unwrap_or("—")
                .to_string(),
            format_opt_f64(bucket.avg_messages),
            format_opt_i64(bucket.max_messages),
            bucket.sample_count.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_assembly_stats_table(buckets: &[AssemblyStatsBucket]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "BUCKET",
        "ACTIVE",
        "DONE",
        "CANCELLED",
        "FAILED",
        "TIMED OUT",
        "AVG DUR ms",
        "SAMPLES",
    ]);
    for bucket in buckets {
        builder.push_record([
            style(bucket.bucket.as_str()).dim().to_string(),
            format_opt_i64(bucket.max_active_assemblies),
            format_opt_i64(bucket.total_completed),
            format_opt_i64(bucket.total_cancelled),
            format_opt_i64(bucket.total_failed),
            format_opt_i64(bucket.total_timed_out),
            format_opt_f64(bucket.avg_duration_ms),
            bucket.sample_count.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_source_stats_table(buckets: &[SourceStatsBucket]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "BUCKET",
        "NODE TYPE",
        "PROCESSED",
        "DROPPED",
        "AVG LAT ms",
        "MAX QUEUE",
        "ERRORS",
        "SAMPLES",
    ]);
    for bucket in buckets {
        builder.push_record([
            style(bucket.bucket.as_str()).dim().to_string(),
            bucket.module_kind.as_deref().unwrap_or("—").to_string(),
            format_opt_i64(bucket.total_events_processed),
            format_opt_i64(bucket.total_events_dropped),
            format_opt_f64(bucket.avg_latency_ms),
            format_opt_i64(bucket.max_queue_depth),
            format_opt_i64(bucket.total_errors),
            bucket.sample_count.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_metric_counters_table(buckets: &[MetricCounterBucket]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["BUCKET", "COMPONENT", "METRIC", "TOTAL", "MAX", "SAMPLES"]);
    for bucket in buckets {
        builder.push_record([
            style(bucket.bucket.as_str()).dim().to_string(),
            bucket.component.as_deref().unwrap_or("—").to_string(),
            bucket.metric_name.as_deref().unwrap_or("—").to_string(),
            format_opt_i64(bucket.total_value),
            format_opt_i64(bucket.max_value),
            bucket.sample_count.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_event_engine_batch_stats_table(buckets: &[EventEngineBatchStatsBucket]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "BUCKET",
        "AVG SIZE",
        "MAX SIZE",
        "AVG LAT ms",
        "MAX LAT ms",
        "DEFERRED",
        "FAILED",
        "SYNTH",
        "BATCHES",
        "COVERAGE %",
    ]);
    for bucket in buckets {
        builder.push_record([
            style(bucket.bucket.as_str()).dim().to_string(),
            format_opt_f64(bucket.avg_batch_size),
            format_opt_i64(bucket.max_batch_size),
            format_opt_f64(bucket.avg_latency_ms),
            format_opt_f64(bucket.max_latency_ms),
            format_opt_i64(bucket.total_deferred),
            format_opt_i64(bucket.total_failed),
            bucket.derived_batches.to_string(),
            bucket.batch_count.to_string(),
            format_opt_f64(bucket.avg_validation_coverage_pct),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_event_engine_validation_table(snapshot: &EventEngineValidationSnapshot) -> String {
    let mut builder = Builder::new();
    builder.push_record(["FIELD", "VALUE"]);
    builder.push_record(["Observed At", snapshot.observed_at.as_str()]);
    builder.push_record(["Batch Size", &snapshot.batch_size.to_string()]);
    builder.push_record(["Fetch→Ack ms", &snapshot.fetch_to_ack_ms.to_string()]);
    builder.push_record(["Deferred", &snapshot.events_deferred.to_string()]);
    builder.push_record(["Failed", &snapshot.events_failed.to_string()]);
    builder.push_record([
        "Had Derived",
        if snapshot.had_derived { "yes" } else { "no" },
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

fn format_opt_f64(value: Option<f64>) -> String {
    value.map_or_else(|| "—".to_string(), |value| format!("{value:.1}"))
}

fn format_opt_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "—".to_string(), |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::rpc::telemetry::GatewayStatsBucket;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn otel_projection_table_summarizes_gateway_metric_projection() -> xtask::TestResult<()> {
        let projection = gateway_stats_to_otel_metrics_projection(vec![GatewayStatsBucket {
            bucket: "2026-06-19T18:00:00Z".to_string(),
            source: "sinex.gateway".to_string(),
            stat_events: 1,
            avg_total_requests: Some(3.0),
            total_rate_limited: Some(0),
            avg_latency_ms: Some(5.0),
            max_p99_latency_ms: Some(9.0),
        }]);

        let table = format_otel_metrics_projection_table(&projection);

        assert!(table.contains("sinex.otel.metrics-projection/v1"));
        assert!(table.contains("sinexctl.metrics.telemetry.gateway-stats"));
        assert!(table.contains("raw_event_payload"));
        Ok(())
    }
}
