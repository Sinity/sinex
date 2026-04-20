use clap::{Args, Subcommand};
use color_eyre::Result;
use console::style;
use sinex_primitives::events::{EventPayload, payloads::ActivitySessionBoundaryPayload};
use sinex_primitives::query::{
    AggregationMode, EventQuery, EventQueryResult, GroupByField, GroupedValue, NumericField,
    QueryResultEvent, SortDirection, TimeRange, TimeSeriesOrder,
};
use sinex_primitives::temporal::{OffsetDateTime, Timestamp};

use crate::client::GatewayClient;

const SESSION_QUERY_LIMIT: i64 = 256;
const SESSION_SOURCE_LIMIT: i64 = 5;

/// Daily activity summary reports
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Summary of today's activity
    sinexctl report today

    # Summary of yesterday's activity
    sinexctl report yesterday
")]
pub struct ReportCommand {
    #[command(subcommand)]
    pub cmd: ReportCommands,
}

impl ReportCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        self.cmd.execute(client).await
    }
}

#[derive(Debug, Subcommand)]
pub enum ReportCommands {
    /// Summary of today's activity (midnight to now)
    Today,
    /// Summary of yesterday's activity (yesterday midnight to today midnight)
    Yesterday,
    /// Cross-source calendar view showing daily activity for a week
    Calendar(CalendarArgs),
}

#[derive(Debug, Args)]
pub struct CalendarArgs {
    /// Show this many days (default: 7)
    #[arg(long, default_value_t = 7)]
    pub days: u32,
    /// Start from this many days ago (default: 0, i.e., ending today)
    #[arg(long, default_value_t = 0)]
    pub offset: u32,
}

impl ReportCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let (time_range, label) = match self {
            Self::Today => {
                let (start, end) = today_range();
                (time_range_new(start, end), label_for_today())
            }
            Self::Yesterday => {
                let (start, end) = yesterday_range();
                (time_range_new(start, end), label_for_yesterday())
            }
            Self::Calendar(args) => {
                return print_calendar(client, args.days, args.offset).await;
            }
        };

        print_report(client, time_range, &label).await
    }
}

// ─── Time range helpers ───────────────────────────────────────────────────────

/// Returns (`today_midnight`, now).
fn today_range() -> (Timestamp, Timestamp) {
    let now = OffsetDateTime::now_utc();
    #[allow(clippy::expect_used)]
    let midnight = Timestamp::new(
        now.date()
            .with_hms(0, 0, 0)
            .expect("midnight is always valid")
            .assume_utc(),
    );
    (midnight, Timestamp::now())
}

/// Returns (`yesterday_midnight`, `today_midnight`).
fn yesterday_range() -> (Timestamp, Timestamp) {
    let now = OffsetDateTime::now_utc();
    let today = now.date();
    let yesterday = today - time::Duration::days(1);

    #[allow(clippy::expect_used)]
    let today_midnight = Timestamp::new(
        today
            .with_hms(0, 0, 0)
            .expect("midnight is always valid")
            .assume_utc(),
    );
    #[allow(clippy::expect_used)]
    let yesterday_midnight = Timestamp::new(
        yesterday
            .with_hms(0, 0, 0)
            .expect("midnight is always valid")
            .assume_utc(),
    );

    (yesterday_midnight, today_midnight)
}

fn time_range_new(start: Timestamp, end: Timestamp) -> TimeRange {
    TimeRange::new(Some(start), Some(end)).expect("start < end by construction")
}

fn label_for_today() -> String {
    let now = OffsetDateTime::now_utc();
    #[allow(clippy::expect_used)]
    now.date()
        .format(time::macros::format_description!("[year]-[month]-[day]"))
        .expect("date format is always valid")
}

fn label_for_yesterday() -> String {
    let now = OffsetDateTime::now_utc();
    let yesterday = now.date() - time::Duration::days(1);
    #[allow(clippy::expect_used)]
    yesterday
        .format(time::macros::format_description!("[year]-[month]-[day]"))
        .expect("date format is always valid")
}

#[derive(Debug, Clone)]
struct SessionReportSummary {
    session_count: i64,
    total_duration_secs: u64,
    avg_duration_secs: Option<u64>,
    longest_session: Option<ActivitySessionBoundaryPayload>,
    by_primary_source: Vec<GroupedValue>,
}

fn session_query_base(time_range: TimeRange) -> EventQuery {
    EventQuery {
        sources: vec![ActivitySessionBoundaryPayload::SOURCE.clone()],
        event_types: vec![ActivitySessionBoundaryPayload::EVENT_TYPE.clone()],
        time_range: Some(time_range),
        ..Default::default()
    }
}

fn parse_session_event(event: QueryResultEvent) -> Option<ActivitySessionBoundaryPayload> {
    serde_json::from_value(event.event.payload).ok()
}

fn grouped_value_to_duration_secs(result: EventQueryResult) -> Option<u64> {
    match result {
        EventQueryResult::GroupedValues { groups, .. } => groups
            .first()
            .map(|group| group.value.max(0.0).round() as u64),
        _ => None,
    }
}

async fn fetch_session_summary(
    client: &GatewayClient,
    time_range: TimeRange,
) -> Result<Option<SessionReportSummary>> {
    let base = session_query_base(time_range);
    let session_count = match client
        .query_events(EventQuery {
            aggregation: Some(AggregationMode::Count),
            ..base.clone()
        })
        .await?
    {
        EventQueryResult::Count { count } => count,
        _ => 0,
    };

    if session_count == 0 {
        return Ok(None);
    }

    let total_duration_secs = grouped_value_to_duration_secs(
        client
            .query_events(EventQuery {
                aggregation: Some(AggregationMode::SumBy {
                    field: GroupByField::Source,
                    value_field: NumericField::PayloadPath("duration_secs".to_string()),
                    limit: 1,
                }),
                ..base.clone()
            })
            .await?,
    )
    .unwrap_or(0);

    let avg_duration_secs = grouped_value_to_duration_secs(
        client
            .query_events(EventQuery {
                aggregation: Some(AggregationMode::AvgBy {
                    field: GroupByField::Source,
                    value_field: NumericField::PayloadPath("duration_secs".to_string()),
                    limit: 1,
                }),
                ..base.clone()
            })
            .await?,
    );

    let by_primary_source = match client
        .query_events(EventQuery {
            aggregation: Some(AggregationMode::SumBy {
                field: GroupByField::PayloadPath("primary_source".to_string()),
                value_field: NumericField::PayloadPath("duration_secs".to_string()),
                limit: SESSION_SOURCE_LIMIT,
            }),
            ..base.clone()
        })
        .await?
    {
        EventQueryResult::GroupedValues { groups, .. } => groups,
        _ => Vec::new(),
    };

    let longest_session = match client
        .query_events(EventQuery {
            limit: SESSION_QUERY_LIMIT,
            direction: SortDirection::Desc,
            ..base
        })
        .await?
    {
        EventQueryResult::Events { events, .. } => events
            .into_iter()
            .filter_map(parse_session_event)
            .max_by_key(|payload| payload.duration_secs),
        _ => None,
    };

    Ok(Some(SessionReportSummary {
        session_count,
        total_duration_secs,
        avg_duration_secs,
        longest_session,
        by_primary_source,
    }))
}

// ─── Report rendering ─────────────────────────────────────────────────────────

async fn print_report(client: &GatewayClient, time_range: TimeRange, label: &str) -> Result<()> {
    println!();
    println!("{}", style(format!("Daily Report: {label}")).bold().cyan());
    println!("{}", style("═".repeat(40)).dim());

    // ── 1. Total event count ──────────────────────────────────────────────────
    let count_query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::Count),
        ..Default::default()
    };

    let total = match client.query_events(count_query).await? {
        EventQueryResult::Count { count } => count,
        _ => 0,
    };

    println!();
    println!("Total events: {}", style(format_count(total)).bold());

    if total == 0 {
        println!();
        println!("{}", style("No events recorded for this period.").dim());
        return Ok(());
    }

    if let Some(session_summary) = fetch_session_summary(client, time_range).await? {
        println!();
        println!("{}", style("Sessions:").bold());
        println!(
            "  {} total  ·  avg {}  ·  focused time {}",
            style(format_count(session_summary.session_count)).bold(),
            style(format_optional_duration(session_summary.avg_duration_secs)).cyan(),
            style(format_duration_compact(session_summary.total_duration_secs)).cyan()
        );

        if let Some(longest_session) = &session_summary.longest_session {
            println!(
                "  Longest: {} → {}  ({})  [{}]",
                style(format_clock_time(longest_session.start_time)).dim(),
                style(format_clock_time(longest_session.end_time)).dim(),
                style(format_duration_compact(longest_session.duration_secs)).bold(),
                style(longest_session.primary_source.to_string()).yellow()
            );
        }

        if !session_summary.by_primary_source.is_empty() {
            println!("  By primary source:");
            for group in &session_summary.by_primary_source {
                println!(
                    "    {:<16} {}",
                    style(&group.key).cyan(),
                    style(format_duration_compact(group.value.max(0.0).round() as u64)).dim()
                );
            }
        }
    }

    // ── 2. Top sources ───────────────────────────────────────────────────────
    let sources_query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::CountBy {
            field: GroupByField::Source,
            limit: 10,
        }),
        direction: SortDirection::Desc,
        ..Default::default()
    };

    if let Ok(EventQueryResult::GroupedCounts { groups }) = client.query_events(sources_query).await
        && !groups.is_empty()
    {
        println!();
        println!("{}", style("Top Sources:").bold());
        for g in &groups {
            println!(
                "  {:<28}  {}",
                style(&g.key).cyan(),
                style(format!("{} events", format_count(g.count))).dim()
            );
        }
    }

    // ── 3. Top event types ───────────────────────────────────────────────────
    let types_query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::CountBy {
            field: GroupByField::EventType,
            limit: 10,
        }),
        direction: SortDirection::Desc,
        ..Default::default()
    };

    if let Ok(EventQueryResult::GroupedCounts { groups }) = client.query_events(types_query).await
        && !groups.is_empty()
    {
        println!();
        println!("{}", style("Top Event Types:").bold());
        for g in &groups {
            println!(
                "  {:<28}  {}",
                style(&g.key).yellow(),
                style(format!("{} events", format_count(g.count))).dim()
            );
        }
    }

    // ── 4. Hourly activity heatmap ───────────────────────────────────────────
    let heatmap_query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::TimeSeries {
            interval_minutes: 60,
            order: TimeSeriesOrder::TimeAsc,
        }),
        ..Default::default()
    };

    if let Ok(EventQueryResult::TimeSeries { buckets }) = client.query_events(heatmap_query).await
        && !buckets.is_empty()
    {
        println!();
        println!("{}", style("Hourly Activity:").bold());
        let max_count = buckets.iter().map(|b| b.count).max().unwrap_or(1).max(1);
        for b in &buckets {
            let hour_str = b
                .bucket
                .inner()
                .format(time::macros::format_description!("[hour]:[minute]"))
                .unwrap_or_else(|_| "??:??".to_string());
            let bar = render_bar(b.count, max_count, 10);
            println!(
                "  {}  {}  {}",
                style(&hour_str).dim(),
                bar,
                style(format!("{:>5}", b.count)).dim()
            );
        }
    }

    println!();
    Ok(())
}

// ─── Calendar view ──────────────────────────────────────────────────────────

async fn print_calendar(client: &GatewayClient, days: u32, offset: u32) -> Result<()> {
    let now = OffsetDateTime::now_utc();
    let end_date = now.date() - time::Duration::days(i64::from(offset));
    let start_date = end_date - time::Duration::days(i64::from(days) - 1);

    println!();
    println!(
        "{}",
        style(format!(
            "Activity Calendar: {} to {}",
            format_date(start_date),
            format_date(end_date)
        ))
        .bold()
        .cyan()
    );
    println!("{}", style("═".repeat(60)).dim());

    let mut max_total = 0i64;
    let mut day_data = Vec::new();

    for i in 0..days {
        let date = start_date + time::Duration::days(i64::from(i));
        #[allow(clippy::expect_used)]
        let day_start =
            Timestamp::new(date.with_hms(0, 0, 0).expect("midnight valid").assume_utc());
        let next_date = date + time::Duration::days(1);
        #[allow(clippy::expect_used)]
        let day_end = Timestamp::new(
            next_date
                .with_hms(0, 0, 0)
                .expect("midnight valid")
                .assume_utc(),
        );

        let time_range = TimeRange::new(Some(day_start), Some(day_end)).expect("day range valid");

        let count_query = EventQuery {
            time_range: Some(time_range),
            aggregation: Some(AggregationMode::Count),
            ..Default::default()
        };

        let total = match client.query_events(count_query).await? {
            EventQueryResult::Count { count } => count,
            _ => 0,
        };

        let sources_query = EventQuery {
            time_range: Some(time_range),
            aggregation: Some(AggregationMode::CountBy {
                field: GroupByField::Source,
                limit: 5,
            }),
            direction: SortDirection::Desc,
            ..Default::default()
        };

        let top_sources = match client.query_events(sources_query).await {
            Ok(EventQueryResult::GroupedCounts { groups }) => groups
                .iter()
                .map(|g| format!("{}:{}", g.key, format_count(g.count)))
                .collect::<Vec<_>>()
                .join(" "),
            _ => String::new(),
        };

        max_total = max_total.max(total);
        day_data.push((date, total, top_sources));
    }

    println!();
    let weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    for (date, total, sources) in &day_data {
        let weekday_idx = date.weekday().number_from_monday() as usize - 1;
        let weekday = weekdays[weekday_idx];
        let bar = render_bar(*total, max_total.max(1), 15);
        let date_str = format_date(*date);

        println!(
            "  {} {} {} {:>8}  {}",
            style(weekday).dim(),
            style(&date_str).cyan(),
            bar,
            style(format_count(*total)).bold(),
            style(sources).dim()
        );
    }

    println!();
    Ok(())
}

fn format_date(date: time::Date) -> String {
    #[allow(clippy::expect_used)]
    date.format(time::macros::format_description!("[year]-[month]-[day]"))
        .expect("date format valid")
}

// ─── Formatting helpers ───────────────────────────────────────────────────────

fn format_clock_time(timestamp: Timestamp) -> String {
    timestamp
        .inner()
        .format(time::macros::format_description!("[hour]:[minute]"))
        .unwrap_or_else(|_| "??:??".to_string())
}

fn format_duration_compact(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if hours > 0 {
        if minutes > 0 {
            format!("{hours}h {minutes}m")
        } else {
            format!("{hours}h")
        }
    } else if minutes > 0 {
        if seconds > 0 {
            format!("{minutes}m {seconds}s")
        } else {
            format!("{minutes}m")
        }
    } else {
        format!("{seconds}s")
    }
}

fn format_optional_duration(duration_secs: Option<u64>) -> String {
    duration_secs.map_or_else(|| "n/a".to_string(), format_duration_compact)
}

/// Format a count with thousands separators.
fn format_count(n: i64) -> String {
    if n < 1_000 {
        return n.to_string();
    }
    // Build string right-to-left inserting commas
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.chars().rev().collect()
}

/// Render a fixed-width bar of filled/empty blocks scaled to [0, max].
fn render_bar(count: i64, max: i64, width: usize) -> String {
    let filled = if max == 0 {
        0
    } else {
        ((count as f64 / max as f64) * width as f64).round() as usize
    };
    let filled = filled.min(width);
    let empty = width - filled;
    format!(
        "{}{}",
        style("█".repeat(filled)).cyan(),
        style("░".repeat(empty)).dim()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::activity::ActivitySourceKind;
    use std::collections::BTreeMap;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn format_duration_compact_handles_hours_minutes_and_seconds() -> TestResult<()> {
        assert_eq!(format_duration_compact(47), "47s");
        assert_eq!(format_duration_compact(120), "2m");
        assert_eq!(format_duration_compact(198 * 60), "3h 18m");
        Ok(())
    }

    #[sinex_test]
    async fn grouped_value_to_duration_secs_reads_first_group_value() -> TestResult<()> {
        let result = EventQueryResult::GroupedValues {
            aggregation: sinex_primitives::query::GroupedValueAggregation::Sum,
            groups: vec![GroupedValue {
                key: "derived.session-detector".to_string(),
                value: 5400.0,
                sample_count: 3,
            }],
        };

        assert_eq!(grouped_value_to_duration_secs(result), Some(5400));
        Ok(())
    }

    #[sinex_test]
    async fn parse_session_event_roundtrips_boundary_payload() -> TestResult<()> {
        let start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let end = start + time::Duration::minutes(42);
        let payload = ActivitySessionBoundaryPayload {
            session_id: "session-7".to_string(),
            start_time: start,
            end_time: end,
            duration_secs: 2520,
            event_count: 4,
            window_count: 2,
            source_count: 2,
            sources: vec!["shell.kitty".to_string(), "wm.hyprland".to_string()],
            activity_sources: vec![ActivitySourceKind::Terminal, ActivitySourceKind::Window],
            activity_source_counts: BTreeMap::from([
                (ActivitySourceKind::Terminal, 3),
                (ActivitySourceKind::Window, 1),
            ]),
            primary_source: ActivitySourceKind::Terminal,
        };

        let event = QueryResultEvent {
            event: sinex_primitives::events::Event {
                id: None,
                source: ActivitySessionBoundaryPayload::SOURCE,
                event_type: ActivitySessionBoundaryPayload::EVENT_TYPE,
                payload: serde_json::to_value(&payload)?,
                ts_orig: Some(end),
                host: sinex_primitives::events::builder::get_hostname(),
                node_run_id: None,
                payload_schema_id: None,
                provenance: sinex_primitives::events::Provenance::Material {
                    id: Id::new(),
                    anchor_byte: 0,
                    offset_start: None,
                    offset_end: None,
                    offset_kind: sinex_primitives::events::OffsetKind::Byte,
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                node_model: None,
            },
            relevance_score: None,
            snippet: None,
        };

        let parsed = parse_session_event(event).expect("boundary payload should parse");
        assert_eq!(parsed.primary_source, ActivitySourceKind::Terminal);
        assert_eq!(parsed.duration_secs, 2520);
        Ok(())
    }
}
