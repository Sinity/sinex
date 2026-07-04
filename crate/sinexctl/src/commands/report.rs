use clap::{Args, Subcommand};
use color_eyre::Result;
use console::style;
use serde::Serialize;
use sinex_primitives::events::{EventPayload, payloads::ActivitySessionBoundaryPayload};
use sinex_primitives::query::{
    AggregationMode, EventQuery, EventQueryResult, GroupByField, GroupedCount, GroupedValue,
    NumericField, QueryResultEvent, SortDirection, TimeBucketEntry, TimeRange, TimeSeriesOrder,
};
use sinex_primitives::temporal::{OffsetDateTime, Timestamp};
use sinex_primitives::views::{
    CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};

use crate::client::GatewayClient;
use crate::fmt::{format_duration_compact_secs, print_finite_envelope};
use crate::model::OutputFormat;

const SESSION_QUERY_LIMIT: i64 = 256;
const SESSION_SOURCE_LIMIT: i64 = 5;
const REPORT_SCHEMA_VERSION: &str = "sinex.activity-report/v1";
const CALENDAR_SCHEMA_VERSION: &str = "sinex.activity-calendar/v1";

/// Daily activity summary reports
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Summary of today's activity
    sinexctl metrics report today

    # Summary of yesterday's activity
    sinexctl metrics report yesterday
")]
pub struct ReportCommand {
    #[command(subcommand)]
    pub cmd: ReportCommands,
}

impl ReportCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        self.cmd.execute(client, format).await
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
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
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
                return run_calendar(client, args.days, args.offset, format).await;
            }
        };

        run_report(client, time_range, &label, format).await
    }
}

// ─── Time range helpers ───────────────────────────────────────────────────────

/// Returns (`today_midnight`, now).
fn today_range() -> (Timestamp, Timestamp) {
    let now = OffsetDateTime::now_utc();
    let midnight = midnight_utc(now.date());
    (midnight, Timestamp::now())
}

/// Returns (`yesterday_midnight`, `today_midnight`).
fn yesterday_range() -> (Timestamp, Timestamp) {
    let now = OffsetDateTime::now_utc();
    let today = now.date();
    let yesterday = today - time::Duration::days(1);

    let today_midnight = midnight_utc(today);
    let yesterday_midnight = midnight_utc(yesterday);

    (yesterday_midnight, today_midnight)
}

fn midnight_utc(date: time::Date) -> Timestamp {
    // 00:00:00 is a structural invariant for every valid calendar date.
    #[allow(clippy::expect_used)]
    Timestamp::new(
        date.with_hms(0, 0, 0)
            .expect("midnight is always valid")
            .assume_utc(),
    )
}

fn time_range_new(start: Timestamp, end: Timestamp) -> TimeRange {
    // Callers construct these ranges from monotonically ordered day boundaries.
    #[allow(clippy::expect_used)]
    TimeRange::new(Some(start), Some(end)).expect("start < end by construction")
}

fn label_for_today() -> String {
    let now = OffsetDateTime::now_utc();
    now.date()
        .format(time::macros::format_description!("[year]-[month]-[day]"))
        .unwrap_or_else(|_| now.date().to_string())
}

fn label_for_yesterday() -> String {
    let now = OffsetDateTime::now_utc();
    let yesterday = now.date() - time::Duration::days(1);
    yesterday
        .format(time::macros::format_description!("[year]-[month]-[day]"))
        .unwrap_or_else(|_| yesterday.to_string())
}

#[derive(Debug, Clone)]
struct SessionReportSummary {
    session_count: i64,
    total_duration_secs: u64,
    avg_duration_secs: Option<u64>,
    longest_session: Option<ActivitySessionBoundaryPayload>,
    by_primary_source: Vec<GroupedValue>,
}

#[derive(Debug, Clone, Serialize)]
struct ActivityReportView {
    schema_version: String,
    label: String,
    window_start: Option<Timestamp>,
    window_end: Option<Timestamp>,
    total_events: i64,
    sessions: Option<SessionReportSummaryView>,
    top_sources: Vec<GroupedCount>,
    top_event_types: Vec<GroupedCount>,
    hourly_activity: Vec<TimeBucketEntry>,
}

impl ActivityReportView {
    fn new(
        label: impl Into<String>,
        time_range: TimeRange,
        total_events: i64,
        sessions: Option<SessionReportSummary>,
        top_sources: Vec<GroupedCount>,
        top_event_types: Vec<GroupedCount>,
        hourly_activity: Vec<TimeBucketEntry>,
    ) -> Self {
        Self {
            schema_version: REPORT_SCHEMA_VERSION.to_string(),
            label: label.into(),
            window_start: time_range.start(),
            window_end: time_range.end(),
            total_events,
            sessions: sessions.map(SessionReportSummaryView::from),
            top_sources,
            top_event_types,
            hourly_activity,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SessionReportSummaryView {
    session_count: i64,
    total_duration_secs: u64,
    avg_duration_secs: Option<u64>,
    longest_session: Option<ActivitySessionBoundaryPayload>,
    by_primary_source: Vec<GroupedValue>,
}

impl From<SessionReportSummary> for SessionReportSummaryView {
    fn from(summary: SessionReportSummary) -> Self {
        Self {
            session_count: summary.session_count,
            total_duration_secs: summary.total_duration_secs,
            avg_duration_secs: summary.avg_duration_secs,
            longest_session: summary.longest_session,
            by_primary_source: summary.by_primary_source,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ActivityCalendarView {
    schema_version: String,
    start_date: String,
    end_date: String,
    days: Vec<ActivityCalendarDayView>,
}

impl ActivityCalendarView {
    fn zero_day_count(&self) -> usize {
        self.days
            .iter()
            .filter(|day| day.total_events == 0)
            .count()
    }
}

#[derive(Debug, Clone, Serialize)]
struct ActivityCalendarDayView {
    date: String,
    total_events: i64,
    top_sources: Vec<GroupedCount>,
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

async fn collect_report_data(
    client: &GatewayClient,
    time_range: TimeRange,
) -> Result<ActivityReportView> {
    let count_query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::Count),
        ..Default::default()
    };
    let total = match client.query_events(count_query).await? {
        EventQueryResult::Count { count } => count,
        _ => 0,
    };

    let session_summary = if total > 0 {
        fetch_session_summary(client, time_range).await?
    } else {
        None
    };

    let sources_query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::CountBy {
            field: GroupByField::Source,
            limit: 10,
        }),
        direction: SortDirection::Desc,
        ..Default::default()
    };
    let top_sources = match client.query_events(sources_query).await {
        Ok(EventQueryResult::GroupedCounts { groups }) => groups,
        _ => Vec::new(),
    };

    let types_query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::CountBy {
            field: GroupByField::EventType,
            limit: 10,
        }),
        direction: SortDirection::Desc,
        ..Default::default()
    };
    let top_event_types = match client.query_events(types_query).await {
        Ok(EventQueryResult::GroupedCounts { groups }) => groups,
        _ => Vec::new(),
    };

    let heatmap_query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::TimeSeries {
            interval_minutes: 60,
            order: TimeSeriesOrder::TimeAsc,
        }),
        ..Default::default()
    };
    let hourly_buckets = match client.query_events(heatmap_query).await {
        Ok(EventQueryResult::TimeSeries { buckets }) => buckets,
        _ => Vec::new(),
    };

    Ok(ActivityReportView::new(
        "",
        time_range,
        total,
        session_summary,
        top_sources,
        top_event_types,
        hourly_buckets,
    ))
}

async fn run_report(
    client: &GatewayClient,
    time_range: TimeRange,
    label: &str,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Table => print_report(client, time_range, label).await,
        _ => {
            let mut report = collect_report_data(client, time_range).await?;
            report.label = label.to_string();
            let envelope = report_envelope(report);
            print_finite_envelope(&envelope, format)?;
            Ok(())
        }
    }
}

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
            style(format_duration_compact_secs(
                session_summary.total_duration_secs
            ))
            .cyan()
        );

        if let Some(longest_session) = &session_summary.longest_session {
            println!(
                "  Longest: {} → {}  ({})  [{}]",
                style(format_clock_time(longest_session.start_time)).dim(),
                style(format_clock_time(longest_session.end_time)).dim(),
                style(format_duration_compact_secs(longest_session.duration_secs)).bold(),
                style(longest_session.primary_source.to_string()).yellow()
            );
        }

        if !session_summary.by_primary_source.is_empty() {
            println!("  By primary source:");
            for group in &session_summary.by_primary_source {
                println!(
                    "    {:<16} {}",
                    style(&group.key).cyan(),
                    style(format_duration_compact_secs(
                        group.value.max(0.0).round() as u64
                    ))
                    .dim()
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

async fn collect_calendar_data(
    client: &GatewayClient,
    days: u32,
    offset: u32,
) -> Result<ActivityCalendarView> {
    let now = OffsetDateTime::now_utc();
    let end_date = now.date() - time::Duration::days(i64::from(offset));
    let start_date = end_date - time::Duration::days(i64::from(days) - 1);

    let mut day_entries = Vec::new();
    for i in 0..days {
        let date = start_date + time::Duration::days(i64::from(i));
        let day_start = midnight_utc(date);
        let next_date = date + time::Duration::days(1);
        let day_end = midnight_utc(next_date);
        let time_range = time_range_new(day_start, day_end);

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
            Ok(EventQueryResult::GroupedCounts { groups }) => groups,
            _ => Vec::new(),
        };

        day_entries.push(ActivityCalendarDayView {
            date: format_date(date),
            total_events: total,
            top_sources,
        });
    }

    Ok(ActivityCalendarView {
        schema_version: CALENDAR_SCHEMA_VERSION.to_string(),
        start_date: format_date(start_date),
        end_date: format_date(end_date),
        days: day_entries,
    })
}

async fn run_calendar(
    client: &GatewayClient,
    days: u32,
    offset: u32,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Table => print_calendar(client, days, offset).await,
        _ => {
            let calendar = collect_calendar_data(client, days, offset).await?;
            let envelope = calendar_envelope(calendar);
            print_finite_envelope(&envelope, format)?;
            Ok(())
        }
    }
}

fn report_envelope(report: ActivityReportView) -> ViewEnvelope<ActivityReportView> {
    let mut envelope = ViewEnvelope::new("sinexctl.metrics.report", report);
    if envelope.payload.total_events == 0 {
        envelope.caveats.push(report_coverage_caveat(
            "sinexctl.metrics.report",
            "activity report contains zero persisted events for the requested window; this report does not inspect source readiness, so capture coverage is unmeasurable here",
            "sinexctl metrics report today",
        ));
    }
    envelope
}

fn calendar_envelope(calendar: ActivityCalendarView) -> ViewEnvelope<ActivityCalendarView> {
    let zero_day_count = calendar.zero_day_count();
    let mut envelope = ViewEnvelope::new("sinexctl.metrics.report.calendar", calendar);
    if zero_day_count > 0 {
        envelope.caveats.push(report_coverage_caveat(
            "sinexctl.metrics.report.calendar",
            format!(
                "activity calendar contains {zero_day_count} zero-event day(s); this report does not inspect source readiness, so capture coverage is unmeasurable for those windows"
            ),
            "sinexctl metrics report calendar",
        ));
    }
    envelope
}

fn report_coverage_caveat(
    source_surface: &'static str,
    message: impl Into<String>,
    command_hint: &'static str,
) -> CaveatView {
    CaveatView {
        id: ReadinessCaveatId::CoverageUnmeasurable.as_str().to_string(),
        message: message.into(),
        ref_: Some(
            SinexObjectRef::new(SinexObjectKind::Command, source_surface)
                .with_label(source_surface)
                .with_command_hint(command_hint),
        ),
    }
}

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
        let day_start = midnight_utc(date);
        let next_date = date + time::Duration::days(1);
        let day_end = midnight_utc(next_date);

        let time_range = time_range_new(day_start, day_end);

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
    date.format(time::macros::format_description!("[year]-[month]-[day]"))
        .unwrap_or_else(|_| date.to_string())
}

// ─── Formatting helpers ───────────────────────────────────────────────────────

fn format_clock_time(timestamp: Timestamp) -> String {
    timestamp
        .inner()
        .format(time::macros::format_description!("[hour]:[minute]"))
        .unwrap_or_else(|_| "??:??".to_string())
}

fn format_optional_duration(duration_secs: Option<u64>) -> String {
    duration_secs.map_or_else(|| "n/a".to_string(), format_duration_compact_secs)
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
#[path = "report_test.rs"]
mod tests;
