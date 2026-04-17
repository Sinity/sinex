use clap::{Args, Subcommand};
use color_eyre::Result;
use console::style;
use sinex_primitives::query::{
    AggregationMode, EventQuery, EventQueryResult, GroupByField, SortDirection, TimeRange,
    TimeSeriesOrder,
};
use sinex_primitives::temporal::{OffsetDateTime, Timestamp};

use crate::client::GatewayClient;

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
