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
        };

        print_report(client, time_range, &label).await
    }
}

// ─── Time range helpers ───────────────────────────────────────────────────────

/// Returns (today_midnight, now).
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

/// Returns (yesterday_midnight, today_midnight).
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
    {
        if !groups.is_empty() {
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

    if let Ok(EventQueryResult::GroupedCounts { groups }) = client.query_events(types_query).await {
        if !groups.is_empty() {
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

    if let Ok(EventQueryResult::TimeSeries { buckets }) = client.query_events(heatmap_query).await {
        if !buckets.is_empty() {
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
    }

    println!();
    Ok(())
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
