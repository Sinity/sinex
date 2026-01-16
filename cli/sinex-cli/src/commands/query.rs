use chrono::{DateTime, Duration, Utc};
use clap::Args;

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::search::{SearchQuery, SearchResult};
use crate::model::OutputFormat;
use crate::Result;

/// Query/search events
#[derive(Debug, Args)]
pub struct QueryCommand {
    /// Free-text search (searches all fields)
    #[arg(short = 'q', long)]
    query: Option<String>,

    /// Filter by source (can be specified multiple times)
    #[arg(long)]
    source: Vec<String>,

    /// Filter by event type (can be specified multiple times)
    #[arg(long)]
    event_type: Vec<String>,

    /// Time range start: "1h", "2d", "2025-01-15", "2025-01-15T10:00:00Z"
    #[arg(long, short = 's')]
    since: Option<String>,

    /// Time range end (default: now)
    #[arg(long, short = 'u')]
    until: Option<String>,

    /// Maximum number of results
    #[arg(long, short = 'n', default_value = "100")]
    limit: i32,

    /// Offset for pagination
    #[arg(long, default_value = "0")]
    offset: i32,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl QueryCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let query = SearchQuery {
            text: self.query.clone(),
            sources: self.source.clone(),
            event_types: self.event_type.clone(),
            start_time: self.since.as_ref().map(|s| parse_time(s)).transpose()?,
            end_time: self.until.as_ref().map(|s| parse_time(s)).transpose()?,
            limit: self.limit,
            offset: self.offset,
        };

        let results = client.search_events(query).await?;

        match self.format {
            OutputFormat::Table => {
                if results.is_empty() {
                    println!("No events found.");
                } else {
                    println!("{}", format_table_results(&results));
                }
            }
            OutputFormat::Json => {
                for result in &results {
                    println!("{}", format_json(result)?);
                }
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&results)?);
            }
        }

        Ok(())
    }
}

/// Parse time string into DateTime
/// Supports:
/// - Relative: "1h", "2d", "30m", "1w"
/// - Absolute: "2025-01-15", "2025-01-15T10:00:00Z"
fn parse_time(s: &str) -> Result<DateTime<Utc>> {
    // Try relative time first (e.g., "1h", "2d")
    if let Some(duration) = parse_relative_time(s) {
        return Ok(Utc::now() - duration);
    }

    // Try absolute timestamp
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try date-only format (YYYY-MM-DD)
    if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let naive_datetime = naive_date.and_hms_opt(0, 0, 0).ok_or_else(|| {
            color_eyre::eyre::eyre!("Invalid date: {}", s)
        })?;
        return Ok(DateTime::from_naive_utc_and_offset(naive_datetime, Utc));
    }

    Err(color_eyre::eyre::eyre!(
        "Invalid time format: '{}'\nSupported formats:\n  Relative: 1h, 2d, 30m, 1w\n  Absolute: 2025-01-15, 2025-01-15T10:00:00Z",
        s
    ))
}

/// Parse relative time string (e.g., "1h", "2d", "30m")
fn parse_relative_time(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Split into number and unit
    let mut num_str = String::new();
    let mut unit = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            num_str.push(ch);
        } else {
            unit.push(ch);
        }
    }

    let num: i64 = num_str.parse().ok()?;

    match unit.as_str() {
        "s" | "sec" | "second" | "seconds" => Some(Duration::seconds(num)),
        "m" | "min" | "minute" | "minutes" => Some(Duration::minutes(num)),
        "h" | "hr" | "hour" | "hours" => Some(Duration::hours(num)),
        "d" | "day" | "days" => Some(Duration::days(num)),
        "w" | "week" | "weeks" => Some(Duration::weeks(num)),
        _ => None,
    }
}

/// Format search results as a table
fn format_table_results(results: &[SearchResult]) -> String {
    use comfy_table::presets::UTF8_FULL;
    use comfy_table::{Cell, CellAlignment, ContentArrangement, Table};
    use console::style;

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("TIMESTAMP").set_alignment(CellAlignment::Left),
            Cell::new("SOURCE").set_alignment(CellAlignment::Left),
            Cell::new("EVENT TYPE").set_alignment(CellAlignment::Left),
            Cell::new("HOST").set_alignment(CellAlignment::Left),
            Cell::new("SNIPPET").set_alignment(CellAlignment::Left),
        ]);

    for result in results {
        let timestamp = result.timestamp.format("%Y-%m-%d %H:%M:%S");
        let snippet = truncate_string(&result.snippet, 60);

        table.add_row(vec![
            Cell::new(style(timestamp).dim().to_string()),
            Cell::new(&result.source),
            Cell::new(&result.event_type),
            Cell::new(style(&result.host).dim().to_string()),
            Cell::new(snippet),
        ]);
    }

    table.to_string()
}

/// Truncate string to max length with ellipsis
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.chars().count() <= max_len {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_len - 3).collect();
        format!("{}...", truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_relative_time() {
        assert_eq!(parse_relative_time("1h"), Some(Duration::hours(1)));
        assert_eq!(parse_relative_time("2d"), Some(Duration::days(2)));
        assert_eq!(parse_relative_time("30m"), Some(Duration::minutes(30)));
        assert_eq!(parse_relative_time("1w"), Some(Duration::weeks(1)));
        assert_eq!(parse_relative_time("15s"), Some(Duration::seconds(15)));

        // Alternative forms
        assert_eq!(parse_relative_time("1hour"), Some(Duration::hours(1)));
        assert_eq!(parse_relative_time("2days"), Some(Duration::days(2)));

        // Invalid
        assert_eq!(parse_relative_time("invalid"), None);
        assert_eq!(parse_relative_time(""), None);
    }

    #[test]
    fn test_parse_absolute_time() {
        let result = parse_time("2025-01-15T10:00:00Z");
        assert!(result.is_ok());

        let result = parse_time("2025-01-15");
        assert!(result.is_ok());
    }

    #[test]
    fn test_truncate_string() {
        assert_eq!(truncate_string("short", 10), "short");
        assert_eq!(truncate_string("this is a very long string", 10), "this is...");
    }
}
