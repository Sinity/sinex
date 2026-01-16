use chrono::{DateTime, Duration, Utc};
use clap::Args;
use console::style;
use inquire::{MultiSelect, Select, Text};

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::search::{SearchQuery, SearchResult};
use crate::model::OutputFormat;
use crate::Result;

/// Query/search events
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Search all events from last hour
    sinexctl query -s 1h

    # Full-text search for 'error'
    sinexctl query -q error -s 24h

    # Filter by source and event type
    sinexctl query --source terminal --event-type command -s 2d

    # Search within a date range
    sinexctl query -s 2025-01-10 -u 2025-01-15

    # Multiple sources (OR filter)
    sinexctl query --source terminal --source filesystem -s 1d

    # Pagination with limit and offset
    sinexctl query -s 7d -n 50 --offset 100

    # Output as JSON for piping
    sinexctl query -s 1h -f json | jq '.event_type'

    # Output as YAML
    sinexctl query -s 1h -f yaml

    # Launch interactive query builder
    sinexctl query -i
")]
pub struct QueryCommand {
    /// Launch interactive query builder
    #[arg(short = 'i', long)]
    interactive: bool,
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
        // Launch interactive mode if requested
        if self.interactive {
            return interactive_query(client, self.format).await;
        }

        let query = SearchQuery {
            text: self.query.clone(),
            sources: self.source.clone(),
            event_types: self.event_type.clone(),
            start_time: self.since.as_ref().map(|s| parse_time(s)).transpose()?,
            end_time: self.until.as_ref().map(|s| parse_time(s)).transpose()?,
            limit: self.limit,
            offset: self.offset,
        };

        execute_query(client, query, self.format).await
    }
}

/// Execute a query and display results
async fn execute_query(
    client: &GatewayClient,
    query: SearchQuery,
    format: OutputFormat,
) -> Result<()> {
    let results = client.search_events(query).await?;

    match format {
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

/// Interactive query builder
async fn interactive_query(client: &GatewayClient, format: OutputFormat) -> Result<()> {
    println!("{}", style("Interactive Query Builder").bold().cyan());
    println!("{}", style("─".repeat(50)).dim());
    println!();

    // Time range selection
    let time_options = vec![
        "Last 15 minutes",
        "Last hour",
        "Last 6 hours",
        "Last 24 hours",
        "Last 7 days",
        "Last 30 days",
        "Custom range...",
    ];
    let time_choice = Select::new("Time range:", time_options.clone())
        .with_starting_cursor(1) // Default to "Last hour"
        .prompt()?;

    let (since, until) = match time_choice {
        "Custom range..." => {
            let since_str = Text::new("Since (e.g., 1h, 2d, 2025-01-15):")
                .with_help_message("Relative: 1h, 2d, 1w | Absolute: 2025-01-15")
                .prompt()?;
            let until_str = Text::new("Until (press Enter for now):")
                .with_help_message("Leave empty for current time")
                .prompt_skippable()?
                .filter(|s| !s.is_empty());

            let since_time = parse_time(&since_str)?;
            let until_time = until_str.map(|s| parse_time(&s)).transpose()?;
            (since_time, until_time)
        }
        _ => {
            let since_time = parse_preset_time(time_choice);
            (since_time, None)
        }
    };

    // Fetch available sources from nodes if possible
    let default_sources = vec![
        "terminal".to_string(),
        "filesystem".to_string(),
        "desktop".to_string(),
        "system".to_string(),
        "health".to_string(),
    ];

    let sources = match client.list_nodes(None).await {
        Ok(nodes) => {
            if nodes.is_empty() {
                default_sources
            } else {
                nodes.iter().map(|n| n.name.clone()).collect()
            }
        }
        Err(_) => default_sources,
    };

    let selected_sources = MultiSelect::new("Sources (Space to select, Enter to confirm):", sources)
        .with_help_message("Leave empty to search all sources")
        .prompt_skippable()?
        .unwrap_or_default();

    // Event types
    let event_types = vec![
        "command".to_string(),
        "file_write".to_string(),
        "file_read".to_string(),
        "file_delete".to_string(),
        "process_start".to_string(),
        "process_exit".to_string(),
        "window_focus".to_string(),
        "clipboard".to_string(),
        "system_event".to_string(),
        "health_check".to_string(),
    ];

    let selected_types =
        MultiSelect::new("Event types (Space to select, Enter to confirm):", event_types)
            .with_help_message("Leave empty to search all event types")
            .prompt_skippable()?
            .unwrap_or_default();

    // Full-text search
    let text = Text::new("Full-text search (optional):")
        .with_help_message("Search across all event fields")
        .prompt_skippable()?
        .filter(|s| !s.is_empty());

    // Limit
    let limit_str = Text::new("Maximum results:")
        .with_default("100")
        .prompt()?;
    let limit: i32 = limit_str.parse().unwrap_or(100);

    // Build query
    let query = SearchQuery {
        text: text.clone(),
        sources: selected_sources.clone(),
        event_types: selected_types.clone(),
        start_time: Some(since),
        end_time: until,
        limit,
        offset: 0,
    };

    // Show equivalent CLI command
    println!();
    println!("{}", style("Equivalent CLI command:").dim());
    print!("  sinexctl query");
    if let Some(ref t) = text {
        print!(" -q '{}'", t);
    }
    for src in &selected_sources {
        print!(" --source {}", src);
    }
    for et in &selected_types {
        print!(" --event-type {}", et);
    }
    // Convert time to CLI arg format
    let since_arg = match time_choice {
        "Last 15 minutes" => "15m".to_string(),
        "Last hour" => "1h".to_string(),
        "Last 6 hours" => "6h".to_string(),
        "Last 24 hours" => "24h".to_string(),
        "Last 7 days" => "7d".to_string(),
        "Last 30 days" => "30d".to_string(),
        _ => since.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    };
    print!(" -s {}", since_arg);
    if limit != 100 {
        print!(" -n {}", limit);
    }
    println!();
    println!();

    // Execute query
    execute_query(client, query, format).await
}

/// Parse preset time ranges
fn parse_preset_time(preset: &str) -> DateTime<Utc> {
    let now = Utc::now();
    match preset {
        "Last 15 minutes" => now - Duration::minutes(15),
        "Last hour" => now - Duration::hours(1),
        "Last 6 hours" => now - Duration::hours(6),
        "Last 24 hours" => now - Duration::hours(24),
        "Last 7 days" => now - Duration::days(7),
        "Last 30 days" => now - Duration::days(30),
        _ => now - Duration::hours(1), // Default fallback
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
        let naive_datetime = naive_date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid date: {}", s))?;
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
        assert_eq!(
            truncate_string("this is a very long string", 10),
            "this is..."
        );
    }
}
