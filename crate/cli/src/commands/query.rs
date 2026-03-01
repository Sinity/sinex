use clap::Args;
use console::style;
use inquire::{MultiSelect, Select, Text};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::query::{
    EventQuery, EventQueryResult, PayloadFilter, QueryResultEvent, SortDirection, TimeRange,
};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::utils::timestamp_helpers::parse_relative_duration;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

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
    source: Vec<EventSource>,

    /// Filter by event type (can be specified multiple times)
    #[arg(long)]
    event_type: Vec<EventType>,

    /// Time range start: "1h", "2d", "2025-01-15", "2025-01-15T10:00:00Z"
    #[arg(long, short = 's')]
    since: Option<String>,

    /// Time range end (default: now)
    #[arg(long, short = 'u')]
    until: Option<String>,

    /// Maximum number of results
    #[arg(long, short = 'n', default_value = "100")]
    limit: i64,

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

        let start_time = self.since.as_ref().map(|s| parse_time(s)).transpose()?;
        let end_time = self.until.as_ref().map(|s| parse_time(s)).transpose()?;
        let time_range = make_time_range(start_time, end_time)?;

        let query = EventQuery {
            sources: self.source.clone(),
            event_types: self.event_type.clone(),
            time_range,
            payload: self
                .query
                .as_ref()
                .map(|t| PayloadFilter::TextSearch { text: t.clone() }),
            limit: self.limit,
            direction: SortDirection::Desc,
            ..Default::default()
        };

        execute_query(client, query, self.format).await
    }
}

/// Create a TimeRange from optional start and end timestamps
fn make_time_range(start: Option<Timestamp>, end: Option<Timestamp>) -> Result<Option<TimeRange>> {
    if start.is_none() && end.is_none() {
        return Ok(None);
    }
    Ok(Some(TimeRange::new(start, end)?))
}

/// Execute a query and display results
async fn execute_query(
    client: &GatewayClient,
    query: EventQuery,
    format: OutputFormat,
) -> Result<()> {
    let result = client.query_events(query).await?;
    match result {
        EventQueryResult::Events { events, .. } => {
            CommandOutput::list(events, "No events found.", format_table_results)
                .display(&format)?;
        }
        other => {
            // Aggregation results — just serialize as JSON
            let json = serde_json::to_string_pretty(&other)?;
            println!("{json}");
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

    let (since, until) = if time_choice == "Custom range..." {
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
    } else {
        let since_time = parse_preset_time(time_choice);
        (since_time, None)
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
                nodes.iter().map(|n| n.node_type.to_string()).collect()
            }
        }
        Err(_) => default_sources,
    };

    let selected_sources =
        MultiSelect::new("Sources (Space to select, Enter to confirm):", sources)
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

    let selected_types = MultiSelect::new(
        "Event types (Space to select, Enter to confirm):",
        event_types,
    )
    .with_help_message("Leave empty to search all event types")
    .prompt_skippable()?
    .unwrap_or_default();

    // Full-text search
    let text = Text::new("Full-text search (optional):")
        .with_help_message("Search across all event fields")
        .prompt_skippable()?
        .filter(|s| !s.is_empty());

    // Limit
    let limit_str = Text::new("Maximum results:").with_default("100").prompt()?;
    let limit: i64 = limit_str.parse().unwrap_or(100);

    // Build query
    let time_range = make_time_range(Some(since), until)?;
    let query = EventQuery {
        sources: selected_sources
            .iter()
            .map(|s| EventSource::new(s.clone()))
            .collect(),
        event_types: selected_types
            .iter()
            .map(|t| EventType::new(t.clone()))
            .collect(),
        time_range,
        payload: text
            .as_ref()
            .map(|t| PayloadFilter::TextSearch { text: t.clone() }),
        limit,
        direction: SortDirection::Desc,
        ..Default::default()
    };

    // Show equivalent CLI command
    println!();
    println!("{}", style("Equivalent CLI command:").dim());
    print!("  sinexctl query");
    if let Some(ref t) = text {
        print!(" -q '{t}'");
    }
    for src in &selected_sources {
        print!(" --source {src}");
    }
    for et in &selected_types {
        print!(" --event-type {et}");
    }
    // Convert time to CLI arg format
    let since_arg = match time_choice {
        "Last 15 minutes" => "15m".to_string(),
        "Last hour" => "1h".to_string(),
        "Last 6 hours" => "6h".to_string(),
        "Last 24 hours" => "24h".to_string(),
        "Last 7 days" => "7d".to_string(),
        "Last 30 days" => "30d".to_string(),
        _ => since
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "invalid".to_string()),
    };
    print!(" -s {since_arg}");
    if limit != 100 {
        print!(" -n {limit}");
    }
    println!();
    println!();

    // Execute query
    execute_query(client, query, format).await
}

/// Parse preset time ranges
fn parse_preset_time(preset: &str) -> Timestamp {
    let now = Timestamp::now();
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

/// Parse time string into Timestamp
/// Supports:
/// - Relative: "1h", "2d", "30m", "1w"
/// - Absolute: "2025-01-15", "2025-01-15T10:00:00Z"
#[allow(clippy::expect_used)]
fn parse_time(s: &str) -> Result<Timestamp> {
    // Try relative time first using sinex-primitives's parse_relative_duration
    if let Some(time_duration) = parse_relative_duration(s) {
        return Ok(Timestamp::now() - time_duration);
    }

    // Try absolute timestamp
    if let Ok(ts) = Timestamp::parse_rfc3339(s) {
        return Ok(ts);
    }

    // Try date-only format (YYYY-MM-DD)
    if let Ok(date) =
        time::Date::parse(s, time::macros::format_description!("[year]-[month]-[day]"))
    {
        return Ok(Timestamp::from(
            date.with_hms(0, 0, 0)
                .expect("midnight is always valid")
                .assume_utc(),
        ));
    }

    Err(color_eyre::eyre::eyre!(
        "Invalid time format: '{}'\nSupported formats:\n  Relative: 1h, 2d, 30m, 1w\n  Absolute: 2025-01-15, 2025-01-15T10:00:00Z",
        s
    ))
}

/// Format search results as a table
fn format_table_results(results: &[QueryResultEvent]) -> String {
    use console::style;
    use tabled::{builder::Builder, settings::Style};

    let mut builder = Builder::new();
    builder.push_record(["TIMESTAMP", "SOURCE", "EVENT TYPE", "HOST", "SNIPPET"]);

    for result in results {
        let timestamp = result
            .event
            .ts_orig
            .map(|ts| {
                ts.format(time::macros::format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                ))
                .unwrap_or_else(|_| "invalid".to_string())
            })
            .unwrap_or_else(|| "unknown".to_string());
        let snippet = result.snippet.as_deref().unwrap_or("");
        let snippet = truncate_string(snippet, 60);

        builder.push_record([
            style(timestamp).dim().to_string(),
            result.event.source.to_string(),
            result.event.event_type.to_string(),
            style(result.event.host.as_str()).dim().to_string(),
            snippet,
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

/// Truncate string to max length with ellipsis, stopping at character boundaries.
fn truncate_string(s: &str, max_len: usize) -> String {
    // Reserve 3 characters for "..." in the truncated case.
    let cutoff = max_len.saturating_sub(3);
    match s.char_indices().nth(max_len) {
        None => s.to_string(),
        Some(_) => match s.char_indices().nth(cutoff) {
            None => s.to_string(),
            Some((byte_pos, _)) => format!("{}...", &s[..byte_pos]),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use sinex_primitives::temporal::Duration;
    use xtask::sandbox::{sinex_proptest, sinex_test};

    #[sinex_test]
    fn test_parse_relative_duration() -> TestResult<()> {
        // Tests for sinex-primitives's parse_relative_duration integrated via parse_time
        assert_eq!(parse_relative_duration("1h"), Some(Duration::hours(1)));
        assert_eq!(parse_relative_duration("2d"), Some(Duration::days(2)));
        assert_eq!(parse_relative_duration("30m"), Some(Duration::minutes(30)));
        assert_eq!(parse_relative_duration("1w"), Some(Duration::weeks(1)));
        assert_eq!(parse_relative_duration("15s"), Some(Duration::seconds(15)));

        // Alternative forms
        assert_eq!(parse_relative_duration("1hour"), Some(Duration::hours(1)));
        assert_eq!(parse_relative_duration("2days"), Some(Duration::days(2)));

        // Invalid
        assert_eq!(parse_relative_duration("invalid"), None);
        assert_eq!(parse_relative_duration(""), None);
        Ok(())
    }

    #[sinex_test]
    fn test_parse_absolute_time() -> TestResult<()> {
        let result = parse_time("2025-01-15T10:00:00Z");
        assert!(result.is_ok());

        let result = parse_time("2025-01-15");
        assert!(result.is_ok());
        Ok(())
    }

    #[sinex_test]
    fn test_truncate_string() -> TestResult<()> {
        assert_eq!(truncate_string("short", 10), "short");
        assert_eq!(
            truncate_string("this is a very long string", 10),
            "this is..."
        );
        Ok(())
    }

    // Property tests for time parsing
    sinex_proptest! {
        fn prop_relative_hours_parses(hours in 1i64..1000) {
            let input = format!("{hours}h");
            let result = parse_relative_duration(&input);
            prop_assert_eq!(result, Some(Duration::hours(hours)));
            Ok(())
        }

        fn prop_relative_days_parses(days in 1i64..365) {
            let input = format!("{days}d");
            let result = parse_relative_duration(&input);
            prop_assert_eq!(result, Some(Duration::days(days)));
            Ok(())
        }

        fn prop_relative_minutes_parses(mins in 1i64..10000) {
            let input = format!("{mins}m");
            let result = parse_relative_duration(&input);
            prop_assert_eq!(result, Some(Duration::minutes(mins)));
            Ok(())
        }

        fn prop_relative_seconds_parses(secs in 1i64..100000) {
            let input = format!("{secs}s");
            let result = parse_relative_duration(&input);
            prop_assert_eq!(result, Some(Duration::seconds(secs)));
            Ok(())
        }

        fn prop_relative_weeks_parses(weeks in 1i64..52) {
            let input = format!("{weeks}w");
            let result = parse_relative_duration(&input);
            prop_assert_eq!(result, Some(Duration::weeks(weeks)));
            Ok(())
        }

        fn prop_truncate_preserves_short_strings(s in ".{0,10}") {
            let result = truncate_string(&s, 10);
            if s.chars().count() <= 10 {
                prop_assert_eq!(result, s);
            }
            Ok(())
        }

        fn prop_truncate_adds_ellipsis_to_long_strings(s in ".{15,100}") {
            let result = truncate_string(&s, 10);
            prop_assert!(result.ends_with("..."));
            prop_assert!(result.chars().count() <= 10);
            Ok(())
        }

        fn prop_truncate_never_exceeds_max_len(s in ".*", max_len in 5usize..100) {
            let result = truncate_string(&s, max_len);
            prop_assert!(result.chars().count() <= max_len);
            Ok(())
        }

        fn prop_relative_duration_with_long_form_hour(hours in 1i64..100) {
            let input = format!("{hours}hour");
            let result = parse_relative_duration(&input);
            prop_assert_eq!(result, Some(Duration::hours(hours)));

            let input_plural = format!("{hours}hours");
            let result_plural = parse_relative_duration(&input_plural);
            prop_assert_eq!(result_plural, Some(Duration::hours(hours)));
            Ok(())
        }

        fn prop_relative_duration_with_long_form_day(days in 1i64..100) {
            let input = format!("{days}day");
            let result = parse_relative_duration(&input);
            prop_assert_eq!(result, Some(Duration::days(days)));

            let input_plural = format!("{days}days");
            let result_plural = parse_relative_duration(&input_plural);
            prop_assert_eq!(result_plural, Some(Duration::days(days)));
            Ok(())
        }

        fn prop_parse_time_relative_produces_past_datetime(hours in 1i64..100) {
            let input = format!("{hours}h");
            let now = Timestamp::now();
            let result = parse_time(&input).unwrap();
            // Result should be in the past
            prop_assert!(result < now);
            // And approximately hours ago (within 1 second tolerance)
            let expected = now - Duration::hours(hours);
            let diff = (result - expected).whole_seconds().abs();
            prop_assert!(diff < 2, "Time difference too large: {} seconds", diff);
            Ok(())
        }

        fn prop_valid_rfc3339_parses(
            year in 2020i32..2030,
            month in 1u32..=12,
            day in 1u32..=28,  // Safe for all months
            hour in 0u32..24,
            minute in 0u32..60,
            second in 0u32..60
        ) {
            let input = format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z"
            );
            let result = parse_time(&input);
            prop_assert!(result.is_ok(), "Failed to parse: {}", input);
            Ok(())
        }

        fn prop_valid_date_only_parses(
            year in 2020i32..2030,
            month in 1u32..=12,
            day in 1u32..=28  // Safe for all months
        ) {
            let input = format!("{year:04}-{month:02}-{day:02}");
            let result = parse_time(&input);
            prop_assert!(result.is_ok(), "Failed to parse: {}", input);
            Ok(())
        }
    }

    #[sinex_test]
    fn test_invalid_time_formats() -> TestResult<()> {
        // Invalid formats should fail
        assert!(parse_time("not-a-date").is_err());
        assert!(parse_time("2025/01/15").is_err()); // Wrong separator
        assert!(parse_time("15-01-2025").is_err()); // Wrong order
        assert!(parse_time("").is_err()); // Empty

        // But these should work
        assert!(parse_time("1h").is_ok());
        assert!(parse_time("2d").is_ok());
        assert!(parse_time("2025-01-15").is_ok());
        assert!(parse_time("2025-01-15T10:00:00Z").is_ok());
        Ok(())
    }

    #[sinex_test]
    fn test_preset_time_ranges() -> TestResult<()> {
        let now = Timestamp::now();

        // Each preset should return a time in the past
        let presets = [
            "Last 15 minutes",
            "Last hour",
            "Last 6 hours",
            "Last 24 hours",
            "Last 7 days",
            "Last 30 days",
        ];

        for preset in presets {
            let result = parse_preset_time(preset);
            assert!(result < now, "Preset '{preset}' should return past time");
        }

        // Verify approximate durations
        let hour_ago = parse_preset_time("Last hour");
        let diff = (now - hour_ago).whole_minutes();
        assert!(
            (58..=62).contains(&diff),
            "Last hour should be ~60 mins ago, got {diff}"
        );
        Ok(())
    }
}
