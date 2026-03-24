use clap::Args;
use console::style;
use inquire::{Select, Text};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::query::{
    EventQuery, EventQueryResult, PayloadFilter, QueryResultEvent, SortDirection, TimeRange,
};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::validation::query_validation::{self, DEFAULT_MAX_LIMIT};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;
use crate::validation::parse_time_input;

/// Query/search events
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Search all events from last hour
    sinexctl query -s 1h

    # Full-text search for 'error'
    sinexctl query -q error -s 24h

    # Filter by source and event type
    sinexctl query --source shell.atuin --event-type shell.command -s 2d

    # Search within a date range
    sinexctl query -s 2025-01-10 -u 2025-01-15

    # Multiple sources (OR filter)
    sinexctl query --source shell.atuin --source desktop.hyprland -s 1d

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

    /// Filter to synthesis events (those with provenance lineage)
    #[arg(long)]
    has_lineage: bool,

    /// Filter to material events (those without provenance lineage)
    #[arg(long, conflicts_with = "has_lineage")]
    no_lineage: bool,

    /// Maximum number of results
    #[arg(long, short = 'n', default_value = "100", value_parser = parse_query_limit_arg)]
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

        let has_lineage = if self.has_lineage {
            Some(true)
        } else if self.no_lineage {
            Some(false)
        } else {
            None
        };

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
            has_lineage,
            ..Default::default()
        };

        execute_query(client, query, self.format).await
    }
}

/// Create a `TimeRange` from optional start and end timestamps
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
        let since_time = parse_preset_time(time_choice)?;
        (since_time, None)
    };

    let selected_sources = Text::new("Sources (comma-separated, optional):")
        .with_help_message(
            "Examples: shell.atuin, desktop.hyprland, system.journal. Leave empty to search all sources.",
        )
        .prompt_skippable()?
        .map(|input| parse_event_sources(&input))
        .transpose()?
        .unwrap_or_default();

    let selected_types = Text::new("Event types (comma-separated, optional):")
        .with_help_message(
            "Examples: shell.command, window.focused, file.created. Leave empty to search all event types.",
        )
        .prompt_skippable()?
        .map(|input| parse_event_types(&input))
        .transpose()?
        .unwrap_or_default();

    // Full-text search
    let text = Text::new("Full-text search (optional):")
        .with_help_message("Search across all event fields")
        .prompt_skippable()?
        .filter(|s| !s.is_empty());

    // Limit
    let limit_str = Text::new("Maximum results:").with_default("100").prompt()?;
    let limit = parse_query_limit_arg(&limit_str)
        .map_err(|error| color_eyre::eyre::eyre!(error))?;

    // Build query
    let time_range = make_time_range(Some(since), until)?;
    let query = EventQuery {
        sources: selected_sources.clone(),
        event_types: selected_types.clone(),
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

fn parse_csv_values(input: &str) -> Vec<String> {
    let mut values = Vec::new();
    for part in input.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !values.iter().any(|existing| existing == trimmed) {
            values.push(trimmed.to_string());
        }
    }
    values
}

fn parse_event_sources(input: &str) -> Result<Vec<EventSource>> {
    parse_csv_values(input)
        .into_iter()
        .map(EventSource::new)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn parse_event_types(input: &str) -> Result<Vec<EventType>> {
    parse_csv_values(input)
        .into_iter()
        .map(EventType::new)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn parse_query_limit_arg(input: &str) -> std::result::Result<i64, String> {
    let parsed: i64 = input
        .parse()
        .map_err(|_| format!("limit must be an integer, got {input:?}"))?;
    if parsed <= 0 {
        return Err(format!("limit must be between 1 and {DEFAULT_MAX_LIMIT}"));
    }
    let parsed_u32 = u32::try_from(parsed)
        .map_err(|_| format!("limit must be between 1 and {DEFAULT_MAX_LIMIT}"))?;
    query_validation::validate_limit(parsed_u32, DEFAULT_MAX_LIMIT)
        .map_err(|error| error.to_string())?;
    Ok(i64::from(parsed_u32))
}

/// Parse preset time ranges
fn parse_preset_time(preset: &str) -> Result<Timestamp> {
    let now = Timestamp::now();
    match preset {
        "Last 15 minutes" => Ok(now - Duration::minutes(15)),
        "Last hour" => Ok(now - Duration::hours(1)),
        "Last 6 hours" => Ok(now - Duration::hours(6)),
        "Last 24 hours" => Ok(now - Duration::hours(24)),
        "Last 7 days" => Ok(now - Duration::days(7)),
        "Last 30 days" => Ok(now - Duration::days(30)),
        _ => Err(color_eyre::eyre::eyre!(
            "unsupported preset time range: {preset}"
        )),
    }
}

/// Parse time string into Timestamp
/// Supports:
/// - Relative: "1h", "2d", "30m", "1w"
/// - Absolute: "2025-01-15", "2025-01-15T10:00:00Z"
#[allow(clippy::expect_used)]
fn parse_time(s: &str) -> Result<Timestamp> {
    parse_time_input(s)
}

/// Format search results as a table
fn format_table_results(results: &[QueryResultEvent]) -> String {
    use console::style;
    use tabled::{builder::Builder, settings::Style};

    let mut builder = Builder::new();
    builder.push_record(["TIMESTAMP", "SOURCE", "EVENT TYPE", "HOST", "SNIPPET"]);

    for result in results {
        let timestamp = result.event.ts_orig.map_or_else(
            || "unknown".to_string(),
            |ts| {
                ts.format(time::macros::format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                ))
                .unwrap_or_else(|_| "invalid".to_string())
            },
        );
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
    use sinex_primitives::utils::timestamp_helpers::parse_relative_duration;
    use xtask::sandbox::{sinex_proptest, sinex_test};

    #[sinex_test]
    async fn test_parse_relative_duration() -> TestResult<()> {
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
    async fn test_parse_absolute_time() -> TestResult<()> {
        let result = parse_time("2025-01-15T10:00:00Z");
        assert!(result.is_ok());

        let result = parse_time("2025-01-15");
        assert!(result.is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn test_truncate_string() -> TestResult<()> {
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
    async fn test_invalid_time_formats() -> TestResult<()> {
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
    async fn test_parse_query_limit_rejects_zero() -> TestResult<()> {
        let err = parse_query_limit_arg("0").expect_err("zero limit should be rejected");
        assert!(err.contains("between 1"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_csv_values_dedupes_and_trims() -> TestResult<()> {
        let parsed = parse_csv_values(" shell.command,window.focused, shell.command ,, ");
        assert_eq!(
            parsed,
            vec!["shell.command".to_string(), "window.focused".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_preset_time_ranges() -> TestResult<()> {
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
            let result = parse_preset_time(preset)?;
            assert!(result < now, "Preset '{preset}' should return past time");
        }

        // Verify approximate durations
        let hour_ago = parse_preset_time("Last hour")?;
        let diff = (now - hour_ago).whole_minutes();
        assert!(
            (58..=62).contains(&diff),
            "Last hour should be ~60 mins ago, got {diff}"
        );

        assert!(parse_preset_time("Invalid preset").is_err());
        Ok(())
    }
}
