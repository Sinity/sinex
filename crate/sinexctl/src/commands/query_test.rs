// Test fixtures use proptest fallible assertions and intentionally unwrap
// values generated inside the same test case.
#![allow(clippy::unwrap_used)]
use super::*;
use proptest::prelude::*;
use sinex_primitives::temporal::Duration;
use sinex_primitives::testing::event_fixture;
use sinex_primitives::utils::timestamp_helpers::parse_relative_duration;
use sinex_primitives::views::{
    EVENT_CARD_LIST_SCHEMA_VERSION, EVENT_QUERY_LIST_SCHEMA_VERSION,
    VIEW_ENVELOPE_SCHEMA_VERSION,
};
use sinex_primitives::{Event, Id, JsonValue, Uuid};
use xtask::TestResult;
use xtask::sandbox::{sinex_proptest, sinex_test};

fn render_count_result(format: OutputFormat) -> TestResult<String> {
    render_non_event_query_result(&EventQueryResult::Count { count: 7 }, format)
}

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
async fn render_non_event_query_result_respects_json_format() -> TestResult<()> {
    let rendered = render_count_result(OutputFormat::Json)?;
    let value: serde_json::Value = serde_json::from_str(&rendered)?;
    assert_eq!(value["count"], serde_json::json!(7));
    Ok(())
}

#[sinex_test]
async fn render_non_event_query_result_respects_yaml_format() -> TestResult<()> {
    let rendered = render_count_result(OutputFormat::Yaml)?;
    let value: serde_yml::Value = serde_yml::from_str(&rendered)?;
    assert_eq!(value["count"], serde_yml::Value::from(7));
    Ok(())
}

#[sinex_test]
async fn render_non_event_query_result_uses_table_renderer_for_counts() -> TestResult<()> {
    let rendered = render_count_result(OutputFormat::Table)?;
    assert!(rendered.contains("Count"));
    assert!(rendered.contains('7'));
    Ok(())
}

#[sinex_test]
async fn render_query_result_preserves_event_pagination_metadata_in_json() -> TestResult<()> {
    let mut event = event_fixture(
        sinex_primitives::EventSource::from_static("test"),
        sinex_primitives::EventType::from_static("test.event"),
        serde_json::json!({"message": "hello"}),
    );
    let event_id = Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7());
    event.id = Some(event_id);

    let rendered = render_query_result(
        &EventQueryResult::Events {
            events: vec![QueryResultEvent {
                event,
                relevance_score: Some(0.75),
                snippet: Some("hello".to_string()),
            }],
            next_cursor: Some(sinex_primitives::query::Cursor::after_id(event_id)),
            total_estimate: Some(42),
        },
        OutputFormat::Json,
        Some(serde_json::json!({ "limit": 1 })),
    )?;
    let value: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.events.query");
    assert_eq!(value["query_echo"]["limit"], serde_json::json!(1));
    assert_eq!(
        value["payload"]["schema_version"],
        EVENT_QUERY_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["total_estimate"], serde_json::json!(42));
    assert!(value["payload"]["next_cursor"]["after"]["id"].is_string());
    assert_eq!(value["payload"]["cards"].as_array().map(Vec::len), Some(1));
    assert_eq!(value["payload"]["cards"][0]["event_type"], "test.event");
    Ok(())
}

#[sinex_test]
async fn render_disclosed_event_cards_preserves_pagination_metadata() -> TestResult<()> {
    let mut event = event_fixture(
        sinex_primitives::EventSource::from_static("test"),
        sinex_primitives::EventType::from_static("test.event"),
        serde_json::json!({"message": "hello"}),
    );
    let event_id = Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7());
    event.id = Some(event_id);
    let view = EventCardListView::from_query_events_with_metadata(
        &[QueryResultEvent {
            event,
            relevance_score: Some(0.75),
            snippet: Some("hello".to_string()),
        }],
        Some(sinex_primitives::query::Cursor::after_id(event_id)),
        Some(42),
    );

    let rendered = render_event_card_query_result(
        &view,
        OutputFormat::Json,
        Some(serde_json::json!({ "limit": 1 })),
    )?;
    let value: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.events.query");
    assert_eq!(value["query_echo"]["limit"], serde_json::json!(1));
    assert_eq!(
        value["payload"]["schema_version"],
        EVENT_CARD_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["total_estimate"], serde_json::json!(42));
    assert!(value["payload"]["next_cursor"]["after"]["id"].is_string());
    assert_eq!(value["payload"]["cards"].as_array().map(Vec::len), Some(1));
    assert_eq!(value["payload"]["cards"][0]["event_type"], "test.event");
    Ok(())
}

#[sinex_test]
async fn render_query_result_emits_event_cards_as_ndjson() -> TestResult<()> {
    let mut event = event_fixture(
        sinex_primitives::EventSource::from_static("test"),
        sinex_primitives::EventType::from_static("test.event"),
        serde_json::json!({"message": "hello"}),
    );
    event.id = Some(Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7()));

    let rendered = render_query_result(
        &EventQueryResult::Events {
            events: vec![QueryResultEvent {
                event,
                relevance_score: None,
                snippet: Some("hello".to_string()),
            }],
            next_cursor: None,
            total_estimate: None,
        },
        OutputFormat::Ndjson,
        None,
    )?;
    let lines: Vec<&str> = rendered.lines().collect();
    assert_eq!(lines.len(), 1);
    let value: serde_json::Value = serde_json::from_str(lines[0])?;
    assert_eq!(value["event_type"], "test.event");
    assert!(value.get("ref").is_some());
    assert!(
        value.get("schema_version").is_none(),
        "ndjson should emit item cards without envelope metadata"
    );
    Ok(())
}

#[sinex_test]
async fn format_event_query_result_table_shows_cursor_and_total_estimate() -> TestResult<()> {
    let mut event = event_fixture(
        sinex_primitives::EventSource::from_static("test"),
        sinex_primitives::EventType::from_static("test.event"),
        serde_json::json!({"message": "hello"}),
    );
    let event_id = Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7());
    event.id = Some(event_id);

    let table = format_event_query_result_table(
        &[QueryResultEvent {
            event,
            relevance_score: None,
            snippet: Some("hello".to_string()),
        }],
        Some(&sinex_primitives::query::Cursor::after_id(event_id)),
        Some(12),
    );

    assert!(table.contains("Displayed 1 event(s)."));
    assert!(table.contains("Approximate total matches: 12."));
    assert!(table.contains("Next cursor:"));
    assert!(table.contains("--cursor-json"));
    Ok(())
}

#[sinex_test]
async fn parse_cursor_json_rejects_invalid_json() -> TestResult<()> {
    let error = parse_cursor_json("{not json]").expect_err("invalid cursor JSON should fail");
    assert!(error.to_string().contains("invalid cursor JSON"));
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
