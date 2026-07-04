// Tests construct known-valid timestamps and parse payloads that are built
// in the same fixture.
#![allow(clippy::expect_used)]
use super::*;
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use std::collections::BTreeMap;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn format_duration_compact_handles_hours_minutes_and_seconds() -> TestResult<()> {
    assert_eq!(format_duration_compact_secs(47), "47s");
    assert_eq!(format_duration_compact_secs(120), "2m");
    assert_eq!(format_duration_compact_secs(198 * 60), "3h 18m");
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
            ts_quality: None,
            host: sinex_primitives::events::builder::get_hostname(),
            module_run_id: None,
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
            automaton_model: None,
            anchor_payload_hash: None,
        },
        relevance_score: None,
        snippet: None,
    };

    let parsed = parse_session_event(event).expect("boundary payload should parse");
    assert_eq!(parsed.primary_source, ActivitySourceKind::Terminal);
    assert_eq!(parsed.duration_secs, 2520);
    Ok(())
}

#[sinex_test]
async fn report_envelope_names_zero_event_window_as_unmeasurable() -> TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let end = start + time::Duration::hours(1);
    let time_range = TimeRange::new(Some(start), Some(end))?;
    let report = ActivityReportView::new(
        "quiet-window",
        time_range,
        0,
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let envelope = report_envelope(report);
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must render");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.metrics.report");
    assert_eq!(parsed["payload"]["schema_version"], REPORT_SCHEMA_VERSION);
    assert_eq!(parsed["payload"]["label"], "quiet-window");
    assert_eq!(parsed["payload"]["total_events"], 0);
    assert_eq!(parsed["caveats"][0]["id"], "coverage.unmeasurable");
    assert!(
        parsed["caveats"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("source readiness")),
        "zero-event report must say why absence is not source-coverage proof"
    );
    Ok(())
}

#[sinex_test]
async fn report_envelope_omits_caveat_for_non_empty_window() -> TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let end = start + time::Duration::hours(1);
    let time_range = TimeRange::new(Some(start), Some(end))?;
    let report = ActivityReportView::new(
        "active-window",
        time_range,
        7,
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let envelope = report_envelope(report);
    assert!(
        envelope.caveats.is_empty(),
        "non-empty report window should not invent readiness caveats"
    );
    Ok(())
}

#[sinex_test]
async fn calendar_envelope_names_zero_days_as_unmeasurable() -> TestResult<()> {
    let calendar = ActivityCalendarView {
        schema_version: CALENDAR_SCHEMA_VERSION.to_string(),
        start_date: "2026-07-01".to_string(),
        end_date: "2026-07-02".to_string(),
        days: vec![
            ActivityCalendarDayView {
                date: "2026-07-01".to_string(),
                total_events: 5,
                top_sources: Vec::new(),
            },
            ActivityCalendarDayView {
                date: "2026-07-02".to_string(),
                total_events: 0,
                top_sources: Vec::new(),
            },
        ],
    };

    let envelope = calendar_envelope(calendar);
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must render");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(
        parsed["source_surface"],
        "sinexctl.metrics.report.calendar"
    );
    assert_eq!(parsed["payload"]["schema_version"], CALENDAR_SCHEMA_VERSION);
    assert_eq!(parsed["payload"]["days"].as_array().map(Vec::len), Some(2));
    assert_eq!(parsed["caveats"][0]["id"], "coverage.unmeasurable");
    assert!(
        parsed["caveats"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("1 zero-event day")),
        "calendar caveat must count zero-event days"
    );
    Ok(())
}
