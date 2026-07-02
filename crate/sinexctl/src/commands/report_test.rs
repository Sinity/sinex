// Tests construct known-valid timestamps and parse payloads that are built
// in the same fixture.
#![allow(clippy::expect_used)]
use super::*;
use sinex_primitives::activity::ActivitySourceKind;
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
