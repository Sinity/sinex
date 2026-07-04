use super::*;
use crate::runtime::Transducer;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::events::payloads::ActivityWindowCloseReason;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp};
use std::collections::BTreeMap;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn attention_stream_consumes_activity_windows_only() -> xtask::sandbox::TestResult<()> {
    let automaton = AttentionStream;

    assert_eq!(
        automaton.input_event_type(),
        ActivityWindowSummaryPayload::EVENT_TYPE.as_static_str()
    );
    assert_eq!(automaton.output_event_type(), "attention.span");
    assert_eq!(automaton.output_event_source(), "derived.attention-stream");
    assert_eq!(
        automaton.input_provenance_filter(),
        InputProvenanceFilter::SynthesizedOnly
    );
    Ok(())
}

#[sinex_test]
async fn attention_stream_maps_activity_window_to_attention_span(
) -> xtask::sandbox::TestResult<()> {
    let start_time = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end_time = Timestamp::from_unix_timestamp(1_700_000_120)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let mut counts = BTreeMap::new();
    counts.insert(ActivitySourceKind::Window, 3);
    counts.insert(ActivitySourceKind::Terminal, 2);

    let input = ActivityWindowSummaryPayload {
        window_id: "activity-window-42".to_string(),
        window_start: start_time,
        window_end: end_time,
        duration_secs: 120,
        event_count: 5,
        source_count: 2,
        sources: vec!["wm.hyprland".to_string(), "terminal.kitty".to_string()],
        activity_sources: vec![ActivitySourceKind::Window, ActivitySourceKind::Terminal],
        activity_source_counts: counts.clone(),
        primary_source: ActivitySourceKind::Window,
        close_reason: ActivityWindowCloseReason::Gap,
    };
    let context = activity_window_context(end_time);
    let parent_id = context.trigger_uuid();

    let output = AttentionStream
        .process(&mut (), input, &context)
        .await?
        .expect("activity window should produce an attention span");

    assert_eq!(output.ts_orig, end_time);
    assert_eq!(output.source_event_ids, vec![parent_id]);
    assert_eq!(output.semantics_version.as_deref(), Some("1.0.0"));
    assert_eq!(
        output.equivalence_key.as_deref(),
        Some("attention-span:activity-window-42")
    );
    assert_eq!(output.payload.span_id, "attention-span:activity-window-42");
    assert_eq!(output.payload.start_time, start_time);
    assert_eq!(output.payload.end_time, end_time);
    assert_eq!(output.payload.duration_secs, 120);
    assert_eq!(output.payload.event_count, 5);
    assert_eq!(output.payload.source_count, 2);
    assert_eq!(output.payload.sources, vec!["wm.hyprland", "terminal.kitty"]);
    assert_eq!(output.payload.activity_source_counts, counts);
    assert_eq!(output.payload.primary_source, ActivitySourceKind::Window);
    assert_eq!(output.payload.source_window_id, "activity-window-42");
    assert_eq!(
        output.payload.source_window_close_reason,
        ActivityWindowCloseReason::Gap
    );
    Ok(())
}

fn activity_window_context(ts_orig: Timestamp) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("derived.activity-window"),
        event_type: EventType::from_static("activity.window.summary"),
        ts_orig: Some(ts_orig),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}
