use super::*;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::{EventSource, EventType, Id};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn analytics_filters_to_trusted_activity_event_types() -> xtask::sandbox::TestResult<()> {
    let automaton = AnalyticsAutomaton::default();

    assert_eq!(automaton.input_event_type(), "*");
    assert_eq!(
        automaton.input_event_types(),
        vec![
            HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str(),
            ActivityWatchWindowActivePayload::EVENT_TYPE.as_static_str(),
            ActivityWatchBrowserTabActivePayload::EVENT_TYPE.as_static_str(),
            PageVisitedPayload::EVENT_TYPE.as_static_str(),
            KittyCommandExecutedPayload::EVENT_TYPE.as_static_str(),
        ]
    );
    assert_eq!(
        automaton.input_provenance_filter(),
        InputProvenanceFilter::MaterialOnly
    );
    Ok(())
}

#[sinex_test]
async fn analytics_default_window_budget_bounds_parent_fan_in(
) -> xtask::sandbox::TestResult<()> {
    let mut automaton = AnalyticsAutomaton::default();
    let mut state = AnalyticsState::default();
    let event_time = Timestamp::now();

    for _ in 0..DEFAULT_WINDOW_MAX_EVENTS {
        let context = trusted_window_context(event_time);
        automaton.accumulate(&mut state, JsonValue::Null, &context).await?;
        assert!(
            !automaton.window_complete(&state),
            "window should accept exactly the default budget before closing"
        );
    }

    let overflow_context = trusted_window_context(event_time);
    automaton
        .accumulate(&mut state, JsonValue::Null, &overflow_context)
        .await?;
    assert!(
        automaton.window_complete(&state),
        "the event after the budget should close the current window"
    );

    let flush_context = AutomatonContext::timer_flush(event_time)?;
    let output = automaton
        .emit(&mut state, &flush_context)
        .await?
        .expect("closed window should emit a summary");

    assert_eq!(
        output.source_event_ids.len(),
        DEFAULT_WINDOW_MAX_EVENTS,
        "default analytics windows should not emit more parents than the warning budget"
    );
    assert_eq!(
        output
            .aggregation
            .as_ref()
            .expect("analytics output should carry aggregation metadata")
            .total_input_count,
        DEFAULT_WINDOW_MAX_EVENTS as u64
    );
    assert_eq!(
        state.event_count, 1,
        "overflow event should seed the next window instead of joining the emitted window"
    );
    assert_eq!(
        state
            .event_ids
            .first()
            .copied()
            .expect("next window seed should keep the overflow event id"),
        overflow_context.trigger_uuid()
    );
    Ok(())
}

fn trusted_window_context(event_time: Timestamp) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("wm.hyprland"),
        event_type: EventType::from_static("window.focused"),
        ts_orig: Some(event_time),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

/// sinex-5s6: an activity window must close after `gap_threshold` of quiet
/// WITHOUT a next event arriving — the clock-driven flush path. Before this, the
/// final window of any activity bout was unqueryable until future activity, and
/// `flush_due` defaulted false for analytics. Feeds a bout, then asserts the
/// flush watermark closes it (as a `Gap`) once the quiet exceeds the threshold,
/// and NOT before.
#[sinex_test]
async fn analytics_window_closes_on_quiet_via_flush() -> xtask::sandbox::TestResult<()> {
    let mut automaton = AnalyticsAutomaton::default();
    let mut state = AnalyticsState::default();
    let start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");

    // A short bout of three events, one minute apart.
    for i in 0..3 {
        let ctx = trusted_window_context(start + time::Duration::seconds(i * 60));
        automaton.accumulate(&mut state, JsonValue::Null, &ctx).await?;
    }
    assert!(
        !automaton.window_complete(&state),
        "bout is still open — no gap-closed next event yet"
    );

    let last_event = start + time::Duration::seconds(120);
    let gap = DEFAULT_WINDOW_GAP_THRESHOLD_SECS;

    // Quiet shorter than the gap threshold must NOT flush.
    assert!(
        !automaton.flush_due(&state, last_event + time::Duration::seconds(gap - 1)),
        "must not close before gap_threshold of quiet"
    );
    // Quiet at/after the gap threshold closes the trailing window.
    assert!(
        automaton.flush_due(&state, last_event + time::Duration::seconds(gap)),
        "gap_threshold of quiet closes the trailing window without a next event"
    );

    let flush_ctx = AutomatonContext::timer_flush(last_event + time::Duration::seconds(gap))?;
    let output = automaton
        .emit(&mut state, &flush_ctx)
        .await?
        .expect("flush must emit the trailing window");
    assert_eq!(
        output.payload.close_reason,
        ActivityWindowCloseReason::Gap,
        "a quiescent flush close is a Gap close (propagates to session completion)"
    );
    assert_eq!(output.payload.event_count, 3);
    assert_eq!(output.payload.window_end, last_event);
    // State is reset after the flush (no pending next-event seed).
    assert!(
        state.window_start.is_none() && state.event_count == 0,
        "flush emit resets the window"
    );
    Ok(())
}

fn trusted_window_context_with_material(
    event_time: Timestamp,
    material_id: Uuid,
    anchor_byte: i64,
) -> AutomatonContext {
    AutomatonContext {
        trigger_material_id: Some(material_id),
        trigger_anchor_byte: Some(anchor_byte),
        ..trusted_window_context(event_time)
    }
}

/// sinex-ecy regression: the window equivalence key must be occurrence-derived,
/// not a processing-order counter. A counter (`activity-window-{n}`) restarts at
/// 0 on every checkpoint reset / replay, so a fresh derived window collides with
/// an unrelated live row and admission's fail-open dedup silently drops it. This
/// drives the automaton twice against fresh state (counter reset) and asserts the
/// key is stable AND specific to the first event's material occurrence.
#[sinex_test]
async fn window_equivalence_key_is_occurrence_stable_across_counter_reset(
) -> xtask::sandbox::TestResult<()> {
    async fn emit_window_for(material: Uuid, anchor: i64) -> xtask::sandbox::TestResult<String> {
        let mut automaton = AnalyticsAutomaton::default();
        let mut state = AnalyticsState::default();
        let t = Timestamp::now();
        // The FIRST contributing event anchors the window's occurrence identity.
        let first = trusted_window_context_with_material(t, material, anchor);
        automaton
            .accumulate(&mut state, JsonValue::Null, &first)
            .await?;
        // Fill the budget with other events, then overflow to close (MaxEventCount).
        for _ in 1..DEFAULT_WINDOW_MAX_EVENTS {
            let ctx = trusted_window_context_with_material(t, Uuid::now_v7(), anchor + 7);
            automaton
                .accumulate(&mut state, JsonValue::Null, &ctx)
                .await?;
        }
        let overflow = trusted_window_context_with_material(t, Uuid::now_v7(), anchor + 99);
        automaton
            .accumulate(&mut state, JsonValue::Null, &overflow)
            .await?;
        assert!(automaton.window_complete(&state));
        let output = automaton
            .emit(&mut state, &AutomatonContext::timer_flush(t)?)
            .await?
            .expect("closed window should emit a summary");
        Ok(output
            .equivalence_key
            .expect("window summary must carry an occurrence-derived equivalence key"))
    }

    let material = Uuid::now_v7();
    let anchor = 4096_i64;

    // Same occurrence, processed twice against fresh state (window_counter restarts
    // at 0, exactly as a checkpoint reset / replay does).
    let k1 = emit_window_for(material, anchor).await?;
    let k2 = emit_window_for(material, anchor).await?;
    assert_eq!(
        k1, k2,
        "same occurrence must yield the same key across a counter reset"
    );
    assert_eq!(k1, format!("activity-window:{material}:{anchor}"));
    assert!(
        !k1.contains("activity-window-"),
        "occurrence key must never be a processing-order counter: {k1}"
    );

    // A different first-event occurrence must not collide with the first window.
    let k3 = emit_window_for(Uuid::now_v7(), anchor).await?;
    assert_ne!(
        k1, k3,
        "distinct occurrences must not share an equivalence key"
    );
    Ok(())
}
