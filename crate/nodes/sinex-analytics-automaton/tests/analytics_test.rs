//! Tests for the analytics automaton's `process()` method.
//!
//! Validates event frequency counting, sliding window management,
//! periodic emission at 100-event boundaries, and state serialization.

use sinex_analytics_automaton::{AnalyticsAutomaton, AnalyticsState};
use sinex_node_sdk::{AutomatonNode, NodeEventContext};
use sinex_primitives::domain::EventSource;
use sinex_primitives::events::EventId;
use sinex_primitives::temporal::Timestamp;
use xtask::sandbox::prelude::*;

fn make_context(event_type: &str) -> NodeEventContext {
    NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: event_type.into(),
        ts_orig: Some(Timestamp::now()),
        event_id: EventId::new().into(),
    }
}

// ── Frequency Counting ──────────────────────────────────────────────────

#[sinex_test]
async fn test_single_event_increments_counter() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();
    let ctx = make_context("file.created");

    automaton
        .process(&mut state, serde_json::json!({}), &ctx)
        .await
        .unwrap();

    assert_eq!(state.event_counts.get("file.created"), Some(&1));
    Ok(())
}

#[sinex_test]
async fn test_multiple_same_type_accumulates() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    for _ in 0..5 {
        let ctx = make_context("file.created");
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }

    assert_eq!(state.event_counts.get("file.created"), Some(&5));
    Ok(())
}

#[sinex_test]
async fn test_different_types_tracked_independently() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    for _ in 0..3 {
        let ctx = make_context("file.created");
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }
    for _ in 0..2 {
        let ctx = make_context("shell.command");
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }

    assert_eq!(state.event_counts.get("file.created"), Some(&3));
    assert_eq!(state.event_counts.get("shell.command"), Some(&2));
    assert_eq!(state.event_counts.len(), 2);
    Ok(())
}

// ── Sliding Window ──────────────────────────────────────────────────────

#[sinex_test]
async fn test_events_added_to_window() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    for _ in 0..10 {
        let ctx = make_context("file.created");
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }

    assert_eq!(state.recent_events.len(), 10);
    Ok(())
}

#[sinex_test]
async fn test_window_capped_at_1000() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    // Push 1050 events — window should never exceed 1000
    for i in 0..1050 {
        let ctx = make_context(&format!("event.type.{}", i % 10));
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }

    assert_eq!(state.recent_events.len(), 1000);
    // Oldest events should have been pruned; most recent should be from the last batch
    let last = state.recent_events.back().unwrap();
    assert_eq!(last.event_type, "event.type.9");
    Ok(())
}

#[sinex_test]
async fn test_window_preserves_event_type() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    let ctx = make_context("window.test.event");
    automaton
        .process(&mut state, serde_json::json!({}), &ctx)
        .await
        .unwrap();

    assert_eq!(state.recent_events.len(), 1);
    assert_eq!(state.recent_events[0].event_type, "window.test.event");
    Ok(())
}

// ── Periodic Emission ───────────────────────────────────────────────────

#[sinex_test]
async fn test_no_emission_before_100_events() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    // Process 99 events — none should emit
    for _ in 0..99 {
        let ctx = make_context("file.created");
        let result = automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
        assert!(result.is_none(), "should not emit before 100th event");
    }

    Ok(())
}

#[sinex_test]
async fn test_emission_at_100th_event() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    // Process 99 events without emission
    for _ in 0..99 {
        let ctx = make_context("file.created");
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }

    // 100th event should emit
    let ctx = make_context("file.created");
    let result = automaton
        .process(&mut state, serde_json::json!({}), &ctx)
        .await
        .unwrap();

    assert!(result.is_some(), "100th event should trigger emission");
    let output = result.unwrap();
    assert!(output.get("top_events").is_some());
    assert_eq!(
        output.get("window_size").and_then(|v| v.as_u64()),
        Some(100)
    );
    Ok(())
}

#[sinex_test]
async fn test_emission_at_200th_event() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    let mut emission_count = 0;
    for _ in 0..200 {
        let ctx = make_context("file.created");
        let result = automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
        if result.is_some() {
            emission_count += 1;
        }
    }

    assert_eq!(emission_count, 2, "should emit at 100 and 200");
    Ok(())
}

#[sinex_test]
async fn test_emission_report_contains_frequency_data() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    // Send 60 file.created and 40 shell.command = 100 total
    for _ in 0..60 {
        let ctx = make_context("file.created");
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }
    for _ in 0..39 {
        let ctx = make_context("shell.command");
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }

    // 100th event triggers emission
    let ctx = make_context("shell.command");
    let result = automaton
        .process(&mut state, serde_json::json!({}), &ctx)
        .await
        .unwrap()
        .expect("100th event should emit");

    let top_events = result
        .get("top_events")
        .expect("report should contain top_events");
    assert_eq!(
        top_events.get("file.created").and_then(|v| v.as_u64()),
        Some(60)
    );
    assert_eq!(
        top_events.get("shell.command").and_then(|v| v.as_u64()),
        Some(40)
    );
    Ok(())
}

// ── State Serialization ─────────────────────────────────────────────────

#[sinex_test]
async fn test_state_serde_roundtrip() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton;
    let mut state = AnalyticsState::default();

    // Build up some state
    for _ in 0..5 {
        let ctx = make_context("file.created");
        automaton
            .process(&mut state, serde_json::json!({}), &ctx)
            .await
            .unwrap();
    }

    // Serialize and deserialize
    let serialized = serde_json::to_string(&state).expect("state should serialize");
    let deserialized: AnalyticsState =
        serde_json::from_str(&serialized).expect("state should deserialize");

    assert_eq!(deserialized.event_counts.get("file.created"), Some(&5));
    assert_eq!(deserialized.recent_events.len(), 5);
    Ok(())
}

#[sinex_test]
async fn test_default_state_is_empty() -> TestResult<()> {
    let state = AnalyticsState::default();

    assert!(state.event_counts.is_empty());
    assert!(state.recent_events.is_empty());
    Ok(())
}
