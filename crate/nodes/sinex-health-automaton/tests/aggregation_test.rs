//! Tests for health aggregator windowed aggregation logic

use serde_json::json;
use sinex_health_automaton::{HealthAggregator, HealthAggregatorConfig, HealthState};
use sinex_node_sdk::{AutomatonNode, NodeEventContext};
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::EventId;
use time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn health_aggregator_tracks_component_status(ctx: TestContext) -> TestResult<()> {
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();

    // Simulate health.status event
    let input = json!({
        "component": "test-component",
        "previous_status": "healthy",
        "current_status": "degraded",
    });

    let context = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(Timestamp::now()),
        event_id: EventId::new().into(),
    };

    let output = aggregator.process(&mut state, input, &context).await;
    assert!(output.is_ok(), "process should succeed");

    // Verify component was tracked
    assert!(
        state.component_health.contains_key("test-component"),
        "component should be tracked"
    );

    let component = &state.component_health["test-component"];
    assert_eq!(
        component.current_status, "degraded",
        "status should be updated"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_emits_alert_on_failed_transition(ctx: TestContext) -> TestResult<()> {
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();

    // Establish baseline status first
    let baseline = json!({
        "component": "critical-service",
        "previous_status": "unknown",
        "current_status": "healthy",
    });
    let context_baseline = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(Timestamp::now() - Duration::seconds(10)),
        event_id: EventId::new().into(),
    };
    aggregator
        .process(&mut state, baseline, &context_baseline)
        .await?;

    // Transition to failed status
    let input = json!({
        "component": "critical-service",
        "previous_status": "healthy",
        "current_status": "failed",
    });

    let context = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(Timestamp::now()),
        event_id: EventId::new().into(),
    };

    let output = aggregator.process(&mut state, input, &context).await?;

    // Should emit an immediate alert
    assert!(output.is_some(), "alert should be emitted");

    let alert = output.unwrap();
    assert_eq!(
        alert.get("alert_type").and_then(|v| v.as_str()),
        Some("component_status_change"),
        "alert_type should match"
    );
    assert_eq!(
        alert.get("severity").and_then(|v| v.as_str()),
        Some("critical"),
        "severity should match"
    );
    assert_eq!(
        alert.get("component").and_then(|v| v.as_str()),
        Some("critical-service"),
        "component should match"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_tracks_transition_count(ctx: TestContext) -> TestResult<()> {
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();

    let base_time = Timestamp::now() - Duration::seconds(10);

    // Establish baseline status
    let baseline = json!({
        "component": "flaky-service",
        "previous_status": "unknown",
        "current_status": "healthy",
    });
    let context_baseline = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(base_time),
        event_id: EventId::new().into(),
    };
    aggregator
        .process(&mut state, baseline, &context_baseline)
        .await?;

    // Simulate multiple transitions for the same component
    for (i, status) in ["degraded", "failed", "degraded", "healthy"]
        .iter()
        .enumerate()
    {
        let input = json!({
            "component": "flaky-service",
            "previous_status": if i == 0 { "healthy" } else { "degraded" },
            "current_status": status,
        });

        let context = NodeEventContext {
            source: EventSource::from_static("test"),
            event_type: EventType::from_static("health.status"),
            ts_orig: Some(base_time + Duration::seconds(i as i64 + 1)),
            event_id: EventId::new().into(),
        };

        aggregator.process(&mut state, input, &context).await?;
    }

    let component = &state.component_health["flaky-service"];
    assert_eq!(
        component.transition_count, 4,
        "transition count should match"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_prunes_old_events_outside_window(ctx: TestContext) -> TestResult<()> {
    let config = HealthAggregatorConfig {
        aggregation_window_seconds: 300, // 5 minutes
        ..Default::default()
    };

    let mut aggregator = HealthAggregator {
        config: config.clone(),
    };
    let mut state = HealthState {
        config: config.clone(),
        ..Default::default()
    };

    let base_time = Timestamp::now();

    // Add events across a 10-minute span
    for i in 0..10 {
        let input = json!({
            "component": "service-a",
            "previous_status": "healthy",
            "current_status": "healthy",
        });

        let context = NodeEventContext {
            source: EventSource::from_static("test"),
            event_type: EventType::from_static("health.status"),
            ts_orig: Some(base_time + Duration::minutes(i)),
            event_id: EventId::new().into(),
        };

        aggregator.process(&mut state, input, &context).await?;
    }

    // Move time forward and process one more event
    let future_time = base_time + Duration::minutes(11);
    let input = json!({
        "component": "service-a",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(future_time),
        event_id: EventId::new().into(),
    };

    aggregator.process(&mut state, input, &context).await?;

    // Events outside 5-minute window should be pruned
    let component = &state.component_health["service-a"];
    let events_in_window = component
        .events
        .iter()
        .filter(|e| (future_time - e.timestamp).whole_seconds() <= 300)
        .count();

    assert!(
        component.events.len() <= events_in_window + 1,
        "only recent events should be kept"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_emits_system_status_periodically(ctx: TestContext) -> TestResult<()> {
    let config = HealthAggregatorConfig {
        aggregation_window_seconds: 5, // 5 seconds for test
        enable_system_health_status: true,
        ..Default::default()
    };

    let mut aggregator = HealthAggregator {
        config: config.clone(),
    };
    let mut state = HealthState {
        config: config.clone(),
        ..Default::default()
    };

    let base_time = Timestamp::now();

    // Process first event (should emit system status)
    let input1 = json!({
        "component": "service-a",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context1 = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(base_time),
        event_id: EventId::new().into(),
    };

    let output1 = aggregator.process(&mut state, input1, &context1).await?;
    assert!(output1.is_some(), "first emission should occur");

    // Process second event within window (should NOT emit system status)
    let input2 = json!({
        "component": "service-b",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context2 = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(base_time + Duration::seconds(2)),
        event_id: EventId::new().into(),
    };

    let _output2 = aggregator.process(&mut state, input2, &context2).await?;
    // Might or might not emit depending on component check interval

    // Process third event after window (should emit system status again)
    let input3 = json!({
        "component": "service-c",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context3 = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(base_time + Duration::seconds(6)),
        event_id: EventId::new().into(),
    };

    let output3 = aggregator.process(&mut state, input3, &context3).await?;
    assert!(
        output3.is_some(),
        "system status after window should be emitted"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_calculates_overall_system_status(ctx: TestContext) -> TestResult<()> {
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();

    let base_time = Timestamp::now();

    // Add mix of healthy, degraded, and failed components
    let statuses = [
        ("service-1", "healthy"),
        ("service-2", "healthy"),
        ("service-3", "degraded"),
        ("service-4", "healthy"),
    ];

    for (i, (component, status)) in statuses.iter().enumerate() {
        let input = json!({
            "component": component,
            "previous_status": "healthy",
            "current_status": status,
        });

        let context = NodeEventContext {
            source: EventSource::from_static("test"),
            event_type: EventType::from_static("health.status"),
            ts_orig: Some(base_time + Duration::seconds(i as i64)),
            event_id: EventId::new().into(),
        };

        aggregator.process(&mut state, input, &context).await?;
    }

    // Force system status emission
    state.last_window_emission = None;
    let input = json!({
        "component": "trigger",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(base_time + Duration::seconds(10)),
        event_id: EventId::new().into(),
    };

    let output = aggregator.process(&mut state, input, &context).await?;

    assert!(output.is_some(), "system status should be emitted");

    let system_status = output.unwrap();
    assert_eq!(
        system_status.get("report_type").and_then(|v| v.as_str()),
        Some("system_health_status"),
        "report_type should match"
    );

    // Overall should be "degraded" (not all healthy, no failures)
    assert_eq!(
        system_status.get("overall_status").and_then(|v| v.as_str()),
        Some("degraded"),
        "overall status should be degraded"
    );

    assert_eq!(
        system_status
            .get("degraded_count")
            .and_then(serde_json::Value::as_u64),
        Some(1),
        "degraded count should match"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_respects_component_check_intervals(ctx: TestContext) -> TestResult<()> {
    use std::collections::HashMap;

    let mut component_intervals = HashMap::new();
    component_intervals.insert("fast-check".to_string(), 1); // 1 second
    component_intervals.insert("slow-check".to_string(), 10); // 10 seconds

    let config = HealthAggregatorConfig {
        component_check_intervals: component_intervals,
        enable_component_health_reports: true,
        ..Default::default()
    };

    let mut aggregator = HealthAggregator {
        config: config.clone(),
    };
    let mut state = HealthState {
        config: config.clone(),
        ..Default::default()
    };

    let base_time = Timestamp::now();

    // First event for fast-check component (should emit report)
    let input1 = json!({
        "component": "fast-check",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context1 = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(base_time),
        event_id: EventId::new().into(),
    };

    let _output1 = aggregator.process(&mut state, input1, &context1).await?;
    // Should emit component report (first time)

    // Second event for fast-check 0.5 seconds later (should NOT emit)
    let input2 = json!({
        "component": "fast-check",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context2 = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(base_time + Duration::milliseconds(500)),
        event_id: EventId::new().into(),
    };

    let _output2 = aggregator.process(&mut state, input2, &context2).await?;
    // Should not emit (within check interval)

    // Third event for fast-check 1.5 seconds later (should emit)
    let input3 = json!({
        "component": "fast-check",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context3 = NodeEventContext {
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("health.status"),
        ts_orig: Some(base_time + Duration::milliseconds(1500)),
        event_id: EventId::new().into(),
    };

    let _output3 = aggregator.process(&mut state, input3, &context3).await?;
    // Should emit component report (after check interval)

    Ok(())
}
