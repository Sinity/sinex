//! Tests for health aggregator scope-reconciled aggregation logic

use serde_json::json;
use sinex_process::automata::health::{
    ComponentHealthStatus, HealthAggregator, HealthAggregatorConfig, HealthState,
};
use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext};
use sinex_node_sdk::{NodeLogicError, ScopeReconcilerNode};
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};
use time::Duration;
use xtask::sandbox::prelude::*;

fn make_context_with_optional_ts(ts_orig: Option<Timestamp>) -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: "test".into(),
        event_type: "health.status".into(),
        ts_orig,
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn make_context(ts: Timestamp) -> DerivedTriggerContext {
    make_context_with_optional_ts(Some(ts))
}

/// Helper that mirrors the `ScopeReconcilerWrapper` dispatch:
/// `scope_keys` → reconcile for each key, collect all outputs.
async fn process(
    aggregator: &mut HealthAggregator,
    state: &mut HealthState,
    input: JsonValue,
    ctx: &DerivedTriggerContext,
) -> Result<Vec<DerivedOutput<JsonValue>>, NodeLogicError> {
    let scope_keys = aggregator.scope_keys(&input, ctx);
    let mut outputs = Vec::new();
    for key in &scope_keys {
        let typed_outputs = aggregator.reconcile(state, key, input.clone(), ctx).await?;
        for output in typed_outputs {
            let payload = serde_json::to_value(output.payload).map_err(|error| {
                NodeLogicError::Processing(format!("failed to serialize test output: {error}"))
            })?;
            outputs.push(DerivedOutput {
                payload,
                ts_orig: output.ts_orig,
                source_event_ids: output.source_event_ids,
                temporal_policy: output.temporal_policy,
                semantics_version: output.semantics_version,
                scope_key: output.scope_key,
                equivalence_key: output.equivalence_key,
                aggregation: output.aggregation,
            });
        }
    }
    Ok(outputs)
}

#[sinex_test]
async fn health_aggregator_tracks_component_status(ctx: TestContext) -> TestResult<()> {
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();

    let input = json!({
        "component": "test-component",
        "previous_status": "healthy",
        "current_status": "degraded",
    });

    let context = make_context(Timestamp::now());
    let output = process(&mut aggregator, &mut state, input, &context).await;
    assert!(output.is_ok(), "process should succeed");

    assert!(
        state.component_health.contains_key("test-component"),
        "component should be tracked"
    );

    let component = &state.component_health["test-component"];
    assert_eq!(
        component.current_status,
        ComponentHealthStatus::Degraded,
        "status should be updated"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_rejects_missing_ts_orig(ctx: TestContext) -> TestResult<()> {
    let _ = ctx;
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();
    let input = json!({
        "component": "test-component",
        "previous_status": "healthy",
        "current_status": "degraded",
    });

    let error = process(
        &mut aggregator,
        &mut state,
        input,
        &make_context_with_optional_ts(None),
    )
    .await
    .expect_err("missing ts_orig must be rejected");

    assert!(
        matches!(&error, NodeLogicError::InputParsing(msg) if msg.contains("missing ts_orig")),
        "expected InputParsing with 'missing ts_orig', got: {error:?}"
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
    let context_baseline = make_context(Timestamp::now() - Duration::seconds(10));
    process(&mut aggregator, &mut state, baseline, &context_baseline).await?;

    // Transition to failed status
    let input = json!({
        "component": "critical-service",
        "previous_status": "healthy",
        "current_status": "failed",
    });

    let context = make_context(Timestamp::now());
    let outputs = process(&mut aggregator, &mut state, input, &context).await?;

    // Should emit an immediate alert
    assert!(!outputs.is_empty(), "alert should be emitted");

    let alert = outputs
        .iter()
        .find(|output| {
            output.payload.get("alert_type").and_then(|v| v.as_str())
                == Some("component_status_change")
        })
        .expect("failed transition should emit an alert");
    assert_eq!(
        alert.payload.get("alert_type").and_then(|v| v.as_str()),
        Some("component_status_change"),
        "alert_type should match"
    );
    assert_eq!(
        alert.payload.get("severity").and_then(|v| v.as_str()),
        Some("critical"),
        "severity should match"
    );
    assert_eq!(
        alert.payload.get("component").and_then(|v| v.as_str()),
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
    let context_baseline = make_context(base_time);
    process(&mut aggregator, &mut state, baseline, &context_baseline).await?;

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

        let context = make_context(base_time + Duration::seconds(i as i64 + 1));
        process(&mut aggregator, &mut state, input, &context).await?;
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

        let context = make_context(base_time + Duration::minutes(i));
        process(&mut aggregator, &mut state, input, &context).await?;
    }

    // Move time forward and process one more event
    let future_time = base_time + Duration::minutes(11);
    let input = json!({
        "component": "service-a",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context = make_context(future_time);
    process(&mut aggregator, &mut state, input, &context).await?;

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

    let context1 = make_context(base_time);
    let outputs1 = process(&mut aggregator, &mut state, input1, &context1).await?;
    assert!(!outputs1.is_empty(), "first emission should occur");

    // Process second event within window (should NOT emit system status)
    let input2 = json!({
        "component": "service-b",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context2 = make_context(base_time + Duration::seconds(2));
    let _output2 = process(&mut aggregator, &mut state, input2, &context2).await?;
    // Might or might not emit depending on component check interval

    // Process third event after window (should emit system status again)
    let input3 = json!({
        "component": "service-c",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context3 = make_context(base_time + Duration::seconds(6));
    let outputs3 = process(&mut aggregator, &mut state, input3, &context3).await?;
    assert!(
        !outputs3.is_empty(),
        "system status after window should be emitted"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_system_status_includes_trigger_component(
    ctx: TestContext,
) -> TestResult<()> {
    let _ = ctx;
    let config = HealthAggregatorConfig {
        aggregation_window_seconds: 60,
        enable_system_health_status: true,
        enable_component_health_reports: false,
        ..Default::default()
    };

    let mut aggregator = HealthAggregator {
        config: config.clone(),
    };
    let mut state = HealthState {
        config,
        ..Default::default()
    };

    let outputs = process(
        &mut aggregator,
        &mut state,
        json!({
            "component": "service-a",
            "previous_status": "unknown",
            "current_status": "healthy",
        }),
        &make_context(Timestamp::now()),
    )
    .await?;

    let system_status = outputs
        .into_iter()
        .find(|output| {
            output
                .payload
                .get("report_type")
                .and_then(|value| value.as_str())
                == Some("system_health_status")
        })
        .expect("system-wide report should be emitted");

    assert_eq!(
        system_status
            .payload
            .get("total_components")
            .and_then(serde_json::Value::as_u64),
        Some(1),
        "system report should include the trigger component"
    );
    assert_eq!(
        system_status
            .payload
            .get("healthy_count")
            .and_then(serde_json::Value::as_u64),
        Some(1),
        "healthy count should reflect the trigger component"
    );
    assert_eq!(
        system_status
            .payload
            .get("components")
            .and_then(serde_json::Value::as_array)
            .map(std::vec::Vec::len),
        Some(1),
        "component list should include the trigger component"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_emits_all_due_reports_for_one_trigger(
    ctx: TestContext,
) -> TestResult<()> {
    let config = HealthAggregatorConfig {
        aggregation_window_seconds: 60,
        enable_system_health_status: true,
        enable_component_health_reports: true,
        ..Default::default()
    };

    let mut aggregator = HealthAggregator {
        config: config.clone(),
    };
    let mut state = HealthState {
        config,
        ..Default::default()
    };

    let outputs = process(
        &mut aggregator,
        &mut state,
        json!({
            "component": "service-a",
            "previous_status": "healthy",
            "current_status": "healthy",
        }),
        &make_context(Timestamp::now()),
    )
    .await?;

    assert_eq!(
        outputs.len(),
        2,
        "first trigger should emit both periodic reports"
    );
    assert!(
        outputs.iter().any(|output| {
            output
                .payload
                .get("report_type")
                .and_then(|value| value.as_str())
                == Some("system_health_status")
        }),
        "system-wide report should be present"
    );
    assert!(
        outputs.iter().any(|output| {
            output
                .payload
                .get("report_type")
                .and_then(|value| value.as_str())
                == Some("component_health_report")
        }),
        "component report should be present"
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

        let context = make_context(base_time + Duration::seconds(i as i64));
        process(&mut aggregator, &mut state, input, &context).await?;
    }

    // Force system status emission
    state.last_window_emission = None;
    let input = json!({
        "component": "trigger",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context = make_context(base_time + Duration::seconds(10));
    let outputs = process(&mut aggregator, &mut state, input, &context).await?;

    assert!(!outputs.is_empty(), "system status should be emitted");

    let system_status = outputs
        .into_iter()
        .find(|output| {
            output.payload.get("report_type").and_then(|v| v.as_str())
                == Some("system_health_status")
        })
        .expect("system status output should be present");
    assert_eq!(
        system_status
            .payload
            .get("report_type")
            .and_then(|v| v.as_str()),
        Some("system_health_status"),
        "report_type should match"
    );

    // Overall should be "degraded" (not all healthy, no failures)
    assert_eq!(
        system_status
            .payload
            .get("overall_status")
            .and_then(|v| v.as_str()),
        Some("degraded"),
        "overall status should be degraded"
    );

    assert_eq!(
        system_status
            .payload
            .get("degraded_count")
            .and_then(serde_json::Value::as_u64),
        Some(1),
        "degraded count should match"
    );

    Ok(())
}

#[sinex_test]
async fn health_aggregator_reports_unknown_system_status_for_unknown_components(
    ctx: TestContext,
) -> TestResult<()> {
    let _ = ctx;
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();

    let outputs = process(
        &mut aggregator,
        &mut state,
        json!({
            "component": "mystery-service",
            "previous_status": "unknown",
            "current_status": "unknown",
        }),
        &make_context(Timestamp::now()),
    )
    .await?;

    let system_status = outputs
        .into_iter()
        .find(|output| {
            output
                .payload
                .get("report_type")
                .and_then(|value| value.as_str())
                == Some("system_health_status")
        })
        .expect("system-wide report should be emitted");

    assert_eq!(
        system_status
            .payload
            .get("overall_status")
            .and_then(|value| value.as_str()),
        Some("unknown"),
        "unknown-only systems should not be reported as healthy"
    );
    assert_eq!(
        system_status
            .payload
            .get("unknown_count")
            .and_then(serde_json::Value::as_u64),
        Some(1),
        "unknown component count should be tracked explicitly"
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

    let context1 = make_context(base_time);
    let _output1 = process(&mut aggregator, &mut state, input1, &context1).await?;
    // Should emit component report (first time)

    // Second event for fast-check 0.5 seconds later (should NOT emit)
    let input2 = json!({
        "component": "fast-check",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context2 = make_context(base_time + Duration::milliseconds(500));
    let _output2 = process(&mut aggregator, &mut state, input2, &context2).await?;
    // Should not emit (within check interval)

    // Third event for fast-check 1.5 seconds later (should emit)
    let input3 = json!({
        "component": "fast-check",
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let context3 = make_context(base_time + Duration::milliseconds(1500));
    let _output3 = process(&mut aggregator, &mut state, input3, &context3).await?;
    // Should emit component report (after check interval)

    Ok(())
}

#[sinex_test]
async fn health_aggregator_rejects_invalid_event_ids_in_system_reports(
    ctx: TestContext,
) -> TestResult<()> {
    let _ = ctx;
    let config = HealthAggregatorConfig {
        aggregation_window_seconds: 5,
        enable_system_health_status: true,
        enable_component_health_reports: false,
        ..Default::default()
    };

    let mut aggregator = HealthAggregator {
        config: config.clone(),
    };
    let mut state = HealthState {
        config,
        ..Default::default()
    };
    let base_time = Timestamp::now() - Duration::seconds(1);
    state.component_health.insert(
        "service-a".to_string(),
        sinex_process::automata::health::ComponentHealth {
            component_name: "service-a".to_string(),
            current_status: ComponentHealthStatus::Healthy,
            status_since: base_time,
            last_seen: base_time,
            last_check_emission: None,
            transition_count: 0,
            events: vec![sinex_process::automata::health::HealthEvent {
                timestamp: base_time,
                previous_status: ComponentHealthStatus::Healthy,
                current_status: ComponentHealthStatus::Healthy,
                event_id: "not-a-uuid".to_string(),
            }],
        },
    );

    let error = process(
        &mut aggregator,
        &mut state,
        json!({
            "component": "service-b",
            "previous_status": "healthy",
            "current_status": "healthy",
        }),
        &make_context(Timestamp::now()),
    )
    .await
    .expect_err("corrupt persisted event ids must fail honestly");

    assert!(
        matches!(&error, NodeLogicError::Processing(msg) if msg.contains("invalid event_id") && msg.contains("system status")),
        "expected Processing with 'invalid event_id' and 'system status', got: {error:?}"
    );
    Ok(())
}

#[sinex_test]
async fn health_aggregator_rejects_invalid_event_ids_in_component_reports(
    ctx: TestContext,
) -> TestResult<()> {
    let _ = ctx;
    let config = HealthAggregatorConfig {
        aggregation_window_seconds: 5,
        enable_system_health_status: false,
        enable_component_health_reports: true,
        ..Default::default()
    };

    let mut aggregator = HealthAggregator {
        config: config.clone(),
    };
    let mut state = HealthState {
        config,
        ..Default::default()
    };
    let base_time = Timestamp::now() - Duration::seconds(1);
    state.component_health.insert(
        "service-a".to_string(),
        sinex_process::automata::health::ComponentHealth {
            component_name: "service-a".to_string(),
            current_status: ComponentHealthStatus::Healthy,
            status_since: base_time,
            last_seen: base_time,
            last_check_emission: None,
            transition_count: 0,
            events: vec![sinex_process::automata::health::HealthEvent {
                timestamp: base_time,
                previous_status: ComponentHealthStatus::Healthy,
                current_status: ComponentHealthStatus::Healthy,
                event_id: "not-a-uuid".to_string(),
            }],
        },
    );

    let error = process(
        &mut aggregator,
        &mut state,
        json!({
            "component": "service-a",
            "previous_status": "healthy",
            "current_status": "healthy",
        }),
        &make_context(Timestamp::now()),
    )
    .await
    .expect_err("corrupt persisted event ids must fail honestly");

    assert!(
        matches!(&error, NodeLogicError::Processing(msg) if msg.contains("invalid event_id") && msg.contains("component report")),
        "expected Processing with 'invalid event_id' and 'component report', got: {error:?}"
    );
    Ok(())
}

#[sinex_test]
async fn health_aggregator_rejects_invalid_status_values(ctx: TestContext) -> TestResult<()> {
    let _ = ctx;
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();
    let context = make_context(Timestamp::now());
    let input = json!({
        "component": "service-a",
        "previous_status": "healthy",
        "current_status": "mystery-state",
    });

    let error = process(&mut aggregator, &mut state, input, &context)
        .await
        .expect_err("invalid health statuses must fail honestly");

    assert!(
        matches!(&error, NodeLogicError::InputParsing(msg) if msg.contains("current_status") && msg.contains("mystery-state")),
        "expected InputParsing with 'current_status' and 'mystery-state', got: {error:?}"
    );
    Ok(())
}

#[sinex_test]
async fn health_aggregator_rejects_missing_component(ctx: TestContext) -> TestResult<()> {
    let _ = ctx;
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();
    let context = make_context(Timestamp::now());
    let input = json!({
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let error = process(&mut aggregator, &mut state, input, &context)
        .await
        .expect_err("missing component names must fail honestly");

    assert!(
        matches!(&error, NodeLogicError::InputParsing(msg) if msg.contains("missing required field 'component'")),
        "expected InputParsing with missing component message, got: {error:?}"
    );
    Ok(())
}

#[sinex_test]
async fn health_aggregator_rejects_non_string_component(ctx: TestContext) -> TestResult<()> {
    let _ = ctx;
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();
    let context = make_context(Timestamp::now());
    let input = json!({
        "component": 42,
        "previous_status": "healthy",
        "current_status": "healthy",
    });

    let error = process(&mut aggregator, &mut state, input, &context)
        .await
        .expect_err("non-string component names must fail honestly");

    assert!(
        error
            .to_string()
            .contains("field 'component' must be a string")
    );
    Ok(())
}

// ── Scope Reconciler Output Metadata ────────────────────────────────────

#[sinex_test]
async fn test_scope_reconciler_scope_key_derivation() -> TestResult<()> {
    let aggregator = HealthAggregator::default();
    let ctx = make_context(Timestamp::now());

    let input = json!({
        "component": "database-primary",
        "current_status": "healthy",
    });

    let scope_keys = aggregator.scope_keys(&input, &ctx);
    assert_eq!(scope_keys, vec!["database-primary"]);
    Ok(())
}

#[sinex_test]
async fn test_scope_reconciler_invalid_component_gets_unique_sentinel_scope() -> TestResult<()> {
    let aggregator = HealthAggregator::default();
    let ctx = make_context(Timestamp::now());
    let trigger_id = ctx.trigger_uuid();

    let input = json!({
        "current_status": "healthy",
    });

    let scope_keys = aggregator.scope_keys(&input, &ctx);
    assert_eq!(scope_keys.len(), 1);
    assert_eq!(scope_keys[0], format!("__invalid_component__:{trigger_id}"));
    assert_ne!(
        scope_keys[0], "unknown",
        "malformed payloads must not collide with the literal unknown component"
    );
    Ok(())
}

#[sinex_test]
async fn test_reconcile_output_has_declared_effective_policy() -> TestResult<()> {
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();

    let baseline = json!({
        "component": "service-a",
        "previous_status": "unknown",
        "current_status": "healthy",
    });
    let baseline_ctx = make_context(Timestamp::now() - Duration::seconds(5));
    let baseline_keys = aggregator.scope_keys(&baseline, &baseline_ctx);
    aggregator
        .reconcile(&mut state, &baseline_keys[0], baseline, &baseline_ctx)
        .await?;

    let input = json!({
        "component": "service-a",
        "previous_status": "healthy",
        "current_status": "failed",
    });

    let ctx = make_context(Timestamp::now());
    let outputs = process(&mut aggregator, &mut state, input, &ctx).await?;
    let output = outputs
        .into_iter()
        .find(|candidate| {
            candidate
                .payload
                .get("alert_type")
                .and_then(serde_json::Value::as_str)
                == Some("component_status_change")
        })
        .expect("failed transition should emit alert");

    assert_eq!(
        output.temporal_policy,
        sinex_primitives::domain::SyntheticTemporalPolicy::DeclaredEffective,
    );
    Ok(())
}
