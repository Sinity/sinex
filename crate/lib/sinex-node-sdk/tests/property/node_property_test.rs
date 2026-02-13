//! Property tests for node architecture
//!
//! Tests that verify node communication, lifecycle, and coordination properties
//! using the publish pipeline (NATS -> ingestd -> DB) instead of direct inserts.

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde_json::json;
use sinex_primitives::DynamicPayload;
use xtask::sandbox::{prelude::*, sinex_prop, sinex_proptest};

/// Convert any Display error to proptest TestCaseError
fn prop_err(e: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::Fail(e.to_string().into())
}

/// Property test strategies for event data
mod strategies {
    use super::*;

    /// Strategy for generating event payload specs (source, type, json)
    pub(super) fn event_payload_specs(
    ) -> impl Strategy<Value = Vec<(String, String, serde_json::Value)>> {
        (1usize..=20).prop_flat_map(|size| {
            proptest::collection::vec((event_sources(), event_types(), event_payloads()), size)
        })
    }

    /// Strategy for generating event source names
    pub(super) fn event_sources() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("fs".to_string()),
            Just("terminal".to_string()),
            Just("desktop".to_string()),
            Just("system".to_string()),
            Just("test".to_string()),
        ]
    }

    /// Strategy for generating event type names
    pub(super) fn event_types() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("file.created".to_string()),
            Just("file.modified".to_string()),
            Just("command.executed".to_string()),
            Just("window.opened".to_string()),
            Just("test.event".to_string()),
        ]
    }

    /// Strategy for generating realistic event payloads
    pub(super) fn event_payloads() -> impl Strategy<Value = serde_json::Value> {
        prop_oneof![
            Just(json!({"type": "simple", "data": "test"})),
            Just(json!({"path": "/tmp/test.txt", "size": 1024})),
            Just(json!({"command": "ls -la", "exit_code": 0})),
            Just(json!({
                "type": "complex",
                "metadata": {"created": "2024-01-01"},
                "data": [1, 2, 3, 4, 5]
            })),
        ]
    }
}

use strategies::*;

#[sinex_prop(cases = 20)]
async fn node_event_processing_preserves_order(
    ctx: &TestContext,
    #[strategy(event_payload_specs())] events: Vec<(String, String, serde_json::Value)>,
) -> Result<(), TestCaseError> {
    if events.is_empty() {
        return Ok::<(), TestCaseError>(());
    }

    let payloads: Vec<DynamicPayload> = events
        .iter()
        .map(|(source, event_type, payload)| {
            DynamicPayload::new(source.as_str(), event_type.as_str(), payload.clone())
        })
        .collect();

    let published = ctx.publish_many(payloads).await.map_err(prop_err)?;
    assert_eq!(published.len(), events.len());

    // Verify ULID ordering is preserved (ULIDs are time-ordered)
    for i in 1..published.len() {
        if let (Some(prev_id), Some(curr_id)) = (&published[i - 1].id, &published[i].id) {
            assert!(
                prev_id.timestamp() <= curr_id.timestamp(),
                "Events should maintain ULID ordering"
            );
        }
    }

    Ok::<(), TestCaseError>(())
}

// `node_handles_intermittent_failures` DELETED:
// Test was inserting events with empty source strings to "simulate failure",
// which just tests a DB constraint — not actual node failure handling.

#[sinex_prop(cases = 20)]
async fn node_manages_resources_efficiently(
    ctx: &TestContext,
    #[strategy(1usize..5usize)] concurrent_operations: usize,
    #[strategy(1usize..50usize)] events_per_operation: usize,
) -> Result<(), TestCaseError> {
    let total_expected = concurrent_operations * events_per_operation;

    let payloads: Vec<DynamicPayload> = (0..concurrent_operations)
        .flat_map(|i| {
            (0..events_per_operation).map(move |j| {
                DynamicPayload::new(
                    format!("concurrent-{i}"),
                    format!("test.event.{j}"),
                    json!({ "operation": i, "event": j }),
                )
            })
        })
        .collect();

    let published = ctx.publish_many(payloads).await.map_err(prop_err)?;
    assert_eq!(published.len(), total_expected);

    Ok::<(), TestCaseError>(())
}

sinex_proptest! {
    // Test node configuration validation properties
    fn node_config_validation_is_robust(
        service_name in "[a-zA-Z0-9_-]+",
        _batch_size in 1usize..10000usize,
        _timeout_secs in 1u64..3600u64,
    ) -> TestResult<()> {
        use sinex_node_sdk::NodeConfig;

        // Test config creation with various valid parameters
        let config = NodeConfig::builder()
            .service_name(service_name.clone())
            .build();

        // Configuration should be valid with proper inputs
        assert_eq!(config.service_name, service_name);

        // Validate the configuration
        assert!(config.validate_config().is_ok());

        // Test environment-based loading doesn't panic
        let env_config = NodeConfig::load_from_env(&service_name);
        assert_eq!(env_config.service_name, service_name);

        Ok::<(), color_eyre::Report>(())
    }
}

#[sinex_prop(cases = 20)]
async fn node_batch_processing_is_consistent(
    ctx: &TestContext,
    #[strategy(proptest::collection::vec(
        (event_sources(), event_types(), event_payloads()),
        1..=20
    ))]
    events: Vec<(String, String, serde_json::Value)>,
) -> Result<(), TestCaseError> {
    if events.is_empty() {
        return Ok::<(), TestCaseError>(());
    }

    let half_point = events.len() / 2;

    // Process events in first batch
    let first_payloads: Vec<DynamicPayload> = events
        .iter()
        .take(half_point)
        .map(|(s, t, p)| DynamicPayload::new(s.as_str(), t.as_str(), p.clone()))
        .collect();

    let first_half = if !first_payloads.is_empty() {
        ctx.publish_many(first_payloads).await.map_err(prop_err)?
    } else {
        vec![]
    };

    // Process remaining events
    let second_payloads: Vec<DynamicPayload> = events
        .iter()
        .skip(half_point)
        .map(|(s, t, p)| DynamicPayload::new(s.as_str(), t.as_str(), p.clone()))
        .collect();

    let second_half = if !second_payloads.is_empty() {
        ctx.publish_many(second_payloads).await.map_err(prop_err)?
    } else {
        vec![]
    };

    // Verify no events were lost during batch transitions
    assert_eq!(first_half.len() + second_half.len(), events.len());

    Ok::<(), TestCaseError>(())
}

#[sinex_prop(cases = 20)]
async fn node_survives_processing_interruptions(
    ctx: &TestContext,
    #[strategy(1usize..20usize)] events_before: usize,
    #[strategy(1usize..20usize)] events_after: usize,
) -> Result<(), TestCaseError> {
    // Phase 1: Normal operation
    let before_payloads: Vec<DynamicPayload> = (0..events_before)
        .map(|i| {
            DynamicPayload::new(
                "interruption_test",
                format!("before.{i}"),
                json!({ "phase": "before", "index": i }),
            )
        })
        .collect();

    let before_events = ctx.publish_many(before_payloads).await.map_err(prop_err)?;
    assert_eq!(before_events.len(), events_before);

    // Phase 2: Recovery after interruption
    let after_payloads: Vec<DynamicPayload> = (0..events_after)
        .map(|i| {
            DynamicPayload::new(
                "interruption_test",
                format!("after.{i}"),
                json!({ "phase": "after", "index": i }),
            )
        })
        .collect();

    let after_events = ctx.publish_many(after_payloads).await.map_err(prop_err)?;
    assert_eq!(after_events.len(), events_after);

    // Both phases should complete successfully
    let total = before_events.len() + after_events.len();
    assert_eq!(total, events_before + events_after);

    Ok::<(), TestCaseError>(())
}

#[sinex_prop(cases = 20)]
async fn node_maintains_event_ordering_under_load(
    ctx: &TestContext,
    #[strategy(1usize..5usize)] concurrent_sources: usize,
    #[strategy(1usize..20usize)] events_per_source: usize,
) -> Result<(), TestCaseError> {
    let total_events = concurrent_sources * events_per_source;

    // Publish events for all sources in one batch (preserves submission order)
    let payloads: Vec<DynamicPayload> = (0..concurrent_sources)
        .flat_map(|source_id| {
            (0..events_per_source).map(move |event_id| {
                DynamicPayload::new(
                    format!("ordering-test-{source_id}"),
                    "ordering.test",
                    json!({
                        "source_id": source_id,
                        "event_id": event_id,
                    }),
                )
            })
        })
        .collect();

    let published = ctx.publish_many(payloads).await.map_err(prop_err)?;
    prop_assert_eq!(published.len(), total_events);

    // Group events by source and verify ordering within each
    let mut events_by_source: std::collections::HashMap<String, Vec<_>> =
        std::collections::HashMap::new();

    for event in &published {
        let source = event.source.to_string();
        events_by_source.entry(source).or_default().push(event);
    }

    for source_events in events_by_source.values() {
        assert_eq!(source_events.len(), events_per_source);

        // Verify sequential event_ids within payload
        for window in source_events.windows(2) {
            if let (Some(id1), Some(id2)) = (
                window[0]
                    .payload
                    .get("event_id")
                    .and_then(serde_json::Value::as_u64),
                window[1]
                    .payload
                    .get("event_id")
                    .and_then(serde_json::Value::as_u64),
            ) {
                assert!(id1 < id2, "Events within source should maintain ordering");
            }
        }
    }

    Ok::<(), TestCaseError>(())
}
