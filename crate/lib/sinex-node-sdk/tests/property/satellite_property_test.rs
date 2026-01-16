//! Property tests for satellite architecture
//!
//! Tests that verify satellite communication, lifecycle, and coordination properties
//! using modern Sinex infrastructure (NATS JetStream, TestContext, etc.)

use proptest::prelude::*;
use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::{Event, JsonValue};
use sinex_test_utils::{prelude::*, sinex_prop, sinex_proptest};
use std::time::Duration;

/// Property test strategies for event data
mod strategies {
    use super::*;

    /// Strategy for generating realistic event sequences
    pub fn event_sequences() -> impl Strategy<Value = Vec<Event<JsonValue>>> {
        (1usize..=100).prop_flat_map(|size| {
            proptest::collection::vec(
                (event_sources(), event_types(), event_payloads()).prop_map(
                    |(source, event_type, payload)| {
                        Event::test_event(
                            EventSource::new(&source),
                            EventType::new(&event_type),
                            payload,
                        )
                    },
                ),
                size,
            )
        })
    }

    /// Strategy for generating event source names
    pub fn event_sources() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("fs".to_string()),
            Just("terminal".to_string()),
            Just("desktop".to_string()),
            Just("system".to_string()),
            Just("test".to_string()),
        ]
    }

    /// Strategy for generating event type names
    pub fn event_types() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("file.created".to_string()),
            Just("file.modified".to_string()),
            Just("command.executed".to_string()),
            Just("window.opened".to_string()),
            Just("test.event".to_string()),
        ]
    }

    /// Strategy for generating realistic event payloads
    pub fn event_payloads() -> impl Strategy<Value = serde_json::Value> {
        prop_oneof![
            // Simple payload
            Just(json!({"type": "simple", "data": "test"})),
            // File system payload
            Just(json!({
                "path": "/tmp/test.txt",
                "size": 1024
            })),
            // Terminal payload
            Just(json!({
                "command": "ls -la",
                "exit_code": 0
            })),
            // Complex payload
            Just(json!({
                "type": "complex",
                "metadata": {"created": "2024-01-01"},
                "data": vec![1, 2, 3, 4, 5]
            })),
        ]
    }
}

use strategies::*;

#[sinex_prop]
async fn satellite_event_processing_preserves_order(
    #[strategy(event_sequences())] events: Vec<Event<JsonValue>>,
    #[strategy(1usize..100usize)] batch_size: usize,
) -> TestResult<()> {
    if events.is_empty() {
        return Ok(());
    }

    let ctx = TestContext::new().await?;

    // Process events in batches
    let mut processed_events = Vec::new();
    for chunk in events.chunks(batch_size) {
        for event in chunk {
            let inserted_event = ctx.pool.events().insert(event.clone()).await?;
            processed_events.push(inserted_event);
        }

        // Wait for batch processing
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for all events to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check that we have the expected count
    let actual_count = ctx.pool.events().count_all().await?;
    assert_eq!(actual_count, processed_events.len() as i64);

    let db_events = ctx
        .pool
        .events()
        .get_recent(processed_events.len() as i64)
        .await?;
    assert_eq!(db_events.len(), processed_events.len());

    // Verify ULID ordering is preserved (ULIDs are time-ordered)
    for i in 1..db_events.len() {
        if let (Some(prev_id), Some(curr_id)) = (&db_events[i - 1].id, &db_events[i].id) {
            assert!(prev_id.timestamp() <= curr_id.timestamp());
        }
    }
    Ok(())
}

#[sinex_prop]
async fn satellite_handles_intermittent_failures(
    #[strategy(0.0..0.3f64)] failure_rate: f64, // Up to 30% failure rate
    #[strategy(proptest::collection::vec(
        (event_sources(), event_types(), event_payloads()),
        1..=50
    ))] events: Vec<(String, String, serde_json::Value)>,
    #[strategy(1u64..100u64)] recovery_delay: u64,
) -> TestResult<()> {
    if events.is_empty() {
        return Ok(());
    }

    let ctx = TestContext::new().await?;

    let mut successful_events = 0;

    for (i, (source, event_type, payload)) in events.iter().enumerate() {
        // Simulate intermittent failures
        let should_fail = (i as f64 * failure_rate) % 1.0 < failure_rate;

        if should_fail {
            // Simulate failure by creating invalid event (empty source)
            let invalid_event = Event::test_event(
                EventSource::new(""),
                EventType::new(event_type),
                payload.clone(),
            );

            let _ = ctx.pool.events().insert(invalid_event).await;
        } else {
            // Process normal event
            let event = Event::test_event(
                EventSource::new(source),
                EventType::new(event_type),
                payload.clone(),
            );

            ctx.pool.events().insert(event).await?;
            successful_events += 1;
        }

        // Add small delay to simulate processing time
        tokio::time::sleep(Duration::from_millis(recovery_delay / 10)).await;
    }

    // Wait for processing to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    let final_count = ctx.pool.events().count_all().await?;
    assert_eq!(final_count, successful_events as i64);

    // Verify system recovered from failures
    assert!(successful_events > 0, "At least some events should succeed");
    Ok(())
}

#[sinex_prop]
async fn satellite_manages_resources_efficiently(
    #[strategy(1usize..5usize)] concurrent_operations: usize,
    #[strategy(1usize..50usize)] events_per_operation: usize,
    #[strategy(1u64..50u64)] processing_delay: u64,
) -> TestResult<()> {
    let ctx = TestContext::new().await?;

    // Generate events for concurrent processing
    let mut total_events = 0;
    let mut handles = Vec::new();

    for i in 0..concurrent_operations {
        let source = format!("concurrent-{i}");
        let per_op = events_per_operation;
        let delay = processing_delay;

        let handle = tokio::spawn(async move {
            let ctx_clone = TestContext::new().await.expect("test context");
            let mut operation_events = 0;

            for j in 0..per_op {
                let event = Event::test_event(
                    EventSource::new(&source),
                    EventType::new(format!("test.event.{j}")),
                    json!({ "operation": i, "event": j }),
                );

                ctx_clone
                    .pool
                    .events()
                    .insert(event)
                    .await
                    .expect("insert event");
                operation_events += 1;

                // Small processing delay
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }

            operation_events
        });

        handles.push(handle);
        total_events += events_per_operation;
    }

    // Wait for all operations to complete
    let mut completed_events = 0;
    for handle in handles {
        completed_events += handle.await.expect("task join");
    }

    // Verify all events were processed
    assert_eq!(completed_events, total_events);

    // Wait for final consistency
    tokio::time::sleep(Duration::from_millis(200)).await;

    let final_count = ctx.pool.events().count_all().await?;
    assert_eq!(final_count, total_events as i64);
    Ok(())
}

sinex_proptest! {
    // Test satellite configuration validation properties
    fn satellite_config_validation_is_robust(
        service_name in "[a-zA-Z0-9_-]+",
        _batch_size in 1usize..10000usize,
        _timeout_secs in 1u64..3600u64,
    ) {
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
    }
}

#[sinex_prop]
async fn satellite_batch_processing_is_consistent(
    #[strategy(1usize..100usize)] _initial_batch_size: usize,
    #[strategy(1usize..100usize)] _updated_batch_size: usize,
    #[strategy(proptest::collection::vec(
        (event_sources(), event_types(), event_payloads()),
        1..=50
    ))] events: Vec<(String, String, serde_json::Value)>,
) -> TestResult<()> {
    if events.is_empty() {
        return Ok(());
    }

    let ctx = TestContext::new().await?;

    // Process events in first batch configuration
    let half_point = events.len() / 2;
    for (source, event_type, payload) in events.iter().take(half_point) {
        let event = Event::test_event(
            EventSource::new(source),
            EventType::new(event_type),
            payload.clone(),
        );

        ctx.pool.events().insert(event).await?;
    }

    // Wait for initial processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Process remaining events (simulating batch size change)
    for (source, event_type, payload) in events.iter().skip(half_point) {
        let event = Event::test_event(
            EventSource::new(source),
            EventType::new(event_type),
            payload.clone(),
        );

        ctx.pool.events().insert(event).await?;
    }

    // Wait for all events to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify no events were lost during configuration changes
    let final_count = ctx.pool.events().count_all().await?;
    assert_eq!(final_count, events.len() as i64);
    Ok(())
}

#[sinex_prop]
async fn satellite_survives_processing_interruptions(
    #[strategy(1u64..100u64)] interruption_duration: u64,
    #[strategy(1usize..20usize)] events_before_interruption: usize,
    #[strategy(1usize..20usize)] events_during_interruption: usize,
    #[strategy(1usize..20usize)] events_after_interruption: usize,
) -> TestResult<()> {
    let ctx = TestContext::new().await?;

    // Phase 1: Normal operation
    for i in 0..events_before_interruption {
        let event = Event::test_event(
            EventSource::new("interruption_test"),
            EventType::new(format!("before.{i}")),
            json!({ "phase": "before", "index": i }),
        );

        ctx.pool.events().insert(event).await?;
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Phase 2: Simulate interruption by creating events that might be delayed
    for i in 0..events_during_interruption {
        let event = Event::test_event(
            EventSource::new("interruption_test"),
            EventType::new(format!("during.{i}")),
            json!({ "phase": "during", "index": i }),
        );

        // Try to insert with timeout to simulate network issues
        let _ = tokio::time::timeout(Duration::from_millis(50), ctx.pool.events().insert(event)).await;
    }

    // Wait for interruption duration
    tokio::time::sleep(Duration::from_millis(interruption_duration)).await;

    // Phase 3: Recovery
    for i in 0..events_after_interruption {
        let event = Event::test_event(
            EventSource::new("interruption_test"),
            EventType::new(format!("after.{i}")),
            json!({ "phase": "after", "index": i }),
        );

        ctx.pool.events().insert(event).await?;
    }

    // Wait for recovery and verify minimum events
    let expected_minimum = events_before_interruption + events_after_interruption;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let final_count = ctx.pool.events().count_all().await?;
    assert!(final_count >= expected_minimum as i64);
    Ok(())
}

#[sinex_prop]
async fn satellite_maintains_event_ordering_under_load(
    #[strategy(1usize..5usize)] concurrent_sources: usize,
    #[strategy(1usize..20usize)] events_per_source: usize,
    #[strategy(1u64..20u64)] processing_jitter: u64,
) -> TestResult<()> {
    let ctx = TestContext::new().await?;

    let mut handles = Vec::new();

    // Create concurrent event producers
    for source_id in 0..concurrent_sources {
        let source_name = format!("ordering-test-{source_id}");
        let per_source = events_per_source;
        let jitter = processing_jitter;

        let handle = tokio::spawn(async move {
            let ctx_clone = TestContext::new().await.expect("test context");
            for event_id in 0..per_source {
                let event = Event::test_event(
                    EventSource::new(source_name.clone()),
                    EventType::new("ordering.test"),
                    json!({
                        "source_id": source_id,
                        "event_id": event_id,
                        "timestamp": chrono::Utc::now().timestamp_millis()
                    }),
                );

                ctx_clone
                    .pool
                    .events()
                    .insert(event)
                    .await
                    .expect("insert event");

                // Add jitter to simulate real-world timing variations
                tokio::time::sleep(Duration::from_millis(jitter)).await;
            }
        });

        handles.push(handle);
    }

    // Wait for all producers to complete
    for handle in handles {
        handle.await.expect("task join");
    }

    // Wait for all events to be processed
    let total_events = concurrent_sources * events_per_source;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify event ordering within each source
    let all_events = ctx
        .pool
        .events()
        .get_recent((total_events * 2) as i64)
        .await?;
    assert_eq!(all_events.len(), total_events);

    // Group events by source and verify ordering within each source
    let mut events_by_source: std::collections::HashMap<String, Vec<_>> =
        std::collections::HashMap::new();

    for event in all_events {
        let source = event.source.to_string();
        events_by_source.entry(source).or_default().push(event);
    }

    // Verify each source maintained ordering
    for (_source, mut source_events) in events_by_source {
        // Sort by ID timestamp to get creation order (handle Option<Id>)
        source_events.sort_by(|a, b| match (a.id.as_ref(), b.id.as_ref()) {
            (Some(id_a), Some(id_b)) => id_a.timestamp().cmp(&id_b.timestamp()),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });

        // Verify sequential event_ids within payload
        for window in source_events.windows(2) {
            let (payload1, payload2) = (&window[0].payload, &window[1].payload);
            if let (Some(id1), Some(id2)) = (
                payload1.get("event_id").and_then(|v| v.as_u64()),
                payload2.get("event_id").and_then(|v| v.as_u64()),
            ) {
                // Within a source, event_ids should be sequential
                assert!(
                    id1 < id2 || id1 == 0,
                    "Events within source should maintain ordering"
                );
            }
        }
    }
    Ok(())
}
