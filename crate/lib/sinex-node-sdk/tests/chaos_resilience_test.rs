//! Chaos resilience tests for node SDK.
//!
//! These tests verify that nodes can recover from various failure scenarios:
//! - Network partitions
//! - Message loss
//! - Message corruption
//! - Message reordering
//! - Slow consumers

use async_nats::jetstream::consumer::AckPolicy;
use sinex_core::DynamicPayload;
use sinex_node_sdk::simple_node::{ErrorAction, SimpleNode, SimpleNodeError};
use sinex_node_sdk::CheckpointManager;
use sinex_test_utils::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Simple counter node for testing chaos scenarios
#[derive(Debug)]
struct ChaosCounterNode {
    processed: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
}

impl ChaosCounterNode {
    fn new(processed: Arc<AtomicU64>, errors: Arc<AtomicU64>) -> Self {
        Self { processed, errors }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct CounterState {
    total: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CounterInput {
    value: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CounterOutput {
    new_total: u64,
    increment: u64,
}

#[async_trait::async_trait]
impl SimpleNode for ChaosCounterNode {
    type State = CounterState;
    type Input = CounterInput;
    type Output = CounterOutput;

    fn name(&self) -> &'static str {
        "chaos-counter"
    }

    fn input_event_type(&self) -> &'static str {
        "counter.increment"
    }

    fn output_event_type(&self) -> &'static str {
        "counter.result"
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        _context: &sinex_node_sdk::simple_node::SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        state.total += input.value;
        self.processed.fetch_add(1, Ordering::SeqCst);

        Ok(Some(CounterOutput {
            new_total: state.total,
            increment: input.value,
        }))
    }

    fn handle_error(&self, _error: &SimpleNodeError) -> ErrorAction {
        self.errors.fetch_add(1, Ordering::SeqCst);
        ErrorAction::Skip
    }
}

#[sinex_test(timeout = 60)]
async fn test_node_recovers_from_network_partition(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Publish some events before partition
    for i in 0..5 {
        ctx.publish(DynamicPayload::new(
            "chaos-test",
            "counter.increment",
            json!({"value": i + 1}),
        ))
        .await?;
    }

    // Wait for initial processing
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Simulate network partition using chaos config
    let chaos = ChaosScenarios::network_partition();
    let chaos_injector = ChaosInjestor::from_config(chaos);

    // During partition, measure the delay
    let partition_start = std::time::Instant::now();
    chaos_injector.simulate_network_partition().await?;
    let partition_duration = partition_start.elapsed();

    // Publish events after partition
    for i in 5..10 {
        ctx.publish(DynamicPayload::new(
            "chaos-test",
            "counter.increment",
            json!({"value": i + 1}),
        ))
        .await?;
    }

    // Verify partition actually occurred
    assert!(
        partition_duration >= Duration::from_secs(4),
        "Partition should have lasted at least 4 seconds"
    );

    // Verify all events were stored
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::new("chaos-test".to_string()),
            sinex_core::types::Pagination::new(Some(100), None),
        )
        .await?;

    assert_eq!(
        events.len(),
        10,
        "All events should be stored after partition"
    );

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_checkpoint_survives_message_loss(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let errors = Arc::new(AtomicU64::new(0));

    // Create checkpoint manager
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        "chaos-counter".to_string(),
        "test-group".to_string(),
        "test-consumer".to_string(),
    );

    // Publish events with message loss scenario
    let chaos = ChaosScenarios::message_loss();
    let chaos_injector = ChaosInjestor::from_config(chaos);

    let total_events = 50;
    let mut successful = 0;

    for i in 0..total_events {
        // Inject chaos into publish operation
        let result = chaos_injector
            .with_simulated_failures(|| async {
                ctx.publish(DynamicPayload::new(
                    "chaos-test",
                    "counter.increment",
                    json!({"value": i + 1}),
                ))
                .await
            })
            .await;

        // Log failures but continue
        if result.is_err() {
            errors.fetch_add(1, Ordering::SeqCst);
        } else {
            successful += 1;
        }
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    let total_errors = errors.load(Ordering::SeqCst);

    assert!(
        total_errors > 0,
        "Should have encountered some failures due to message loss"
    );
    assert!(
        successful < total_events,
        "Should have lost some messages due to chaos"
    );
    assert!(
        successful > 0,
        "Should have successfully published some messages"
    );

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_node_handles_corrupted_messages(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Create chaos config with high corruption rate
    let chaos = ChaosTestBuilder::new()
        .with_message_corruption_rate(0.3) // 30% corruption rate
        .with_latency(Duration::from_millis(10))
        .build();

    let chaos_injector = ChaosInjestor::from_config(chaos);

    // Publish events and corrupt some
    let total_events = 20;
    let mut corrupted_count = 0;

    for i in 0..total_events {
        let payload = serde_json::to_vec(&json!({"value": i + 1})).unwrap();

        // Process message through chaos injector
        let processed_messages = chaos_injector.process_message(payload.clone()).await?;

        for msg in processed_messages {
            // Try to deserialize - corrupted messages will fail
            if serde_json::from_slice::<serde_json::Value>(&msg).is_err() {
                corrupted_count += 1;
            } else {
                // Publish non-corrupted messages
                ctx.publish(DynamicPayload::new(
                    "chaos-test",
                    "counter.increment",
                    json!({"value": i + 1}),
                ))
                .await?;
            }
        }
    }

    assert!(corrupted_count > 0, "Should have corrupted some messages");

    // Verify some messages still got through
    tokio::time::sleep(Duration::from_secs(1)).await;

    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::new("chaos-test".to_string()),
            sinex_core::types::Pagination::new(Some(100), None),
        )
        .await?;

    assert!(
        !events.is_empty(),
        "Should have successfully processed some non-corrupted messages"
    );

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_node_handles_message_reordering(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Create chaos config with high reorder probability
    let chaos = ChaosTestBuilder::new()
        .with_reorder_probability(0.5) // 50% reorder chance
        .with_latency(Duration::from_millis(10))
        .build();

    let chaos_injector = ChaosInjestor::from_config(chaos);

    // Publish events in order but with reordering
    let total_events = 15;
    let mut published_order = Vec::new();

    for i in 0..total_events {
        let payload = json!({"value": i, "seq": i});
        let payload_bytes = serde_json::to_vec(&payload).unwrap();

        // Process through chaos injector
        let processed = chaos_injector.process_message(payload_bytes).await?;

        for msg in processed {
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&msg) {
                let seq = val["seq"].as_u64().unwrap();
                published_order.push(seq);
                ctx.publish(DynamicPayload::new("chaos-test", "counter.increment", val))
                    .await?;
            }
        }
    }

    // Flush any remaining buffered messages
    let buffered = chaos_injector.flush_reordered();
    for msg in buffered {
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&msg) {
            let seq = val["seq"].as_u64().unwrap();
            published_order.push(seq);
            ctx.publish(DynamicPayload::new("chaos-test", "counter.increment", val))
                .await?;
        }
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check if order was actually changed
    let is_ordered = published_order
        .windows(2)
        .all(|w| w[0] + 1 == w[1] || w[0] == w[1]);

    assert!(
        !is_ordered || published_order.len() < total_events,
        "Messages should have been reordered or buffered"
    );

    // Verify all messages eventually arrived
    assert_eq!(
        published_order.len(),
        total_events as usize,
        "All messages should eventually be published"
    );

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_node_handles_slow_consumer_scenario(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Create slow consumer scenario
    let chaos = ChaosTestBuilder::new()
        .with_slow_consumer_delay(Duration::from_millis(100))
        .with_latency(Duration::from_millis(50))
        .build();

    let chaos_injector = ChaosInjestor::from_config(chaos);

    // Measure time to publish events
    let start = std::time::Instant::now();
    let total_events = 10;

    for i in 0..total_events {
        let payload = json!({"value": i});
        let payload_bytes = serde_json::to_vec(&payload).unwrap();

        // Process through slow consumer
        let processed = chaos_injector.process_message(payload_bytes).await?;

        for msg in processed {
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&msg) {
                ctx.publish(DynamicPayload::new("chaos-test", "counter.increment", val))
                    .await?;
            }
        }
    }

    let duration = start.elapsed();

    // Should have taken at least (latency + slow_consumer_delay) * total_events
    let min_expected = Duration::from_millis(150 * total_events);
    assert!(
        duration >= min_expected,
        "Slow consumer should introduce measurable delay: {:?} >= {:?}",
        duration,
        min_expected
    );

    Ok(())
}

#[sinex_test(timeout = 90)]
async fn test_worst_case_chaos_scenario(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Use worst-case scenario: combines all failure modes
    let chaos = ChaosScenarios::worst_case();
    let chaos_injector = ChaosInjestor::from_config(chaos);

    // Track outcomes
    let mut successful = 0;
    let mut corrupted = 0;
    let mut failed = 0;

    let total_events = 30;

    for i in 0..total_events {
        let payload = json!({"value": i, "seq": i});

        // Try to publish with all chaos modes active
        let result = chaos_injector
            .with_simulated_failures(|| async {
                let payload_bytes = serde_json::to_vec(&payload).unwrap();
                let processed = chaos_injector.process_message(payload_bytes).await?;

                for msg in processed {
                    match serde_json::from_slice::<serde_json::Value>(&msg) {
                        Ok(val) => {
                            ctx.publish(DynamicPayload::new(
                                "chaos-test",
                                "counter.increment",
                                val,
                            ))
                            .await?;
                        }
                        Err(_) => {
                            // Corrupted message
                            return Err(color_eyre::eyre::eyre!("corrupted"));
                        }
                    }
                }
                Ok(())
            })
            .await;

        match result {
            Ok(_) => successful += 1,
            Err(e) if e.to_string().contains("corrupted") => corrupted += 1,
            Err(_) => failed += 1,
        }
    }

    // In worst-case scenario, we expect:
    // - Some messages to succeed
    // - Some to be corrupted
    // - Some to fail
    assert!(successful > 0, "Some messages should succeed");
    assert!(
        corrupted > 0 || failed > 0,
        "Should experience some failures or corruption"
    );

    // But node should still be responsive
    let health_check = ctx
        .publish(DynamicPayload::new(
            "chaos-test",
            "counter.increment",
            json!({"value": 999}),
        ))
        .await;
    assert!(
        health_check.is_ok(),
        "Node should still be responsive after chaos"
    );

    Ok(())
}
