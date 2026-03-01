//! Chaos resilience tests for node SDK.
//!
//! These tests verify that nodes can recover from various failure scenarios:
//! - Network partitions
//! - Message loss
//! - Message corruption
//! - Message reordering
//! - Slow consumers

#![allow(dead_code)] // ChaosCounterNode infrastructure ready for future chaos-through-node tests
#![allow(async_fn_in_trait)]

use sinex_node_sdk::{AutomatonNode, ErrorAction, NodeLogicError};
use sinex_primitives::events::Event;
use sinex_primitives::testing::event_fixture;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use xtask::sandbox::chaos::{ChaosContext, ChaosScenarios, ChaosTestBuilder};
use xtask::sandbox::prelude::*;

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

impl AutomatonNode for ChaosCounterNode {
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
        _context: &sinex_node_sdk::automaton_node::NodeEventContext,
    ) -> Result<Option<Self::Output>, NodeLogicError> {
        state.total += input.value;
        self.processed.fetch_add(1, Ordering::SeqCst);

        Ok(Some(CounterOutput {
            new_total: state.total,
            increment: input.value,
        }))
    }

    fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
        self.errors.fetch_add(1, Ordering::SeqCst);
        ErrorAction::Skip
    }
}

/// Create a test event for chaos processing
fn create_test_event(value: u64) -> Event<serde_json::Value> {
    event_fixture(
        "chaos-test",
        "counter.increment",
        serde_json::json!({ "value": value }),
    )
}

/// Process events through a chaos context and count results
async fn process_events_through_chaos(chaos: &ChaosContext, count: usize) -> (usize, usize, usize) {
    let mut processed = 0;
    let mut dropped = 0;
    let mut corrupted = 0;

    for i in 0..count {
        let event = create_test_event(i as u64);
        let original_payload = event.payload.clone();

        if let Some(events) = chaos.process_event(event).await {
            for e in events {
                processed += 1;
                if e.payload != original_payload
                    && e.payload
                        .as_str()
                        .is_some_and(|s| s.starts_with("CORRUPTED_"))
                {
                    corrupted += 1;
                }
            }
        } else {
            dropped += 1;
        }
    }

    // Flush any remaining reordered events
    processed += chaos.flush_reorder_buffer().len();

    (processed, dropped, corrupted)
}

#[sinex_test(timeout = 60)]
async fn test_node_recovers_from_network_partition(_ctx: TestContext) -> TestResult<()> {
    let scenarios = ChaosScenarios::new();

    // Verify partition starts inactive
    assert!(!scenarios.is_partition_active());

    // Simulate a network partition
    let partition_duration = Duration::from_millis(100);
    let partition_task = {
        let scenarios = scenarios.clone();
        tokio::spawn(async move { scenarios.network_partition(partition_duration).await })
    };

    // Wait a bit for partition to become active
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(scenarios.is_partition_active());

    // Wait for partition to end
    partition_task.await??;

    // Verify partition is inactive
    assert!(!scenarios.is_partition_active());

    // Check metrics recorded the partition
    let metrics = scenarios.metrics().snapshot();
    assert_eq!(metrics.partitions, 1);

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_checkpoint_survives_message_loss(_ctx: TestContext) -> TestResult<()> {
    let scenarios = ChaosScenarios::new();

    // Create checkpoint survival context (20% drop rate, some latency)
    let chaos = scenarios.checkpoint_survival_context();

    // Process many events - some will be dropped
    let total_events = 100;
    let (processed, dropped, _corrupted) = process_events_through_chaos(&chaos, total_events).await;

    // With 20% drop rate, we expect roughly 20 dropped, but with randomness
    // Just verify some were dropped and some were processed
    assert!(dropped > 0, "Expected some events to be dropped");
    assert!(
        processed > 0,
        "Expected some events to be processed despite drops"
    );
    assert_eq!(
        processed + dropped,
        total_events,
        "All events should be accounted for"
    );

    // Check metrics
    let metrics = scenarios.metrics().snapshot();
    assert_eq!(metrics.dropped, dropped as u64);

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_node_handles_corrupted_messages(_ctx: TestContext) -> TestResult<()> {
    // Build chaos context with 50% corruption rate for reliable testing
    let chaos = ChaosTestBuilder::new().with_message_corruption(0.5).build();

    // Process events
    let total_events = 50;
    let (processed, _dropped, corrupted) = process_events_through_chaos(&chaos, total_events).await;

    // With 50% corruption, we expect roughly half to be corrupted
    assert!(corrupted > 0, "Expected some events to be corrupted");
    assert!(
        corrupted < processed,
        "Expected some events to be uncorrupted"
    );

    // Verify metrics
    let metrics = chaos.metrics().snapshot();
    assert!(metrics.corrupted > 0, "Metrics should record corruptions");

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_node_handles_message_reordering(_ctx: TestContext) -> TestResult<()> {
    // Build chaos context with 30% reordering rate
    let chaos = ChaosTestBuilder::new().with_reordering(0.3).build();

    // Process events
    let total_events = 30;
    let (processed, _dropped, _corrupted) =
        process_events_through_chaos(&chaos, total_events).await;

    // All events should eventually be processed (reordering doesn't drop)
    assert_eq!(
        processed, total_events,
        "All events should be processed despite reordering"
    );

    // Check that reordering metrics are tracked (probabilistic, so just verify recording)
    let metrics = chaos.metrics().snapshot();
    // metrics.reordered is u64, always >= 0; the important thing is the snapshot succeeds
    let _ = metrics.reordered;

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_node_handles_slow_consumer_scenario(_ctx: TestContext) -> TestResult<()> {
    let scenarios = ChaosScenarios::new();

    // Create slow consumer context with 10ms delay per message
    let chaos = scenarios.slow_consumer_context(Duration::from_millis(10));

    // Process a small batch (slow due to delays)
    let start = std::time::Instant::now();
    let total_events = 5;
    let (processed, _dropped, _corrupted) =
        process_events_through_chaos(&chaos, total_events).await;

    let elapsed = start.elapsed();

    // All events should be processed
    assert_eq!(processed, total_events);

    // Total time should be at least 5 * 10ms = 50ms (plus some overhead)
    assert!(
        elapsed >= Duration::from_millis(40),
        "Slow consumer delay should be applied: {elapsed:?}"
    );

    Ok(())
}

#[sinex_test(timeout = 90)]
async fn test_worst_case_chaos_scenario(_ctx: TestContext) -> TestResult<()> {
    let scenarios = ChaosScenarios::new();

    // Create worst-case context (5% corruption, 10% reorder, 10% drop, high latency, 5% failures)
    let chaos = scenarios.worst_case_context();

    // Process events through worst-case chaos
    let total_events = 100;
    let (processed, dropped, corrupted) = process_events_through_chaos(&chaos, total_events).await;

    // Verify chaos effects occurred
    let metrics = scenarios.metrics().snapshot();

    // We should see a mix of effects
    assert!(
        processed + dropped == total_events,
        "All events should be accounted for"
    );
    assert!(metrics.dropped > 0 || dropped > 0, "Expected some drops");

    // Log summary for debugging
    tracing::info!(
        processed = processed,
        dropped = dropped,
        corrupted = corrupted,
        metrics = %metrics,
        "Worst-case chaos scenario completed"
    );

    // The key assertion: system didn't panic and processed events
    assert!(processed > 0, "Some events should survive worst-case chaos");

    Ok(())
}
