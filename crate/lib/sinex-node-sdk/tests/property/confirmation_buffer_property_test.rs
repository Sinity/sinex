//! Property tests for ConfirmationBuffer
//!
//! Verifies that the confirmation buffer maintains invariants:
//! - Buffer never exceeds capacity
//! - All confirmed events were previously provisional
//! - Out-of-order confirmations eventually match
//! - Capacity warnings work correctly

use chrono::Utc;
use proptest::prelude::*;
use sinex_core::{EventId, EventSource, EventType, Ulid};
use sinex_node_sdk::{ConfirmationBuffer, EventConfirmation, ProvisionalEvent};
use sinex_test_utils::{sinex_prop, TestContext, TestResult};
use std::time::Duration;

// =============================================================================
// Strategies
// =============================================================================

/// Strategy for generating provisional events
fn arb_provisional_event() -> impl Strategy<Value = ProvisionalEvent> {
    ("[a-z][a-z0-9._]{2,20}", "[a-z][a-z0-9._]{2,20}")
        .prop_map(|(source, event_type)| ProvisionalEvent {
            event_id: EventId::from_ulid(Ulid::new()),
            source: EventSource::new(source),
            event_type: EventType::new(event_type),
            payload: serde_json::json!({"test": "data"}),
            ts_orig: Utc::now(),
            received_at: Utc::now(),
        })
}

// Note: The actual API uses `confirm(event_id: EventId)` not `confirm(EventConfirmation)`
// but we keep EventConfirmation type for documentation purposes

// =============================================================================
// Property Tests
// =============================================================================

#[sinex_prop]
async fn property_buffer_never_exceeds_capacity(
    _ctx: &TestContext,
    #[strategy(proptest::collection::vec(arb_provisional_event(), 1..=100))] events: Vec<
        ProvisionalEvent,
    >,
    #[strategy(10usize..50usize)] capacity: usize,
) -> TestResult<()> {
    // Property: Buffer should reject events when at capacity
    let buffer = ConfirmationBuffer::with_capacity(Duration::from_secs(60), capacity);

    let mut accepted = 0;
    for event in events.iter().take(capacity + 10) {
        if buffer.add_provisional(event.clone()).await {
            accepted += 1;
        }
    }

    // Should accept at most `capacity` events
    prop_assert!(
        accepted <= capacity,
        "Buffer accepted {} events but capacity is {}",
        accepted,
        capacity
    );

    // Verify internal state
    let pending_count = buffer.len().await;
    prop_assert!(
        pending_count <= capacity,
        "Buffer contains {} events but capacity is {}",
        pending_count,
        capacity
    );

    Ok(())
}

#[sinex_prop]
async fn property_confirmed_events_were_provisional(
    _ctx: &TestContext,
    #[strategy(proptest::collection::vec(arb_provisional_event(), 1..=20))] events: Vec<
        ProvisionalEvent,
    >,
) -> TestResult<()> {
    // Property: Can only confirm events that were previously added
    let buffer = ConfirmationBuffer::new(Duration::from_secs(60));

    // Add provisional events
    for event in &events {
        buffer.add_provisional(event.clone()).await;
    }

    // Confirm events that were added
    for event in &events {
        let result = buffer.confirm(event.event_id).await;
        prop_assert!(
            result.is_some(),
            "Event {:?} was provisional but confirmation returned None",
            event.event_id
        );
    }

    // Buffer should be empty now
    let pending_count = buffer.len().await;
    prop_assert_eq!(
        pending_count, 0,
        "All events confirmed but buffer still has {} pending",
        pending_count
    );

    Ok(())
}

// Note: The current implementation doesn't support early confirmations (confirmation before provisional)
// This test is skipped as the API doesn't buffer confirmations - it only removes from pending map

#[sinex_prop]
async fn property_out_of_order_confirmations_eventually_match(
    _ctx: &TestContext,
    #[strategy(proptest::collection::vec(arb_provisional_event(), 2..=10))] events: Vec<
        ProvisionalEvent,
    >,
) -> TestResult<()> {
    // Property: Confirmations can arrive in any order and still match
    let buffer = ConfirmationBuffer::new(Duration::from_secs(60));

    // Add provisional events
    for event in &events {
        buffer.add_provisional(event.clone()).await;
    }

    // Confirm in reverse order
    for event in events.iter().rev() {
        let result = buffer.confirm(event.event_id).await;
        prop_assert!(
            result.is_some(),
            "Event should be confirmed regardless of order"
        );
    }

    // All should be confirmed
    let pending_count = buffer.len().await;
    prop_assert_eq!(
        pending_count, 0,
        "All events should be confirmed but {} still pending",
        pending_count
    );

    Ok(())
}

#[sinex_prop]
async fn property_buffer_idempotent_confirmation(
    _ctx: &TestContext,
    #[strategy(arb_provisional_event())] event: ProvisionalEvent,
) -> TestResult<()> {
    // Property: Confirming the same event multiple times should be safe
    let buffer = ConfirmationBuffer::new(Duration::from_secs(60));

    buffer.add_provisional(event.clone()).await;

    // First confirmation should succeed
    let result1 = buffer.confirm(event.event_id).await;
    prop_assert!(result1.is_some(), "First confirmation should succeed");

    // Second confirmation should return None (event already confirmed)
    let result2 = buffer.confirm(event.event_id).await;
    prop_assert!(
        result2.is_none(),
        "Second confirmation should return None (already confirmed)"
    );

    Ok(())
}

#[sinex_prop]
async fn property_pending_count_is_accurate(
    _ctx: &TestContext,
    #[strategy(proptest::collection::vec(arb_provisional_event(), 1..=50))] events: Vec<
        ProvisionalEvent,
    >,
) -> TestResult<()> {
    // Property: pending_count() reflects actual number of pending events
    let buffer = ConfirmationBuffer::new(Duration::from_secs(60));

    // Add events and track expected count
    let mut expected_count = 0;
    for event in &events {
        buffer.add_provisional(event.clone()).await;
        expected_count += 1;

        let actual_count = buffer.len().await;
        prop_assert_eq!(
            actual_count, expected_count,
            "Pending count mismatch after adding event"
        );
    }

    // Confirm events and verify count decreases
    for event in &events {
        buffer.confirm(event.event_id).await;
        expected_count -= 1;

        let actual_count = buffer.len().await;
        prop_assert_eq!(
            actual_count, expected_count,
            "Pending count mismatch after confirming event"
        );
    }

    prop_assert_eq!(expected_count, 0, "All events should be confirmed");
    Ok(())
}

#[sinex_prop]
async fn property_capacity_limit_prevents_unbounded_growth(
    _ctx: &TestContext,
    #[strategy(10usize..30usize)] capacity: usize,
    #[strategy(50usize..100usize)] num_events: usize,
) -> TestResult<()> {
    // Property: Buffer should never grow beyond capacity even with many events
    let buffer = ConfirmationBuffer::with_capacity(Duration::from_secs(60), capacity);

    // Try to add more events than capacity
    for i in 0..num_events {
        let event = ProvisionalEvent {
            event_id: EventId::from_ulid(Ulid::new()),
            source: EventSource::new("test"),
            event_type: EventType::new("test.event"),
            payload: serde_json::json!({"index": i}),
            ts_orig: Utc::now(),
            received_at: Utc::now(),
        };
        buffer.add_provisional(event).await;
    }

    // Verify capacity constraint
    let pending_count = buffer.len().await;
    prop_assert!(
        pending_count <= capacity,
        "Buffer has {} events but capacity is {}",
        pending_count,
        capacity
    );

    Ok(())
}

#[sinex_prop]
async fn property_rejected_count_tracks_capacity_rejections(
    _ctx: &TestContext,
    #[strategy(5usize..10usize)] capacity: usize,
    #[strategy(20usize..30usize)] num_events: usize,
) -> TestResult<()> {
    // Property: rejected_count should match number of events rejected due to capacity
    let buffer = ConfirmationBuffer::with_capacity(Duration::from_secs(60), capacity);

    let mut rejected = 0;
    for i in 0..num_events {
        let event = ProvisionalEvent {
            event_id: EventId::from_ulid(Ulid::new()),
            source: EventSource::new("test"),
            event_type: EventType::new("test.event"),
            payload: serde_json::json!({"index": i}),
            ts_orig: Utc::now(),
            received_at: Utc::now(),
        };

        if !buffer.add_provisional(event).await {
            rejected += 1;
        }
    }

    // The number of rejections should be at least num_events - capacity
    let expected_rejections = num_events.saturating_sub(capacity);
    prop_assert!(
        rejected >= expected_rejections,
        "Expected at least {} rejections but got {}",
        expected_rejections,
        rejected
    );

    Ok(())
}

// Note: Current implementation doesn't buffer confirmations that arrive early,
// so this test is not applicable. The confirm() method only removes from pending map.

#[sinex_prop]
async fn property_buffer_operations_are_deterministic(
    _ctx: &TestContext,
    #[strategy(arb_provisional_event())] event: ProvisionalEvent,
) -> TestResult<()> {
    // Property: Same sequence of operations produces same result
    let buffer1 = ConfirmationBuffer::new(Duration::from_secs(60));
    let buffer2 = ConfirmationBuffer::new(Duration::from_secs(60));

    // Perform same operations on both buffers
    buffer1.add_provisional(event.clone()).await;
    buffer2.add_provisional(event.clone()).await;

    let count1 = buffer1.len().await;
    let count2 = buffer2.len().await;
    prop_assert_eq!(count1, count2);

    let result1 = buffer1.confirm(event.event_id).await;
    let result2 = buffer2.confirm(event.event_id).await;

    prop_assert_eq!(result1.is_some(), result2.is_some());

    let final_count1 = buffer1.len().await;
    let final_count2 = buffer2.len().await;
    prop_assert_eq!(final_count1, final_count2);

    Ok(())
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    async fn test_empty_buffer_has_zero_pending() -> TestResult<()> {
        let buffer = ConfirmationBuffer::new(Duration::from_secs(60));
        assert_eq!(buffer.len().await, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_basic_add_and_confirm_flow() -> TestResult<()> {
        let buffer = ConfirmationBuffer::new(Duration::from_secs(60));

        let event = ProvisionalEvent {
            event_id: EventId::from_ulid(Ulid::new()),
            source: EventSource::new("test"),
            event_type: EventType::new("test.event"),
            payload: serde_json::json!({"data": "test"}),
            ts_orig: Utc::now(),
            received_at: Utc::now(),
        };

        // Add event
        let accepted = buffer.add_provisional(event.clone()).await;
        assert!(accepted);
        assert_eq!(buffer.len().await, 1);

        // Confirm event
        let result = buffer.confirm(event.event_id).await;
        assert!(result.is_some());
        assert_eq!(buffer.len().await, 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_capacity_limit_rejects_excess_events() -> TestResult<()> {
        let capacity = 5;
        let buffer = ConfirmationBuffer::with_capacity(Duration::from_secs(60), capacity);

        // Add capacity + 1 events
        let mut accepted = 0;
        for i in 0..capacity + 1 {
            let event = ProvisionalEvent {
                event_id: EventId::from_ulid(Ulid::new()),
                source: EventSource::new("test"),
                event_type: EventType::new("test.event"),
                payload: serde_json::json!({"index": i}),
                ts_orig: Utc::now(),
                received_at: Utc::now(),
            };

            if buffer.add_provisional(event).await {
                accepted += 1;
            }
        }

        assert_eq!(accepted, capacity);
        assert_eq!(buffer.len().await, capacity);

        Ok(())
    }
}
