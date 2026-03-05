//! Comprehensive tests for `ConfirmationBuffer`
//!
//! Tests concurrent access, edge cases, and error handling scenarios.

use sinex_node_sdk::confirmation_handler::{ConfirmationBuffer, ProvisionalEvent};
use sinex_node_sdk::prelude::*;
use sinex_primitives::Uuid;
use xtask::sandbox::sinex_test;
use xtask::sandbox::timing::Timeouts;

use std::sync::Arc;
use tokio::sync::Barrier;

fn make_event() -> ProvisionalEvent {
    ProvisionalEvent {
        event_id: Uuid::now_v7().into(),
        source: EventSource::from_static("test-source"),
        event_type: EventType::from_static("test.event.type"),
        payload: serde_json::json!({"key": "value"}),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    }
}

#[sinex_test]
async fn confirm_non_existent_event_returns_none() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));
    let non_existent_id = Uuid::now_v7().into();

    let result = buffer.confirm(non_existent_id).await;
    assert!(
        result.is_none(),
        "Confirming non-existent event should return None"
    );

    Ok(())
}

#[sinex_test]
async fn double_confirm_same_event_returns_none_second_time() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));
    let event = make_event();
    let event_id = event.event_id;

    buffer.add_provisional(event).await;

    // First confirm succeeds
    let first = buffer.confirm(event_id).await;
    assert!(first.is_some(), "First confirm should succeed");

    // Second confirm returns None
    let second = buffer.confirm(event_id).await;
    assert!(second.is_none(), "Second confirm should return None");

    Ok(())
}

#[sinex_test]
async fn add_duplicate_event_overwrites_existing() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));
    let event_id = Uuid::now_v7();

    let event1 = ProvisionalEvent {
        event_id: event_id.into(),
        source: EventSource::from_static("source1"),
        event_type: EventType::from_static("type1"),
        payload: serde_json::json!({"version": 1}),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    let event2 = ProvisionalEvent {
        event_id: event_id.into(),
        source: EventSource::from_static("source2"),
        event_type: EventType::from_static("type2"),
        payload: serde_json::json!({"version": 2}),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    buffer.add_provisional(event1).await;
    buffer.add_provisional(event2).await;

    // Buffer should still have only 1 event
    assert_eq!(buffer.len().await, 1);

    // Confirm should return the second event (overwrite)
    let confirmed = buffer
        .confirm(event_id.into())
        .await
        .expect("Should confirm");
    assert_eq!(confirmed.source, "source2".into());
    assert_eq!(confirmed.event_type, "type2".into());

    Ok(())
}

#[sinex_test]
async fn multiple_events_can_be_added_and_confirmed_independently() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));

    let event1 = make_event();
    let event2 = make_event();
    let event3 = make_event();

    let id1 = event1.event_id;
    let id2 = event2.event_id;
    let id3 = event3.event_id;

    buffer.add_provisional(event1).await;
    buffer.add_provisional(event2).await;
    buffer.add_provisional(event3).await;

    assert_eq!(buffer.len().await, 3);

    // Confirm out of order
    let confirmed2 = buffer.confirm(id2).await;
    assert!(confirmed2.is_some());
    assert_eq!(buffer.len().await, 2);

    let confirmed1 = buffer.confirm(id1).await;
    assert!(confirmed1.is_some());
    assert_eq!(buffer.len().await, 1);

    let confirmed3 = buffer.confirm(id3).await;
    assert!(confirmed3.is_some());
    assert_eq!(buffer.len().await, 0);
    assert!(buffer.is_empty().await);

    Ok(())
}

#[sinex_test]
async fn concurrent_add_operations_are_safe() -> TestResult<()> {
    let buffer = Arc::new(ConfirmationBuffer::new(std::time::Duration::from_secs(
        Timeouts::STANDARD,
    )));
    let barrier = Arc::new(Barrier::new(10));

    let mut handles = Vec::new();
    let mut event_ids = Vec::new();

    for _ in 0..10 {
        let event = make_event();
        event_ids.push(event.event_id);

        let buffer_clone = buffer.clone();
        let barrier_clone = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier_clone.wait().await;
            buffer_clone.add_provisional(event).await;
        }));
    }

    for handle in handles {
        handle.await?;
    }

    assert_eq!(buffer.len().await, 10);

    // All events should be confirmable
    for id in event_ids {
        assert!(buffer.confirm(id).await.is_some());
    }

    Ok(())
}

#[sinex_test]
async fn concurrent_confirm_operations_are_safe() -> TestResult<()> {
    let buffer = Arc::new(ConfirmationBuffer::new(std::time::Duration::from_secs(
        Timeouts::STANDARD,
    )));

    // Add events first
    let mut event_ids = Vec::new();
    for _ in 0..10 {
        let event = make_event();
        event_ids.push(event.event_id);
        buffer.add_provisional(event).await;
    }

    let barrier = Arc::new(Barrier::new(10));
    let mut handles = Vec::new();

    // Concurrently confirm all events
    for id in event_ids {
        let buffer_clone = buffer.clone();
        let barrier_clone = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier_clone.wait().await;
            buffer_clone.confirm(id).await
        }));
    }

    let mut success_count = 0;
    for handle in handles {
        if handle.await?.is_some() {
            success_count += 1;
        }
    }

    // All 10 confirms should succeed
    assert_eq!(success_count, 10);
    assert!(buffer.is_empty().await);

    Ok(())
}

#[sinex_test]
async fn timeout_check_with_no_events_returns_empty() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_millis(100));

    let timed_out = buffer.check_timeouts().await;
    assert!(timed_out.is_empty());

    Ok(())
}

#[sinex_test]
async fn timeout_check_with_fresh_events_returns_empty() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));

    let event = make_event();
    buffer.add_provisional(event).await;

    let timed_out = buffer.check_timeouts().await;
    assert!(timed_out.is_empty(), "Fresh event should not be timed out");

    Ok(())
}

#[sinex_test]
async fn timeout_check_identifies_only_expired_events() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::SHORT));

    // Add fresh event
    let fresh_event = make_event();
    let fresh_id = fresh_event.event_id;
    buffer.add_provisional(fresh_event).await;

    // Add old event with backdated received_at
    let old_id = Uuid::now_v7();
    let old_event = ProvisionalEvent {
        event_id: old_id.into(),
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("test.old"),
        payload: serde_json::json!({}),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now() - time::Duration::seconds(10),
    };
    buffer.add_provisional(old_event).await;

    let timed_out = buffer.check_timeouts().await;
    assert_eq!(timed_out.len(), 1);
    assert_eq!(timed_out[0], old_id.into());

    // Fresh event should still be in buffer
    assert!(buffer.confirm(fresh_id).await.is_some());

    Ok(())
}

#[sinex_test]
async fn remove_timed_out_with_empty_list_does_nothing() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));

    let event = make_event();
    buffer.add_provisional(event).await;

    let removed = buffer.remove_timed_out(&[]).await;
    assert!(removed.is_empty());
    assert_eq!(buffer.len().await, 1);

    Ok(())
}

#[sinex_test]
async fn remove_timed_out_with_non_existent_ids_returns_empty() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));

    let event = make_event();
    buffer.add_provisional(event).await;

    let non_existent_ids = vec![Uuid::now_v7().into(), Uuid::now_v7().into()];
    let removed = buffer.remove_timed_out(&non_existent_ids).await;

    assert!(removed.is_empty());
    assert_eq!(buffer.len().await, 1); // Original event still there

    Ok(())
}

#[sinex_test]
async fn remove_timed_out_with_mixed_ids() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));

    let event1 = make_event();
    let event2 = make_event();
    let id1 = event1.event_id;
    let id2 = event2.event_id;

    buffer.add_provisional(event1).await;
    buffer.add_provisional(event2).await;

    // Remove one existing and one non-existent
    let non_existent = Uuid::now_v7();
    let removed = buffer.remove_timed_out(&[id1, non_existent.into()]).await;

    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].event_id, id1);
    assert_eq!(buffer.len().await, 1);

    // id2 should still be confirmable
    assert!(buffer.confirm(id2).await.is_some());

    Ok(())
}

#[sinex_test]
async fn is_empty_reflects_buffer_state() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));

    assert!(buffer.is_empty().await);

    let event = make_event();
    let id = event.event_id;
    buffer.add_provisional(event).await;

    assert!(!buffer.is_empty().await);

    buffer.confirm(id).await;
    assert!(buffer.is_empty().await);

    Ok(())
}

#[sinex_test]
async fn event_payload_preserved_through_buffer() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(Timeouts::STANDARD));

    let complex_payload = serde_json::json!({
        "nested": {
            "array": [1, 2, 3],
            "object": {"key": "value"}
        },
        "unicode": "日本語テスト",
        "number": 42.5
    });

    let event = ProvisionalEvent {
        event_id: Uuid::now_v7().into(),
        source: EventSource::from_static("complex-source"),
        event_type: EventType::from_static("complex.event"),
        payload: complex_payload.clone(),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    let id = event.event_id;
    buffer.add_provisional(event).await;

    let confirmed = buffer.confirm(id).await.expect("Should confirm");
    assert_eq!(confirmed.payload, complex_payload);
    assert_eq!(confirmed.source, "complex-source".into());
    assert_eq!(confirmed.event_type, "complex.event".into());

    Ok(())
}
