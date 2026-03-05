use sinex_node_sdk::{ConfirmationBuffer, ProvisionalEvent};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::builder::EventId;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_confirmation_buffer_add_and_confirm() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(60));

    let event_id = EventId::new();
    let event = ProvisionalEvent {
        event_id,
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("test.event"),
        payload: serde_json::json!({"data": "test"}),
        ts_orig: sinex_primitives::temporal::now(),
        received_at: sinex_primitives::temporal::now(),
    };

    assert!(buffer.add_provisional(event.clone()).await);
    assert_eq!(buffer.len().await, 1);

    let confirmed = buffer.confirm(event_id).await;
    assert!(confirmed.is_some());
    assert_eq!(buffer.len().await, 0);
    Ok(())
}

#[sinex_test]
async fn test_confirmation_buffer_timeout() -> TestResult<()> {
    let buffer = ConfirmationBuffer::new(std::time::Duration::from_millis(100));

    let event_id = EventId::new();
    let mut event = ProvisionalEvent {
        event_id,
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("test.event"),
        payload: serde_json::json!({"data": "test"}),
        ts_orig: sinex_primitives::temporal::now(),
        received_at: sinex_primitives::temporal::now(),
    };

    event.received_at = event.received_at - time::Duration::seconds(1);
    assert!(buffer.add_provisional(event).await);

    let timed_out = buffer.check_timeouts().await;
    assert_eq!(timed_out.len(), 1);
    assert_eq!(timed_out[0], event_id);

    let removed = buffer.remove_timed_out(&timed_out).await;
    assert_eq!(removed.len(), 1);
    assert_eq!(buffer.len().await, 0);
    Ok(())
}

#[sinex_test]
async fn test_confirmation_buffer_capacity_limit() -> TestResult<()> {
    let max_capacity = 5;
    let buffer = ConfirmationBuffer::with_capacity(std::time::Duration::from_secs(60), max_capacity);

    for i in 0..max_capacity {
        let event_id = EventId::new();
        let event = ProvisionalEvent {
            event_id,
            source: format!("test-{i}").into(),
            event_type: EventType::from_static("test.event"),
            payload: serde_json::json!({"index": i}),
            ts_orig: sinex_primitives::temporal::now(),
            received_at: sinex_primitives::temporal::now(),
        };
        assert!(buffer.add_provisional(event).await, "Should accept event {i}");
    }

    assert_eq!(buffer.len().await, max_capacity);

    let event_id = EventId::new();
    let overflow_event = ProvisionalEvent {
        event_id,
        source: EventSource::from_static("overflow"),
        event_type: EventType::from_static("test.event"),
        payload: serde_json::json!({"overflow": true}),
        ts_orig: sinex_primitives::temporal::now(),
        received_at: sinex_primitives::temporal::now(),
    };
    assert!(!buffer.add_provisional(overflow_event).await, "Should reject overflow");
    assert_eq!(buffer.rejected_count(), 1);
    Ok(())
}
