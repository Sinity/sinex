#![cfg(feature = "messaging")]

use sinex_node_sdk::automaton_event_handler::AutomatonEventHandler;
use sinex_node_sdk::confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::builder::EventId;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_automaton_event_handler_basic() -> TestResult<()> {
    let handler = AutomatonEventHandler::new();

    let event_id = EventId::new();
    let provisional = ProvisionalEvent {
        event_id,
        source: EventSource::from_static("test"),
        event_type: EventType::from_static("test.event"),
        payload: serde_json::json!({"data": "test"}),
        ts_orig: sinex_primitives::temporal::now(),
        received_at: sinex_primitives::temporal::now(),
    };

    handler
        .handle_confirmed(&provisional)
        .await
        .expect("handle_confirmed should succeed");

    assert_eq!(handler.processed_count().await, 1);

    let ids = handler.processed_event_ids().await;
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0], event_id);
    Ok(())
}

#[sinex_test]
async fn test_automaton_event_handler_multiple_events() -> TestResult<()> {
    let handler = AutomatonEventHandler::new();

    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event_id = EventId::new();
        event_ids.push(event_id);

        let provisional = ProvisionalEvent {
            event_id,
            source: format!("test{i}").into(),
            event_type: EventType::from_static("test.event"),
            payload: serde_json::json!({"index": i}),
            ts_orig: sinex_primitives::temporal::now(),
            received_at: sinex_primitives::temporal::now(),
        };

        handler
            .handle_confirmed(&provisional)
            .await
            .expect("handle_confirmed should succeed");
    }

    assert_eq!(handler.processed_count().await, 10);

    let tracked_ids = handler.processed_event_ids().await;
    assert_eq!(tracked_ids.len(), 10);
    assert_eq!(tracked_ids, event_ids);
    Ok(())
}
