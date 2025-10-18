use std::time::Duration;

use sinex_core::{Event, JsonValue};
use sinex_test_utils::{create_enhanced_event_sender, sinex_test};
use tokio::sync::mpsc;

#[sinex_test]
async fn test_enhanced_event_sender() -> color_eyre::eyre::Result<()> {
    let (tx, mut rx) = mpsc::channel::<Event<JsonValue>>(10);
    let sender = create_enhanced_event_sender(tx, "test_source".to_string());

    let event = Event::<JsonValue>::test_event(
        sinex_core::EventSource::new("test_source"),
        sinex_core::EventType::new("test_event"),
        serde_json::json!({}),
    );

    assert!(sender.send_event(event, "test context").await.is_ok());

    let received = rx.recv().await.unwrap();
    assert_eq!(received.event_type.as_str(), "test_event");

    let stats = sender.get_stats();
    assert_eq!(stats.sent, 1);
    assert_eq!(stats.errors, 0);

    let metrics = sender.get_performance_metrics();
    assert_eq!(metrics.send_attempts, 1);
    assert_eq!(metrics.send_successes, 1);
    assert_eq!(metrics.send_failures, 0);
    assert_eq!(metrics.success_rate, 1.0);
    Ok(())
}

#[sinex_test]
async fn test_enhanced_sender_timeout() -> color_eyre::eyre::Result<()> {
    let (tx, _rx) = mpsc::channel::<Event<JsonValue>>(1);
    let sender = create_enhanced_event_sender(tx, "test_source".to_string());

    let event1 = Event::<JsonValue>::test_event(
        sinex_core::EventSource::new("test_source"),
        sinex_core::EventType::new("test_event"),
        serde_json::json!({}),
    );
    let _ = sender.send_event(event1, "fill channel").await;

    let event2 = Event::<JsonValue>::test_event(
        sinex_core::EventSource::new("test_source"),
        sinex_core::EventType::new("test_event"),
        serde_json::json!({}),
    );

    let result = sender
        .send_event_timeout(event2, Duration::from_millis(10), "timeout test")
        .await;

    assert!(result.is_err());

    let metrics = sender.get_performance_metrics();
    assert!(metrics.send_failures > 0);
    Ok(())
}
