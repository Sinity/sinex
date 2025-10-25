//! JetStream consumer integration tests

use async_nats::jetstream;
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_core::JsonValue;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn consume_event_from_jetstream(ctx: TestContext) {
    // Publish event to events.raw stream
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client);

    let event_id = Ulid::new();
    let payload = json!({
        "id": event_id.to_string(),
        "source": "test",
        "event_type": "test.event",
        "ts_orig": "2024-01-01T00:00:00Z",
        "host": "test-host",
        "payload": {"data": "test"}
    });

    let subject = ctx.env().nats_subject("events.raw.test");
    js.publish(subject, payload.to_string().into())
        .await
        .expect("publish failed");

    // Wait for consumer to process
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assert event persisted to DB
    let event = ctx
        .events()
        .get_by_id(&event_id.into())
        .await
        .expect("fetch failed");
    assert_eq!(event.id, event_id.into());
    assert_eq!(event.source.as_str(), "test");
}

#[sinex_test]
async fn consumer_publishes_confirmation(ctx: TestContext) {
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let event_id = Ulid::new();
    let payload = json!({
        "id": event_id.to_string(),
        "source": "test",
        "event_type": "test.event",
        "ts_orig": "2024-01-01T00:00:00Z",
        "host": "test-host",
        "payload": {"data": "test"}
    });

    let subject = ctx.env().nats_subject("events.raw.test");
    js.publish(subject, payload.to_string().into())
        .await
        .expect("publish failed");

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check confirmation stream
    let conf_subject = ctx
        .env()
        .nats_subject(&format!("events.confirmations.{}", event_id));
    let stream = js
        .get_stream(&ctx.env().nats_subject("events_confirmations"))
        .await
        .expect("stream not found");

    // Confirmation should exist
    assert!(stream.info().await.unwrap().state.messages > 0);
}
