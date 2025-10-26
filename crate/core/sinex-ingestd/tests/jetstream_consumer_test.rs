//! JetStream consumer integration tests

use async_nats::jetstream;
use color_eyre::eyre::Result;
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
#[ignore = "Requires TestContext NATS infrastructure - implement in task 1.2"]
async fn consume_event_from_jetstream(ctx: TestContext) -> Result<()> {
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
        .await?
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let event = ctx
        .pool
        .events()
        .get_by_id(&event_id.into())
        .await?
        .expect("Event should exist in database");

    assert_eq!(event.id, event_id.into());
    assert_eq!(event.source.as_str(), "test");

    Ok(())
}

#[sinex_test]
#[ignore = "Requires TestContext NATS infrastructure - implement in task 1.2"]
async fn consumer_publishes_confirmation(ctx: TestContext) -> Result<()> {
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
        .await?
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let stream = js
        .get_stream(&ctx.env().nats_subject("events_confirmations"))
        .await?;

    assert!(stream.info().await?.state.messages > 0);

    Ok(())
}
