//! JetStream consumer integration tests

use async_nats::jetstream;
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_core::DbPoolExt;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::JetStreamConsumer;
use sinex_test_utils::{sinex_test, TestContext};
use std::sync::Arc;

#[ignore = "requires full ingestd pipeline"]
#[sinex_test]
async fn consume_event_from_jetstream() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();

    // Create validator (validation disabled for tests)
    let validator = EventValidator::new(false);

    // Create JetStream context and manually create the events stream
    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();

    // Bootstrap the events_raw stream before starting consumer
    let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: stream_name,
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    // Start JetStream consumer in background
    let consumer = JetStreamConsumer::new(nats_client.clone(), pool.clone(), Arc::new(validator));
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    // Wait for consumer to fully initialize
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Publish test event
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
    eprintln!("Publishing to subject: {}", subject);
    js.publish(subject.clone(), payload.to_string().into())
        .await?
        .await?;
    eprintln!("Published event {} to {}", event_id, subject);

    // Wait for consumer to process with retries
    let mut event = None;
    for attempt in 0..20 {
        event = ctx.pool.events().get_by_id(event_id.into()).await?;
        if event.is_some() {
            eprintln!("Event found after {} attempts", attempt + 1);
            break;
        }
        eprintln!("Attempt {}: Event not found yet, waiting...", attempt + 1);
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    let event = event.expect("Event should exist in database after 10 seconds");

    assert_eq!(event.id.as_ref().unwrap().as_ulid(), &event_id);
    assert_eq!(event.source.as_str(), "test");

    drop(consumer_handle);
    Ok(())
}

#[ignore = "requires full ingestd pipeline"]
#[sinex_test]
async fn consumer_publishes_confirmation() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();

    // Create validator (validation disabled for tests)
    let validator = EventValidator::new(false);

    // Create JetStream context and manually create streams
    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();

    // Bootstrap the events_raw and confirmations streams
    let events_stream_name = env.nats_stream_name("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: events_stream_name,
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let confirmations_stream = env.nats_stream_name("SINEX_EVENTS_CONFIRMATIONS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: confirmations_stream,
        subjects: vec![env.nats_subject("events.confirmations.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages_per_subject: 1,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    // Start JetStream consumer in background
    let consumer = JetStreamConsumer::new(nats_client.clone(), pool.clone(), Arc::new(validator));
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    // Wait for consumer to initialize
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Publish test event
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

    // Wait for processing
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Check for confirmation in stream
    let mut stream = js
        .get_stream(&ctx.env().nats_stream_name("SINEX_EVENTS_CONFIRMATIONS"))
        .await?;

    assert!(
        stream.info().await?.state.messages > 0,
        "Should have at least one confirmation message"
    );

    drop(consumer_handle);
    Ok(())
}
