//! JetStream consumer integration tests

use async_nats::jetstream;
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_core::DbPoolExt;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::{sinex_test, TestContext};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;

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
        subjects: vec![env.nats_subject("events.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    // Start JetStream consumer in background
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    // Wait for consumer to fully initialize
    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

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
    let event = timeout(Duration::from_secs(10), async {
        loop {
            if let Some(event) = ctx.pool.events().get_by_id(event_id.into()).await? {
                break Ok::<_, color_eyre::Report>(event);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    assert_eq!(event.id.as_ref().unwrap().as_ulid(), &event_id);
    assert_eq!(event.source.as_str(), "test");

    consumer_handle.abort();
    Ok(())
}

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
        subjects: vec![env.nats_subject("events.>")],
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
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    // Wait for consumer to initialize
    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

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
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check for confirmation in stream
    let mut stream = js
        .get_stream(&ctx.env().nats_stream_name("SINEX_EVENTS_CONFIRMATIONS"))
        .await?;

    assert!(
        stream.info().await?.state.messages > 0,
        "Should have at least one confirmation message"
    );

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn invalid_timestamp_routes_to_dlq_and_allows_progress() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();

    // Ensure streams exist
    let events_stream_name = env.nats_stream_name("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: events_stream_name,
        subjects: vec![env.nats_subject("events.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    // Publish invalid event (bad timestamp)
    let bad_event_id = Ulid::new();
    let bad_payload = json!({
        "id": bad_event_id.to_string(),
        "source": "test",
        "event_type": "test.bad_timestamp",
        "ts_orig": "not-a-timestamp",
        "host": "test-host",
        "payload": {"data": "invalid"}
    });
    let subject = env.nats_subject("events.raw.test");
    js.publish(subject.clone(), bad_payload.to_string().into())
        .await?
        .await?;

    // Publish valid event afterwards to ensure pipeline keeps moving
    let good_event_id = Ulid::new();
    let good_payload = json!({
        "id": good_event_id.to_string(),
        "source": "test",
        "event_type": "test.good",
        "ts_orig": "2024-01-01T00:00:00Z",
        "host": "test-host",
        "payload": {"data": "ok"}
    });
    js.publish(subject, good_payload.to_string().into())
        .await?
        .await?;

    // Valid event should make it to the database
    timeout(Duration::from_secs(10), async {
        loop {
            if pool
                .events()
                .get_by_id(good_event_id.into())
                .await?
                .is_some()
            {
                break Ok::<_, color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    // Verify DLQ received the bad event
    let dlq_stream_name = env.nats_subject("events_dlq");
    let mut dlq_stream = js.get_stream(&dlq_stream_name).await?;
    let state = dlq_stream.info().await?.state;
    assert!(state.messages > 0, "DLQ should contain the rejected event");

    // Ensure the poisoned event never landed in the core events table
    assert!(
        pool.events()
            .get_by_id(bad_event_id.into())
            .await?
            .is_none(),
        "Invalid timestamp event should not be persisted"
    );

    consumer_handle.abort();
    Ok(())
}
