//! JetStream Dead Letter Queue integration tests

use async_nats::jetstream;
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_core::DbPoolExt;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::{
    sinex_test, EphemeralNats, EventOverrides, TestContext, TestSatellitePublisher,
};
use std::sync::Arc;
use tokio::sync::RwLock;

#[sinex_test]
async fn test_invalid_event_routed_to_dlq() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(true);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();

    let base_stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: base_stream.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let dlq_stream = format!("{base_stream}_DLQ");
    js.get_or_create_stream(jetstream::stream::Config {
        name: dlq_stream,
        subjects: vec![env.nats_subject("events.dlq.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let topology = JetStreamTopology::new(&env, base_stream.clone(), "ingestd".to_string());
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let _consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let publisher = TestSatellitePublisher::new(nats_client.clone(), "test");
    let event_id = publisher
        .publish_event_with_overrides(
            "test.invalid",
            json!({"data": "test"}),
            EventOverrides {
                ts_orig: Some("invalid-timestamp-format".to_string()),
                ..Default::default()
            },
        )
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let event_in_db = pool.events().get_by_id(event_id.into()).await?;
    assert!(
        event_in_db.is_none(),
        "Invalid event should not be stored in main events table"
    );

    let mut dlq_stream = js.get_stream(&format!("{}_DLQ", base_stream)).await?;

    let info = dlq_stream.info().await?;
    assert!(
        info.state.messages > 0,
        "DLQ should contain at least one message"
    );

    Ok(())
}

#[sinex_test]
async fn test_malformed_json_routed_to_dlq() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();

    let base_stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: base_stream.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let dlq_stream = format!("{base_stream}_DLQ");
    js.get_or_create_stream(jetstream::stream::Config {
        name: dlq_stream,
        subjects: vec![env.nats_subject("events.dlq.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let topology = JetStreamTopology::new(&env, base_stream.clone(), "ingestd".to_string());
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let _consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let publisher = TestSatellitePublisher::new(nats_client.clone(), "test");
    let malformed_json = b"{\"id\": \"not-closed\"";
    publisher
        .publish_raw_event_bytes("test.malformed", malformed_json, None)
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let mut dlq_stream = js.get_stream(&format!("{}_DLQ", base_stream)).await?;

    let info = dlq_stream.info().await?;
    assert!(
        info.state.messages > 0,
        "DLQ should contain malformed JSON message"
    );

    Ok(())
}

#[sinex_test]
async fn test_missing_required_fields_routed_to_dlq() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();

    let base_stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: base_stream.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let dlq_stream = format!("{base_stream}_DLQ");
    js.get_or_create_stream(jetstream::stream::Config {
        name: dlq_stream,
        subjects: vec![env.nats_subject("events.dlq.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let topology = JetStreamTopology::new(&env, base_stream.clone(), "ingestd".to_string());
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let _consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let publisher = TestSatellitePublisher::new(nats_client.clone(), "test");
    let incomplete_payload = json!({
        "id": Ulid::new().to_string(),
        "source": "test"
    });
    publisher
        .publish_raw_event_bytes(
            "test.missing_fields",
            serde_json::to_vec(&incomplete_payload)?,
            None,
        )
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let mut dlq_stream = js.get_stream(&format!("{}_DLQ", base_stream)).await?;

    let info = dlq_stream.info().await?;
    assert!(
        info.state.messages > 0,
        "DLQ should contain event with missing fields"
    );

    Ok(())
}
