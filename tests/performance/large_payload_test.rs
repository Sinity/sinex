//! JetStream large payload handling tests.
//!
//! Ensures that sizeable messages can be published, stored, and consumed
//! without fragmentation issues.

use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy},
    Context as JetStream,
};
use color_eyre::eyre::Result;
use futures::StreamExt;
use serde_json::json;
use sinex_core::types::ulid::Ulid;
use sinex_test_utils::{prelude::*, EphemeralNats};
use std::time::Duration as StdDuration;

async fn provision(js: &JetStream, stream: &str, subject: &str) -> Result<()> {
    let config = StreamConfig {
        name: stream.to_string(),
        subjects: vec![subject.to_string()],
        retention: RetentionPolicy::Limits,
        max_msgs: 32,
        max_age: StdDuration::from_secs(300),
        ..Default::default()
    };
    js.get_or_create_stream(config).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_large_payload_roundtrip(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client);

    let stream = format!("perf_large_payload_{}", Ulid::new());
    let subject = format!("perf.large.{}", Ulid::new());
    provision(&js, &stream, &subject).await?;

    // Create a 512 KiB payload to exercise chunking limits.
    const PAYLOAD_SIZE: usize = 512 * 1024;
    let mut payload = Vec::with_capacity(PAYLOAD_SIZE);
    payload.resize(PAYLOAD_SIZE, b'A');

    let data = serde_json::to_vec(&json!({
        "id": Ulid::new().to_string(),
        "size_bytes": PAYLOAD_SIZE,
        "payload": base64::encode(&payload),
    }))?;

    let ack = js.publish(&subject, data.into()).await?;
    ack.await?;

    let consumer = js
        .get_or_create_consumer(
            &stream,
            ConsumerConfig {
                durable_name: Some("large-payload-consumer".to_string()),
                name: Some("large-payload-consumer".to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: subject.clone(),
                ack_wait: StdDuration::from_secs(30),
                max_ack_pending: 8,
                ..Default::default()
            },
        )
        .await?;

    let mut messages = consumer
        .fetch()
        .max_messages(1)
        .expires(StdDuration::from_secs(2))
        .messages()
        .await?;

    let message = messages
        .next()
        .await
        .ok_or_else(|| color_eyre::eyre::eyre!("expected one large message"))??;

    // Ensure payload length matches after decode.
    let decoded: serde_json::Value = serde_json::from_slice(&message.payload)?;
    let encoded_payload = decoded["payload"].as_str().unwrap();
    let bytes = base64::decode(encoded_payload)?;
    color_eyre::eyre::ensure!(
        bytes.len() == PAYLOAD_SIZE,
        "payload size mismatch: expected {PAYLOAD_SIZE}, observed {}",
        bytes.len()
    );

    message.ack().await?;
    js.delete_consumer(&stream, "large-payload-consumer").await?;
    js.delete_stream(&stream).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_large_batch_drain(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client);

    let stream = format!("perf_large_batch_{}", Ulid::new());
    let subject = format!("perf.large.batch.{}", Ulid::new());
    provision(&js, &stream, &subject).await?;

    for idx in 0..20 {
        let payload = vec![idx as u8; 64 * 1024];
        let message = serde_json::to_vec(&json!({
            "sequence": idx,
            "payload": base64::encode(&payload)
        }))?;
        js.publish(&subject, message.into()).await?.await?;
    }

    let consumer = js
        .get_or_create_consumer(
            &stream,
            ConsumerConfig {
                durable_name: Some("large-batch-consumer".to_string()),
                name: Some("large-batch-consumer".to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: subject.clone(),
                ack_wait: StdDuration::from_secs(60),
                max_ack_pending: 64,
                ..Default::default()
            },
        )
        .await?;

    let mut total = 0usize;
    loop {
        let mut batch = consumer
            .fetch()
            .max_messages(10)
            .expires(StdDuration::from_secs(1))
            .messages()
            .await?;

        let mut handled = false;
        while let Some(message) = batch.next().await {
            let message = message?;
            total += 1;
            message.ack().await?;
            handled = true;
        }

        if !handled {
            break;
        }
    }

    color_eyre::eyre::ensure!(total == 20, "expected to drain 20 large messages");

    js.delete_consumer(&stream, "large-batch-consumer").await?;
    js.delete_stream(&stream).await?;
    Ok(())
}
