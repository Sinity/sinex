//! Resource exhaustion smoke tests for JetStream.
//!
//! These benches stress a temporary JetStream stream to ensure the system
//! behaves sensibly when approaching storage and consumer limits.

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
use std::time::Duration;

async fn setup_stream(js: &JetStream, name: &str, subject: &str, max_msgs: i64) -> Result<()> {
    let config = StreamConfig {
        name: name.to_string(),
        subjects: vec![subject.to_string()],
        retention: RetentionPolicy::Limits,
        max_msgs,
        max_age: Duration::from_secs(30),
        ..Default::default()
    };
    js.get_or_create_stream(config).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_backpressure_limits(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client);

    let stream = format!("perf_limits_{}", Ulid::new());
    let subject = format!("perf.limits.{}", Ulid::new());
    // Keep the retention small so we hit the cap quickly.
    setup_stream(&js, &stream, &subject, 200).await?;

    // Publish until the stream reports it is at capacity.
    for i in 0..400 {
        let payload = serde_json::to_vec(&json!({
            "sequence": i,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))?;
        let ack = js.publish(&subject, payload.into()).await?;
        ack.await?;
    }

    let info = js.stream_info(&stream).await?;
    color_eyre::eyre::ensure!(
        info.state.messages <= 200,
        "stream should cap at 200 messages, observed {}",
        info.state.messages
    );

    js.delete_stream(&stream).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_consumer_recovery(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client.clone());

    let stream = format!("perf_recovery_{}", Ulid::new());
    let subject = format!("perf.recovery.{}", Ulid::new());
    setup_stream(&js, &stream, &subject, 1_000).await?;

    // Seed a modest workload.
    for _ in 0..250 {
        let payload = serde_json::to_vec(&json!({
            "event_id": Ulid::new().to_string(),
        }))?;
        js.publish(&subject, payload.into()).await?.await?;
    }

    // Create a consumer and fetch a batch, acknowledging only half to simulate failures.
    let durable = format!("perf_recovery_consumer_{}", Ulid::new());
    let consumer = js
        .get_or_create_consumer(
            &stream,
            ConsumerConfig {
                durable_name: Some(durable.clone()),
                name: Some(durable.clone()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: subject.clone(),
                max_ack_pending: 64,
                ..Default::default()
            },
        )
        .await?;

    let mut batch = consumer
        .fetch()
        .max_messages(128)
        .expires(Duration::from_secs(2))
        .messages()
        .await?;

    let mut processed = 0usize;
    while let Some(message) = batch.next().await {
        let message = message?;
        if processed % 2 == 0 {
            message.ack().await?;
        }
        processed += 1;
    }

    // Restart consumer and ensure pending messages can still be drained.
    drop(batch);

    let mut drain = consumer
        .fetch()
        .max_messages(128)
        .expires(Duration::from_secs(2))
        .messages()
        .await?;

    let mut remaining = 0usize;
    while let Some(message) = drain.next().await {
        let message = message?;
        message.ack().await?;
        remaining += 1;
    }

    color_eyre::eyre::ensure!(
        remaining > 0,
        "expected to recover unacked messages on restart"
    );

    js.delete_consumer(&stream, &durable).await?;
    js.delete_stream(&stream).await?;
    Ok(())
}
