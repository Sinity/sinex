//! JetStream bottleneck identification suites.
//!
//! These benches exercise JetStream under stress to ensure we can detect and
//! surface bottlenecks such as ack backlog and redelivery pressure.

use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy},
    Context as JetStream,
};
use color_eyre::eyre::{eyre, Result};
use futures::StreamExt;
use serde_json::json;
use sinex_primitives::ulid::Ulid;
use xtask::sandbox::{prelude::*, timing_utils::Timeouts, EphemeralNats};
use std::time::{Duration as StdDuration, Instant};

async fn create_stream(js: &JetStream, name: &str, subject: &str) -> Result<()> {
    let config = StreamConfig {
        name: name.to_string(),
        subjects: vec![subject.to_string()],
        retention: RetentionPolicy::WorkQueue,
        max_age: StdDuration::from_secs(Timeouts::CI),
        ..Default::default()
    };
    js.get_or_create_stream(config).await?;
    Ok(())
}

async fn create_consumer(
    js: &JetStream,
    stream: &str,
    subject: &str,
    durable: &str,
    ack_wait: StdDuration,
) -> Result<async_nats::jetstream::consumer::Consumer> {
    let stream_handle = js.get_stream(stream).await?;
    stream_handle
        .get_or_create_consumer(
            durable,
            ConsumerConfig {
                durable_name: Some(durable.to_string()),
                name: Some(durable.to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: subject.to_string(),
                ack_wait,
                max_ack_pending: 64,
                ..Default::default()
            },
        )
        .await
}

#[sinex_bench]
async fn jetstream_ack_backlog_detection() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client.clone());

    let stream = format!("perf_bottleneck_{}", Ulid::new());
    let subject = format!("perf.bottleneck.{}", Ulid::new());
    create_stream(&js, &stream, &subject).await?;

    for idx in 0..120 {
        let payload = serde_json::to_vec(&json!({ "sequence": idx }))?;
        js.publish(&subject, payload.into()).await?.await?;
    }

    let durable = format!("perf_bottleneck_consumer_{}", Ulid::new());
    let consumer = create_consumer(
        &js,
        &stream,
        &subject,
        &durable,
        StdDuration::from_millis(600),
    )
    .await?;

    let mut backlog_messages = Vec::new();
    // Consume but intentionally leave half unacked to build backlog.
    let mut batch = consumer
        .fetch()
        .max_messages(60)
        .expires(StdDuration::from_secs(1))
        .messages()
        .await?;

    let mut processed = 0usize;
    while let Some(message) = batch.next().await {
        let message = message?;
        if processed % 2 == 0 {
            message.ack().await?;
        } else {
            backlog_messages.push(message);
        }
        processed += 1;
    }

    // Wait for ack wait timeout to elapse so JetStream counts pending work.
    tokio::time::sleep(StdDuration::from_millis(1000)).await;

    let info = consumer.info().await?;
    color_eyre::eyre::ensure!(
        info.num_ack_pending > 0,
        "expected ack backlog, observed {:?}",
        info
    );
    color_eyre::eyre::ensure!(
        info.num_redelivered >= backlog_messages.len() as u64,
        "expected redeliveries to be tracked"
    );

    // Drain outstanding backlog to restore health.
    for message in backlog_messages {
        message.ack().await?;
    }

    let after = consumer.info().await?;
    color_eyre::eyre::ensure!(
        after.num_ack_pending == 0,
        "ack backlog should have cleared, observed {:?}",
        after
    );

    let stream_handle = js.get_stream(&stream).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_detect_publish_pressure() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client.clone());

    let stream = format!("perf_bottleneck_publish_{}", Ulid::new());
    let subject = format!("perf.bottleneck.publish.{}", Ulid::new());
    create_stream(&js, &stream, &subject).await?;

    // Publish messages in rapid bursts and monitor stream lag.
    let start = Instant::now();
    for idx in 0..500 {
        let payload = serde_json::to_vec(&json!({ "sequence": idx }))?;
        js.publish(&subject, payload.into()).await?.await?;
    }
    let publish_duration = start.elapsed();

    let info = js.get_stream(&stream).await?.info().await?;
    color_eyre::eyre::ensure!(
        publish_duration.as_secs_f64() < 2.0,
        "publish took too long: {:?}",
        publish_duration
    );
    color_eyre::eyre::ensure!(
        info.state.messages == 500,
        "expected 500 messages in stream, observed {}",
        info.state.messages
    );

    // Consumer drains to confirm backlog clears quickly.
    let durable = format!("perf_bottleneck_publish_consumer_{}", Ulid::new());
    let consumer =
        create_consumer(&js, &stream, &subject, &durable, StdDuration::from_secs(2)).await?;
    let mut consumed = 0usize;
    while consumed < 500 {
        let mut batch = consumer
            .fetch()
            .max_messages(64)
            .expires(StdDuration::from_secs(1))
            .messages()
            .await?;
        let mut handled = false;
        while let Some(message) = batch.next().await {
            let message = message?;
            message.ack().await?;
            consumed += 1;
            handled = true;
        }
        if !handled {
            break;
        }
    }

    color_eyre::eyre::ensure!(consumed == 500, "expected to consume 500 messages");

    let stream_handle = js.get_stream(&stream).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream).await?;
    Ok(())
}
