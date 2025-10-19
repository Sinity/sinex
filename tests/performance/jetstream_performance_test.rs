//! JetStream performance smoke tests.
//!
//! These benches exercise the JetStream publish/consume path that replaced the
//! legacy Redis Streams infrastructure. The goal is to keep a lightweight set of
//! throughput/latency measurements that run against an ephemeral NATS server so
//! we can spot obvious regressions while the more complete benchmarking suite is
//! rebuilt.

use async_nats::jetstream::{
    consumer::{
        pull::{Config as ConsumerConfig, Consumer},
        AckPolicy, DeliverPolicy,
    },
    stream::{Config as StreamConfig, RetentionPolicy},
    Context as JetStream,
};
use color_eyre::eyre::Result;
use futures::StreamExt;
use serde_json::json;
use sinex_core::types::ulid::Ulid;
use sinex_test_utils::{prelude::*, EphemeralNats};
use std::time::{Duration as StdDuration, Instant};

/// Helper to publish a batch of messages and report the elapsed time.
async fn publish_batch(
    js: &JetStream,
    subject: &str,
    batch_id: usize,
    message_count: usize,
) -> Result<(usize, StdDuration)> {
    let start = Instant::now();
    let mut published = 0;

    for idx in 0..message_count {
        let payload = serde_json::to_vec(&json!({
            "id": Ulid::new().to_string(),
            "batch": batch_id,
            "payload_index": idx,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))?;

        let ack = js.publish(subject, payload.into()).await?;
        ack.await?;
        published += 1;
    }

    Ok((published, start.elapsed()))
}

async fn create_stream(js: &JetStream, stream_name: &str, subject: &str) -> Result<()> {
    let config = StreamConfig {
        name: stream_name.to_string(),
        subjects: vec![subject.to_string()],
        retention: RetentionPolicy::Limits,
        max_age: StdDuration::from_secs(60),
        ..Default::default()
    };

    js.get_or_create_stream(config).await?;
    Ok(())
}

async fn create_pull_consumer(
    js: &JetStream,
    stream_name: &str,
    subject: &str,
    durable_name: &str,
) -> Result<Consumer> {
    js
        .get_or_create_consumer(
            stream_name,
            ConsumerConfig {
                durable_name: Some(durable_name.to_string()),
                name: Some(durable_name.to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: subject.to_string(),
                max_ack_pending: 256,
                ..Default::default()
            },
        )
        .await
}

#[sinex_bench]
async fn jetstream_publish_throughput(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client);

    let stream_name = format!("perf_publish_{}", Ulid::new());
    let subject = format!("perf.publish.{}", Ulid::new());
    create_stream(&js, &stream_name, &subject).await?;

    let batch_sizes = [100usize, 250usize, 500usize];
    for (batch_idx, size) in batch_sizes.iter().enumerate() {
        let (published, elapsed) = publish_batch(&js, &subject, batch_idx, *size).await?;
        let throughput = published as f64 / elapsed.as_secs_f64();
        eprintln!(
            "Batch {} published {} messages in {:?} ({:.1} msgs/sec)",
            batch_idx + 1,
            published,
            elapsed,
            throughput
        );
        color_eyre::eyre::ensure!(
            throughput > 200.0,
            "expected publish throughput > 200 msgs/sec, observed {:.1}",
            throughput
        );
    }

    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_consumer_latency(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client);

    let stream_name = format!("perf_consume_{}", Ulid::new());
    let subject = format!("perf.consume.{}", Ulid::new());
    create_stream(&js, &stream_name, &subject).await?;

    // Seed messages
    let total_messages = 500usize;
    let (published, _) = publish_batch(&js, &subject, 0, total_messages).await?;
    color_eyre::eyre::ensure!(
        published == total_messages,
        "expected to seed {} messages, published {}",
        total_messages,
        published
    );

    let durable = format!("perf_consumer_{}", Ulid::new());
    let consumer = create_pull_consumer(&js, &stream_name, &subject, &durable).await?;

    let mut latency_samples = Vec::with_capacity(total_messages);
    let mut received = 0usize;

    while received < total_messages {
        let mut messages = consumer
            .fetch()
            .max_messages(50)
            .expires(StdDuration::from_secs(2))
            .messages()
            .await?;

        while let Some(message) = messages.next().await {
            let message = message?;
            let elapsed = message.info().redelivery_count;
            let start = Instant::now();
            message.ack().await?;
            latency_samples.push(start.elapsed());
            received += 1;
            if elapsed > 0 {
                eprintln!("redelivery detected for message {}", received);
            }
        }
    }

    latency_samples.sort();
    let avg_latency = latency_samples
        .iter()
        .copied()
        .sum::<StdDuration>()
        / (latency_samples.len() as u32);
    let p95_latency = latency_samples
        .get((latency_samples.len() as f64 * 0.95) as usize)
        .copied()
        .unwrap_or_default();

    eprintln!(
        "Consumed {} messages. avg latency {:?}, p95 {:?}",
        received, avg_latency, p95_latency
    );

    color_eyre::eyre::ensure!(
        avg_latency < StdDuration::from_millis(50),
        "expected avg latency < 50ms, observed {:?}",
        avg_latency
    );
    color_eyre::eyre::ensure!(
        p95_latency < StdDuration::from_millis(150),
        "expected p95 latency < 150ms, observed {:?}",
        p95_latency
    );

    js.delete_stream(&stream_name).await?;
    Ok(())
}
