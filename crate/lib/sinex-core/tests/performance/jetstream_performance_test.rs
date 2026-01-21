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
use color_eyre::eyre::eyre;
use futures::StreamExt;
use serde_json::json;
use sinex_core::types::ulid::Ulid;
use sinex_test_utils::{prelude::*, timing_utils::Timeouts, EphemeralNats};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;
use tokio::{task::JoinSet, time::sleep};

/// Helper to publish a batch of messages and report the elapsed time.
async fn publish_batch(
    js: &JetStream,
    subject: &str,
    batch_id: usize,
    message_count: usize,
) -> TestResult<(usize, StdDuration)> {
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

async fn create_stream(js: &JetStream, stream_name: &str, subject: &str) -> TestResult<()> {
    let config = StreamConfig {
        name: stream_name.to_string(),
        subjects: vec![subject.to_string()],
        retention: RetentionPolicy::Limits,
        max_age: StdDuration::from_secs(Timeouts::LONG),
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
    ack_wait: StdDuration,
    max_ack_pending: i32,
) -> TestResult<Consumer> {
    let stream = js.get_stream(stream_name).await?;
    stream
        .get_or_create_consumer(
            durable_name,
            ConsumerConfig {
                durable_name: Some(durable_name.to_string()),
                name: Some(durable_name.to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                ack_wait,
                filter_subject: subject.to_string(),
                max_ack_pending,
                ..Default::default()
            },
        )
        .await
}

#[sinex_bench]
async fn jetstream_publish_throughput() -> TestResult<()> {
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
async fn jetstream_concurrent_consumer_distribution() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client);

    let stream_name = format!("perf_fair_{}", Ulid::new());
    let subject = format!("perf.fair.{}", Ulid::new());
    let config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: RetentionPolicy::WorkQueue,
        max_age: StdDuration::from_secs(Timeouts::EXTENDED),
        ..Default::default()
    };
    js.get_or_create_stream(config).await?;

    let total_messages = 240usize;
    for idx in 0..total_messages {
        let payload = serde_json::to_vec(&json!({
            "sequence": idx,
            "payload": format!("message-{idx}")
        }))?;
        js.publish(&subject, payload.into()).await?.await?;
    }

    let durable = format!("perf_fair_consumer_{}", Ulid::new());
    let consumer = Arc::new(
        create_pull_consumer(
            &js,
            &stream_name,
            &subject,
            &durable,
            StdDuration::from_secs(Timeouts::STANDARD),
            512,
        )
        .await?,
    );

    let processed_total = Arc::new(AtomicUsize::new(0));
    let distribution: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut join_set = JoinSet::new();
    let workers = 3usize;

    for worker_id in 0..workers {
        let consumer = consumer.clone();
        let processed_total = processed_total.clone();
        let distribution = distribution.clone();
        join_set.spawn(async move {
            let worker_name = format!("worker-{worker_id}");
            let mut processed_by_worker = 0usize;
            loop {
                if processed_total.load(Ordering::SeqCst) >= total_messages {
                    break;
                }

                let mut batch = consumer
                    .fetch()
                    .max_messages(16)
                    .expires(StdDuration::from_secs(1))
                    .messages()
                    .await
                    .map_err(|err| eyre!("fetch failed: {err}"))?;

                let mut handled_any = false;
                while let Some(message) = batch.next().await {
                    let message = message.map_err(|err| eyre!("message fetch failed: {err}"))?;
                    message
                        .ack()
                        .await
                        .map_err(|err| eyre!("ack failed: {err}"))?;
                    processed_total.fetch_add(1, Ordering::SeqCst);
                    processed_by_worker += 1;
                    handled_any = true;

                    if processed_total.load(Ordering::SeqCst) >= total_messages {
                        break;
                    }
                }

                if !handled_any {
                    break;
                }
            }

            distribution
                .lock()
                .await
                .insert(worker_name, processed_by_worker);

            Ok::<_, color_eyre::Report>(())
        });
    }

    while let Some(result) = join_set.join_next().await {
        result??;
    }

    let counts = distribution.lock().await;
    let total_processed: usize = counts.values().sum();
    color_eyre::eyre::ensure!(
        total_processed == total_messages,
        "expected {total_messages} messages processed, observed {total_processed}"
    );

    if let (Some(min), Some(max)) = (counts.values().min(), counts.values().max()) {
        let imbalance = max - min;
        color_eyre::eyre::ensure!(
            imbalance <= total_messages / workers / 2,
            "work distribution imbalance too high (min {min}, max {max}, diff {imbalance})"
        );
    }

    let stream_handle = js.get_stream(&stream_name).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_redelivery_on_expired_ack() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client);

    let stream_name = format!("perf_redelivery_{}", Ulid::new());
    let subject = format!("perf.redelivery.{}", Ulid::new());
    create_stream(&js, &stream_name, &subject).await?;

    js.publish(
        &subject,
        serde_json::to_vec(&json!({
            "id": Ulid::new().to_string(),
            "purpose": "redelivery-test"
        }))?
        .into(),
    )
    .await?
    .await?;

    let durable = format!("perf_redelivery_consumer_{}", Ulid::new());
    let consumer = create_pull_consumer(
        &js,
        &stream_name,
        &subject,
        &durable,
        StdDuration::from_millis(500),
        8,
    )
    .await?;

    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .expires(StdDuration::from_secs(1))
        .messages()
        .await?;

    let first = batch
        .next()
        .await
        .ok_or_else(|| eyre!("expected first delivery"))??;
    let sequence = first.info().stream_sequence;
    // Intentionally skip ack to trigger redelivery.

    sleep(StdDuration::from_millis(900)).await;

    let mut redelivery = consumer
        .fetch()
        .max_messages(1)
        .expires(StdDuration::from_secs(1))
        .messages()
        .await?;

    let mut redelivered = false;
    while let Some(message) = redelivery.next().await {
        let message = message?;
        if message.info().stream_sequence == sequence && message.info().redelivery_count >= 1 {
            redelivered = true;
            message.ack().await?;
            break;
        } else {
            message.ack().await?;
        }
    }

    color_eyre::eyre::ensure!(redelivered, "expected message redelivery after ack wait");

    let stream_handle = js.get_stream(&stream_name).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_sustained_publish_throughput() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client.clone());

    let stream_name = format!("perf_sustained_publish_{}", Ulid::new());
    let subject = format!("perf.sustained.publish.{}", Ulid::new());
    create_stream(&js, &stream_name, &subject).await?;

    let total_messages = 2_000usize;
    let payload = serde_json::to_vec(&json!({
        "kind": "sustained-test",
        "payload": "a".repeat(256)
    }))?;

    let start = Instant::now();
    for _ in 0..total_messages {
        js.publish(&subject, payload.clone().into()).await?.await?;
    }
    let elapsed = start.elapsed();

    let throughput = total_messages as f64 / elapsed.as_secs_f64();
    color_eyre::eyre::ensure!(
        throughput > 500.0,
        "expected publish throughput > 500 msgs/sec, observed {:.1}",
        throughput
    );

    let durable = format!("perf_sustained_consumer_{}", Ulid::new());
    let consumer = create_pull_consumer(
        &js,
        &stream_name,
        &subject,
        &durable,
        StdDuration::from_secs(Timeouts::LONG),
        1024,
    )
    .await?;

    let mut drained = 0usize;
    loop {
        let mut batch = consumer
            .fetch()
            .max_messages(128)
            .expires(StdDuration::from_secs(1))
            .messages()
            .await?;

        let mut handled = false;
        while let Some(message) = batch.next().await {
            let message = message?;
            message.ack().await?;
            drained += 1;
            handled = true;
        }

        if !handled {
            break;
        }
    }

    color_eyre::eyre::ensure!(
        drained == total_messages,
        "expected to drain {} messages, received {}",
        total_messages,
        drained
    );

    let stream_handle = js.get_stream(&stream_name).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_consumer_latency() -> TestResult<()> {
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
    let consumer = create_pull_consumer(
        &js,
        &stream_name,
        &subject,
        &durable,
        StdDuration::from_secs(Timeouts::STANDARD),
        512,
    )
    .await?;

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
    let avg_latency =
        latency_samples.iter().copied().sum::<StdDuration>() / (latency_samples.len() as u32);
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