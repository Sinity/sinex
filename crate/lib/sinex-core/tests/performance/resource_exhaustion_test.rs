//! Resource exhaustion smoke tests for JetStream.
//!
//! These benches stress a temporary JetStream stream to ensure the system
//! behaves sensibly when approaching storage and consumer limits.

use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, Consumer, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy},
    Context as JetStream,
};
use futures::StreamExt;
use serde_json::json;
use sinex_core::types::ulid::Ulid;
use sinex_test_utils::{prelude::*, EphemeralNats};
use std::time::{Duration, Instant};

async fn setup_stream(js: &JetStream, name: &str, subject: &str, max_msgs: i64) -> TestResult<()> {
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

async fn create_consumer(
    js: &JetStream,
    stream: &str,
    subject: &str,
    durable: &str,
    ack_wait: Duration,
) -> TestResult<Consumer> {
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
        .map_err(Into::into)
}

#[sinex_bench]
async fn jetstream_backpressure_limits() -> TestResult<()> {
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

    let info = js.get_stream(&stream).await?.info().await?;
    color_eyre::eyre::ensure!(
        info.state.messages <= 200,
        "stream should cap at 200 messages, observed {}",
        info.state.messages
    );

    js.delete_stream(&stream).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_consumer_recovery() -> TestResult<()> {
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
    let stream_handle = js.get_stream(&stream).await?;
    let consumer = stream_handle
        .get_or_create_consumer(
            &durable,
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

    let stream_handle = js.get_stream(&stream).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_high_concurrency_publish() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client.clone());

    let stream = format!("perf_concurrency_{}", Ulid::new());
    let subject = format!("perf.concurrency.{}", Ulid::new());
    setup_stream(&js, &stream, &subject, 10_000).await?;

    let workers = 5usize;
    let messages_per_worker = 500usize;
    let payload = serde_json::to_vec(&json!({ "kind": "concurrency" }))?;

    let mut join_set = tokio::task::JoinSet::new();
    let start = Instant::now();
    for worker_id in 0..workers {
        let js = js.clone();
        let subject = subject.clone();
        let payload = payload.clone();
        join_set.spawn(async move {
            for i in 0..messages_per_worker {
                let mut data = payload.clone();
                let mut json_val: serde_json::Value = serde_json::from_slice(&data)?;
                if let serde_json::Value::Object(ref mut obj) = json_val {
                    obj.insert(
                        "index".to_string(),
                        serde_json::json!({
                            "worker": worker_id,
                            "seq": i
                        }),
                    );
                }
                data = serde_json::to_vec(&json_val)?;
                js.publish(&subject, data.into()).await?.await?;
            }
            TestResult::<()>::Ok(())
        });
    }

    while let Some(result) = join_set.join_next().await {
        result??;
    }
    let publish_duration = start.elapsed();

    let total_messages = workers * messages_per_worker;
    let throughput = total_messages as f64 / publish_duration.as_secs_f64();
    color_eyre::eyre::ensure!(
        throughput > 800.0,
        "expected high-concurrency throughput > 800 msgs/sec, observed {:.1}",
        throughput
    );

    let durable = format!("perf_concurrency_consumer_{}", Ulid::new());
    let consumer =
        create_consumer(&js, &stream, &subject, &durable, Duration::from_secs(30)).await?;

    let mut processed = 0usize;
    loop {
        let mut batch = consumer
            .fetch()
            .max_messages(256)
            .expires(Duration::from_secs(1))
            .messages()
            .await?;

        let mut handled = false;
        while let Some(message) = batch.next().await {
            let message = message?;
            message.ack().await?;
            processed += 1;
            handled = true;
        }

        if !handled {
            break;
        }
    }

    color_eyre::eyre::ensure!(
        processed == total_messages,
        "expected to drain {} messages after concurrency test, observed {}",
        total_messages,
        processed
    );

    let stream_handle = js.get_stream(&stream).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream).await?;
    Ok(())
}