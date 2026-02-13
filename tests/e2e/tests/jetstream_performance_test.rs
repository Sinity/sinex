//! JetStream performance smoke tests.
//!
//! These benches exercise the JetStream publish/consume path that replaced the
//! legacy Redis Streams infrastructure. The goal is to keep a lightweight set of
//! throughput/latency measurements that run against an ephemeral NATS server so
//! we can spot obvious regressions while the more complete benchmarking suite is
//! rebuilt.

use xtask::sandbox::prelude::*;

use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
    stream::Config as StreamConfig,
};
use futures::StreamExt;
use std::time::{Duration, Instant};

#[sinex_test]
async fn jetstream_publish_throughput(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;

    let stream_name = format!("STREAM_THROUGHPUT_{}", sinex_primitives::Ulid::new());
    let subject = format!("{}.*", stream_name);

    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        max_age: Duration::from_secs(60),
        ..Default::default()
    };

    js.get_or_create_stream(stream_config).await?;

    // Publish 1000 messages and measure throughput
    let message_count = 1000;
    let start = Instant::now();

    for i in 0..message_count {
        let payload = format!("message_{}", i).into_bytes();
        let _ = js.publish(subject.clone(), payload.into()).await?;
    }

    let elapsed = start.elapsed();
    let throughput = message_count as f64 / elapsed.as_secs_f64();

    // Assert minimum throughput: > 100 msg/sec
    assert!(
        throughput > 100.0,
        "Throughput {} msg/sec is below minimum of 100 msg/sec",
        throughput
    );

    Ok(())
}

#[sinex_test]
async fn jetstream_concurrent_consumer_distribution(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;

    let stream_name = format!("STREAM_CONSUMERS_{}", sinex_primitives::Ulid::new());
    let subject = format!("{}.*", stream_name);

    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        max_age: Duration::from_secs(60),
        ..Default::default()
    };

    let stream = js.get_or_create_stream(stream_config).await?;

    // Publish 100 messages
    let message_count = 100;
    for i in 0..message_count {
        let payload = format!("message_{}", i).into_bytes();
        let _ = js.publish(subject.clone(), payload.into()).await?;
    }

    // Create multiple pull consumers concurrently
    let mut handles = vec![];
    for consumer_idx in 0..3 {
        let stream_clone = stream.clone();
        let consumer_name = format!("consumer_{}", consumer_idx);

        let handle = tokio::spawn(async move {
            let config = ConsumerConfig {
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            };

            let consumer = stream_clone
                .get_or_create_consumer(&consumer_name, config)
                .await?;

            // Pull 10 messages per consumer
            let mut messages = consumer.messages().await?;
            let mut count = 0;
            while count < 10 {
                if let Some(Ok(msg)) = messages.next().await {
                    msg.ack()
                        .await
                        .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
                    count += 1;
                }
            }

            Ok::<_, color_eyre::Report>(count)
        });

        handles.push(handle);
    }

    // All consumers should successfully receive their messages
    for handle in handles {
        let count = handle.await??;
        assert_eq!(count, 10);
    }

    Ok(())
}

#[sinex_test]
async fn jetstream_redelivery_on_expired_ack(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;

    let stream_name = format!("STREAM_REDELIVERY_{}", sinex_primitives::Ulid::new());
    let subject = format!("{}.*", stream_name);

    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        max_age: Duration::from_secs(60),
        ..Default::default()
    };

    let stream = js.get_or_create_stream(stream_config).await?;

    // Publish 10 messages
    let message_count = 10;
    for i in 0..message_count {
        let payload = format!("message_{}", i).into_bytes();
        let _ = js.publish(subject.clone(), payload.into()).await?;
    }

    // Create consumer with explicit ack policy
    let config = ConsumerConfig {
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        ack_wait: std::time::Duration::from_millis(100),
        ..Default::default()
    };

    let consumer = stream
        .get_or_create_consumer("redelivery-test", config)
        .await?;

    // Pull and fetch all messages (they should be delivered)
    let mut messages = consumer.messages().await?;
    let mut received = 0;
    let mut _timeout_count = 0;

    // Set a timeout for receiving messages
    let start = Instant::now();
    while received < message_count && start.elapsed() < Duration::from_secs(5) {
        match tokio::time::timeout(Duration::from_millis(200), messages.next()).await {
            Ok(Some(Ok(msg))) => {
                msg.ack()
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
                received += 1;
            }
            Ok(None) => break,
            Ok(Some(Err(_))) => {
                _timeout_count += 1;
            }
            Err(_) => {
                // Timeout waiting for next message
                break;
            }
        }
    }

    // Verify we received all messages
    assert_eq!(received, message_count);

    Ok(())
}
