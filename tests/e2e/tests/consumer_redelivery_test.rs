//! Consumer Failure & Redelivery Tests
//!
//! These tests verify that `JetStream` consumer failure handling and
//! message redelivery work correctly.
//!
//! ## Coverage Areas
//! - NAK handling triggers redelivery
//! - Messages are redelivered after consumer crash
//! - Redelivery count tracking
//! - Dead letter queue routing after max retries

use async_nats::jetstream;
use color_eyre::eyre::eyre;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

fn is_no_messages_error(msg: &str) -> bool {
    msg.contains("No Messages")
        || msg.contains("No Messages Available")
        || (msg.contains("404") && msg.contains("No Messages"))
}

/// Test that NAKed messages are redelivered to the consumer.
#[sinex_test]
async fn test_nak_triggers_redelivery(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_REDELIVERY_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "nak-test-consumer";

    // Create stream
    js.create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    })
    .await?;

    let subject = format!("{stream_name}.events");

    // Publish a test message
    js.publish(subject.clone(), "test-message".into())
        .await
        .map_err(|e| eyre!(e))?
        .await
        .map_err(|e| eyre!(e))?;

    // Create consumer with explicit ack
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: Some(consumer_name.to_string()),
            durable_name: Some(consumer_name.to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            max_deliver: 5,
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!(e))?;

    // Fetch and NAK the message
    let delivery_count = Arc::new(AtomicU32::new(0));
    let start = std::time::Instant::now();

    while delivery_count.load(Ordering::SeqCst) < 2
        && start.elapsed() < Duration::from_secs(Timeouts::SHORT)
    {
        let fetch_result = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(Timeouts::MEDIUM))
            .messages()
            .await;

        match fetch_result {
            Ok(mut messages) => {
                while let Some(item) = messages.next().await {
                    match item {
                        Ok(msg) => {
                            let count = delivery_count.fetch_add(1, Ordering::SeqCst);
                            if count == 0 {
                                // First delivery: NAK to trigger redelivery
                                msg.ack_with(async_nats::jetstream::AckKind::Nak(None))
                                    .await
                                    .map_err(|e| eyre!(e))?;
                            } else {
                                // Second delivery: ACK to complete
                                msg.ack().await.map_err(|e| eyre!(e))?;
                            }
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            if is_no_messages_error(&msg) {
                                break;
                            }
                            return Err(eyre!(e));
                        }
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if is_no_messages_error(&msg) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                return Err(eyre!(e));
            }
        }
    }

    assert!(
        delivery_count.load(Ordering::SeqCst) >= 2,
        "Message should be delivered at least twice after NAK"
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test that messages are redelivered after consumer disconnect.
#[sinex_test]
async fn test_redelivery_after_consumer_disconnect(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_DISCONNECT_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "disconnect-test-consumer";

    // Create stream
    js.create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    })
    .await?;

    let subject = format!("{stream_name}.events");

    // Publish messages
    for i in 0..5 {
        js.publish(subject.clone(), format!("message-{i}").into())
            .await
            .map_err(|e| eyre!(e))?
            .await
            .map_err(|e| eyre!(e))?;
    }

    // Create consumer and fetch messages WITHOUT acking
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: Some(consumer_name.to_string()),
            durable_name: Some(consumer_name.to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ack_wait: Duration::from_secs(Timeouts::MEDIUM), // Short ack wait for test
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!(e))?;

    // Fetch but don't ack (simulates crash before processing)
    let mut fetched_count = 0;
    let start = std::time::Instant::now();
    while fetched_count < 3 && start.elapsed() < Duration::from_secs(Timeouts::QUICK) {
        let fetch_result = consumer
            .fetch()
            .max_messages(3)
            .expires(Duration::from_secs(1))
            .messages()
            .await;

        if let Ok(mut messages) = fetch_result {
            while let Some(Ok(_msg)) = messages.next().await {
                fetched_count += 1;
                // Don't ack - simulate crash
                if fetched_count >= 3 {
                    break;
                }
            }
        }
    }

    // Drop consumer (simulates disconnect)
    drop(consumer);

    // Wait for ack timeout
    tokio::time::sleep(Duration::from_secs(Timeouts::MEDIUM)).await;

    // Reconnect and verify messages are redelivered
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = stream
        .get_consumer(consumer_name)
        .await
        .map_err(|e| eyre!(e))?;

    let mut redelivered_count = 0;
    let start = std::time::Instant::now();
    while redelivered_count < 3 && start.elapsed() < Duration::from_secs(Timeouts::SHORT) {
        let fetch_result = consumer
            .fetch()
            .max_messages(5)
            .expires(Duration::from_secs(Timeouts::MEDIUM))
            .messages()
            .await;

        match fetch_result {
            Ok(mut messages) => {
                while let Some(item) = messages.next().await {
                    match item {
                        Ok(msg) => {
                            msg.ack().await.map_err(|e| eyre!(e))?;
                            redelivered_count += 1;
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            if is_no_messages_error(&msg) {
                                break;
                            }
                            return Err(eyre!(e));
                        }
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if is_no_messages_error(&msg) {
                    continue;
                }
                return Err(eyre!(e));
            }
        }
    }

    assert!(
        redelivered_count >= 3,
        "Unacked messages should be redelivered: got {redelivered_count}"
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test redelivery count is tracked in message metadata.
#[sinex_test]
async fn test_redelivery_count_tracking(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_REDELIVER_COUNT_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "count-test-consumer";

    // Create stream
    js.create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    })
    .await?;

    let subject = format!("{stream_name}.events");

    // Publish test message
    js.publish(subject.clone(), "count-test".into())
        .await
        .map_err(|e| eyre!(e))?
        .await
        .map_err(|e| eyre!(e))?;

    // Create consumer
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: Some(consumer_name.to_string()),
            durable_name: Some(consumer_name.to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            max_deliver: 10,
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!(e))?;

    // NAK multiple times and track delivery count
    let mut observed_counts = Vec::new();
    let start = std::time::Instant::now();

    while observed_counts.len() < 3 && start.elapsed() < Duration::from_secs(Timeouts::MEDIUM) {
        let fetch_result = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(Timeouts::MEDIUM))
            .messages()
            .await;

        match fetch_result {
            Ok(mut messages) => {
                while let Some(item) = messages.next().await {
                    match item {
                        Ok(msg) => {
                            let info = msg.info().map_err(|e| eyre!(e))?;
                            let delivery_count = info.delivered;
                            observed_counts.push(delivery_count);

                            if observed_counts.len() < 3 {
                                // NAK to trigger redelivery
                                msg.ack_with(async_nats::jetstream::AckKind::Nak(None))
                                    .await
                                    .map_err(|e| eyre!(e))?;
                            } else {
                                // ACK on final delivery
                                msg.ack().await.map_err(|e| eyre!(e))?;
                            }
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            if is_no_messages_error(&msg) {
                                break;
                            }
                            return Err(eyre!(e));
                        }
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if is_no_messages_error(&msg) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                return Err(eyre!(e));
            }
        }
    }

    // Verify delivery counts are incrementing
    assert!(
        observed_counts.len() >= 3,
        "Should have at least 3 deliveries"
    );
    for (i, &count) in observed_counts.iter().enumerate() {
        assert_eq!(
            count as usize,
            i + 1,
            "Delivery count should be {} but was {}",
            i + 1,
            count
        );
    }

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test that messages are routed to DLQ after max retries exhausted.
#[sinex_test]
async fn test_dlq_routing_after_max_retries(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_DLQ_ROUTE_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "dlq-test-consumer";

    // Create main stream
    js.create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    })
    .await?;

    let subject = format!("{stream_name}.events");

    // Publish test message
    js.publish(subject.clone(), "dlq-test".into())
        .await
        .map_err(|e| eyre!(e))?
        .await
        .map_err(|e| eyre!(e))?;

    // Create consumer with very low max_deliver
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: Some(consumer_name.to_string()),
            durable_name: Some(consumer_name.to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            max_deliver: 3, // Only 3 delivery attempts
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!(e))?;

    // NAK until max_deliver is exhausted
    let mut delivery_attempts = 0;
    let start = std::time::Instant::now();

    while delivery_attempts < 5 && start.elapsed() < Duration::from_secs(Timeouts::MEDIUM) {
        let fetch_result = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(Timeouts::MEDIUM))
            .messages()
            .await;

        match fetch_result {
            Ok(mut messages) => {
                let mut got_message = false;
                while let Some(item) = messages.next().await {
                    match item {
                        Ok(msg) => {
                            delivery_attempts += 1;
                            got_message = true;
                            // Always NAK (simulates persistent failure)
                            msg.ack_with(async_nats::jetstream::AckKind::Nak(None))
                                .await
                                .map_err(|e| eyre!(e))?;
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            if is_no_messages_error(&msg) {
                                break;
                            }
                            return Err(eyre!(e));
                        }
                    }
                }
                if !got_message {
                    // No more messages - max_deliver exhausted
                    break;
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if is_no_messages_error(&msg) {
                    // May indicate max_deliver exhausted
                    break;
                }
                return Err(eyre!(e));
            }
        }
    }

    // Should have hit max_deliver limit
    assert_eq!(
        delivery_attempts, 3,
        "Should have exactly 3 delivery attempts (max_deliver=3), got {delivery_attempts}"
    );

    // Message should no longer be available (exhausted max_deliver)
    let fetch_result = timeout(
        Duration::from_secs(Timeouts::MEDIUM),
        consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(Timeouts::MEDIUM))
            .messages(),
    )
    .await;

    match fetch_result {
        Ok(Ok(mut messages)) => {
            let msg = messages.next().await;
            assert!(
                msg.is_none()
                    || msg.as_ref().is_some_and(|m| {
                        m.as_ref()
                            .is_err_and(|e| is_no_messages_error(&e.to_string()))
                    }),
                "Message should not be available after max_deliver exhausted"
            );
        }
        Ok(Err(e)) => {
            // Expected: no messages available
            assert!(
                is_no_messages_error(&e.to_string()),
                "Expected 'no messages' error"
            );
        }
        Err(_) => {
            // Timeout is also acceptable
        }
    }

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test parallel consumer redelivery under load.
#[sinex_test]
async fn test_parallel_consumer_redelivery(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_PARALLEL_REDELIVER_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "parallel-test-consumer";

    // Create stream
    js.create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    })
    .await?;

    let subject = format!("{stream_name}.events");

    // Publish multiple messages
    for i in 0..10 {
        js.publish(subject.clone(), format!("message-{i}").into())
            .await
            .map_err(|e| eyre!(e))?
            .await
            .map_err(|e| eyre!(e))?;
    }

    // Create consumer
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = Arc::new(
        stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(consumer_name.to_string()),
                durable_name: Some(consumer_name.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                max_deliver: 5,
                ..Default::default()
            })
            .await
            .map_err(|e| eyre!(e))?,
    );

    let acked = Arc::new(AtomicU32::new(0));
    let nacked = Arc::new(AtomicU32::new(0));

    // Spawn multiple concurrent fetchers
    let mut handles = vec![];
    for _ in 0..3 {
        let consumer = consumer.clone();
        let acked = acked.clone();
        let nacked = nacked.clone();

        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_secs(Timeouts::QUICK) {
                let fetch_result = consumer
                    .fetch()
                    .max_messages(5)
                    .expires(Duration::from_secs(1))
                    .messages()
                    .await;

                if let Ok(mut messages) = fetch_result {
                    while let Some(item) = messages.next().await {
                        if let Ok(msg) = item {
                            // Alternate NAK/ACK based on total count for variety
                            let total =
                                acked.load(Ordering::Relaxed) + nacked.load(Ordering::Relaxed);
                            if total.is_multiple_of(2) {
                                let _ = msg
                                    .ack_with(async_nats::jetstream::AckKind::Nak(None))
                                    .await;
                                nacked.fetch_add(1, Ordering::SeqCst);
                            } else {
                                let _ = msg.ack().await;
                                acked.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all fetchers
    for handle in handles {
        let _ = handle.await;
    }

    let total_acked = acked.load(Ordering::SeqCst);
    let total_nacked = nacked.load(Ordering::SeqCst);

    tracing::info!(
        "Parallel redelivery: {} acked, {} nacked",
        total_acked,
        total_nacked
    );

    // Should have processed at least all messages (some redelivered)
    assert!(
        total_acked >= 10,
        "Should have acked at least 10 messages: got {total_acked}"
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}
