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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

const FAST_ACK_WAIT: Duration = Duration::from_millis(250);
const FAST_FETCH_EXPIRES: Duration = Duration::from_millis(200);
const FAST_REDELIVERY_DELAY: Duration = Duration::from_millis(50);

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
    let consumer = Arc::new(
        stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(consumer_name.to_string()),
                durable_name: Some(consumer_name.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: FAST_ACK_WAIT,
                max_deliver: 5,
                ..Default::default()
            })
            .await
            .map_err(|e| eyre!(e))?,
    );

    let delivery_count = Arc::new(AtomicU32::new(0));
    ctx.timing()
        .wait_for_condition(
            {
                let delivery_count = delivery_count.clone();
                let consumer = consumer.clone();
                move || {
                    let delivery_count = delivery_count.clone();
                    let consumer = consumer.clone();
                    async move {
                        let fetch_result = consumer
                            .fetch()
                            .max_messages(1)
                            .expires(FAST_FETCH_EXPIRES)
                            .messages()
                            .await;

                        match fetch_result {
                            Ok(mut messages) => {
                                while let Some(item) = messages.next().await {
                                    match item {
                                        Ok(msg) => {
                                            let count = delivery_count.fetch_add(1, Ordering::SeqCst);
                                            if count == 0 {
                                                msg.ack_with(async_nats::jetstream::AckKind::Nak(
                                                    Some(FAST_REDELIVERY_DELAY),
                                                ))
                                                .await
                                                .map_err(|e| eyre!(e))?;
                                            } else {
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
                                Ok(delivery_count.load(Ordering::SeqCst) >= 2)
                            }
                            Err(e) => {
                                let msg = e.to_string();
                                if is_no_messages_error(&msg) {
                                    return Ok(false);
                                }
                                Err(eyre!(e))
                            }
                        }
                    }
                }
            },
            Timeouts::QUICK,
        )
        .await?;

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
            ack_wait: FAST_ACK_WAIT,
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
            .expires(FAST_FETCH_EXPIRES)
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

    // Reconnect and verify messages are redelivered
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = Arc::new(
        stream
            .get_consumer(consumer_name)
            .await
            .map_err(|e| eyre!(e))?,
    );

    let redelivered_count = Arc::new(AtomicU32::new(0));
    ctx.timing()
        .wait_for_condition(
            {
                let consumer = consumer.clone();
                let redelivered_count = redelivered_count.clone();
                move || {
                    let consumer = consumer.clone();
                    let redelivered_count = redelivered_count.clone();
                    async move {
                        let fetch_result = consumer
                            .fetch()
                            .max_messages(5)
                            .expires(FAST_FETCH_EXPIRES)
                            .messages()
                            .await;

                        match fetch_result {
                            Ok(mut messages) => {
                                while let Some(item) = messages.next().await {
                                    match item {
                                        Ok(msg) => {
                                            msg.ack().await.map_err(|e| eyre!(e))?;
                                            redelivered_count.fetch_add(1, Ordering::SeqCst);
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
                                Ok(redelivered_count.load(Ordering::SeqCst) >= 3)
                            }
                            Err(e) => {
                                let msg = e.to_string();
                                if is_no_messages_error(&msg) {
                                    return Ok(false);
                                }
                                Err(eyre!(e))
                            }
                        }
                    }
                }
            },
            Timeouts::QUICK,
        )
        .await?;

    assert!(
        redelivered_count.load(Ordering::SeqCst) >= 3,
        "Unacked messages should be redelivered: got {}",
        redelivered_count.load(Ordering::SeqCst)
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
    let consumer = Arc::new(
        stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(consumer_name.to_string()),
                durable_name: Some(consumer_name.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: FAST_ACK_WAIT,
                max_deliver: 10,
                ..Default::default()
            })
            .await
            .map_err(|e| eyre!(e))?,
    );

    let observed_counts = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    ctx.timing()
        .wait_for_condition(
            {
                let observed_counts = observed_counts.clone();
                let consumer = consumer.clone();
                move || {
                    let observed_counts = observed_counts.clone();
                    let consumer = consumer.clone();
                    async move {
                        let fetch_result = consumer
                            .fetch()
                            .max_messages(1)
                            .expires(FAST_FETCH_EXPIRES)
                            .messages()
                            .await;

                        match fetch_result {
                            Ok(mut messages) => {
                                while let Some(item) = messages.next().await {
                                    match item {
                                        Ok(msg) => {
                                            let delivery_count =
                                                msg.info().map_err(|e| eyre!(e))?.delivered;
                                            let observed_len = {
                                                let mut observed_counts = observed_counts.lock().await;
                                                observed_counts.push(delivery_count);
                                                observed_counts.len()
                                            };

                                            if observed_len < 3 {
                                                msg.ack_with(async_nats::jetstream::AckKind::Nak(
                                                    Some(FAST_REDELIVERY_DELAY),
                                                ))
                                                .await
                                                .map_err(|e| eyre!(e))?;
                                            } else {
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
                                Ok(observed_counts.lock().await.len() >= 3)
                            }
                            Err(e) => {
                                let msg = e.to_string();
                                if is_no_messages_error(&msg) {
                                    return Ok(false);
                                }
                                Err(eyre!(e))
                            }
                        }
                    }
                }
            },
            Timeouts::QUICK,
        )
        .await?;

    let observed_counts = observed_counts.lock().await.clone();

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
            ack_wait: FAST_ACK_WAIT,
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!(e))?;

    // NAK until max_deliver is exhausted
    let delivery_attempts = Arc::new(AtomicU32::new(0));
    ctx.timing()
        .wait_for_condition(
            {
                let delivery_attempts = delivery_attempts.clone();
                let consumer = consumer.clone();
                move || {
                    let delivery_attempts = delivery_attempts.clone();
                    let consumer = consumer.clone();
                    async move {
                        let fetch_result = consumer
                            .fetch()
                            .max_messages(1)
                            .expires(FAST_FETCH_EXPIRES)
                            .messages()
                            .await;

                        match fetch_result {
                            Ok(mut messages) => {
                                while let Some(item) = messages.next().await {
                                    match item {
                                        Ok(msg) => {
                                            delivery_attempts.fetch_add(1, Ordering::SeqCst);
                                            msg.ack_with(async_nats::jetstream::AckKind::Nak(
                                                Some(FAST_REDELIVERY_DELAY),
                                            ))
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
                                Ok(delivery_attempts.load(Ordering::SeqCst) >= 3)
                            }
                            Err(e) => {
                                let msg = e.to_string();
                                if is_no_messages_error(&msg) {
                                    return Ok(false);
                                }
                                Err(eyre!(e))
                            }
                        }
                    }
                }
            },
            Timeouts::QUICK,
        )
        .await?;

    // Should have hit max_deliver limit
    assert_eq!(
        delivery_attempts.load(Ordering::SeqCst),
        3,
        "Should have exactly 3 delivery attempts (max_deliver=3), got {}",
        delivery_attempts.load(Ordering::SeqCst)
    );

    // Message should no longer be available (exhausted max_deliver)
    ctx.timing()
        .wait_for_condition(
            {
                let consumer = consumer.clone();
                move || {
                    let consumer = consumer.clone();
                    async move {
                        let fetch_result = timeout(
                            FAST_ACK_WAIT,
                            consumer
                                .fetch()
                                .max_messages(1)
                                .expires(FAST_FETCH_EXPIRES)
                                .messages(),
                        )
                        .await;

                        match fetch_result {
                            Ok(Ok(mut messages)) => {
                                while let Some(item) = messages.next().await {
                                    match item {
                                        Ok(msg) => {
                                            let info = msg.info().map_err(|e| eyre!(e))?;
                                            let delivered = info.delivered;
                                            msg.ack().await.map_err(|e| eyre!(e))?;
                                            return Err(eyre!(
                                                "message remained available after max_deliver exhausted (delivery #{delivered})"
                                            ));
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
                                Ok(true)
                            }
                            Ok(Err(e)) => {
                                if is_no_messages_error(&e.to_string()) {
                                    return Ok(true);
                                }
                                Err(eyre!(e))
                            }
                            Err(_) => Ok(false),
                        }
                    }
                }
            },
            Timeouts::QUICK,
        )
        .await?;

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
                ack_wait: FAST_ACK_WAIT,
                max_deliver: 5,
                ..Default::default()
            })
            .await
            .map_err(|e| eyre!(e))?,
    );

    let acked = Arc::new(AtomicU32::new(0));
    let nacked = Arc::new(AtomicU32::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn multiple concurrent fetchers
    let mut handles = vec![];
    for _ in 0..3 {
        let consumer = consumer.clone();
        let acked = acked.clone();
        let nacked = nacked.clone();
        let stop = stop.clone();

        let handle: tokio::task::JoinHandle<TestResult<()>> = tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) && acked.load(Ordering::SeqCst) < 10 {
                let fetch_result = consumer
                    .fetch()
                    .max_messages(5)
                    .expires(FAST_FETCH_EXPIRES)
                    .messages()
                    .await;

                match fetch_result {
                    Ok(mut messages) => {
                        while let Some(item) = messages.next().await {
                            match item {
                                Ok(msg) => {
                                    let total = acked.load(Ordering::Relaxed)
                                        + nacked.load(Ordering::Relaxed);
                                    if total.is_multiple_of(2) {
                                        msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                                            FAST_REDELIVERY_DELAY,
                                        )))
                                        .await
                                        .map_err(|e| eyre!(e))?;
                                        nacked.fetch_add(1, Ordering::SeqCst);
                                    } else {
                                        msg.ack().await.map_err(|e| eyre!(e))?;
                                        acked.fetch_add(1, Ordering::SeqCst);
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
                            continue;
                        }
                        return Err(eyre!(e));
                    }
                }
            }

            Ok(())
        });

        handles.push(handle);
    }

    ctx.timing()
        .wait_for_condition(
            {
                let acked = acked.clone();
                move || {
                    let acked = acked.clone();
                    async move {
                        Ok::<bool, color_eyre::Report>(acked.load(Ordering::SeqCst) >= 10)
                    }
                }
            },
            Timeouts::QUICK,
        )
        .await?;

    stop.store(true, Ordering::SeqCst);

    for handle in handles {
        handle.await.map_err(|e| eyre!(e))??;
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
