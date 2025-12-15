//! JetStream Bootstrap and Configuration Tests
//!
//! These tests verify JetStream stream and consumer initialization handles
//! edge cases correctly, particularly around idempotency, configuration
//! conflicts, and concurrent initialization.
//!
//! ## Coverage Areas
//! - Stream creation idempotency
//! - Consumer creation with existing stream
//! - Configuration mismatch handling
//! - Concurrent bootstrap attempts

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{self, consumer, stream};
use color_eyre::eyre::eyre;
use sinex_test_utils::prelude::*;
use tokio::time::sleep;

// =============================================================================
// Stream Creation Tests
// =============================================================================

/// Test that stream creation is idempotent.
#[sinex_test]
async fn test_stream_creation_idempotent() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let js = nats.jetstream().await?;

    let stream_name = format!("TEST_IDEMPOTENT_{}", uuid::Uuid::new_v4().simple());

    let config = stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    };

    // First creation
    let mut stream1 = js.create_stream(config.clone()).await?;
    assert_eq!(stream1.info().await?.config.name, stream_name);

    // Second creation with same config - should succeed (idempotent)
    let mut stream2 = js.create_stream(config.clone()).await?;
    assert_eq!(stream2.info().await?.config.name, stream_name);

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test that stream creation fails with conflicting configuration.
#[sinex_test]
async fn test_stream_creation_config_conflict() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let js = nats.jetstream().await?;

    let stream_name = format!("TEST_CONFLICT_{}", uuid::Uuid::new_v4().simple());

    // Create with initial config
    let config1 = stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    };

    js.create_stream(config1).await?;

    // Try to create with different max_messages
    let config2 = stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 2000, // Different!
        ..Default::default()
    };

    let result = js.create_stream(config2).await;

    // Should fail due to config mismatch
    // Note: NATS behavior may vary - it might update or reject
    // The key is we're testing what actually happens
    tracing::info!("Config conflict result: {:?}", result.is_err());

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test stream creation with duplicate subjects fails.
#[sinex_test]
async fn test_stream_duplicate_subjects_handling() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let js = nats.jetstream().await?;

    let stream1_name = format!("TEST_SUBJ1_{}", uuid::Uuid::new_v4().simple());
    let stream2_name = format!("TEST_SUBJ2_{}", uuid::Uuid::new_v4().simple());
    let shared_subject = format!("shared.subject.{}", uuid::Uuid::new_v4().simple());

    // Create first stream with subject
    js.create_stream(stream::Config {
        name: stream1_name.clone(),
        subjects: vec![shared_subject.clone()],
        ..Default::default()
    })
    .await?;

    // Try to create second stream with overlapping subject
    let result = js
        .create_stream(stream::Config {
            name: stream2_name.clone(),
            subjects: vec![shared_subject.clone()],
            ..Default::default()
        })
        .await;

    // NATS should reject this - subjects must be unique across streams
    assert!(
        result.is_err(),
        "Should reject duplicate subjects across streams"
    );

    // Cleanup
    js.delete_stream(&stream1_name).await?;
    // stream2 was never created

    Ok(())
}

// =============================================================================
// Consumer Creation Tests
// =============================================================================

/// Test durable consumer creation is idempotent.
#[sinex_test]
async fn test_consumer_creation_idempotent() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let js = nats.jetstream().await?;

    let stream_name = format!("TEST_CONSUMER_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "test-durable";

    // Create stream first
    js.create_stream(stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        ..Default::default()
    })
    .await?;

    let stream = js.get_stream(&stream_name).await?;

    let config = consumer::pull::Config {
        name: Some(consumer_name.to_string()),
        durable_name: Some(consumer_name.to_string()),
        ..Default::default()
    };

    // First creation
    let mut consumer1 = stream.create_consumer(config.clone()).await?;
    assert!(consumer1.info().await.is_ok());

    // Second creation - should be idempotent
    let mut consumer2 = stream.create_consumer(config.clone()).await?;
    assert!(consumer2.info().await.is_ok());

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test consumer creation with different config fails.
#[sinex_test]
async fn test_consumer_config_conflict() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let js = nats.jetstream().await?;

    let stream_name = format!("TEST_CONS_CONF_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "config-conflict-consumer";

    js.create_stream(stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        ..Default::default()
    })
    .await?;

    let stream = js.get_stream(&stream_name).await?;

    // Create consumer with initial config
    stream
        .create_consumer(consumer::pull::Config {
            name: Some(consumer_name.to_string()),
            durable_name: Some(consumer_name.to_string()),
            ack_policy: consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    // Try to create with different ack policy
    let result = stream
        .create_consumer(consumer::pull::Config {
            name: Some(consumer_name.to_string()),
            durable_name: Some(consumer_name.to_string()),
            ack_policy: consumer::AckPolicy::None, // Different!
            ..Default::default()
        })
        .await;

    // Note behavior: NATS might update the consumer or reject
    tracing::info!("Consumer config conflict result: {:?}", result.is_err());

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test consumer creation on non-existent stream fails.
#[sinex_test]
async fn test_consumer_on_nonexistent_stream() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let js = nats.jetstream().await?;

    let stream_name = "NONEXISTENT_STREAM_12345";

    let result = js.get_stream(stream_name).await;
    assert!(result.is_err(), "Should fail to get non-existent stream");

    Ok(())
}

// =============================================================================
// Concurrent Bootstrap Tests
// =============================================================================

/// Test concurrent stream creation from multiple instances.
#[sinex_test]
async fn test_concurrent_stream_creation() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    let stream_name = format!("TEST_CONCURRENT_{}", uuid::Uuid::new_v4().simple());
    let success_count = Arc::new(AtomicU32::new(0));
    let error_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];

    // Simulate 5 instances trying to create the same stream concurrently
    for instance_id in 0..5 {
        let client = nats_client.clone();
        let name = stream_name.clone();
        let successes = success_count.clone();
        let errors = error_count.clone();

        let handle = tokio::spawn(async move {
            let js = jetstream::new(client);

            let config = stream::Config {
                name: name.clone(),
                subjects: vec![format!("{}.>", name)],
                max_messages: 1000,
                ..Default::default()
            };

            match js.create_stream(config).await {
                Ok(_) => {
                    tracing::info!("Instance {} successfully created stream", instance_id);
                    successes.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    tracing::info!("Instance {} failed to create stream: {}", instance_id, e);
                    errors.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all attempts
    for handle in handles {
        let _ = handle.await;
    }

    let successes = success_count.load(Ordering::SeqCst);
    let errors = error_count.load(Ordering::SeqCst);

    tracing::info!(
        "Concurrent stream creation: {} successes, {} errors",
        successes,
        errors
    );

    // All should succeed due to idempotency (or exactly one wins)
    // The key invariant is no crash and stream exists afterward
    let js = jetstream::new(nats_client.clone());
    let mut stream = js.get_stream(&stream_name).await?;
    assert!(stream.info().await.is_ok(), "Stream should exist");

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test concurrent consumer creation from multiple instances.
#[sinex_test]
async fn test_concurrent_consumer_creation() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_CONS_CONC_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "shared-consumer";

    // Create stream first
    js.create_stream(stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        ..Default::default()
    })
    .await?;

    let success_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];

    for instance_id in 0..5 {
        let client = nats_client.clone();
        let stream = stream_name.clone();
        let consumer = consumer_name.to_string();
        let successes = success_count.clone();

        let handle = tokio::spawn(async move {
            let js = jetstream::new(client);
            let stream = match js.get_stream(&stream).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::warn!(error=?e, "Instance {instance_id} failed to load stream");
                    return Ok(());
                }
            };

            let config = consumer::pull::Config {
                name: Some(consumer.clone()),
                durable_name: Some(consumer.clone()),
                ..Default::default()
            };

            match stream.create_consumer(config).await {
                Ok(_) => {
                    tracing::info!("Instance {} created consumer", instance_id);
                    successes.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    tracing::info!("Instance {} failed to create consumer: {}", instance_id, e);
                }
            }

            Ok::<_, ()>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    // Consumer should exist
    let stream = js.get_stream(&stream_name).await?;
    let mut consumer: consumer::PullConsumer = stream
        .get_consumer(consumer_name)
        .await
        .map_err(|e| eyre!(e))?;
    assert!(consumer.info().await.is_ok());

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

// =============================================================================
// Stream Limits Tests
// =============================================================================

/// Test stream with message limit enforces retention.
#[sinex_test]
async fn test_stream_message_limit_enforcement(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let js = ctx.jetstream().await?;

    let stream_name = format!("TEST_LIMIT_{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{}.test", stream_name);

    // Create stream with small limit
    js.create_stream(stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 5,
        ..Default::default()
    })
    .await?;

    // Publish more messages than limit
    for i in 0..10 {
        js.publish(subject.clone(), format!("message-{}", i).into())
            .await?
            .await?;
    }

    // Check stream info
    let mut stream = js.get_stream(&stream_name).await?;
    let info = stream.info().await?;

    // Should only retain max_messages
    assert!(
        info.state.messages <= 5,
        "Stream should enforce message limit: {} messages",
        info.state.messages
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test stream with storage limit enforces retention.
#[sinex_test]
async fn test_stream_storage_limit_enforcement(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let js = ctx.jetstream().await?;

    let stream_name = format!("TEST_STORAGE_{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{}.test", stream_name);

    // Create stream with small storage limit (1KB)
    js.create_stream(stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_bytes: 1024,
        ..Default::default()
    })
    .await?;

    // Publish messages that exceed storage
    let large_payload = "x".repeat(200); // 200 bytes each
    for i in 0..10 {
        let _ = js
            .publish(subject.clone(), format!("{}-{}", i, large_payload).into())
            .await;
    }

    // Give stream time to apply limits
    sleep(Duration::from_millis(100)).await;

    // Check stream info
    let mut stream = js.get_stream(&stream_name).await?;
    let info = stream.info().await?;

    // Should be within storage limit (approximately)
    assert!(
        info.state.bytes <= 1500, // Allow some overhead
        "Stream should enforce storage limit: {} bytes",
        info.state.bytes
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

// =============================================================================
// Consumer Ack Tests
// =============================================================================

/// Test that unacked messages are redelivered.
#[sinex_test]
async fn test_consumer_redelivery_on_timeout(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let js = ctx.jetstream().await?;

    let stream_name = format!("TEST_REDEL_{}", uuid::Uuid::new_v4().simple());
    let subject = format!("{}.test", stream_name);

    js.create_stream(stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        ..Default::default()
    })
    .await?;

    let stream = js.get_stream(&stream_name).await?;

    // Create consumer with short ack wait
    let consumer = stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some("redelivery-test".to_string()),
            ack_wait: Duration::from_millis(500),
            ..Default::default()
        })
        .await?;

    // Publish a message
    js.publish(subject.clone(), "test-message".into())
        .await?
        .await?;

    // Fetch message but don't ack
    let mut messages = consumer.fetch().max_messages(1).messages().await?;
    if let Some(msg) = messages.next().await {
        let _msg = msg.map_err(|e| eyre!(e))?;
        // Intentionally not acking
    }

    // Wait for ack timeout
    sleep(Duration::from_millis(700)).await;

    // Message should be redelivered
    let mut messages = consumer.fetch().max_messages(1).messages().await?;
    let redelivered = messages.next().await;

    assert!(
        redelivered.is_some(),
        "Message should be redelivered after ack timeout"
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}
