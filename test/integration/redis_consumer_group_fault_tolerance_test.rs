//! Redis Consumer Group Fault Tolerance Tests
//!
//! These tests replace the commented-out orphaned work recovery tests,
//! providing equivalent functionality for the Redis Streams architecture.
//! Tests Redis Consumer Group fault tolerance, PEL recovery, and message
//! redelivery patterns.

use crate::common::prelude::*;
use crate::common::satellite_test_utils::*;
use redis::{AsyncCommands, RedisResult};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

/// Test Redis Consumer Group recovery after consumer crash
#[sinex_test]
async fn test_consumer_crash_recovery(ctx: TestContext) -> TestResult {
    let redis_client = ctx.redis_client().await?;
    let stream_key = "test:consumer:crash:stream";
    let group_name = "crash-test-group";
    let consumer_name = "crash-test-consumer";

    // Clean up any existing stream/group
    let _: RedisResult<()> = redis_client.del(stream_key).await;

    // Create consumer group
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0", true)
        .await;

    // Add test messages to stream
    let mut message_ids = Vec::new();
    for i in 0..5 {
        let message_id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[("event_type", "test.event"), ("data", &format!("message-{}", i))],
            )
            .await?;
        message_ids.push(message_id);
    }

    // Simulate consumer reading messages but crashing before ACK
    let messages: Vec<redis::streams::StreamReadReply> = redis_client
        .xreadgroup(
            group_name,
            consumer_name,
            &[(stream_key, ">")],
            Some(redis::streams::StreamReadOptions::default().count(3)),
        )
        .await?;

    // Verify we read 3 messages
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].keys.len(), 1);
    assert_eq!(messages[0].keys[0].ids.len(), 3);

    // Get the message IDs we read
    let read_message_ids: Vec<String> = messages[0].keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Simulate consumer crash (consumer disappears without ACKing)
    // Messages should now be in the Pending Entry List (PEL)

    // Check pending messages
    let pending_info: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 10)
        .await?;

    assert_eq!(
        pending_info.len(),
        3,
        "Should have 3 pending messages after consumer crash"
    );

    // Verify the pending messages are the ones we read
    for pending in &pending_info {
        assert!(
            read_message_ids.contains(&pending.id),
            "Pending message {} should be in read messages",
            pending.id
        );
        assert_eq!(pending.consumer, consumer_name);
    }

    // Simulate recovery: claim pending messages with a new consumer
    let recovery_consumer = "recovery-consumer";
    let claimed_messages: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            recovery_consumer,
            1, // min_idle_time in ms (very low for immediate claiming)
            &read_message_ids,
        )
        .await?;

    assert_eq!(
        claimed_messages.len(),
        3,
        "Should claim all 3 pending messages"
    );

    // Process and acknowledge the claimed messages
    let mut processed_count = 0;
    for claimed in claimed_messages {
        // Simulate processing
        assert!(claimed.id.len() > 0);
        processed_count += 1;

        // Acknowledge the message
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&claimed.id])
            .await?;
        assert_eq!(ack_result, 1, "Should acknowledge 1 message");
    }

    assert_eq!(processed_count, 3, "Should process all claimed messages");

    // Verify no messages remain pending
    let final_pending: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 10)
        .await?;

    assert_eq!(
        final_pending.len(),
        0,
        "Should have no pending messages after recovery"
    );

    // Verify remaining messages can be processed normally
    let remaining_messages: Vec<redis::streams::StreamReadReply> = redis_client
        .xreadgroup(
            group_name,
            recovery_consumer,
            &[(stream_key, ">")],
            Some(redis::streams::StreamReadOptions::default().count(5)),
        )
        .await?;

    assert_eq!(remaining_messages.len(), 1);
    assert_eq!(remaining_messages[0].keys.len(), 1);
    assert_eq!(
        remaining_messages[0].keys[0].ids.len(),
        2,
        "Should have 2 remaining messages"
    );

    // Acknowledge remaining messages
    for msg in &remaining_messages[0].keys[0].ids {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&msg.id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    // Final verification: no pending messages and stream is fully processed
    let final_pending_check: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 10)
        .await?;

    assert_eq!(final_pending_check.len(), 0, "All messages should be processed");

    Ok(())
}

/// Test consumer group scaling with message distribution
#[sinex_test]
async fn test_consumer_group_scaling(ctx: TestContext) -> TestResult {
    let redis_client = ctx.redis_client().await?;
    let stream_key = "test:scaling:stream";
    let group_name = "scaling-test-group";

    // Clean up
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0", true)
        .await;

    // Add many messages
    let message_count = 20;
    for i in 0..message_count {
        let _: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[("event_type", "scale.test"), ("data", &format!("msg-{}", i))],
            )
            .await?;
    }

    // Track messages processed by each consumer
    let processed_messages = Arc::new(Mutex::new(HashMap::<String, Vec<String>>::new()));

    // Start multiple consumers in the same group
    let consumer_count = 4;
    let mut join_set = JoinSet::new();

    for consumer_id in 0..consumer_count {
        let consumer_name = format!("consumer-{}", consumer_id);
        let redis_client = ctx.redis_client().await?;
        let processed_clone = Arc::clone(&processed_messages);

        join_set.spawn(async move {
            let mut consumer_messages = Vec::new();
            let mut total_processed = 0;

            // Process messages for a limited time
            let start_time = Instant::now();
            while start_time.elapsed() < Duration::from_secs(3) && total_processed < message_count {
                let messages: Vec<redis::streams::StreamReadReply> = redis_client
                    .xreadgroup(
                        group_name,
                        &consumer_name,
                        &[(stream_key, ">")],
                        Some(
                            redis::streams::StreamReadOptions::default()
                                .count(3)
                                .block(100), // 100ms timeout
                        ),
                    )
                    .await
                    .unwrap_or_default();

                if messages.is_empty() {
                    sleep(Duration::from_millis(10)).await;
                    continue;
                }

                for stream_msgs in messages {
                    for key in stream_msgs.keys {
                        for msg in key.ids {
                            consumer_messages.push(msg.id.clone());
                            total_processed += 1;

                            // Acknowledge the message
                            let _: i64 = redis_client
                                .xack(stream_key, group_name, &[&msg.id])
                                .await
                                .unwrap_or(0);
                        }
                    }
                }
            }

            // Store processed messages
            let mut processed = processed_clone.lock().await;
            processed.insert(consumer_name.clone(), consumer_messages);

            (consumer_name, total_processed)
        });
    }

    // Wait for all consumers to complete
    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        results.push(result?);
    }

    // Verify message distribution
    let processed = processed_messages.lock().await;
    let mut total_processed = 0;
    let mut all_processed_messages = Vec::new();

    for (consumer_name, messages) in processed.iter() {
        println!("Consumer {} processed {} messages", consumer_name, messages.len());
        total_processed += messages.len();
        all_processed_messages.extend(messages.clone());
    }

    // Verify all messages were processed exactly once
    assert_eq!(
        total_processed, message_count,
        "All messages should be processed exactly once"
    );

    // Verify no duplicate processing
    all_processed_messages.sort();
    let mut unique_messages = all_processed_messages.clone();
    unique_messages.dedup();
    assert_eq!(
        all_processed_messages.len(),
        unique_messages.len(),
        "No message should be processed more than once"
    );

    // Verify load distribution (each consumer should process some messages)
    let non_empty_consumers = processed.values().filter(|msgs| !msgs.is_empty()).count();
    assert!(
        non_empty_consumers >= 2,
        "At least 2 consumers should process messages for load distribution"
    );

    Ok(())
}

/// Test consumer group timeout and redelivery
#[sinex_test]
async fn test_consumer_timeout_redelivery(ctx: TestContext) -> TestResult {
    let redis_client = ctx.redis_client().await?;
    let stream_key = "test:timeout:stream";
    let group_name = "timeout-test-group";
    let slow_consumer = "slow-consumer";
    let fast_consumer = "fast-consumer";

    // Clean up
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0", true)
        .await;

    // Add test messages
    let mut message_ids = Vec::new();
    for i in 0..3 {
        let message_id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[("event_type", "timeout.test"), ("data", &format!("msg-{}", i))],
            )
            .await?;
        message_ids.push(message_id);
    }

    // Slow consumer reads messages but doesn't ACK (simulating slow processing)
    let slow_messages: Vec<redis::streams::StreamReadReply> = redis_client
        .xreadgroup(
            group_name,
            slow_consumer,
            &[(stream_key, ">")],
            Some(redis::streams::StreamReadOptions::default().count(3)),
        )
        .await?;

    assert_eq!(slow_messages.len(), 1);
    assert_eq!(slow_messages[0].keys[0].ids.len(), 3);

    let slow_message_ids: Vec<String> = slow_messages[0].keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Wait for messages to be considered idle
    sleep(Duration::from_millis(50)).await;

    // Fast consumer claims idle messages
    let claimed_messages: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            fast_consumer,
            10, // min_idle_time in ms
            &slow_message_ids,
        )
        .await?;

    assert_eq!(
        claimed_messages.len(),
        3,
        "Fast consumer should claim all idle messages"
    );

    // Fast consumer processes and acknowledges messages
    let mut processed_count = 0;
    for claimed in claimed_messages {
        processed_count += 1;
        
        // Acknowledge the message
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&claimed.id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    assert_eq!(processed_count, 3, "All messages should be processed");

    // Verify no pending messages remain
    let pending: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 10)
        .await?;

    assert_eq!(pending.len(), 0, "No messages should remain pending");

    // Verify slow consumer can't ACK claimed messages
    for msg_id in &slow_message_ids {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[msg_id])
            .await?;
        assert_eq!(ack_result, 0, "Slow consumer shouldn't be able to ACK claimed messages");
    }

    Ok(())
}

/// Test consumer group state consistency under concurrent operations
#[sinex_test]
async fn test_consumer_group_state_consistency(ctx: TestContext) -> TestResult {
    let redis_client = ctx.redis_client().await?;
    let stream_key = "test:consistency:stream";
    let group_name = "consistency-test-group";

    // Clean up
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0", true)
        .await;

    // Concurrent operations: producers and consumers
    let mut join_set = JoinSet::new();
    let processed_messages = Arc::new(Mutex::new(Vec::<String>::new()));

    // Producer task
    let producer_redis = ctx.redis_client().await?;
    join_set.spawn(async move {
        let mut produced = 0;
        for i in 0..10 {
            let _: String = producer_redis
                .xadd(
                    stream_key,
                    "*",
                    &[("event_type", "consistency.test"), ("data", &format!("msg-{}", i))],
                )
                .await
                .unwrap();
            produced += 1;
            sleep(Duration::from_millis(10)).await;
        }
        produced
    });

    // Consumer tasks
    for consumer_id in 0..2 {
        let consumer_name = format!("consumer-{}", consumer_id);
        let consumer_redis = ctx.redis_client().await?;
        let processed_clone = Arc::clone(&processed_messages);

        join_set.spawn(async move {
            let mut consumer_processed = 0;
            let start_time = Instant::now();

            while start_time.elapsed() < Duration::from_secs(2) {
                let messages: Vec<redis::streams::StreamReadReply> = consumer_redis
                    .xreadgroup(
                        group_name,
                        &consumer_name,
                        &[(stream_key, ">")],
                        Some(
                            redis::streams::StreamReadOptions::default()
                                .count(2)
                                .block(50),
                        ),
                    )
                    .await
                    .unwrap_or_default();

                if messages.is_empty() {
                    sleep(Duration::from_millis(5)).await;
                    continue;
                }

                for stream_msgs in messages {
                    for key in stream_msgs.keys {
                        for msg in key.ids {
                            // Add to processed list
                            {
                                let mut processed = processed_clone.lock().await;
                                processed.push(msg.id.clone());
                            }

                            // Acknowledge
                            let _: i64 = consumer_redis
                                .xack(stream_key, group_name, &[&msg.id])
                                .await
                                .unwrap_or(0);
                            
                            consumer_processed += 1;
                        }
                    }
                }
            }

            (consumer_name, consumer_processed)
        });
    }

    // Wait for all tasks to complete
    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        results.push(result?);
    }

    // Verify consistency
    let processed = processed_messages.lock().await;
    
    // Check that all messages were processed exactly once
    let mut sorted_processed = processed.clone();
    sorted_processed.sort();
    let mut unique_processed = sorted_processed.clone();
    unique_processed.dedup();

    assert_eq!(
        sorted_processed.len(),
        unique_processed.len(),
        "No message should be processed more than once"
    );

    // Verify no pending messages remain
    let pending: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 20)
        .await?;

    assert_eq!(pending.len(), 0, "No messages should remain pending");

    // Verify producer/consumer counts match
    let producer_count = results.iter().find(|r| r.0.starts_with("producer")).map(|r| r.1).unwrap_or(0);
    let consumer_total: usize = results.iter()
        .filter(|r| r.0.starts_with("consumer"))
        .map(|r| r.1)
        .sum();

    assert_eq!(
        producer_count, 10,
        "Producer should have produced 10 messages"
    );

    assert_eq!(
        consumer_total, processed.len(),
        "Consumer totals should match processed messages"
    );

    Ok(())
}

/// Test consumer group failure recovery with checkpointing
#[sinex_test]
async fn test_consumer_group_checkpoint_recovery(ctx: TestContext) -> TestResult {
    let redis_client = ctx.redis_client().await?;
    let stream_key = "test:checkpoint:stream";
    let group_name = "checkpoint-test-group";
    let consumer_name = "checkpoint-consumer";

    // Clean up
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0", true)
        .await;

    // Add test messages
    let message_count = 10;
    let mut message_ids = Vec::new();
    for i in 0..message_count {
        let message_id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[("event_type", "checkpoint.test"), ("data", &format!("msg-{}", i))],
            )
            .await?;
        message_ids.push(message_id);
    }

    // Process first batch and checkpoint
    let first_batch: Vec<redis::streams::StreamReadReply> = redis_client
        .xreadgroup(
            group_name,
            consumer_name,
            &[(stream_key, ">")],
            Some(redis::streams::StreamReadOptions::default().count(5)),
        )
        .await?;

    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0].keys[0].ids.len(), 5);

    let first_batch_ids: Vec<String> = first_batch[0].keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Acknowledge first batch (simulating successful processing)
    for msg_id in &first_batch_ids {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[msg_id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    // Simulate checkpoint save (in real implementation this would be in database)
    let checkpoint_id = &first_batch_ids[4]; // Last processed message

    // Process second batch but crash before ACK
    let second_batch: Vec<redis::streams::StreamReadReply> = redis_client
        .xreadgroup(
            group_name,
            consumer_name,
            &[(stream_key, ">")],
            Some(redis::streams::StreamReadOptions::default().count(5)),
        )
        .await?;

    assert_eq!(second_batch.len(), 1);
    assert_eq!(second_batch[0].keys[0].ids.len(), 5);

    let second_batch_ids: Vec<String> = second_batch[0].keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Simulate crash - don't ACK second batch
    // Messages should be in PEL

    // Verify second batch is pending
    let pending: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 10)
        .await?;

    assert_eq!(pending.len(), 5, "Second batch should be pending");

    // Simulate recovery: new consumer starts from checkpoint
    let recovery_consumer = "recovery-consumer";

    // In real implementation, we'd load checkpoint from database
    // For test, we know the checkpoint_id

    // Claim pending messages for recovery
    let claimed_messages: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            recovery_consumer,
            1, // min_idle_time
            &second_batch_ids,
        )
        .await?;

    assert_eq!(claimed_messages.len(), 5, "Should claim all pending messages");

    // Process and acknowledge claimed messages
    for claimed in claimed_messages {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&claimed.id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    // Verify recovery is complete
    let final_pending: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 10)
        .await?;

    assert_eq!(final_pending.len(), 0, "No messages should remain pending");

    // Verify all messages were processed
    let no_more_messages: Vec<redis::streams::StreamReadReply> = redis_client
        .xreadgroup(
            group_name,
            recovery_consumer,
            &[(stream_key, ">")],
            Some(redis::streams::StreamReadOptions::default().count(1)),
        )
        .await?;

    assert_eq!(no_more_messages.len(), 0, "All messages should be processed");

    Ok(())
}

/// Test handling of duplicate consumer names and group management
#[sinex_test]
async fn test_consumer_group_management(ctx: TestContext) -> TestResult {
    let redis_client = ctx.redis_client().await?;
    let stream_key = "test:management:stream";
    let group_name = "management-test-group";

    // Clean up
    let _: RedisResult<()> = redis_client.del(stream_key).await;

    // Create group
    let create_result: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0", true)
        .await;
    assert!(create_result.is_ok(), "Should create group successfully");

    // Try to create same group again - should fail
    let duplicate_result: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0", false)
        .await;
    assert!(duplicate_result.is_err(), "Should fail to create duplicate group");

    // Add message
    let message_id: String = redis_client
        .xadd(
            stream_key,
            "*",
            &[("event_type", "management.test"), ("data", "test-message")],
        )
        .await?;

    // Multiple consumers with same name should work (last one wins)
    let consumer_name = "test-consumer";
    
    // First consumer reads
    let messages1: Vec<redis::streams::StreamReadReply> = redis_client
        .xreadgroup(
            group_name,
            consumer_name,
            &[(stream_key, ">")],
            Some(redis::streams::StreamReadOptions::default().count(1)),
        )
        .await?;

    assert_eq!(messages1.len(), 1);
    assert_eq!(messages1[0].keys[0].ids.len(), 1);

    // Second consumer with same name can also read (but message is already claimed)
    let messages2: Vec<redis::streams::StreamReadReply> = redis_client
        .xreadgroup(
            group_name,
            consumer_name,
            &[(stream_key, ">")],
            Some(redis::streams::StreamReadOptions::default().count(1)),
        )
        .await?;

    assert_eq!(messages2.len(), 0, "Same consumer name should not get new messages");

    // Verify message is pending for the consumer
    let pending: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 10)
        .await?;

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].consumer, consumer_name);

    // Acknowledge message
    let ack_result: i64 = redis_client
        .xack(stream_key, group_name, &[&message_id])
        .await?;
    assert_eq!(ack_result, 1);

    // Verify no pending messages
    let final_pending: Vec<redis::streams::StreamPendingReply> = redis_client
        .xpending(stream_key, group_name, Some("-"), Some("+"), 10)
        .await?;

    assert_eq!(final_pending.len(), 0);

    Ok(())
}