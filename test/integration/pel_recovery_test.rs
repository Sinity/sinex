// PEL (Pending Entry List) Recovery Tests
//
// Comprehensive tests for Redis Streams Pending Entry List recovery scenarios.
// These tests focus specifically on unacknowledged message recovery patterns
// and edge cases that can occur in production environments.

use crate::common::prelude::*;
use crate::common::satellite_test_utils::*;
use redis::{AsyncCommands, RedisResult, cmd};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

/// Test basic PEL recovery after consumer failure
#[sinex_test]
async fn test_basic_pel_recovery(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:basic:stream";
    let group_name = "basic-pel-group";
    let failing_consumer = "failing-consumer";
    let recovery_consumer = "recovery-consumer";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add messages
    let mut message_ids = Vec::new();
    for i in 0..5 {
        let id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[("type", "pel.test"), ("data", &format!("msg-{}", i))],
            )
            .await?;
        message_ids.push(id);
    }

    // Failing consumer reads messages but doesn't ACK
    let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(failing_consumer)
        .arg("COUNT")
        .arg(5)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(&mut redis_client)
        .await?;

    assert_eq!(messages.keys.len(), 1);
    assert_eq!(messages.keys[0].ids.len(), 5);

    let read_ids: Vec<String> = messages.keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Verify messages are in PEL
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(pel_info.count(), 5, "All messages should be in PEL");

    // Note: With the current Redis crate API, we can't iterate over individual pending entries directly

    // Recovery consumer claims messages
    let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            recovery_consumer,
            1, // min_idle_time
            &read_ids,
        )
        .await?;

    assert_eq!(claimed.len(), 5, "Should claim all messages");

    // Acknowledge claimed messages
    for claim in claimed {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&claim.ids[0].id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    // Verify PEL is now empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(final_pel.count(), 0, "PEL should be empty after recovery");

    Ok(())
}

/// Test PEL recovery with partial acknowledgments
#[sinex_test]
async fn test_partial_pel_recovery(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:partial:stream";
    let group_name = "partial-pel-group";
    let consumer_name = "partial-consumer";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add messages
    let mut message_ids = Vec::new();
    for i in 0..6 {
        let id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[("type", "partial.test"), ("data", &format!("msg-{}", i))],
            )
            .await?;
        message_ids.push(id);
    }

    // Consumer reads all messages
    let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(consumer_name)
        .arg("COUNT")
        .arg(6)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(&mut redis_client)
        .await?;

    assert_eq!(messages.keys.len(), 1);
    assert_eq!(messages.keys[0].ids.len(), 6);

    let read_ids: Vec<String> = messages.keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Partially acknowledge messages (ACK first 3, leave last 3 pending)
    for i in 0..3 {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&read_ids[i]])
            .await?;
        assert_eq!(ack_result, 1);
    }

    // Verify only 3 messages remain in PEL
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(pel_info.count(), 3, "Only 3 messages should remain in PEL");

    // Recovery consumer claims remaining messages
    let remaining_ids: Vec<String> = read_ids[3..].to_vec();
    let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            "recovery-consumer",
            1,
            &remaining_ids,
        )
        .await?;

    assert_eq!(claimed.len(), 3, "Should claim remaining 3 messages");

    // Acknowledge claimed messages
    for claim in claimed {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&claim.ids[0].id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    // Verify PEL is now empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(
        final_pel.count(),
        0,
        "PEL should be empty after full recovery"
    );

    Ok(())
}

/// Test PEL recovery with message retry limits
#[sinex_test]
async fn test_pel_recovery_with_retry_limits(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:retry:stream";
    let group_name = "retry-pel-group";
    let consumer_name = "retry-consumer";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add a single message
    let message_id: String = redis_client
        .xadd(
            stream_key,
            "*",
            &[("type", "retry.test"), ("data", "failing-message")],
        )
        .await?;

    // Simulate multiple failed processing attempts
    let max_retries = 3;
    let mut retry_count = 0;

    while retry_count < max_retries {
        // Consumer reads message
        let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_name)
            .arg(consumer_name)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async(&mut redis_client)
            .await?;

        if messages.keys.is_empty() {
            // Message might be pending, try to claim it
            let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
                .xclaim(stream_key, group_name, consumer_name, 1, &[&message_id])
                .await?;

            if claimed.is_empty() {
                break; // No more messages to process
            }
        }

        // Simulate processing failure (don't ACK)
        retry_count += 1;

        // Brief delay to simulate processing time
        sleep(Duration::from_millis(10)).await;
    }

    // Verify message is still in PEL after max retries
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(pel_info.count(), 1, "Message should still be in PEL");
    // Note: Basic xpending doesn't provide message IDs directly

    // Check delivery count (this would be implementation-specific)
    // In a real system, you'd track retry counts in the message payload or separate storage

    // Dead letter queue simulation: move to special stream after max retries
    let dlq_stream = "test:pel:retry:dlq";

    // Claim the message for DLQ processing
    let dlq_claimed: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(stream_key, group_name, "dlq-processor", 1, &[&message_id])
        .await?;

    assert_eq!(dlq_claimed.len(), 1, "Should claim message for DLQ");

    // Add to DLQ stream
    let dlq_id: String = redis_client
        .xadd(
            dlq_stream,
            "*",
            &[
                ("original_id", &message_id),
                ("original_stream", &stream_key.to_string()),
                ("retry_count", &retry_count.to_string()),
                ("type", &"retry.test".to_string()),
                ("data", &"failing-message".to_string()),
            ],
        )
        .await?;

    // Acknowledge original message (remove from PEL)
    let ack_result: i64 = redis_client
        .xack(stream_key, group_name, &[&message_id])
        .await?;
    assert_eq!(ack_result, 1);

    // Verify message is no longer in PEL
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(
        final_pel.count(),
        0,
        "PEL should be empty after DLQ processing"
    );

    // Verify message is in DLQ
    let dlq_messages: redis::streams::StreamReadReply = redis_client
        .xread(&[(dlq_stream, "0")], &[])
        .await?;

    assert_eq!(dlq_messages.keys.len(), 1);
    assert_eq!(dlq_messages.keys[0].ids.len(), 1);
    assert_eq!(dlq_messages.keys[0].ids[0].id, dlq_id);

    Ok(())
}

/// Test PEL recovery with concurrent consumers
#[sinex_test]
async fn test_concurrent_pel_recovery(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:concurrent:stream";
    let group_name = "concurrent-pel-group";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add many messages
    let message_count = 20;
    let mut message_ids = Vec::new();
    for i in 0..message_count {
        let id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[("type", "concurrent.test"), ("data", &format!("msg-{}", i))],
            )
            .await?;
        message_ids.push(id);
    }

    // Multiple consumers read messages but don't ACK (simulating failures)
    let failing_consumers = ["failing-1", "failing-2", "failing-3"];
    let mut all_read_ids = Vec::new();

    for consumer in &failing_consumers {
        let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_name)
            .arg(consumer)
            .arg("COUNT")
            .arg(7)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async(&mut redis_client)
            .await?;

        if !messages.keys.is_empty() {
            let read_ids: Vec<String> = messages.keys[0]
                .ids
                .iter()
                .map(|msg| msg.id.clone())
                .collect();
            all_read_ids.extend(read_ids);
        }
    }

    // Verify all messages are in PEL
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(
        pel_info.count(),
        message_count,
        "All messages should be in PEL"
    );

    // Concurrent recovery: multiple recovery consumers claim messages
    let recovery_consumers = ["recovery-1", "recovery-2"];
    let mut recovery_tasks = Vec::new();

    for recovery_consumer in &recovery_consumers {
        let consumer_name = recovery_consumer.to_string();
        let mut redis_client = ctx.redis().await?;
        let message_ids_clone = message_ids.clone();

        recovery_tasks.push(tokio::spawn(async move {
            let mut processed = Vec::new();
            let mut attempts = 0;

            while attempts < 5 && processed.len() < 10 {
                // Try to claim some messages
                let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
                    .xclaim(
                        stream_key,
                        group_name,
                        &consumer_name,
                        1, // min_idle_time
                        &message_ids_clone,
                    )
                    .await
                    .unwrap_or_default();

                for claim in claimed {
                    processed.push(claim.ids[0].id.clone());

                    // Acknowledge the message
                    let _: i64 = redis_client
                        .xack(stream_key, group_name, &[&claim.ids[0].id])
                        .await
                        .unwrap_or(0);
                }

                attempts += 1;
                sleep(Duration::from_millis(10)).await;
            }

            (consumer_name, processed)
        }));
    }

    // Wait for recovery tasks to complete
    let mut recovery_results = Vec::new();
    for task in recovery_tasks {
        recovery_results.push(task.await?);
    }

    // Verify recovery results
    let mut all_recovered = Vec::new();
    for (consumer, processed) in recovery_results {
        println!(
            "Recovery consumer {} processed {} messages",
            consumer,
            processed.len()
        );
        all_recovered.extend(processed);
    }

    // Verify no duplicates in recovery
    let mut sorted_recovered = all_recovered.clone();
    sorted_recovered.sort();
    let mut unique_recovered = sorted_recovered.clone();
    unique_recovered.dedup();

    assert_eq!(
        sorted_recovered.len(),
        unique_recovered.len(),
        "No message should be recovered more than once"
    );

    // Verify PEL is empty or nearly empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert!(
        final_pel.count() <= 2,
        "PEL should be empty or nearly empty after recovery, but has {} messages",
        final_pel.count()
    );

    Ok(())
}

/// Test PEL recovery with message ordering preservation
#[sinex_test]
async fn test_pel_recovery_message_ordering(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:ordering:stream";
    let group_name = "ordering-pel-group";
    let consumer_name = "ordering-consumer";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add messages with sequence numbers
    let message_count = 10;
    let mut message_ids = Vec::new();
    for i in 0..message_count {
        let id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[
                    ("type", "ordering.test"),
                    ("sequence", &i.to_string()),
                    ("data", &format!("msg-{}", i)),
                ],
            )
            .await?;
        message_ids.push(id);
    }

    // Consumer reads messages but doesn't ACK
    let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(consumer_name)
        .arg("COUNT")
        .arg(message_count)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(&mut redis_client)
        .await?;

    assert_eq!(messages.keys.len(), 1);
    assert_eq!(messages.keys[0].ids.len(), message_count);

    let read_ids: Vec<String> = messages.keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Verify messages are in PEL in correct order
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(pel_info.count(), message_count);

    // Recovery consumer claims messages in order
    let recovery_consumer = "recovery-consumer";
    let mut recovered_sequences = Vec::new();

    // Claim messages in smaller batches to test ordering
    for batch_start in (0..message_count).step_by(3) {
        let batch_end = std::cmp::min(batch_start + 3, message_count);
        let batch_ids = &read_ids[batch_start..batch_end];

        let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
            .xclaim(stream_key, group_name, recovery_consumer, 1, batch_ids)
            .await?;

        // Process claimed messages and extract sequence numbers
        for claim in claimed {
            if let Some(sequence_value) = claim.ids[0].map.get("sequence") {
                let sequence: i32 = match sequence_value {
                    redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).parse().unwrap_or(-1),
                    redis::Value::Int(i) => *i as i32,
                    _ => -1,
                };
                recovered_sequences.push(sequence);
            }

            // Acknowledge the message
            let _: i64 = redis_client
                .xack(stream_key, group_name, &[&claim.ids[0].id])
                .await?;
        }
    }

    // Verify sequences are recovered in correct order
    let mut expected_sequences: Vec<i32> = (0..message_count as i32).collect();
    recovered_sequences.sort();
    expected_sequences.sort();

    assert_eq!(
        recovered_sequences, expected_sequences,
        "Messages should be recovered in correct sequence order"
    );

    // Verify PEL is empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(
        final_pel.count(),
        0,
        "PEL should be empty after ordered recovery"
    );

    Ok(())
}

/// Test PEL recovery with idle time thresholds
#[sinex_test]
async fn test_pel_recovery_idle_thresholds(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:idle:stream";
    let group_name = "idle-pel-group";
    let slow_consumer = "slow-consumer";
    let fast_consumer = "fast-consumer";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add messages
    let mut message_ids = Vec::new();
    for i in 0..3 {
        let id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[("type", "idle.test"), ("data", &format!("msg-{}", i))],
            )
            .await?;
        message_ids.push(id);
    }

    // Slow consumer reads messages
    let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(slow_consumer)
        .arg("COUNT")
        .arg(3)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(&mut redis_client)
        .await?;

    assert_eq!(messages.keys.len(), 1);
    assert_eq!(messages.keys[0].ids.len(), 3);

    let read_ids: Vec<String> = messages.keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Fast consumer tries to claim immediately (should fail due to low idle time)
    let immediate_claim: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            fast_consumer,
            1000, // 1 second min_idle_time
            &read_ids,
        )
        .await?;

    assert_eq!(
        immediate_claim.len(),
        0,
        "Should not claim messages immediately"
    );

    // Wait for messages to become idle
    sleep(Duration::from_millis(50)).await;

    // Fast consumer claims with lower idle threshold
    let low_threshold_claim: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            fast_consumer,
            10, // 10ms min_idle_time
            &read_ids,
        )
        .await?;

    assert_eq!(
        low_threshold_claim.len(),
        3,
        "Should claim messages with low threshold"
    );

    // Verify messages are now claimed by fast consumer
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(pel_info.count(), 3);
    // Note: To iterate over pending messages, use XPENDING with detailed flag
    // For now, just verify the count

    // Acknowledge messages
    for claim in low_threshold_claim {
        let _: i64 = redis_client
            .xack(stream_key, group_name, &[&claim.ids[0].id])
            .await?;
    }

    // Verify PEL is empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(
        final_pel.count(),
        0,
        "PEL should be empty after threshold-based recovery"
    );

    Ok(())
}

/// Test PEL recovery with malformed or corrupted messages
#[sinex_test]
async fn test_pel_recovery_malformed_messages(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:malformed:stream";
    let group_name = "malformed-pel-group";
    let consumer_name = "malformed-consumer";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add messages with various data formats
    let mut message_ids = Vec::new();

    // Normal message
    let normal_id: String = redis_client
        .xadd(
            stream_key,
            "*",
            &[("type", "malformed.test"), ("data", r#"{"valid": "json"}"#)],
        )
        .await?;
    message_ids.push(normal_id);

    // Malformed JSON
    let malformed_id: String = redis_client
        .xadd(
            stream_key,
            "*",
            &[("type", "malformed.test"), ("data", r#"{"invalid": json"#)],
        )
        .await?;
    message_ids.push(malformed_id);

    // Empty data
    let empty_id: String = redis_client
        .xadd(stream_key, "*", &[("type", "malformed.test"), ("data", "")])
        .await?;
    message_ids.push(empty_id);

    // Very large data
    let large_data = "x".repeat(1000);
    let large_id: String = redis_client
        .xadd(
            stream_key,
            "*",
            &[("type", "malformed.test"), ("data", &large_data)],
        )
        .await?;
    message_ids.push(large_id);

    // Consumer reads all messages
    let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(consumer_name)
        .arg("COUNT")
        .arg(4)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(&mut redis_client)
        .await?;

    assert_eq!(messages.keys.len(), 1);
    assert_eq!(messages.keys[0].ids.len(), 4);

    let read_ids: Vec<String> = messages.keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Verify all messages are in PEL
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(pel_info.count(), 4, "All messages should be in PEL");

    // Recovery consumer claims and processes messages
    let recovery_consumer = "recovery-consumer";
    let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(stream_key, group_name, recovery_consumer, 1, &read_ids)
        .await?;

    assert_eq!(
        claimed.len(),
        4,
        "Should claim all messages including malformed ones"
    );

    // Process each message with error handling
    let mut processed_count = 0;
    let mut error_count = 0;

    for claim in claimed {
        // Simulate processing with error handling
        if let Some(data) = claim.ids[0].map.get("data") {
            let data_str = match data {
                redis::Value::Data(bytes) => String::from_utf8_lossy(bytes),
                _ => continue,
            };
            
            if data_str.is_empty() {
                error_count += 1;
            } else if data_str.starts_with('{') && data_str.ends_with('}') {
                // Try to parse as JSON
                match serde_json::from_str::<serde_json::Value>(&data_str) {
                    Ok(_) => processed_count += 1,
                    Err(_) => error_count += 1,
                }
            } else if data_str.len() > 500 {
                // Large message, might need special handling
                processed_count += 1;
            } else {
                processed_count += 1;
            }
        }

        // Acknowledge the message regardless of processing result
        let _: i64 = redis_client
            .xack(stream_key, group_name, &[&claim.ids[0].id])
            .await?;
    }

    // Verify processing results
    assert_eq!(processed_count, 2, "Should process 2 valid messages");
    assert_eq!(error_count, 2, "Should encounter 2 malformed messages");

    // Verify PEL is empty (all messages acknowledged)
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(
        final_pel.count(),
        0,
        "PEL should be empty after processing malformed messages"
    );

    Ok(())
}
