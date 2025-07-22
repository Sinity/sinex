// PEL (Pending Entry List) Recovery Tests
//
// Comprehensive tests for Redis Streams Pending Entry List recovery scenarios.
// These tests focus specifically on unacknowledged message recovery patterns
// and edge cases that can occur in production environments.

use crate::common::prelude::*;
use redis::{AsyncCommands, RedisResult, cmd};

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

    // Verify PEL has 3 remaining messages
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(pel_info.count(), 3, "3 messages should remain in PEL");

    // Claim and process remaining messages
    let remaining_ids: Vec<&str> = read_ids[3..].iter().map(|s| s.as_str()).collect();
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

    // ACK remaining messages
    for claim in claimed {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&claim.ids[0].id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    // Verify PEL is empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(final_pel.count(), 0, "PEL should be empty");

    Ok(())
}

/// Test PEL recovery with multiple consumers
#[sinex_test]
async fn test_multi_consumer_pel_recovery(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:multi:stream";
    let group_name = "multi-pel-group";
    let consumers = ["consumer-1", "consumer-2", "consumer-3"];

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add messages and distribute to consumers
    let messages_per_consumer = 4;
    let mut all_message_ids = Vec::new();

    for (consumer_idx, consumer) in consumers.iter().enumerate() {
        // Add messages for this consumer
        for i in 0..messages_per_consumer {
            let id: String = redis_client
                .xadd(
                    stream_key,
                    "*",
                    &[
                        ("type", "multi.test"),
                        ("consumer", consumer),
                        ("data", &format!("c{}-msg{}", consumer_idx, i)),
                    ],
                )
                .await?;
            all_message_ids.push((consumer.to_string(), id));
        }

        // Consumer reads their messages
        let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_name)
            .arg(consumer)
            .arg("COUNT")
            .arg(messages_per_consumer)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async(&mut redis_client)
            .await?;

        assert_eq!(messages.keys[0].ids.len(), messages_per_consumer);
    }

    // Verify total PEL count
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(
        pel_info.count(),
        (consumers.len() * messages_per_consumer) as i64,
        "All messages should be in PEL"
    );

    // Simulate consumer-2 failure - recovery consumer claims its messages
    let consumer_2_ids: Vec<&str> = all_message_ids
        .iter()
        .filter(|(consumer, _)| consumer == "consumer-2")
        .map(|(_, id)| id.as_str())
        .collect();

    let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            "recovery-consumer",
            1,
            &consumer_2_ids,
        )
        .await?;

    assert_eq!(
        claimed.len(),
        messages_per_consumer,
        "Should claim all consumer-2 messages"
    );

    // ACK recovered messages
    for claim in claimed {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&claim.ids[0].id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    // Other consumers ACK their messages normally
    for (consumer, id) in &all_message_ids {
        if consumer != "consumer-2" {
            let ack_result: i64 = redis_client
                .xack(stream_key, group_name, &[id])
                .await?;
            assert_eq!(ack_result, 1);
        }
    }

    // Verify PEL is empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(final_pel.count(), 0, "PEL should be empty");

    Ok(())
}

/// Test PEL recovery with idle timeout scenarios
#[sinex_test]
async fn test_idle_timeout_pel_recovery(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:idle:stream";
    let group_name = "idle-pel-group";
    let consumer = "idle-consumer";

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

    // Consumer reads messages
    let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(consumer)
        .arg("COUNT")
        .arg(3)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(&mut redis_client)
        .await?;

    let read_ids: Vec<String> = messages.keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Try to claim immediately (should fail due to min idle time)
    let claimed_immediate: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            "recovery-consumer",
            5000, // 5 seconds min idle time
            &read_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )
        .await?;

    assert_eq!(
        claimed_immediate.len(),
        0,
        "Should not claim messages that haven't been idle long enough"
    );

    // Wait a bit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Try with shorter idle time
    let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            "recovery-consumer",
            50, // 50ms min idle time
            &read_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )
        .await?;

    assert_eq!(claimed.len(), 3, "Should claim all messages after idle time");

    // ACK messages
    for claim in claimed {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[&claim.ids[0].id])
            .await?;
        assert_eq!(ack_result, 1);
    }

    Ok(())
}

/// Test PEL recovery with forced takeover
#[sinex_test]
async fn test_forced_pel_takeover(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:force:stream";
    let group_name = "force-pel-group";
    let original_consumer = "original-consumer";
    let takeover_consumer = "takeover-consumer";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add critical messages
    let mut message_ids = Vec::new();
    for i in 0..3 {
        let id: String = redis_client
            .xadd(
                stream_key,
                "*",
                &[
                    ("type", "critical"),
                    ("priority", "high"),
                    ("data", &format!("critical-msg-{}", i)),
                ],
            )
            .await?;
        message_ids.push(id);
    }

    // Original consumer reads messages
    let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(original_consumer)
        .arg("COUNT")
        .arg(3)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(&mut redis_client)
        .await?;

    let read_ids: Vec<String> = messages.keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Force takeover with JUSTID flag (just change ownership, don't return messages)
    let _: Vec<String> = cmd("XCLAIM")
        .arg(stream_key)
        .arg(group_name)
        .arg(takeover_consumer)
        .arg(0) // min idle time = 0 for forced takeover
        .arg(&read_ids)
        .arg("FORCE")
        .arg("JUSTID")
        .query_async(&mut redis_client)
        .await?;

    // Verify messages are now owned by takeover consumer
    // ACK messages as takeover consumer
    for id in &read_ids {
        let ack_result: i64 = redis_client
            .xack(stream_key, group_name, &[id])
            .await?;
        assert_eq!(ack_result, 1, "Takeover consumer should be able to ACK");
    }

    // Verify PEL is empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(final_pel.count(), 0, "PEL should be empty after forced takeover");

    Ok(())
}

/// Test PEL recovery with message redelivery count
#[sinex_test]
async fn test_pel_redelivery_count(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:redelivery:stream";
    let group_name = "redelivery-pel-group";
    let consumers = ["consumer-1", "consumer-2", "consumer-3"];

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add a message
    let message_id: String = redis_client
        .xadd(
            stream_key,
            "*",
            &[("type", "redelivery.test"), ("data", "test-message")],
        )
        .await?;

    // Simulate multiple failed delivery attempts
    for consumer in &consumers {
        // Read message
        let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_name)
            .arg(consumer)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async(&mut redis_client)
            .await?;

        if messages.keys[0].ids.is_empty() {
            // Message is already pending, claim it
            let _: Vec<redis::streams::StreamClaimReply> = redis_client
                .xclaim(stream_key, group_name, consumer, 1, &[&message_id])
                .await?;
        }

        // Don't ACK - simulate processing failure
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Final recovery with successful processing
    let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            "final-consumer",
            1,
            &[&message_id],
        )
        .await?;

    assert_eq!(claimed.len(), 1, "Should claim the message");

    // The claimed message should show multiple delivery attempts
    // Note: The delivery count is available in the full XPENDING output but not
    // directly in the claim response with the current Redis crate API

    // ACK the message
    let ack_result: i64 = redis_client
        .xack(stream_key, group_name, &[&message_id])
        .await?;
    assert_eq!(ack_result, 1);

    Ok(())
}

/// Test PEL recovery with concurrent claims
#[sinex_test]
async fn test_concurrent_pel_claims(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:concurrent:stream";
    let group_name = "concurrent-pel-group";
    let original_consumer = "original-consumer";
    let recovery_consumers = ["recovery-1", "recovery-2", "recovery-3"];

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Add messages
    let message_count = 9;
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

    // Original consumer reads all messages
    let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(original_consumer)
        .arg("COUNT")
        .arg(message_count)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(&mut redis_client)
        .await?;

    let read_ids: Vec<String> = messages.keys[0]
        .ids
        .iter()
        .map(|msg| msg.id.clone())
        .collect();

    // Wait to ensure messages are idle
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Concurrent recovery attempts
    let mut handles = Vec::new();
    for (idx, recovery_consumer) in recovery_consumers.iter().enumerate() {
        let redis_client = ctx.redis().await?;
        let stream_key = stream_key.to_string();
        let group_name = group_name.to_string();
        let recovery_consumer = recovery_consumer.to_string();
        
        // Each recovery consumer tries to claim a portion of messages
        let start_idx = idx * 3;
        let end_idx = (idx + 1) * 3;
        let ids_to_claim = read_ids[start_idx..end_idx].to_vec();

        let handle = tokio::spawn(async move {
            let mut redis = redis_client;
            let claimed: Vec<redis::streams::StreamClaimReply> = redis
                .xclaim(
                    &stream_key,
                    &group_name,
                    &recovery_consumer,
                    50, // min idle time
                    &ids_to_claim.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )
                .await
                .unwrap();

            // ACK claimed messages
            for claim in &claimed {
                let _: i64 = redis
                    .xack(&stream_key, &group_name, &[&claim.ids[0].id])
                    .await
                    .unwrap();
            }

            claimed.len()
        });

        handles.push(handle);
    }

    // Wait for all recovery attempts
    let results: Vec<usize> = join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // All messages should be claimed and ACKed
    let total_claimed: usize = results.iter().sum();
    assert_eq!(
        total_claimed, message_count,
        "All messages should be claimed by recovery consumers"
    );

    // Verify PEL is empty
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(final_pel.count(), 0, "PEL should be empty");

    Ok(())
}

/// Test PEL recovery patterns for production scenarios
#[sinex_test]
async fn test_production_pel_recovery_patterns(ctx: TestContext) -> TestResult {
    let mut redis_client = ctx.redis().await?;
    let stream_key = "test:pel:production:stream";
    let group_name = "production-pel-group";

    // Setup
    let _: RedisResult<()> = redis_client.del(stream_key).await;
    let _: RedisResult<()> = redis_client
        .xgroup_create(stream_key, group_name, "0")
        .await;

    // Simulate production message flow
    let message_types = vec![
        ("event.created", "low"),
        ("event.updated", "medium"),
        ("event.critical", "high"),
        ("event.batch", "low"),
    ];

    let mut message_ids = HashMap::new();
    for (msg_type, priority) in &message_types {
        for i in 0..3 {
            let id: String = redis_client
                .xadd(
                    stream_key,
                    "*",
                    &[
                        ("type", *msg_type),
                        ("priority", *priority),
                        ("data", &format!("{}-{}", msg_type, i)),
                    ],
                )
                .await?;
            message_ids.insert(id.clone(), (*msg_type, *priority));
        }
    }

    // Multiple consumers process different message types
    let consumer_assignments = vec![
        ("consumer-critical", vec!["event.critical"]),
        ("consumer-updates", vec!["event.updated"]),
        ("consumer-batch", vec!["event.created", "event.batch"]),
    ];

    // Simulate partial processing
    for (consumer, _types) in &consumer_assignments {
        let messages: redis::streams::StreamReadReply = cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_name)
            .arg(consumer)
            .arg("COUNT")
            .arg(4)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async(&mut redis_client)
            .await?;

        // Simulate some messages being processed, others failing
        if !messages.keys.is_empty() && !messages.keys[0].ids.is_empty() {
            // ACK only half of the messages
            for (idx, msg) in messages.keys[0].ids.iter().enumerate() {
                if idx % 2 == 0 {
                    let _: i64 = redis_client
                        .xack(stream_key, group_name, &[&msg.id])
                        .await?;
                }
            }
        }
    }

    // Check PEL status
    let pel_info: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    println!("PEL contains {} unprocessed messages", pel_info.count());

    // Recovery process - claim old messages based on priority
    let pending_ids: Vec<String> = message_ids.keys().cloned().collect();
    
    // Recovery consumer claims all pending messages
    let claimed: Vec<redis::streams::StreamClaimReply> = redis_client
        .xclaim(
            stream_key,
            group_name,
            "recovery-consumer",
            10, // min idle time
            &pending_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )
        .await?;

    // Process claimed messages
    for claim in claimed {
        if !claim.ids.is_empty() {
            let _: i64 = redis_client
                .xack(stream_key, group_name, &[&claim.ids[0].id])
                .await?;
        }
    }

    // Final verification
    let final_pel: redis::streams::StreamPendingReply = redis_client
        .xpending(stream_key, group_name)
        .await?;

    assert_eq!(
        final_pel.count(),
        0,
        "All messages should be processed after recovery"
    );

    Ok(())
}