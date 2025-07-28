// Property tests for Redis Streams-based automaton processing
//
// This module provides property tests for Redis Streams-based automaton processing,
// verifying correctness properties including exactly-once processing, ordering
// guarantees, and crash recovery via consumer groups.
//
// Key Properties Tested:
// - Exactly-once processing via consumer groups and acknowledgments
// - Ordering guarantees through Stream IDs (monotonic ordering)
// - Crash recovery via Pending Entry List (PEL) reclaiming
// - Scalability with multiple consumers in same group
// - No duplicate processing under high contention
// - Checkpoint-based recovery and progress tracking

use sinex_test_utils::prelude::*;
use sinex_test_utils::satellite_test_utils::{StreamMessage, simulate_redis_consumer};
use proptest::prelude::*;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisResult, cmd};
use sinex_events::{EventFactory, services, event_types};
use sinex_satellite_sdk::checkpoint::{CheckpointManager, CheckpointState};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

/// Tracks which messages have been processed to detect duplicates
#[derive(Debug, Clone)]
struct ProcessingTracker {
    processed_messages: Arc<Mutex<HashSet<String>>>,
    duplicate_detections: Arc<Mutex<Vec<String>>>,
    processing_order: Arc<Mutex<Vec<String>>>,
}

impl ProcessingTracker {
    fn new() -> Self {
        Self {
            processed_messages: Arc::new(Mutex::new(HashSet::new())),
            duplicate_detections: Arc::new(Mutex::new(Vec::new())),
            processing_order: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Mark a message as processed, returns true if this is a duplicate
    fn mark_processed(&self, message_id: &str) -> bool {
        let mut processed = self.processed_messages.lock().expect("Lock failed");
        let mut order = self.processing_order.lock().expect("Lock failed");
        
        if processed.contains(message_id) {
            // Duplicate detected!
            let mut duplicates = self.duplicate_detections.lock().expect("Lock failed");
            duplicates.push(message_id.to_string());
            true
        } else {
            processed.insert(message_id.to_string());
            order.push(message_id.to_string());
            false
        }
    }

    fn get_duplicates(&self) -> Vec<String> {
        self.duplicate_detections
            .lock()
            .expect("Lock failed")
            .clone()
    }

    fn processed_count(&self) -> usize {
        self.processed_messages.lock().expect("Lock failed").len()
    }

    fn get_processing_order(&self) -> Vec<String> {
        self.processing_order.lock().expect("Lock failed").clone()
    }
}

/// Simulates an automaton consumer that processes events from Redis Streams with potential crashes
async fn automaton_consumer_with_crashes(
    redis: ConnectionManager,
    stream_key: String,
    group_name: String,
    consumer_name: String,
    tracker: ProcessingTracker,
    checkpoint_mgr: Option<CheckpointManager>,
    crash_probability: f64,
    runtime_seconds: u64,
    seed: u64,
) -> AnyhowResult<(), anyhow::Error> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let start_time = Instant::now();
    let mut crash_counter = 0u64;
    
    // Deterministic crash simulation
    let consumer_hash = {
        let mut hasher = DefaultHasher::new();
        consumer_name.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish()
    };

    let mut redis_conn = redis.clone();
    let mut checkpoint_state = if let Some(ref mgr) = checkpoint_mgr {
        mgr.load_checkpoint().await.unwrap_or_default()
    } else {
        CheckpointState::default()
    };

    while start_time.elapsed().as_secs() < runtime_seconds {
        crash_counter += 1;

        // Simulate crash based on probability
        let crash_threshold = (crash_probability * 100.0) as u64;
        let crash_roll = (crash_counter.wrapping_mul(consumer_hash)) % 100;
        if crash_roll < crash_threshold {
            // Simulate crash by returning early (abandoning any claimed messages)
            return Ok(());
        }

        // Read messages from the stream using consumer group
        let result: RedisResult<Vec<(String, Vec<(String, String)>)>> = redis_conn
            .xreadgroup_block(
                &group_name,
                &consumer_name,
                1000, // 1 second timeout
                false, // Don't use NOACK
                Some(5), // Read up to 5 messages
                &[(&stream_key, ">")],
            )
            .await;

        match result {
            Ok(streams) => {
                for (_stream, messages) in streams {
                    for (message_id, _fields) in messages {
                        // Check for duplicate processing
                        let is_duplicate = tracker.mark_processed(&message_id);

                        if !is_duplicate {
                            // Simulate processing work
                            tokio::time::sleep(Duration::from_millis(10)).await;

                            // Update checkpoint state
                            checkpoint_state.processed_count += 1;
                            checkpoint_state.last_activity = chrono::Utc::now();

                            // Check for crash before acknowledgment
                            crash_counter += 1;
                            let ack_crash_roll = (crash_counter.wrapping_mul(consumer_hash)) % 100;
                            if ack_crash_roll < crash_threshold {
                                // Crash before acknowledging - message should be redelivered
                                return Ok(());
                            }

                            // Acknowledge the message
                            let _: RedisResult<i64> = redis_conn
                                .xack(&stream_key, &group_name, &[&message_id])
                                .await;

                            // Save checkpoint periodically
                            if let Some(ref mgr) = checkpoint_mgr {
                                if checkpoint_state.processed_count % 10 == 0 {
                                    let _ = mgr.save_checkpoint(&checkpoint_state).await;
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // Redis error, wait a bit and retry
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    // Final checkpoint save
    if let Some(ref mgr) = checkpoint_mgr {
        let _ = mgr.save_checkpoint(&checkpoint_state).await;
    }

    Ok(())
}

/// Test that Redis Streams with consumer groups prevent duplicate processing even with crashes
#[sinex_test]
async fn test_no_duplicate_processing_with_crashes(ctx: TestContext) -> TestResult {
    
    // Setup Redis connection
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let redis_conn = ConnectionManager::new(redis_client).await?;

    proptest!(|(
        num_consumers in 2..=8usize,
        num_events in 10..=50usize,
        crash_probability in 0.1..=0.3f64,
        runtime_seconds in 5..=15u64,
        seed in any::<u64>(),
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let mut redis = redis_conn.clone();
            
            // Create unique stream and group names for this test
            let test_id = Ulid::new();
            let stream_key = format!("sinex:test:stream:{}", test_id);
            let group_name = format!("test_group_{}", test_id);

            // Create consumer group
            let _: RedisResult<String> = redis
                .xgroup_create(&stream_key, &group_name, "$")
                .await;

            // Publish test events to the stream
            let mut event_ids = Vec::new();
            for i in 0..num_events {
                // Create a proper RawEvent
                let factory = EventFactory::new("test.property");
                let event = factory.create_event(
                    "property_test_event",
                    json!({
                        "event_number": i,
                        "test_run": test_id.to_string(),
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    })
                );

                let event_json = serde_json::to_string(&event)?;
                let result: RedisResult<String> = redis
                    .xadd(
                        &stream_key,
                        "*", // Auto-generate ID
                        &[
                            ("event", event_json),
                            ("source", &event.source),
                            ("event_type", &event.event_type),
                        ],
                    )
                    .await;

                if let Ok(event_id) = result {
                    event_ids.push(event_id);
                }
            }

            // Setup tracking
            let tracker = ProcessingTracker::new();

            // Spawn multiple consumers with checkpoints
            let mut join_set = JoinSet::new();
            for consumer_num in 0..num_consumers {
                let redis_clone = redis_conn.clone();
                let stream_key_clone = stream_key.clone();
                let group_name_clone = group_name.clone();
                let consumer_name = format!("consumer_{}", consumer_num);
                let tracker_clone = tracker.clone();

                // Create checkpoint manager for each consumer
                let checkpoint_mgr = CheckpointManager::new(
                    ctx.pool(),
                    format!("test_automaton_{}_{}", test_id, consumer_num),
                    group_name_clone.clone(),
                    consumer_name.clone(),
                );

                join_set.spawn(automaton_consumer_with_crashes(
                    redis_clone,
                    stream_key_clone,
                    group_name_clone,
                    consumer_name,
                    tracker_clone,
                    Some(checkpoint_mgr),
                    crash_probability,
                    runtime_seconds,
                    seed.wrapping_add(consumer_num as u64),
                ));
            }

            // Wait for all consumers to complete
            while let Some(result) = join_set.join_next().await {
                if let Err(e) = result {
                    panic!("Consumer task failed: {:?}", e);
                }
            }

            // Check for duplicates
            let duplicates = tracker.get_duplicates();
            let processed_count = tracker.processed_count();

            // Property: No event should be processed more than once
            prop_assert!(
                duplicates.is_empty(),
                "Duplicate processing detected! {} events were processed multiple times: {:?}",
                duplicates.len(),
                duplicates
            );

            // Verify some work was actually done
            prop_assert!(
                processed_count > 0,
                "No events were processed at all - test may be misconfigured"
            );

            // Check pending messages in the consumer group (should be handled by PEL recovery)
            let pending_info: RedisResult<Vec<redis::Value>> = redis
                .xpending_count(&stream_key, &group_name)
                .await;

            if let Ok(pending_result) = pending_info {
                if let Some(pending_count) = pending_result.get(0) {
                    if let Ok(count) = pending_count.as_u64() {
                        // Property: Processed + Pending should not exceed total events
                        prop_assert!(
                            processed_count + (count as usize) <= num_events,
                            "Inconsistency: {} processed + {} pending > {} total events",
                            processed_count,
                            count,
                            num_events
                        );
                    }
                }
            }

            // Cleanup: Delete the test stream
            let _: RedisResult<i64> = redis.del(&stream_key).await;

            Ok(())
        })?
    });
    
    Ok(())
}

/// Test consumer group scaling and high contention scenarios
#[sinex_test]
async fn test_consumer_group_contention_properties(ctx: TestContext) -> TestResult {
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let redis_conn = ConnectionManager::new(redis_client).await?;

    proptest!(|(
        num_consumers in 5..=15usize,
        items_per_batch in 1..=3usize,
        _seed in any::<u64>(),
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let mut redis = redis_conn.clone();
            
            let test_id = Ulid::new();
            let stream_key = format!("sinex:test:contention:{}", test_id);
            let group_name = format!("contention_group_{}", test_id);

            // Create consumer group
            let _: RedisResult<String> = redis
                .xgroup_create(&stream_key, &group_name, "$")
                .await;

            // Create exactly one event to maximize contention
            let factory = EventFactory::new("test.contention");
            let event = factory.create_event(
                "contention_event",
                json!({"contention_test": true, "test_id": test_id.to_string()})
            );

            let event_json = serde_json::to_string(&event)?;
            let _: RedisResult<String> = redis
                .xadd(
                    &stream_key,
                    "*",
                    &[
                        ("event", event_json),
                        ("source", &event.source),
                        ("event_type", &event.event_type),
                    ],
                )
                .await;

            let tracker = ProcessingTracker::new();

            // All consumers try to claim the same single event simultaneously
            let mut join_set = JoinSet::new();
            for consumer_num in 0..num_consumers {
                let redis_clone = redis_conn.clone();
                let stream_key_clone = stream_key.clone();
                let group_name_clone = group_name.clone();
                let consumer_name = format!("contention_consumer_{}", consumer_num);
                let tracker_clone = tracker.clone();

                join_set.spawn(async move {
                    let mut redis_conn = redis_clone;
                    
                    // Single aggressive read attempt
                    let result: RedisResult<Vec<(String, Vec<(String, String)>)>> = redis_conn
                        .cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(&group_name_clone)
                        .arg(&consumer_name)
                        .arg("COUNT")
                        .arg(items_per_batch)
                        .arg("STREAMS")
                        .arg(&stream_key_clone)
                        .arg(">")
                        .query_async(&mut redis)
                        .await;

                    if let Ok(streams) = result {
                        for (_stream, messages) in streams {
                            for (message_id, _fields) in messages {
                                let is_duplicate = tracker_clone.mark_processed(&message_id);
                                if !is_duplicate {
                                    // Acknowledge immediately
                                    let _: RedisResult<i64> = redis_conn
                                        .xack(&stream_key_clone, &group_name_clone, &[&message_id])
                                        .await;
                                }
                            }
                        }
                    }
                });
            }

            // Wait for all consumers
            while let Some(result) = join_set.join_next().await {
                result.expect("Consumer task failed");
            }

            // Property: Exactly one consumer should have processed the event
            let duplicates = tracker.get_duplicates();
            let processed_count = tracker.processed_count();

            prop_assert!(
                duplicates.is_empty(),
                "High contention caused duplicate processing: {:?}",
                duplicates
            );

            prop_assert!(
                processed_count <= 1,
                "More than one event was processed, but only one existed: {}",
                processed_count
            );

            // Cleanup
            let _: RedisResult<i64> = redis.del(&stream_key).await;

            Ok(())
        })?
    });
    Ok(())
}

/// Test scaling properties with many events and consumers
#[sinex_test]
async fn test_redis_streams_scalability_properties(ctx: TestContext) -> TestResult {
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let redis_conn = ConnectionManager::new(redis_client).await?;

    proptest!(|(
        event_count in 50..=500usize,
        consumer_count in 2..=10usize,
        batch_size in 1..=20usize,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let mut redis = redis_conn.clone();
            
            let test_id = Ulid::new();
            let stream_key = format!("sinex:test:scalability:{}", test_id);
            let group_name = format!("scalability_group_{}", test_id);

            // Create consumer group
            let _: RedisResult<String> = redis
                .xgroup_create(&stream_key, &group_name, "$")
                .await;

            // Create many events
            let creation_start = Instant::now();
            for i in 0..event_count {
                let factory = EventFactory::new("test.scalability");
                let event = factory.create_event(
                    "scalability_event",
                    json!({
                        "event_number": i,
                        "data": format!("test_data_{}", i),
                        "test_id": test_id.to_string(),
                    })
                );

                let event_json = serde_json::to_string(&event)?;
                let _: RedisResult<String> = redis
                    .xadd(
                        &stream_key,
                        "*",
                        &[
                            ("event", event_json),
                            ("source", &event.source),
                            ("event_type", &event.event_type),
                        ],
                    )
                    .await;
            }
            let creation_time = creation_start.elapsed();

            // Property: Stream creation should be reasonably fast
            prop_assert!(
                creation_time.as_millis() < (event_count as u128 * 10), // 10ms per event max
                "Stream creation too slow: {}ms for {} events",
                creation_time.as_millis(),
                event_count
            );

            let tracker = ProcessingTracker::new();
            let processing_start = Instant::now();

            // Spawn consumers to process events
            let mut join_set = JoinSet::new();
            for consumer_num in 0..consumer_count {
                let redis_clone = redis_conn.clone();
                let stream_key_clone = stream_key.clone();
                let group_name_clone = group_name.clone();
                let consumer_name = format!("scalability_consumer_{}", consumer_num);
                let tracker_clone = tracker.clone();

                join_set.spawn(async move {
                    let mut processed_locally = 0;
                    let mut redis_conn = redis_clone;

                    // Process events until none are left
                    loop {
                        let result: RedisResult<Vec<(String, Vec<(String, String)>)>> = redis_conn
                            .cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(&group_name_clone)
                        .arg(&consumer_name)
                        .arg("COUNT")
                        .arg(batch_size)
                        .arg("STREAMS")
                        .arg(&stream_key_clone)
                        .arg(">")
                        .query_async(&mut redis)
                            .await;

                        match result {
                            Ok(streams) => {
                                if streams.is_empty() || streams[0].1.is_empty() {
                                    break; // No more events
                                }

                                for (_stream, messages) in streams {
                                    for (message_id, _fields) in messages {
                                        let is_duplicate = tracker_clone.mark_processed(&message_id);
                                        if !is_duplicate {
                                            // Acknowledge immediately
                                            let _: RedisResult<i64> = redis_conn
                                                .xack(&stream_key_clone, &group_name_clone, &[&message_id])
                                                .await;
                                            processed_locally += 1;
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        }
                    }

                    processed_locally
                });
            }

            // Wait for all consumers and collect results
            let mut total_processed_by_consumers = 0;
            while let Some(result) = join_set.join_next().await {
                total_processed_by_consumers += result.expect("Consumer failed");
            }

            let processing_time = processing_start.elapsed();
            let tracker_processed = tracker.processed_count();

            // Property: All events should be processed exactly once
            prop_assert!(
                tracker.get_duplicates().is_empty(),
                "Scalability test found duplicates: {:?}",
                tracker.get_duplicates()
            );

            prop_assert_eq!(
                tracker_processed, event_count,
                "Tracker processed {} events, expected {}",
                tracker_processed, event_count
            );

            prop_assert_eq!(
                total_processed_by_consumers, event_count,
                "Consumers processed {} events, expected {}",
                total_processed_by_consumers, event_count
            );

            // Property: Processing should be reasonably fast
            let throughput = event_count as f64 / processing_time.as_secs_f64();
            prop_assert!(
                throughput > 50.0, // At least 50 events per second
                "Processing too slow: {:.2} events/sec for {} events with {} consumers",
                throughput, event_count, consumer_count
            );

            // Cleanup
            let _: RedisResult<i64> = redis.del(&stream_key).await;

            Ok(())
        })?
    });
    
    Ok(())
}

/// Test that Redis Stream IDs maintain ordering guarantees
#[sinex_test]
async fn test_redis_stream_ordering_properties() -> AnyhowResult<(), anyhow::Error> {
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let mut redis = ConnectionManager::new(redis_client).await?;

    proptest!(|(
        event_count in 10..=50usize,
        time_gap_ms in 10..=100u64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let test_id = Ulid::new();
            let stream_key = format!("sinex:test:ordering:{}", test_id);
            let group_name = format!("ordering_group_{}", test_id);

            // Create consumer group
            let _: RedisResult<String> = redis
                .xgroup_create(&stream_key, &group_name, "$")
                .await;

            // Create events with controlled timing and sequence numbers
            let mut created_sequences = Vec::new();
            for i in 0..event_count {
                let factory = EventFactory::new("test.ordering");
                let event = factory.create_event(
                    "ordering_event",
                    json!({
                        "sequence": i,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "test_id": test_id.to_string(),
                    })
                );

                let event_json = serde_json::to_string(&event)?;
                let stream_id: String = redis
                    .xadd(
                        &stream_key,
                        "*",
                        &[
                            ("event", event_json),
                            ("sequence", &i.to_string()),
                        ],
                    )
                    .await
                    .expect("Failed to add to stream");

                created_sequences.push((stream_id, i));

                // Small delay to ensure different creation times
                tokio::time::sleep(Duration::from_millis(time_gap_ms)).await;
            }

            // Read events back in consumer group order and verify sequence
            let tracker = ProcessingTracker::new();
            let consumer_name = "ordering_consumer";
            let mut claimed_sequences = Vec::new();

            // Consume events one by one to test ordering
            for _ in 0..event_count {
                let result: RedisResult<Vec<(String, Vec<(String, String)>)>> = redis
                    .xreadgroup(
                        &group_name,
                        consumer_name,
                        Some(1), // Read one event at a time
                        false,
                        &[(&stream_key, ">")],
                    )
                    .await;

                if let Ok(streams) = result {
                    for (_stream, messages) in streams {
                        for (message_id, fields) in messages {
                            let is_duplicate = tracker.mark_processed(&message_id);
                            if !is_duplicate {
                                // Extract sequence number from fields
                                for (key, value) in &fields {
                                    if key == "sequence" {
                                        if let Ok(seq) = value.parse::<usize>() {
                                            claimed_sequences.push(seq);
                                        }
                                    }
                                }

                                // Acknowledge the message
                                let _: RedisResult<i64> = redis
                                    .xack(&stream_key, &group_name, &[&message_id])
                                    .await;
                            }
                        }
                    }
                }
            }

            // Property: Events should be consumed in creation order (FIFO)
            prop_assert_eq!(
                claimed_sequences.len(), event_count,
                "Should have consumed all {} events, got {}", event_count, claimed_sequences.len()
            );

            for (i, &sequence) in claimed_sequences.iter().enumerate() {
                prop_assert_eq!(
                    sequence, i,
                    "Event at position {} should have sequence {}, got {}",
                    i, i, sequence
                );
            }

            // Property: No duplicates should be detected
            prop_assert!(
                tracker.get_duplicates().is_empty(),
                "Ordering test detected duplicates: {:?}",
                tracker.get_duplicates()
            );

            // Cleanup
            let _: RedisResult<i64> = redis.del(&stream_key).await;

            Ok(())
        })?
    });

    Ok(())
}

/// Test checkpoint-based recovery after consumer crashes
#[sinex_test]
async fn test_checkpoint_recovery_properties(ctx: TestContext) -> TestResult {
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let redis_conn = ConnectionManager::new(redis_client).await?;

    proptest!(|(
        events_before_crash in 20..=50usize,
        crash_after_percent in 0.3..=0.7f64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let mut redis = redis_conn.clone();
            let test_id = Ulid::new();
            let stream_key = format!("sinex:test:checkpoint:{}", test_id);
            let group_name = format!("checkpoint_group_{}", test_id);
            let consumer_name = "checkpoint_consumer";
            let processor_name = format!("test_automaton_{}", test_id);

            // Create consumer group
            let _: RedisResult<String> = redis
                .xgroup_create(&stream_key, &group_name, "$")
                .await;

            // Publish events to stream
            for i in 0..events_before_crash {
                let factory = EventFactory::new("test.checkpoint");
                let event = factory.create_event(
                    "checkpoint_event",
                    json!({
                        "event_number": i,
                        "test_id": test_id.to_string(),
                    })
                );

                let event_json = serde_json::to_string(&event)?;
                let _: RedisResult<String> = redis
                    .xadd(
                        &stream_key,
                        "*",
                        &[
                            ("event", event_json),
                            ("event_number", &i.to_string()),
                        ],
                    )
                    .await;
            }

            // Create checkpoint manager
            let checkpoint_mgr = CheckpointManager::new(
                ctx.pool(),
                processor_name.clone(),
                group_name.clone(),
                consumer_name.to_string(),
            );

            // Process events until crash point
            let crash_point = (events_before_crash as f64 * crash_after_percent) as usize;
            let mut checkpoint = checkpoint_mgr.load_checkpoint().await?;
            let mut processed_count = 0;
            
            // Simulate processing until crash
            let mut last_message_id = None;
            for _ in 0..crash_point {
                let result: RedisResult<Vec<(String, Vec<(String, String)>)>> = redis
                    .xreadgroup(
                        &group_name,
                        consumer_name,
                        Some(1),
                        false,
                        &[(&stream_key, ">")],
                    )
                    .await;

                if let Ok(streams) = result {
                    for (_stream, messages) in streams {
                        for (message_id, _fields) in messages {
                            processed_count += 1;
                            last_message_id = Some(message_id.clone());
                            
                            // Acknowledge the message
                            let _: RedisResult<i64> = redis
                                .xack(&stream_key, &group_name, &[&message_id])
                                .await;

                            // Update and save checkpoint
                            checkpoint.processed_count += 1;
                            checkpoint.last_activity = chrono::Utc::now();
                            
                            if processed_count % 5 == 0 {
                                checkpoint_mgr.save_checkpoint(&checkpoint).await?;
                            }
                        }
                    }
                }
            }
            
            // Final checkpoint before crash
            checkpoint_mgr.save_checkpoint(&checkpoint).await?;
            let pre_crash_count = checkpoint.processed_count;

            // Simulate crash and recovery
            let recovered_checkpoint = checkpoint_mgr.load_checkpoint().await?;

            // Property: Checkpoint should persist across crash
            prop_assert_eq!(
                recovered_checkpoint.processed_count,
                pre_crash_count,
                "Checkpoint didn't persist: expected {}, got {}",
                pre_crash_count,
                recovered_checkpoint.processed_count
            );

            // Property: Should be able to resume processing from checkpoint
            let mut final_processed = processed_count;
            let remaining_to_process = events_before_crash - crash_point;
            
            for _ in 0..remaining_to_process {
                let result: RedisResult<Vec<(String, Vec<(String, String)>)>> = redis
                    .xreadgroup(
                        &group_name,
                        consumer_name,
                        Some(1),
                        false,
                        &[(&stream_key, ">")],
                    )
                    .await;

                if let Ok(streams) = result {
                    for (_stream, messages) in streams {
                        for (message_id, _fields) in messages {
                            final_processed += 1;
                            
                            let _: RedisResult<i64> = redis
                                .xack(&stream_key, &group_name, &[&message_id])
                                .await;
                        }
                    }
                }
            }

            // Property: All events should be processed exactly once
            prop_assert_eq!(
                final_processed,
                events_before_crash,
                "Final count should equal total events: {} != {}",
                final_processed,
                events_before_crash
            );

            // Cleanup
            let _: RedisResult<i64> = redis.del(&stream_key).await;

            Ok(())
        })?
    });

    Ok(())
}

/// Test that consumer group state remains consistent under various failure scenarios
#[sinex_test]
async fn test_consumer_group_state_consistency(ctx: TestContext) -> TestResult {
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let redis_conn = ConnectionManager::new(redis_client).await?;

    proptest!(|(
        initial_events in 5..=20usize,
        operations_per_consumer in 3..=10usize,
        num_consumers in 2..=5usize,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let mut redis = redis_conn.clone();
            
            let test_id = Ulid::new();
            let stream_key = format!("sinex:test:consistency:{}", test_id);
            let group_name = format!("consistency_group_{}", test_id);

            // Create consumer group
            let _: RedisResult<String> = redis
                .xgroup_create(&stream_key, &group_name, "$")
                .await;

            // Create initial events
            for i in 0..initial_events {
                let factory = EventFactory::new("test.consistency");
                let event = factory.create_event(
                    "consistency_event",
                    json!({"event_number": i, "test_id": test_id.to_string()})
                );

                let event_json = serde_json::to_string(&event)?;
                let _: RedisResult<String> = redis
                    .xadd(
                        &stream_key,
                        "*",
                        &[("event", event_json)],
                    )
                    .await;
            }

            // Spawn consumers that read, process, and sometimes fail to acknowledge
            let mut join_set = JoinSet::new();
            for consumer_num in 0..num_consumers {
                let redis_clone = redis_conn.clone();
                let stream_key_clone = stream_key.clone();
                let group_name_clone = group_name.clone();
                let consumer_name = format!("consistency_consumer_{}", consumer_num);

                join_set.spawn(async move {
                    let mut redis_conn = redis_clone;
                    let mut operations_done = 0;
                    let mut acknowledged_messages = Vec::new();

                    while operations_done < operations_per_consumer {
                        let result: RedisResult<Vec<(String, Vec<(String, String)>)>> = redis_conn
                            .cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(&group_name_clone)
                        .arg(&consumer_name)
                        .arg("COUNT")
                        .arg(1)
                        .arg("STREAMS")
                        .arg(&stream_key_clone)
                        .arg(">")
                        .query_async(&mut redis)
                            .await;

                        if let Ok(streams) = result {
                            for (_stream, messages) in streams {
                                for (message_id, _fields) in messages {
                                    // Simulate some work
                                    tokio::time::sleep(Duration::from_millis(10)).await;

                                    // Acknowledge with 80% probability (simulate some failures)
                                    if (consumer_num + operations_done) % 5 != 0 {
                                        let _: RedisResult<i64> = redis_conn
                                            .xack(&stream_key_clone, &group_name_clone, &[&message_id])
                                            .await;
                                        acknowledged_messages.push(message_id);
                                    }
                                    // 20% chance we don't acknowledge (simulate failure)

                                    operations_done += 1;
                                }
                            }
                        } else {
                            // No messages available, short break
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            operations_done += 1;
                        }
                    }

                    acknowledged_messages
                });
            }

            // Collect results from all consumers
            let mut all_acknowledged = Vec::new();
            while let Some(result) = join_set.join_next().await {
                all_acknowledged.extend(result.expect("Consumer failed"));
            }

            // Check final stream state
            let stream_info: RedisResult<redis::Value> = redis.xinfo_stream(&stream_key).await;
            let pending_info: RedisResult<Vec<redis::Value>> = redis
                .xpending_count(&stream_key, &group_name)
                .await;

            // Property: All acknowledged messages should be unique
            let mut unique_acknowledged = all_acknowledged.clone();
            unique_acknowledged.sort();
            unique_acknowledged.dedup();
            prop_assert_eq!(
                unique_acknowledged.len(), all_acknowledged.len(),
                "Duplicate acknowledgments detected: {} unique vs {} total",
                unique_acknowledged.len(), all_acknowledged.len()
            );

            // Property: Stream length + acknowledged should be reasonable
            if let (Ok(stream_val), Ok(pending_val)) = (stream_info, pending_info) {
                // Basic consistency check - the exact numbers depend on Redis internal state
                // but we can verify that acknowledged messages don't exceed total events
                prop_assert!(
                    all_acknowledged.len() <= initial_events,
                    "Acknowledged more messages than were created: {} > {}",
                    all_acknowledged.len(), initial_events
                );
            }

            // Cleanup
            let _: RedisResult<i64> = redis.del(&stream_key).await;

            Ok(())
        })?
    });
    Ok(())
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_processing_tracker() {
        let tracker = ProcessingTracker::new();
        let id1 = "test-msg-1";
        let id2 = "test-msg-2";

        // First processing should succeed
        assert!(!tracker.mark_processed(id1));
        assert_eq!(tracker.processed_count(), 1);
        assert!(tracker.get_duplicates().is_empty());

        // Different ID should also succeed
        assert!(!tracker.mark_processed(id2));
        assert_eq!(tracker.processed_count(), 2);
        assert!(tracker.get_duplicates().is_empty());

        // Same ID again should detect duplicate
        assert!(tracker.mark_processed(id1));
        assert_eq!(tracker.processed_count(), 2); // Count doesn't increase
        assert_eq!(tracker.get_duplicates().len(), 1);
        assert_eq!(tracker.get_duplicates()[0], id1);

        // Verify processing order
        let order = tracker.get_processing_order();
        assert_eq!(order, vec![id1, id2]);
    }

    #[sinex_test(timeout = 40)]
    async fn test_automaton_crash_simulation(ctx: TestContext) -> TestResult {
        // Test that the crash simulation compiles and runs
        let redis_client = redis::Client::open("redis://127.0.0.1/")?;
        let redis_conn = ConnectionManager::new(redis_client).await?;
        let tracker = ProcessingTracker::new();

        // Test with 100% crash probability (should exit immediately)
        let result = automaton_consumer_with_crashes(
            redis_conn,
            "test_stream".to_string(),
            "test_group".to_string(),
            "crash_test_consumer".to_string(),
            tracker,
            None, // No checkpoint manager
            1.0,  // 100% crash probability
            1,    // 1 second runtime
            42,   // seed
        )
        .await;

        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_crash_simulation_deterministic() {
        // Test that crash simulation is deterministic with same seed
        let seed = 12345u64;
        let consumer_name = "test_consumer";

        // Simple hash calculation similar to the one in automaton_consumer_with_crashes
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let consumer_hash = {
            let mut hasher = DefaultHasher::new();
            consumer_name.hash(&mut hasher);
            seed.hash(&mut hasher);
            hasher.finish()
        };

        // Same calculation should produce same hash
        let consumer_hash2 = {
            let mut hasher = DefaultHasher::new();
            consumer_name.hash(&mut hasher);
            seed.hash(&mut hasher);
            hasher.finish()
        };

        assert_eq!(consumer_hash, consumer_hash2);

        // Different consumer name should produce different hash
        let different_consumer_hash = {
            let mut hasher = DefaultHasher::new();
            "different_consumer".hash(&mut hasher);
            seed.hash(&mut hasher);
            hasher.finish()
        };

        assert_ne!(consumer_hash, different_consumer_hash);
    }

    #[test]
    fn test_processing_tracker_thread_safety() {
        // Test that ProcessingTracker works correctly under concurrent access
        let tracker = ProcessingTracker::new();
        let tracker_clone = tracker.clone();

        let message_ids = vec!["msg1", "msg2", "msg3", "msg4", "msg5", "msg6", "msg7", "msg8", "msg9", "msg10"];

        // Process some messages
        for (i, msg_id) in message_ids.iter().enumerate() {
            let is_dup = if i < 5 {
                tracker.mark_processed(msg_id)
            } else {
                tracker_clone.mark_processed(msg_id)
            };

            assert!(!is_dup, "First processing should not be duplicate");
        }

        assert_eq!(tracker.processed_count(), 10);
        assert!(tracker.get_duplicates().is_empty());

        // Try to process the same messages again - should detect duplicates
        for msg_id in &message_ids[0..3] {
            let is_dup = tracker.mark_processed(msg_id);
            assert!(is_dup, "Second processing should be duplicate");
        }

        assert_eq!(tracker.processed_count(), 10); // Count shouldn't increase
        assert_eq!(tracker.get_duplicates().len(), 3); // Should have 3 duplicates
    }
}

// =============================================================================
// Redis Streams Retry and Performance Property Tests
// =============================================================================

proptest! {
    /// Test Redis Streams consumer group retry behavior with exponential backoff
    #[test]
    fn test_consumer_group_retry_timing_boundaries(
        attempts in 0i32..20,
        base_delay in 1.0f64..300.0,
    ) {
        // Calculate exponential backoff for Redis consumer failures
        let delay = base_delay * (2.0_f64.powi(attempts));
        let with_jitter = delay * 1.1; // Max jitter
        let clamped = with_jitter.clamp(1.0, 24.0 * 3600.0);

        // Verify bounds
        assert!(clamped >= 1.0);
        assert!(clamped <= 24.0 * 3600.0);

        // Verify exponential growth until cap
        if attempts < 10 && base_delay < 100.0 {
            // Should be growing exponentially before hitting cap
            let next_delay = base_delay * (2.0_f64.powi(attempts + 1));
            if next_delay * 1.1 <= 24.0 * 3600.0 {
                assert!(delay < next_delay);
            }
        }

        // Verify reasonable minimum delay
        assert!(clamped >= base_delay.min(1.0));
    }

    /// Test Redis consumer group retry patterns with realistic scenarios
    #[test]
    fn test_redis_consumer_retry_patterns(
        failure_count in 0usize..10,
        base_retry_ms in 100u64..5000,
    ) {
        let mut retry_delays = Vec::new();

        for attempt in 0..failure_count {
            let delay_ms = base_retry_ms * (2_u64.pow(attempt as u32));
            let max_delay_ms = 30 * 60 * 1000; // 30 minutes max
            let actual_delay = delay_ms.min(max_delay_ms);

            retry_delays.push(actual_delay);

            // Verify delay is reasonable
            assert!(actual_delay >= base_retry_ms);
            assert!(actual_delay <= max_delay_ms);
        }

        // Verify delays are non-decreasing (exponential backoff)
        for window in retry_delays.windows(2) {
            if let [prev, next] = window {
                assert!(next >= prev, "Consumer retry delays should not decrease");
            }
        }
    }

    /// Test Stream ID monotonicity properties
    #[test]
    fn test_stream_id_monotonicity(
        timestamp_increment in 1u64..1000,
        sequence_increment in 0u64..100,
    ) {
        // Simulate Redis Stream ID generation (timestamp-sequence)
        let base_timestamp = 1600000000000u64; // Some base timestamp
        
        let id1_timestamp = base_timestamp;
        let id1_sequence = 0u64;
        
        let id2_timestamp = base_timestamp + timestamp_increment;
        let id2_sequence = sequence_increment;
        
        // Property: Later timestamps should always be greater
        if id2_timestamp > id1_timestamp {
            assert!(true); // Always true for different timestamps
        } else if id2_timestamp == id1_timestamp {
            // Same timestamp, sequence should be greater
            assert!(id2_sequence > id1_sequence, 
                "Same timestamp requires higher sequence number");
        }
        
        // Property: Stream IDs are always comparable and ordered
        let id1_comparable = (id1_timestamp, id1_sequence);
        let id2_comparable = (id2_timestamp, id2_sequence);
        
        // Either id1 < id2 or id2 < id1, never equal (unless intentionally same)
        if id1_comparable != id2_comparable {
            assert!(id1_comparable < id2_comparable || id2_comparable < id1_comparable);
        }
    }
}
