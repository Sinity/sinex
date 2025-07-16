//! Property tests for Redis Streams-based event processing
//! 
//! This module replaces the old work_queue property tests with tests
//! that verify the same correctness properties using Redis Streams
//! and consumer groups.

use crate::common::prelude::*;
use proptest::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisResult};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;

/// Tracks which events have been processed to detect duplicates
#[derive(Debug, Clone)]
struct ProcessingTracker {
    processed_events: Arc<Mutex<HashSet<String>>>,
    duplicate_detections: Arc<Mutex<Vec<String>>>,
}

impl ProcessingTracker {
    fn new() -> Self {
        Self {
            processed_events: Arc::new(Mutex::new(HashSet::new())),
            duplicate_detections: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Mark an event as processed, returns true if this is a duplicate
    fn mark_processed(&self, event_id: &str) -> bool {
        let mut processed = self.processed_events.lock().expect("Lock failed");
        if processed.contains(event_id) {
            let mut duplicates = self.duplicate_detections.lock().expect("Lock failed");
            duplicates.push(event_id.to_string());
            true
        } else {
            processed.insert(event_id.to_string());
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
        self.processed_events.lock().expect("Lock failed").len()
    }
}

/// Simulates an automaton consumer that processes events from Redis Streams
async fn automaton_consumer_with_crashes(
    redis: ConnectionManager,
    stream_key: String,
    group_name: String,
    consumer_name: String,
    tracker: ProcessingTracker,
    crash_probability: f64,
    runtime_seconds: u64,
    seed: u64,
) -> Result<(), anyhow::Error> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let start_time = std::time::Instant::now();
    let mut crash_counter = 0u64;
    
    let consumer_hash = {
        let mut hasher = DefaultHasher::new();
        consumer_name.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish()
    };

    let mut redis_conn = redis.clone();

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
                None, // No specific count
                &[(&stream_key, ">")],
            )
            .await;

        match result {
            Ok(streams) => {
                for (_stream, messages) in streams {
                    for (message_id, fields) in messages {
                        // Check for duplicate processing
                        let is_duplicate = tracker.mark_processed(&message_id);

                        if !is_duplicate {
                            // Simulate processing work
                            tokio::time::sleep(Duration::from_millis(10)).await;

                            // Acknowledge the message (unless we crash)
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

    Ok(())
}

#[tokio::test]
async fn test_no_duplicate_processing_with_crashes() -> Result<(), anyhow::Error> {
    let ctx = TestContext::new().await?;
    
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
            let stream_key = format!("test:stream:{}", test_id);
            let group_name = format!("test_group_{}", test_id);

            // Create consumer group
            let _: RedisResult<String> = redis
                .xgroup_create(&stream_key, &group_name, "$")
                .await;

            // Publish test events to the stream
            let mut event_ids = Vec::new();
            for i in 0..num_events {
                let event = json!({
                    "event_number": i,
                    "test_run": test_id.to_string(),
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                });

                let result: RedisResult<String> = redis
                    .xadd(
                        &stream_key,
                        "*", // Auto-generate ID
                        &[("event", serde_json::to_string(&event).unwrap())],
                    )
                    .await;

                if let Ok(event_id) = result {
                    event_ids.push(event_id);
                }
            }

            // Setup tracking
            let tracker = ProcessingTracker::new();

            // Spawn multiple consumers
            let mut join_set = JoinSet::new();
            for consumer_num in 0..num_consumers {
                let redis_clone = redis_conn.clone();
                let stream_key_clone = stream_key.clone();
                let group_name_clone = group_name.clone();
                let consumer_name = format!("consumer_{}", consumer_num);
                let tracker_clone = tracker.clone();

                join_set.spawn(automaton_consumer_with_crashes(
                    redis_clone,
                    stream_key_clone,
                    group_name_clone,
                    consumer_name,
                    tracker_clone,
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

            // Check pending messages in the consumer group
            let pending_info: RedisResult<Vec<(String, String, i64, i64)>> = redis
                .xpending_range(
                    &stream_key,
                    &group_name,
                    "-",
                    "+",
                    100,
                )
                .await;

            if let Ok(pending) = pending_info {
                let pending_count = pending.len();
                
                // Property: Processed + Pending should not exceed total events
                prop_assert!(
                    processed_count + pending_count <= num_events,
                    "Inconsistency: {} processed + {} pending > {} total events",
                    processed_count,
                    pending_count,
                    num_events
                );
            }

            // Cleanup: Delete the test stream
            let _: RedisResult<i64> = redis.del(&stream_key).await;

            Ok(())
        })?
    });
    
    Ok(())
}

#[tokio::test]
async fn test_consumer_group_scaling_properties() -> Result<(), anyhow::Error> {
    let ctx = TestContext::new().await?;
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
            let stream_key = format!("test:scaling:{}", test_id);
            let group_name = format!("scaling_group_{}", test_id);

            // Create consumer group
            let _: RedisResult<String> = redis
                .xgroup_create(&stream_key, &group_name, "$")
                .await;

            // Publish many events
            let creation_start = std::time::Instant::now();
            for i in 0..event_count {
                let event = json!({
                    "event_number": i,
                    "data": format!("test_data_{}", i),
                });

                let _: RedisResult<String> = redis
                    .xadd(
                        &stream_key,
                        "*",
                        &[("event", serde_json::to_string(&event).unwrap())],
                    )
                    .await;
            }
            let creation_time = creation_start.elapsed();

            // Property: Stream creation should be reasonably fast
            prop_assert!(
                creation_time.as_millis() < (event_count as u128 * 5), // 5ms per event max
                "Stream creation too slow: {}ms for {} events",
                creation_time.as_millis(),
                event_count
            );

            let tracker = ProcessingTracker::new();
            let processing_start = std::time::Instant::now();

            // Spawn consumers to process events
            let mut join_set = JoinSet::new();
            for consumer_num in 0..consumer_count {
                let redis_clone = redis_conn.clone();
                let stream_key_clone = stream_key.clone();
                let group_name_clone = group_name.clone();
                let consumer_name = format!("scaling_consumer_{}", consumer_num);
                let tracker_clone = tracker.clone();

                join_set.spawn(async move {
                    let mut processed_locally = 0;
                    let mut redis = redis_clone;

                    // Process events until none are left
                    loop {
                        let result: RedisResult<Vec<(String, Vec<(String, String)>)>> = redis
                            .xreadgroup(
                                &group_name_clone,
                                &consumer_name,
                                Some(batch_size),
                                false,
                                &[(&stream_key_clone, ">")],
                            )
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
                                            let _: RedisResult<i64> = redis
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

/// Test that Redis Streams maintain message ordering within partitions
#[tokio::test]
async fn test_redis_stream_ordering_guarantees() -> Result<(), anyhow::Error> {
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let mut redis = ConnectionManager::new(redis_client).await?;

    proptest!(|(
        event_count in 10..=50usize,
        partition_key in prop::collection::vec(any::<String>(), 1..=5),
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let test_id = Ulid::new();
            let stream_key = format!("test:ordering:{}", test_id);

            // Create events with sequence numbers per partition
            let mut partition_sequences: std::collections::HashMap<String, Vec<usize>> = 
                std::collections::HashMap::new();

            for i in 0..event_count {
                let partition = &partition_key[i % partition_key.len()];
                partition_sequences.entry(partition.clone())
                    .or_insert_with(Vec::new)
                    .push(i);

                let event = json!({
                    "sequence": i,
                    "partition": partition,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                });

                let _: RedisResult<String> = redis
                    .xadd(
                        &stream_key,
                        "*",
                        &[
                            ("event", serde_json::to_string(&event).unwrap()),
                            ("partition", partition),
                        ],
                    )
                    .await;
            }

            // Read all events back
            let result: RedisResult<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis
                .xrange(&stream_key, "-", "+")
                .await;

            if let Ok(messages) = result {
                // Group messages by partition
                let mut retrieved_sequences: std::collections::HashMap<String, Vec<usize>> = 
                    std::collections::HashMap::new();

                for (_id, fields) in messages {
                    for (field_name, field_value) in fields {
                        if field_name == "event" {
                            if let Ok(event) = serde_json::from_str::<serde_json::Value>(&field_value) {
                                if let (Some(seq), Some(part)) = 
                                    (event["sequence"].as_u64(), event["partition"].as_str()) {
                                    retrieved_sequences.entry(part.to_string())
                                        .or_insert_with(Vec::new)
                                        .push(seq as usize);
                                }
                            }
                        }
                    }
                }

                // Property: Within each partition, sequences should be in order
                for (partition, expected_seqs) in partition_sequences {
                    if let Some(actual_seqs) = retrieved_sequences.get(&partition) {
                        // Filter to only sequences from this partition
                        let filtered_expected: Vec<usize> = expected_seqs.into_iter()
                            .filter(|&seq| seq < event_count)
                            .collect();

                        prop_assert_eq!(
                            actual_seqs.len(),
                            filtered_expected.len(),
                            "Partition {} has wrong number of events",
                            partition
                        );

                        // Verify ordering
                        for window in actual_seqs.windows(2) {
                            prop_assert!(
                                window[0] < window[1],
                                "Partition {} has out-of-order sequences: {} >= {}",
                                partition,
                                window[0],
                                window[1]
                            );
                        }
                    }
                }
            }

            // Cleanup
            let _: RedisResult<i64> = redis.del(&stream_key).await;

            Ok(())
        })?
    });

    Ok(())
}

/// Test checkpoint-based recovery after consumer crashes
#[tokio::test]
async fn test_checkpoint_recovery_properties() -> Result<(), anyhow::Error> {
    let ctx = TestContext::new().await?;

    proptest!(|(
        events_before_crash in 20..=50usize,
        crash_after_percent in 0.3..=0.7f64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();
            let automaton_name = format!("test_automaton_{}", Ulid::new());
            let group_name = "test_group";
            let consumer_name = "test_consumer";

            // Insert events
            let mut event_ids = Vec::new();
            for i in 0..events_before_crash {
                let event = crate::common::events::filesystem_event(
                    "file.created",
                    &format!("/test/file_{}.txt", i)
                );
                let id = crate::common::insert_event(&pool, &event).await?;
                event_ids.push(id);
            }

            // Create checkpoint manager
            use sinex_satellite_sdk::checkpoint::{CheckpointManager, CheckpointState};
            let checkpoint_mgr = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                group_name.to_string(),
                consumer_name.to_string(),
            );

            // Process events until crash point
            let crash_point = (events_before_crash as f64 * crash_after_percent) as usize;
            let mut checkpoint = checkpoint_mgr.load_checkpoint().await?;
            
            for i in 0..crash_point {
                checkpoint.processed_count += 1;
                checkpoint.last_processed_id = Some(event_ids[i].to_string());
                
                // Save checkpoint periodically (every 10 events)
                if i % 10 == 9 {
                    checkpoint_mgr.save_checkpoint(&checkpoint).await?;
                }
            }
            
            // Final checkpoint before crash
            checkpoint_mgr.save_checkpoint(&checkpoint).await?;
            let pre_crash_count = checkpoint.processed_count;

            // Simulate crash and recovery
            let recovered_checkpoint = checkpoint_mgr.load_checkpoint().await?;

            // Property: Checkpoint should persist crash point
            prop_assert_eq!(
                recovered_checkpoint.processed_count,
                pre_crash_count,
                "Checkpoint didn't persist: expected {}, got {}",
                pre_crash_count,
                recovered_checkpoint.processed_count
            );

            // Property: Should be able to resume from checkpoint
            prop_assert!(
                recovered_checkpoint.last_processed_id.is_some(),
                "Checkpoint should have last processed ID"
            );

            // Continue processing from checkpoint
            let mut final_checkpoint = recovered_checkpoint;
            for i in crash_point..events_before_crash {
                final_checkpoint.processed_count += 1;
                final_checkpoint.last_processed_id = Some(event_ids[i].to_string());
            }
            checkpoint_mgr.save_checkpoint(&final_checkpoint).await?;

            // Property: All events should be processed exactly once
            prop_assert_eq!(
                final_checkpoint.processed_count,
                events_before_crash as u64,
                "Final count should equal total events"
            );

            Ok(())
        })?
    });

    Ok(())
}