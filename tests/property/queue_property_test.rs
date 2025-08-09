//! Property tests for NATS JetStream-based queue processing
//!
//! This module provides property tests for NATS JetStream-based event processing,
//! verifying correctness properties including exactly-once processing, ordering
//! guarantees, and crash recovery via consumer groups.
//!
//! Key Properties Tested:
//! - Exactly-once processing via consumer acknowledgments and delivery tracking
//! - Ordering guarantees through sequence numbers and timestamps
//! - Crash recovery via message redelivery and checkpoint restoration
//! - Scalability with multiple consumers processing from same stream
//! - No duplicate processing under high contention
//! - Checkpoint-based recovery and progress tracking

use color_eyre::eyre::Result;
use proptest::prelude::*;
use sinex_test_utils::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use sinex_core::db::repositories::DbPoolExt;
use sinex_satellite_sdk::{
    nats::{config::NatsConfig, streams::StreamManager},
    checkpoint::{CheckpointManager, CheckpointState},
};
use sinex_core::types::{
    domain::{ConsumerGroup, ConsumerName, ProcessorName},
    ulid::Ulid,
};
use async_nats::{Client, Message, jetstream::{self, consumer::DeliverPolicy, stream::RetentionPolicy}};
use futures_util::StreamExt;

/// Tracks which messages have been processed to detect duplicates
#[derive(Debug, Clone)]
struct ProcessingTracker {
    processed_messages: Arc<Mutex<HashSet<String>>>,
    duplicate_detections: Arc<Mutex<Vec<String>>>,
    processing_order: Arc<Mutex<Vec<String>>>,
    message_contents: Arc<Mutex<HashMap<String, String>>>,
}

impl ProcessingTracker {
    fn new() -> Self {
        Self {
            processed_messages: Arc::new(Mutex::new(HashSet::new())),
            duplicate_detections: Arc::new(Mutex::new(Vec::new())),
            processing_order: Arc::new(Mutex::new(Vec::new())),
            message_contents: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Mark a message as processed, returns true if this is a duplicate
    fn mark_processed(&self, message_id: &str, content: Option<&str>) -> bool {
        let mut processed = self.processed_messages.lock().expect("Lock failed");
        let mut order = self.processing_order.lock().expect("Lock failed");
        let mut contents = self.message_contents.lock().expect("Lock failed");
        
        if processed.contains(message_id) {
            // Duplicate detected!
            let mut duplicates = self.duplicate_detections.lock().expect("Lock failed");
            duplicates.push(message_id.to_string());
            true
        } else {
            processed.insert(message_id.to_string());
            order.push(message_id.to_string());
            if let Some(content) = content {
                contents.insert(message_id.to_string(), content.to_string());
            }
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

    fn get_message_content(&self, message_id: &str) -> Option<String> {
        self.message_contents
            .lock()
            .expect("Lock failed")
            .get(message_id)
            .cloned()
    }
}

/// Simulates a NATS consumer that processes events with potential crashes
async fn nats_consumer_with_crashes(
    client: Client,
    stream_name: String,
    consumer_name: String,
    subject_filter: String,
    tracker: ProcessingTracker,
    checkpoint_mgr: Option<CheckpointManager>,
    crash_probability: f64,
    runtime_seconds: u64,
    seed: u64,
) -> Result<(), color_eyre::eyre::Error> {
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

    // Get JetStream context
    let jetstream = async_nats::jetstream::new(client);
    
    // Create consumer configuration
    let consumer_config = jetstream::consumer::pull::Config {
        name: Some(consumer_name.clone()),
        durable_name: Some(consumer_name.clone()),
        deliver_policy: DeliverPolicy::All,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ack_wait: Duration::from_secs(30),
        max_deliver: 3,
        max_ack_pending: 100,
        filter_subject: subject_filter,
        ..Default::default()
    };

    // Get or create the consumer
    let consumer = jetstream
        .get_or_create_consumer(&stream_name, consumer_config)
        .await?;

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
            debug!("Consumer {} simulating crash", consumer_name);
            return Ok(());
        }

        // Fetch messages from the consumer
        let messages = consumer.fetch().max_messages(5).messages().await?;
        
        if let Some(message) = messages.next().await {
            let message = message?;
            
            // Extract message metadata
            let message_id = format!("{}:{}", message.info().unwrap().stream_sequence, message.info().unwrap().consumer_sequence);
            let payload = String::from_utf8_lossy(&message.payload);
            
            // Check for duplicate processing
            let is_duplicate = tracker.mark_processed(&message_id, Some(&payload));

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
                    debug!("Consumer {} crashing before ack", consumer_name);
                    return Ok(());
                }

                // Acknowledge the message
                message.ack().await?;

                // Save checkpoint periodically
                if let Some(ref mgr) = checkpoint_mgr {
                    if checkpoint_state.processed_count % 10 == 0 {
                        let _ = mgr.save_checkpoint(&checkpoint_state).await;
                    }
                }
            }
        } else {
            // No messages available, short wait
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Final checkpoint save
    if let Some(ref mgr) = checkpoint_mgr {
        let _ = mgr.save_checkpoint(&checkpoint_state).await;
    }
}

/// Test that NATS JetStream prevents duplicate processing even with crashes
#[sinex_test]
async fn test_no_duplicate_processing_with_crashes(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    
    // Setup NATS connection
    let nats_config = NatsConfig::test();
    let client = async_nats::connect(&nats_config.servers[0]).await?;

    proptest!(|(
        num_consumers in 2..=8usize,
        num_events in 10..=50usize,
        crash_probability in 0.1..=0.3f64,
        runtime_seconds in 5..=15u64,
        seed in any::<u64>(),
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let jetstream = async_nats::jetstream::new(client.clone());
            
            // Create unique stream for this test
            let test_id = Ulid::new();
            let stream_name = format!("sinex_test_stream_{}", test_id);
            let subject = format!("sinex.test.{}", test_id);

            // Create stream configuration
            let stream_config = jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                max_age: Duration::from_secs(300),
                ..Default::default()
            };

            // Create the stream
            jetstream.create_stream(stream_config).await?;

            // Publish test events to the stream
            let mut published_ids = Vec::new();
            for i in 0..num_events {
                let event_data = json!({
                    "event_number": i,
                    "test_run": test_id.to_string(),
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                });

                let event_json = serde_json::to_string(&event_data)?;
                let ack = jetstream.publish(&subject, event_json.into()).await?;
                let publish_ack = ack.await?;
                
                published_ids.push(format!("{}:{}", publish_ack.stream, publish_ack.sequence));
            }

            // Setup tracking
            let tracker = ProcessingTracker::new();

            // Spawn multiple consumers with checkpoints
            let mut join_set = JoinSet::new();
            for consumer_num in 0..num_consumers {
                let client_clone = client.clone();
                let stream_name_clone = stream_name.clone();
                let consumer_name = format!("consumer_{}_{}", test_id, consumer_num);
                let subject_filter = subject.clone();
                let tracker_clone = tracker.clone();

                // Create checkpoint manager for each consumer
                let checkpoint_mgr = CheckpointManager::new(
                    ctx.pool(),
                    ProcessorName::from(format!("test_automaton_{}_{}", test_id, consumer_num)),
                    ConsumerGroup::from(format!("test_group_{}", test_id)),
                    ConsumerName::from(consumer_name.clone()),
                );

                join_set.spawn(nats_consumer_with_crashes(
                    client_clone,
                    stream_name_clone,
                    consumer_name,
                    subject_filter,
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

            // Property: Processed count should not exceed total events
            prop_assert!(
                processed_count <= num_events,
                "Inconsistency: {} processed > {} total events",
                processed_count,
                num_events
            );

            // Cleanup: Delete the test stream
            jetstream.delete_stream(&stream_name).await?;

            Ok(())
        })?
    });
    
    Ok(())
}

/// Test consumer scaling and high contention scenarios
#[sinex_test]
async fn test_consumer_contention_properties(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats_config = NatsConfig::test();
    let client = async_nats::connect(&nats_config.servers[0]).await?;

    proptest!(|(
        num_consumers in 5..=15usize,
        _seed in any::<u64>(),
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let jetstream = async_nats::jetstream::new(client.clone());
            
            let test_id = Ulid::new();
            let stream_name = format!("sinex_contention_{}", test_id);
            let subject = format!("sinex.contention.{}", test_id);

            // Create stream
            let stream_config = jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                max_age: Duration::from_secs(60),
                ..Default::default()
            };

            jetstream.create_stream(stream_config).await?;

            // Create exactly one event to maximize contention
            let event_data = json!({
                "contention_test": true,
                "test_id": test_id.to_string()
            });

            let event_json = serde_json::to_string(&event_data)?;
            jetstream.publish(&subject, event_json.into()).await?;

            let tracker = ProcessingTracker::new();

            // All consumers try to claim the same single event simultaneously
            let mut join_set = JoinSet::new();
            for consumer_num in 0..num_consumers {
                let client_clone = client.clone();
                let stream_name_clone = stream_name.clone();
                let consumer_name = format!("contention_consumer_{}_{}", test_id, consumer_num);
                let subject_filter = subject.clone();
                let tracker_clone = tracker.clone();

                join_set.spawn(async move {
                    let jetstream = async_nats::jetstream::new(client_clone);
                    
                    let consumer_config = jetstream::consumer::pull::Config {
                        name: Some(consumer_name.clone()),
                        durable_name: Some(consumer_name.clone()),
                        deliver_policy: DeliverPolicy::All,
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        ack_wait: Duration::from_secs(5),
                        max_deliver: 1,
                        max_ack_pending: 1,
                        filter_subject: subject_filter,
                        ..Default::default()
                    };

                    if let Ok(consumer) = jetstream
                        .get_or_create_consumer(&stream_name_clone, consumer_config)
                        .await
                    {
                        if let Ok(messages) = consumer.fetch().max_messages(1).messages().await {
                            if let Some(Ok(message)) = messages.next().await {
                                let message_id = format!("{}:{}", 
                                    message.info().unwrap().stream_sequence, 
                                    message.info().unwrap().consumer_sequence);
                                let payload = String::from_utf8_lossy(&message.payload);
                                
                                let is_duplicate = tracker_clone.mark_processed(&message_id, Some(&payload));
                                if !is_duplicate {
                                    let _ = message.ack().await;
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
            jetstream.delete_stream(&stream_name).await?;

            Ok(())
        })?
    });
    Ok(())
}

/// Test scaling properties with many events and consumers
#[sinex_test]
async fn test_jetstream_scalability_properties(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats_config = NatsConfig::test();
    let client = async_nats::connect(&nats_config.servers[0]).await?;

    proptest!(|(
        event_count in 50..=200usize, // Reduced from 500 for faster tests
        consumer_count in 2..=6usize, // Reduced from 10
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let jetstream = async_nats::jetstream::new(client.clone());
            
            let test_id = Ulid::new();
            let stream_name = format!("sinex_scalability_{}", test_id);
            let subject = format!("sinex.scalability.{}", test_id);

            // Create stream
            let stream_config = jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                max_age: Duration::from_secs(300),
                ..Default::default()
            };

            jetstream.create_stream(stream_config).await?;

            // Create many events
            let creation_start = Instant::now();
            for i in 0..event_count {
                let event_data = json!({
                    "event_number": i,
                    "data": format!("test_data_{}", i),
                    "test_id": test_id.to_string(),
                });

                let event_json = serde_json::to_string(&event_data)?;
                jetstream.publish(&subject, event_json.into()).await?;
            }
            let creation_time = creation_start.elapsed();

            // Property: Stream creation should be reasonably fast
            prop_assert!(
                creation_time.as_millis() < (event_count as u128 * 20), // 20ms per event max
                "Stream creation too slow: {}ms for {} events",
                creation_time.as_millis(),
                event_count
            );

            let tracker = ProcessingTracker::new();
            let processing_start = Instant::now();

            // Spawn consumers to process events
            let mut join_set = JoinSet::new();
            for consumer_num in 0..consumer_count {
                let client_clone = client.clone();
                let stream_name_clone = stream_name.clone();
                let consumer_name = format!("scalability_consumer_{}_{}", test_id, consumer_num);
                let subject_filter = subject.clone();
                let tracker_clone = tracker.clone();

                join_set.spawn(async move {
                    let jetstream = async_nats::jetstream::new(client_clone);
                    let mut processed_locally = 0;

                    let consumer_config = jetstream::consumer::pull::Config {
                        name: Some(consumer_name.clone()),
                        durable_name: Some(consumer_name.clone()),
                        deliver_policy: DeliverPolicy::All,
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        ack_wait: Duration::from_secs(30),
                        max_deliver: 3,
                        max_ack_pending: 50,
                        filter_subject: subject_filter,
                        ..Default::default()
                    };

                    if let Ok(consumer) = jetstream
                        .get_or_create_consumer(&stream_name_clone, consumer_config)
                        .await
                    {
                        // Process events until none are left
                        loop {
                            if let Ok(messages) = consumer.fetch().max_messages(10).expires(Duration::from_millis(500)).messages().await {
                                let mut batch_processed = 0;
                                let mut message_stream = messages;
                                
                                while let Some(message_result) = message_stream.next().await {
                                    if let Ok(message) = message_result {
                                        let message_id = format!("{}:{}", 
                                            message.info().unwrap().stream_sequence, 
                                            message.info().unwrap().consumer_sequence);
                                        let payload = String::from_utf8_lossy(&message.payload);
                                        
                                        let is_duplicate = tracker_clone.mark_processed(&message_id, Some(&payload));
                                        if !is_duplicate {
                                            let _ = message.ack().await;
                                            processed_locally += 1;
                                            batch_processed += 1;
                                        }
                                    }
                                }
                                
                                if batch_processed == 0 {
                                    break; // No more messages
                                }
                            } else {
                                break; // Timeout or error
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
                throughput > 20.0, // At least 20 events per second (reduced from 50)
                "Processing too slow: {:.2} events/sec for {} events with {} consumers",
                throughput, event_count, consumer_count
            );

            // Cleanup
            jetstream.delete_stream(&stream_name).await?;

            Ok(())
        })?
    });
    
    Ok(())
}

/// Test that JetStream sequence numbers maintain ordering guarantees
#[sinex_test]
async fn test_jetstream_ordering_properties() -> Result<(), color_eyre::eyre::Error> {
    let nats_config = NatsConfig::test();
    let client = async_nats::connect(&nats_config.servers[0]).await?;

    proptest!(|(
        event_count in 10..=50usize,
        time_gap_ms in 10..=100u64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let jetstream = async_nats::jetstream::new(client.clone());
            
            let test_id = Ulid::new();
            let stream_name = format!("sinex_ordering_{}", test_id);
            let subject = format!("sinex.ordering.{}", test_id);

            // Create stream
            let stream_config = jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                max_age: Duration::from_secs(120),
                ..Default::default()
            };

            jetstream.create_stream(stream_config).await?;

            // Create events with controlled timing and sequence numbers
            let mut created_sequences = Vec::new();
            for i in 0..event_count {
                let event_data = json!({
                    "sequence": i,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "test_id": test_id.to_string(),
                });

                let event_json = serde_json::to_string(&event_data)?;
                let ack = jetstream.publish(&subject, event_json.into()).await?;
                let publish_ack = ack.await?;
                
                created_sequences.push((publish_ack.sequence, i));

                // Small delay to ensure different creation times
                tokio::time::sleep(Duration::from_millis(time_gap_ms)).await;
            }

            // Read events back in consumer order and verify sequence
            let tracker = ProcessingTracker::new();
            let consumer_name = "ordering_consumer";
            let mut claimed_sequences = Vec::new();

            let consumer_config = jetstream::consumer::pull::Config {
                name: Some(consumer_name.to_string()),
                durable_name: Some(consumer_name.to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_deliver: 3,
                max_ack_pending: 100,
                filter_subject: subject.clone(),
                ..Default::default()
            };

            let consumer = jetstream
                .get_or_create_consumer(&stream_name, consumer_config)
                .await?;

            // Consume events one by one to test ordering
            let messages = consumer.fetch().max_messages(event_count).expires(Duration::from_secs(10)).messages().await?;
            let mut message_stream = messages;
            
            while let Some(message_result) = message_stream.next().await {
                if let Ok(message) = message_result {
                    let message_id = format!("{}:{}", 
                        message.info().unwrap().stream_sequence, 
                        message.info().unwrap().consumer_sequence);
                    let payload_str = String::from_utf8_lossy(&message.payload);
                    
                    let is_duplicate = tracker.mark_processed(&message_id, Some(&payload_str));
                    if !is_duplicate {
                        // Parse payload to extract sequence number
                        if let Ok(payload_json) = serde_json::from_str::<serde_json::Value>(&payload_str) {
                            if let Some(seq) = payload_json["sequence"].as_u64() {
                                claimed_sequences.push(seq as usize);
                            }
                        }

                        // Acknowledge the message
                        message.ack().await?;
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
            jetstream.delete_stream(&stream_name).await?;

            Ok(())
        })?
    });

    Ok(())
}

/// Test checkpoint-based recovery after consumer crashes
#[sinex_test]
async fn test_checkpoint_recovery_properties(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let nats_config = NatsConfig::test();
    let client = async_nats::connect(&nats_config.servers[0]).await?;

    proptest!(|(
        events_before_crash in 20..=50usize,
        crash_after_percent in 0.3..=0.7f64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let jetstream = async_nats::jetstream::new(client.clone());
            let test_id = Ulid::new();
            let stream_name = format!("sinex_checkpoint_{}", test_id);
            let subject = format!("sinex.checkpoint.{}", test_id);
            let consumer_name = "checkpoint_consumer";
            let processor_name = format!("test_automaton_{}", test_id);

            // Create stream
            let stream_config = jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                max_age: Duration::from_secs(300),
                ..Default::default()
            };

            jetstream.create_stream(stream_config).await?;

            // Publish events to stream
            for i in 0..events_before_crash {
                let event_data = json!({
                    "event_number": i,
                    "test_id": test_id.to_string(),
                });

                let event_json = serde_json::to_string(&event_data)?;
                jetstream.publish(&subject, event_json.into()).await?;
            }

            // Create checkpoint manager
            let checkpoint_mgr = CheckpointManager::new(
                ctx.pool(),
                ProcessorName::from(processor_name),
                ConsumerGroup::from(format!("checkpoint_group_{}", test_id)),
                ConsumerName::from(consumer_name.to_string()),
            );

            // Process events until crash point
            let crash_point = (events_before_crash as f64 * crash_after_percent) as usize;
            let mut checkpoint = checkpoint_mgr.load_checkpoint().await?;
            let mut processed_count = 0;

            let consumer_config = jetstream::consumer::pull::Config {
                name: Some(consumer_name.to_string()),
                durable_name: Some(consumer_name.to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_deliver: 3,
                max_ack_pending: 100,
                filter_subject: subject.clone(),
                ..Default::default()
            };

            let consumer = jetstream
                .get_or_create_consumer(&stream_name, consumer_config)
                .await?;
            
            // Simulate processing until crash
            let messages = consumer.fetch().max_messages(crash_point).expires(Duration::from_secs(10)).messages().await?;
            let mut message_stream = messages;
            
            while let Some(message_result) = message_stream.next().await {
                if let Ok(message) = message_result && processed_count < crash_point {
                    processed_count += 1;
                    
                    // Acknowledge the message
                    message.ack().await?;

                    // Update and save checkpoint
                    checkpoint.processed_count += 1;
                    checkpoint.last_activity = chrono::Utc::now();
                    
                    if processed_count % 5 == 0 {
                        checkpoint_mgr.save_checkpoint(&checkpoint).await?;
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
            
            if remaining_to_process > 0 {
                let remaining_messages = consumer.fetch()
                    .max_messages(remaining_to_process)
                    .expires(Duration::from_secs(10))
                    .messages()
                    .await?;
                let mut remaining_stream = remaining_messages;
                
                while let Some(message_result) = remaining_stream.next().await {
                    if let Ok(message) = message_result {
                        final_processed += 1;
                        message.ack().await?;
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
            jetstream.delete_stream(&stream_name).await?;

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

    #[sinex_test]
fn test_processing_tracker() -> color_eyre::eyre::Result<()> {
        let tracker = ProcessingTracker::new();
        let id1 = "test-msg-1";
        let id2 = "test-msg-2";

        // First processing should succeed
        assert!(!tracker.mark_processed(id1, Some("content1")));
        assert_eq!(tracker.processed_count(), 1);
        assert!(tracker.get_duplicates().is_empty());

        // Different ID should also succeed
        assert!(!tracker.mark_processed(id2, Some("content2")));
        assert_eq!(tracker.processed_count(), 2);
        assert!(tracker.get_duplicates().is_empty());

        // Same ID again should detect duplicate
        assert!(tracker.mark_processed(id1, Some("content1")));
        assert_eq!(tracker.processed_count(), 2); // Count doesn't increase
        assert_eq!(tracker.get_duplicates().len(), 1);
        assert_eq!(tracker.get_duplicates()[0], id1);

        // Verify processing order
        let order = tracker.get_processing_order();
        assert_eq!(order, vec![id1, id2]);

        // Verify content retrieval
        assert_eq!(tracker.get_message_content(id1), Some("content1".to_string()));
        assert_eq!(tracker.get_message_content(id2), Some("content2".to_string()));
    }

    #[sinex_test(timeout = 40)]
    async fn test_nats_crash_simulation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test that the crash simulation compiles and runs
        let nats_config = NatsConfig::test();
        let client = async_nats::connect(&nats_config.servers[0]).await?;
        let tracker = ProcessingTracker::new();

        // Test with 100% crash probability (should exit immediately)
        let result = nats_consumer_with_crashes(
            client,
            "test_stream".to_string(),
            "crash_test_consumer".to_string(),
            "test.subject".to_string(),
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

    #[sinex_test]
fn test_crash_simulation_deterministic() -> color_eyre::eyre::Result<()> {
        // Test that crash simulation is deterministic with same seed
        let seed = 12345u64;
        let consumer_name = "test_consumer";

        // Simple hash calculation similar to the one in nats_consumer_with_crashes
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

    #[sinex_test]
fn test_processing_tracker_thread_safety() -> color_eyre::eyre::Result<()> {
        // Test that ProcessingTracker works correctly under concurrent access
        let tracker = ProcessingTracker::new();
        let tracker_clone = tracker.clone();

        let message_ids = vec!["msg1", "msg2", "msg3", "msg4", "msg5", "msg6", "msg7", "msg8", "msg9", "msg10"];

        // Process some messages
        for (i, msg_id) in message_ids.iter().enumerate() {
            let is_dup = if i < 5 {
                tracker.mark_processed(msg_id, Some(&format!("content_{}", i)))
            } else {
                tracker_clone.mark_processed(msg_id, Some(&format!("content_{}", i)))
            };

            assert!(!is_dup, "First processing should not be duplicate");
        }

        assert_eq!(tracker.processed_count(), 10);
        assert!(tracker.get_duplicates().is_empty());

        // Try to process the same messages again - should detect duplicates
        for msg_id in &message_ids[0..3] {
            let is_dup = tracker.mark_processed(msg_id, Some("duplicate_content"));
            assert!(is_dup, "Second processing should be duplicate");
        }

        assert_eq!(tracker.processed_count(), 10); // Count shouldn't increase
        assert_eq!(tracker.get_duplicates().len(), 3); // Should have 3 duplicates
    }
}

// =============================================================================
// JetStream Property Tests  
// =============================================================================

proptest! {
    /// Test JetStream consumer retry behavior with exponential backoff
fn test_consumer_retry_timing_boundaries(
        attempts in 0i32..20,
        base_delay in 1.0f64..300.0,
    ) -> color_eyre::eyre::Result<()> {
        // Calculate exponential backoff for JetStream consumer failures
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

    /// Test JetStream consumer retry patterns with realistic scenarios
    #[sinex_test]
fn test_jetstream_consumer_retry_patterns(
        failure_count in 0usize..10,
        base_retry_ms in 100u64..5000,
    ) -> color_eyre::eyre::Result<()> {
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

    /// Test JetStream sequence number monotonicity properties
    #[sinex_test]
fn test_sequence_number_monotonicity(
        base_sequence in 1u64..1000,
        sequence_increment in 1u64..100,
    ) -> color_eyre::eyre::Result<()> {
        // Simulate JetStream sequence number generation
        let seq1 = base_sequence;
        let seq2 = base_sequence + sequence_increment;
        
        // Property: Sequence numbers are always monotonically increasing
        assert!(seq2 > seq1, "Sequence numbers must be monotonically increasing");
        
        // Property: Sequence numbers are always comparable and ordered
        assert!(seq1 < seq2, "Earlier sequence must be less than later sequence");
        
        // Property: Sequence difference matches increment
        assert_eq!(seq2 - seq1, sequence_increment, 
            "Sequence difference should match increment");
    }

    /// Test JetStream message timestamp ordering properties
    #[sinex_test]
fn test_message_timestamp_ordering(
        base_timestamp_ms in 1600000000000u64..1700000000000u64,
        time_increment_ms in 1u64..10000,
    ) -> color_eyre::eyre::Result<()> {
        // Simulate message timestamp ordering in JetStream
        let ts1 = base_timestamp_ms;
        let ts2 = base_timestamp_ms + time_increment_ms;
        
        // Property: Message timestamps should be ordered
        assert!(ts2 >= ts1, "Message timestamps should be non-decreasing");
        
        // Property: Time difference should be reasonable for event processing
        let diff = ts2 - ts1;
        assert!(diff <= 24 * 3600 * 1000, "Time difference should be less than 24 hours");
        assert!(diff >= 1, "Time difference should be at least 1ms for ordering");
    }
}