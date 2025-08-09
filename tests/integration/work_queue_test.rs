//! Work Queue Integration Tests
//!
//! This module tests work queue functionality including event processing pipelines,
//! work item claiming, processing, and completion patterns. These tests focus on
//! integration scenarios where events flow through the NATS-based work distribution
//! system and are processed by multiple workers.
//!
//! Key scenarios tested:
//! - Event-to-work-item conversion and processing  
//! - Multi-worker concurrent processing
//! - Work item claiming and completion patterns
//! - Queue cleanup and cascading deletions
//! - Worker coordination and load balancing

use color_eyre::eyre::Result;
use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy},
    Context,
};
use futures::StreamExt;
use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
use sinex_test_utils::prelude::*;
use sinex_core::types::{
    domain::{ConsumerGroup, ConsumerName, ProcessorName},
    ulid::Ulid,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{debug, info};

/// Tracks work item processing across multiple workers
#[derive(Debug, Clone)]
struct WorkTracker {
    processed_items: Arc<Mutex<HashSet<String>>>,
    work_assignments: Arc<Mutex<HashMap<String, String>>>, // item_id -> worker_id
    completion_times: Arc<Mutex<HashMap<String, Instant>>>,
}

impl WorkTracker {
    fn new() -> Self {
        Self {
            processed_items: Arc::new(Mutex::new(HashSet::new())),
            work_assignments: Arc::new(Mutex::new(HashMap::new())),
            completion_times: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn claim_work_item(&self, item_id: &str, worker_id: &str) -> bool {
        let mut assignments = self.work_assignments.lock().expect("Lock failed");
        
        if assignments.contains_key(item_id) {
            false // Already claimed
        } else {
            assignments.insert(item_id.to_string(), worker_id.to_string());
            true
        }
    }

    fn complete_work_item(&self, item_id: &str) -> bool {
        let mut processed = self.processed_items.lock().expect("Lock failed");
        let mut completions = self.completion_times.lock().expect("Lock failed");
        
        if processed.contains(item_id) {
            false // Already completed
        } else {
            processed.insert(item_id.to_string());
            completions.insert(item_id.to_string(), Instant::now());
            true
        }
    }

    fn get_processed_count(&self) -> usize {
        self.processed_items.lock().expect("Lock failed").len()
    }

    fn get_worker_assignments(&self) -> HashMap<String, String> {
        self.work_assignments.lock().expect("Lock failed").clone()
    }

    fn get_assigned_worker(&self, item_id: &str) -> Option<String> {
        self.work_assignments
            .lock()
            .expect("Lock failed")
            .get(item_id)
            .cloned()
    }
}

/// Simulates a worker that processes work items from NATS stream
async fn simulate_work_processor(
    client: async_nats::Client,
    stream_name: String,
    consumer_name: String,
    subject_filter: String,
    worker_id: String,
    tracker: WorkTracker,
    max_items: usize,
    processing_delay: Duration,
) -> Result<usize, color_eyre::eyre::Error> {
    let jetstream = async_nats::jetstream::new(client);
    
    let consumer_config = ConsumerConfig {
        name: Some(consumer_name.clone()),
        durable_name: Some(consumer_name.clone()),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        ack_wait: Duration::from_secs(30),
        max_deliver: 3,
        max_ack_pending: 10,
        filter_subject: subject_filter,
        ..Default::default()
    };

    let consumer = jetstream
        .get_or_create_consumer(&stream_name, consumer_config)
        .await?;

    let mut processed_count = 0;
    let start_time = Instant::now();
    let timeout_duration = Duration::from_secs(30);

    while processed_count < max_items && start_time.elapsed() < timeout_duration {
        // Fetch work items
        match consumer.fetch().max_messages(1).expires(Duration::from_secs(1)).messages().await {
            Ok(messages) => {
                let mut messages = messages;
                if let Some(message) = messages.next().await {
                    let message = message?;
                    
                    // Extract work item metadata
                    let message_info = message.info()?;
                    let work_item_id = format!("work_{}_{}", message_info.stream_sequence, message_info.consumer_sequence);
                    
                    // Try to claim the work item
                    if tracker.claim_work_item(&work_item_id, &worker_id) {
                        debug!("Worker {} claimed work item {}", worker_id, work_item_id);
                        
                        // Parse work item payload
                        let payload: serde_json::Value = serde_json::from_slice(&message.payload)?;
                        
                        // Simulate processing work
                        tokio::time::sleep(processing_delay).await;
                        
                        // Mark work item as completed
                        if tracker.complete_work_item(&work_item_id) {
                            processed_count += 1;
                            debug!("Worker {} completed work item {}", worker_id, work_item_id);
                        }
                        
                        // Acknowledge the message
                        message.ack().await?;
                    } else {
                        // Work item already claimed by another worker, nack it
                        message.nak(None).await?;
                    }
                }
            }
            Err(_) => {
                // Timeout or no messages, continue
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    Ok(processed_count)
}

/// Test basic work queue event processing pipeline
#[sinex_test]
async fn test_work_queue_event_processing_pipeline(ctx: TestContext) -> Result<(), color_eyre::eyre::Error> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client.clone());
    
    let test_id = Ulid::new();
    let stream_name = format!("sinex_work_queue_{}", test_id);
    let subject = format!("work.events.{}", test_id);
    
    // Create work queue stream
    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: RetentionPolicy::WorkQueue,
        max_age: Duration::from_secs(300),
        ..Default::default()
    };
    
    jetstream.create_stream(stream_config).await?;
    
    // Create work items representing events to be processed
    let work_items = vec![
        json!({
            "event_id": Ulid::new().to_string(),
            "event_type": "filesystem.file_created",
            "priority": 1,
            "processor_target": "file-processor",
            "payload": {
                "path": "/tmp/test1.txt",
                "size": 1024
            }
        }),
        json!({
            "event_id": Ulid::new().to_string(),
            "event_type": "terminal.command_executed", 
            "priority": 2,
            "processor_target": "terminal-processor",
            "payload": {
                "command": "ls -la",
                "exit_code": 0
            }
        }),
        json!({
            "event_id": Ulid::new().to_string(),
            "event_type": "system.service_status_changed",
            "priority": 1,
            "processor_target": "system-processor", 
            "payload": {
                "service": "postgresql",
                "status": "active"
            }
        }),
    ];
    
    // Publish work items to the stream
    for (i, work_item) in work_items.iter().enumerate() {
        let work_json = serde_json::to_string(work_item)?;
        let ack = jetstream.publish(&subject, work_json.into()).await?;
        ack.await?;
        debug!("Published work item {}: {}", i + 1, work_item["event_type"]);
    }
    
    // Create work tracker
    let tracker = WorkTracker::new();
    
    // Start multiple workers to process work items concurrently
    let mut join_set = JoinSet::new();
    let num_workers = 2;
    
    for worker_num in 0..num_workers {
        let client_clone = client.clone();
        let stream_name_clone = stream_name.clone();
        let consumer_name = format!("work_consumer_{}_{}", test_id, worker_num);
        let subject_clone = subject.clone();
        let worker_id = format!("worker_{}", worker_num);
        let tracker_clone = tracker.clone();
        
        join_set.spawn(simulate_work_processor(
            client_clone,
            stream_name_clone,
            consumer_name,
            subject_clone,
            worker_id,
            tracker_clone,
            work_items.len(), // Each worker can process up to all items
            Duration::from_millis(50), // Processing delay
        ));
    }
    
    // Wait for all workers to complete
    let mut total_processed = 0;
    while let Some(result) = join_set.join_next().await {
        let worker_processed = result??;
        total_processed += worker_processed;
        debug!("Worker completed, processed {} items", worker_processed);
    }
    
    // Verify work processing results
    let final_processed = tracker.get_processed_count();
    assert_eq!(final_processed, work_items.len(), 
        "All work items should be processed exactly once");
    
    // Verify work distribution
    let assignments = tracker.get_worker_assignments();
    assert_eq!(assignments.len(), work_items.len(),
        "Each work item should be assigned to exactly one worker");
    
    // Cleanup
    jetstream.delete_stream(&stream_name).await?;
    
    info!("✅ Work queue event processing pipeline test completed successfully");
    Ok(())
}

/// Test multi-worker concurrent work claiming and processing
#[sinex_test]
async fn test_concurrent_work_claiming_patterns(ctx: TestContext) -> Result<(), color_eyre::eyre::Error> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client.clone());
    
    let test_id = Ulid::new();
    let stream_name = format!("sinex_concurrent_{}", test_id);
    let subject = format!("work.concurrent.{}", test_id);
    
    // Create stream
    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: RetentionPolicy::WorkQueue,
        max_age: Duration::from_secs(180),
        ..Default::default()
    };
    
    jetstream.create_stream(stream_config).await?;
    
    // Create many work items to test concurrent claiming
    let num_work_items = 20;
    for i in 0..num_work_items {
        let work_item = json!({
            "work_id": format!("concurrent_work_{}", i),
            "event_type": "test.concurrent_processing",
            "priority": (i % 3) + 1,
            "payload": {
                "data": format!("work_data_{}", i)
            }
        });
        
        let work_json = serde_json::to_string(&work_item)?;
        jetstream.publish(&subject, work_json.into()).await?;
    }
    
    // Create work tracker
    let tracker = WorkTracker::new();
    
    // Start many workers competing for work items
    let mut join_set = JoinSet::new();
    let num_workers = 5;
    
    for worker_num in 0..num_workers {
        let client_clone = client.clone();
        let stream_name_clone = stream_name.clone();
        let consumer_name = format!("concurrent_consumer_{}_{}", test_id, worker_num);
        let subject_clone = subject.clone();
        let worker_id = format!("concurrent_worker_{}", worker_num);
        let tracker_clone = tracker.clone();
        
        join_set.spawn(simulate_work_processor(
            client_clone,
            stream_name_clone,
            consumer_name,
            subject_clone,
            worker_id,
            tracker_clone,
            num_work_items, // Workers compete for all items
            Duration::from_millis(20), // Fast processing
        ));
    }
    
    // Wait for all workers
    while let Some(result) = join_set.join_next().await {
        result??;
    }
    
    // Verify concurrent processing results
    let processed_count = tracker.get_processed_count();
    assert_eq!(processed_count, num_work_items,
        "All {} work items should be processed exactly once", num_work_items);
    
    let assignments = tracker.get_worker_assignments();
    assert_eq!(assignments.len(), num_work_items,
        "Each work item should be assigned to exactly one worker");
    
    // Verify work distribution across workers
    let mut worker_counts: HashMap<String, usize> = HashMap::new();
    for worker_id in assignments.values() {
        *worker_counts.entry(worker_id.clone()).or_insert(0) += 1;
    }
    
    // Each worker should have processed at least some work
    assert!(worker_counts.len() > 1, 
        "Work should be distributed across multiple workers");
    
    let total_work: usize = worker_counts.values().sum();
    assert_eq!(total_work, num_work_items,
        "Total work across all workers should equal total work items");
    
    // Cleanup
    jetstream.delete_stream(&stream_name).await?;
    
    info!("✅ Concurrent work claiming patterns test completed successfully");
    Ok(())
}

/// Test work queue cleanup and completion tracking
#[sinex_test]
async fn test_work_queue_cleanup_and_tracking(ctx: TestContext) -> Result<(), color_eyre::eyre::Error> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client.clone());
    
    let test_id = Ulid::new();
    let stream_name = format!("sinex_cleanup_{}", test_id);
    let subject = format!("work.cleanup.{}", test_id);
    
    // Create stream
    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: RetentionPolicy::WorkQueue,
        max_age: Duration::from_secs(120),
        ..Default::default()
    };
    
    jetstream.create_stream(stream_config).await?;
    
    // Create work items that need cleanup tracking
    let processor_name = format!("cleanup_processor_{}", test_id);
    let work_items = vec![
        json!({
            "processor_name": processor_name,
            "event_type": "cleanup.file_processing",
            "action": "delete_temp_files",
            "payload": {
                "temp_dir": "/tmp/sinex_test",
                "pattern": "*.tmp"
            }
        }),
        json!({
            "processor_name": processor_name,
            "event_type": "cleanup.cache_cleanup", 
            "action": "clear_expired_cache",
            "payload": {
                "cache_name": "event_cache",
                "max_age_hours": 24
            }
        }),
        json!({
            "processor_name": processor_name,
            "event_type": "cleanup.log_rotation",
            "action": "rotate_logs",
            "payload": {
                "log_dir": "/var/log/sinex",
                "keep_days": 7
            }
        }),
    ];
    
    // Publish work items
    for work_item in &work_items {
        let work_json = serde_json::to_string(work_item)?;
        jetstream.publish(&subject, work_json.into()).await?;
    }
    
    // Create a single worker for cleanup processing
    let tracker = WorkTracker::new();
    let cleanup_worker = simulate_work_processor(
        client.clone(),
        stream_name.clone(),
        format!("cleanup_consumer_{}", test_id),
        subject.clone(),
        "cleanup_worker".to_string(),
        tracker.clone(),
        work_items.len(),
        Duration::from_millis(30),
    );
    
    // Process cleanup work
    let processed_count = cleanup_worker.await?;
    
    // Verify all cleanup work was processed
    assert_eq!(processed_count, work_items.len(),
        "All cleanup work items should be processed");
    
    let final_processed = tracker.get_processed_count();
    assert_eq!(final_processed, work_items.len(),
        "Tracker should show all items completed");
    
    // Verify no remaining work in the queue
    let remaining_consumer = jetstream
        .create_consumer_on_stream(
            ConsumerConfig {
                durable_name: Some(format!("remaining_check_{}", test_id)),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            },
            &stream_name,
        )
        .await?;
    
    // Try to fetch any remaining messages (should be none)
    let remaining_messages = remaining_consumer.fetch()
        .max_messages(10)
        .expires(Duration::from_secs(2))
        .messages()
        .await;
    
    match remaining_messages {
        Ok(messages) => {
            let mut message_stream = messages;
            let mut remaining_count = 0;
            
            while let Some(message) = message_stream.next().await {
                remaining_count += 1;
                if let Ok(msg) = message {
                    msg.ack().await?;
                }
            }
            
            assert_eq!(remaining_count, 0, 
                "No work should remain in queue after processing");
        }
        Err(_) => {
            // No messages available - this is expected
        }
    }
    
    // Cleanup
    jetstream.delete_stream(&stream_name).await?;
    
    info!("✅ Work queue cleanup and tracking test completed successfully");
    Ok(())
}

/// Test work item priority and processor targeting
#[sinex_test]
async fn test_work_priority_and_targeting(ctx: TestContext) -> Result<(), color_eyre::eyre::Error> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client.clone());
    
    let test_id = Ulid::new();
    let stream_name = format!("sinex_priority_{}", test_id);
    let subject_base = format!("work.priority.{}", test_id);
    
    // Create streams for different priority levels
    let priority_streams = vec![
        (format!("{}.high", subject_base), 1),
        (format!("{}.medium", subject_base), 2), 
        (format!("{}.low", subject_base), 3),
    ];
    
    for (subject, _priority) in &priority_streams {
        let stream_config = StreamConfig {
            name: format!("{}_{}", stream_name, subject.split('.').last().unwrap()),
            subjects: vec![subject.clone()],
            retention: RetentionPolicy::WorkQueue,
            max_age: Duration::from_secs(180),
            ..Default::default()
        };
        
        jetstream.create_stream(stream_config).await?;
    }
    
    // Create prioritized work items
    let high_priority_work = vec![
        json!({
            "priority": 1,
            "target_processor": "critical_processor",
            "event_type": "system.critical_alert",
            "payload": { "alert": "Database connection lost" }
        }),
        json!({
            "priority": 1, 
            "target_processor": "critical_processor",
            "event_type": "security.intrusion_detected",
            "payload": { "source_ip": "192.168.1.100" }
        }),
    ];
    
    let medium_priority_work = vec![
        json!({
            "priority": 2,
            "target_processor": "standard_processor", 
            "event_type": "application.user_activity",
            "payload": { "user": "test_user", "action": "login" }
        }),
        json!({
            "priority": 2,
            "target_processor": "standard_processor",
            "event_type": "system.resource_warning",
            "payload": { "resource": "memory", "usage": 85 }
        }),
    ];
    
    let low_priority_work = vec![
        json!({
            "priority": 3,
            "target_processor": "background_processor",
            "event_type": "maintenance.cleanup",
            "payload": { "task": "temp_file_cleanup" }
        }),
        json!({
            "priority": 3,
            "target_processor": "background_processor", 
            "event_type": "analytics.daily_report",
            "payload": { "report_date": "2024-01-15" }
        }),
    ];
    
    // Publish work items to appropriate priority streams
    for work_item in &high_priority_work {
        let work_json = serde_json::to_string(work_item)?;
        jetstream.publish(&format!("{}.high", subject_base), work_json.into()).await?;
    }
    
    for work_item in &medium_priority_work {
        let work_json = serde_json::to_string(work_item)?;
        jetstream.publish(&format!("{}.medium", subject_base), work_json.into()).await?;
    }
    
    for work_item in &low_priority_work {
        let work_json = serde_json::to_string(work_item)?;
        jetstream.publish(&format!("{}.low", subject_base), work_json.into()).await?;
    }
    
    // Create targeted workers for each priority level
    let mut join_set = JoinSet::new();
    let tracker = WorkTracker::new();
    
    for (subject, priority) in &priority_streams {
        let client_clone = client.clone();
        let stream_name_clone = format!("{}_{}", stream_name, subject.split('.').last().unwrap());
        let consumer_name = format!("priority_consumer_{}_{}", test_id, priority);
        let subject_clone = subject.clone();
        let worker_id = format!("priority_worker_{}", priority);
        let tracker_clone = tracker.clone();
        
        join_set.spawn(simulate_work_processor(
            client_clone,
            stream_name_clone,
            consumer_name,
            subject_clone,
            worker_id,
            tracker_clone,
            10, // Max items per worker
            Duration::from_millis(25),
        ));
    }
    
    // Wait for all workers
    while let Some(result) = join_set.join_next().await {
        result??;
    }
    
    // Verify processing results
    let total_work_items = high_priority_work.len() + medium_priority_work.len() + low_priority_work.len();
    let processed_count = tracker.get_processed_count();
    
    assert_eq!(processed_count, total_work_items,
        "All work items across all priorities should be processed");
    
    let assignments = tracker.get_worker_assignments();
    assert_eq!(assignments.len(), total_work_items,
        "All work items should be assigned to workers");
    
    // Verify priority-based worker assignment
    let worker_assignments: Vec<String> = assignments.values().cloned().collect();
    assert!(worker_assignments.contains(&"priority_worker_1".to_string()),
        "High priority work should be processed by priority worker 1");
    assert!(worker_assignments.contains(&"priority_worker_2".to_string()),
        "Medium priority work should be processed by priority worker 2");
    assert!(worker_assignments.contains(&"priority_worker_3".to_string()),
        "Low priority work should be processed by priority worker 3");
    
    // Cleanup streams
    for (subject, _) in &priority_streams {
        let stream_name_to_delete = format!("{}_{}", stream_name, subject.split('.').last().unwrap());
        jetstream.delete_stream(&stream_name_to_delete).await?;
    }
    
    info!("✅ Work priority and targeting test completed successfully");
    Ok(())
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[sinex_test]
fn test_work_tracker_basic_operations() -> color_eyre::eyre::Result<()> {
        let tracker = WorkTracker::new();
        
        // Test work item claiming
        assert!(tracker.claim_work_item("item1", "worker1"));
        assert!(!tracker.claim_work_item("item1", "worker2")); // Already claimed
        assert!(tracker.claim_work_item("item2", "worker2"));
        
        // Test work item completion
        assert!(tracker.complete_work_item("item1"));
        assert!(!tracker.complete_work_item("item1")); // Already completed
        assert_eq!(tracker.get_processed_count(), 1);
        
        // Test worker assignment tracking
        let assignments = tracker.get_worker_assignments();
        assert_eq!(assignments.get("item1"), Some(&"worker1".to_string()));
        assert_eq!(assignments.get("item2"), Some(&"worker2".to_string()));
        
        assert_eq!(tracker.get_assigned_worker("item1"), Some("worker1".to_string()));
        assert_eq!(tracker.get_assigned_worker("item2"), Some("worker2".to_string()));
        assert_eq!(tracker.get_assigned_worker("nonexistent"), None);
    }
    
    #[sinex_test]
fn test_work_tracker_concurrent_claiming() -> color_eyre::eyre::Result<()> {
        let tracker = WorkTracker::new();
        let tracker_clone = tracker.clone();
        
        // Simulate concurrent claiming
        assert!(tracker.claim_work_item("concurrent_item", "worker_a"));
        assert!(!tracker_clone.claim_work_item("concurrent_item", "worker_b"));
        
        // Complete from either reference
        assert!(tracker_clone.complete_work_item("concurrent_item"));
        assert_eq!(tracker.get_processed_count(), 1);
    }
    
    #[sinex_test]
fn test_work_tracker_multiple_workers() -> color_eyre::eyre::Result<()> {
        let tracker = WorkTracker::new();
        
        let work_items = vec!["work1", "work2", "work3", "work4", "work5"];
        let workers = vec!["worker_a", "worker_b", "worker_c"];
        
        // Assign work items to workers
        for (i, item) in work_items.iter().enumerate() {
            let worker = workers[i % workers.len()];
            assert!(tracker.claim_work_item(item, worker));
        }
        
        // Complete all work items
        for item in &work_items {
            assert!(tracker.complete_work_item(item));
        }
        
        assert_eq!(tracker.get_processed_count(), work_items.len());
        
        // Verify worker distribution
        let assignments = tracker.get_worker_assignments();
        let mut worker_counts: HashMap<String, usize> = HashMap::new();
        
        for worker in assignments.values() {
            *worker_counts.entry(worker.clone()).or_insert(0) += 1;
        }
        
        // Should have work distributed across all workers
        assert_eq!(worker_counts.len(), workers.len());
        let total_assigned: usize = worker_counts.values().sum();
        assert_eq!(total_assigned, work_items.len());
    }
    Ok(())
}