//! Comprehensive integration tests for Automaton worker claim/process/complete cycle
//!
//! This test suite validates the complete worker lifecycle including:
//! - Work queue item claiming with SELECT FOR UPDATE SKIP LOCKED
//! - Event processing through the worker pipeline
//! - Work item completion and status updates
//! - Error handling and retry logic
//! - Worker idempotency and concurrency safety
//! - Dead letter queue (DLQ) integration

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::json;
use sinex_automaton::{create_work_entries, get_active_manifests, EventScanner, ScannerConfig, WorkRouter};
use sinex_db::{
    create_pool, models::{AgentManifest, WorkQueueItem}, work_queue::{
        add_to_work_queue, claim_work_queue_items, complete_work_queue_item, 
        fail_work_queue_item, get_work_item_by_id, get_dlq_items, insert_dlq_event, DlqEventParams
    }, 
    events::insert_event_with_validator,
    agent::upsert_agent_manifest, AgentManifestParams, DbPool, JsonValue, RawEvent
};
use sinex_ulid::Ulid;
use sinex_worker::EventProcessor;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::Duration as StdDuration;
use tokio::{time::sleep, sync::Barrier};
use futures::future::join_all;

// Test result type
type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Helper to create database pool for testing
async fn setup_test_database() -> Result<DbPool> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
    create_pool(&database_url).await
}

/// Helper to create a test raw event
fn create_test_event(source: &str, event_type: &str, payload: JsonValue) -> RawEvent {
    RawEvent {
        id: Ulid::new(),
        source: source.to_string(),
        event_type: event_type.to_string(),
        ts_ingest: Utc::now(),
        ts_orig: None,
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: None,
        payload,
    }
}

/// Helper to create a test agent manifest
fn create_test_manifest(
    agent_name: &str, 
    status: &str, 
    subscriptions: JsonValue
) -> AgentManifest {
    AgentManifest {
        agent_name: agent_name.to_string(),
        description: Some("Test agent".to_string()),
        version: "1.0.0".to_string(),
        status: status.to_string(),
        agent_type: "processor".to_string(),
        config_template_json: None,
        produces_event_types: None,
        subscribes_to_event_types: Some(subscriptions),
        required_capabilities: None,
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: None,
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

/// Mock event processor for testing worker behavior
struct MockEventProcessor {
    agent_name: String,
    batch_size: i32,
    poll_interval: u64,
    should_fail: Arc<AtomicBool>,
    processing_calls: Arc<Mutex<Vec<Ulid>>>,
    processing_delay: StdDuration,
    fail_after_attempts: Arc<Mutex<Option<i32>>>,
    processed_count: Arc<AtomicU64>,
}

impl MockEventProcessor {
    fn new(agent_name: &str, batch_size: i32) -> Self {
        Self {
            agent_name: agent_name.to_string(),
            batch_size,
            poll_interval: 1,
            should_fail: Arc::new(AtomicBool::new(false)),
            processing_calls: Arc::new(Mutex::new(Vec::new())),
            processing_delay: StdDuration::from_millis(10),
            fail_after_attempts: Arc::new(Mutex::new(None)),
            processed_count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn set_should_fail(&self, should_fail: bool) {
        self.should_fail.store(should_fail, Ordering::Relaxed);
    }

    fn set_fail_after_attempts(&self, attempts: Option<i32>) {
        *self.fail_after_attempts.lock().unwrap() = attempts;
    }

    fn get_processing_calls(&self) -> Vec<Ulid> {
        self.processing_calls.lock().unwrap().clone()
    }

    fn get_processed_count(&self) -> u64 {
        self.processed_count.load(Ordering::Relaxed)
    }

}

#[async_trait]
impl EventProcessor for MockEventProcessor {
    async fn process_event(&self, _pool: &DbPool, item: &WorkQueueItem) -> Result<()> {
        // Record the call
        self.processing_calls.lock().unwrap().push(item.raw_event_id);

        // Simulate processing time
        sleep(self.processing_delay).await;

        // Check if we should fail based on attempts
        if let Some(fail_after) = *self.fail_after_attempts.lock().unwrap() {
            if item.attempts >= fail_after {
                return Err(anyhow!("Mock failure after {} attempts", item.attempts));
            }
        }

        // Check if we should fail unconditionally
        if self.should_fail.load(Ordering::Relaxed) {
            return Err(anyhow!("Mock processing failure"));
        }

        self.processed_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    fn batch_size(&self) -> i32 {
        self.batch_size
    }

    fn poll_interval_secs(&self) -> u64 {
        self.poll_interval
    }
}

/// Register a test agent in the database
async fn register_test_agent(
    pool: &DbPool,
    agent_name: &str,
    subscriptions: JsonValue,
) -> Result<()> {
    let params = AgentManifestParams {
        agent_name: agent_name.to_string(),
        version: "1.0.0".to_string(),
        description: Some("Test agent".to_string()),
        agent_type: "processor".to_string(),
        config_template_json: json!({}),
        produces_event_types: json!({}),
        subscribes_to_event_types: subscriptions,
        required_capabilities: json!({}),
    };
    upsert_agent_manifest(pool, params).await?;
    Ok(())
}

#[tokio::test]
async fn test_work_router_basic_routing() -> TestResult {
    let _pool = setup_test_database().await?;

    // Create test manifests
    let manifests = vec![
        create_test_manifest(
            "filesystem-agent",
            "running",
            json!({
                "fs": ["file.created", "file.modified"],
                "shell.kitty": ["command.executed"]
            }),
        ),
        create_test_manifest(
            "metrics-agent", 
            "running",
            json!({
                "*": ["heartbeat", "metric"]
            }),
        ),
        create_test_manifest(
            "stopped-agent",
            "stopped", 
            json!({
                "fs": ["file.created"]
            }),
        ),
    ];

    let router = WorkRouter::from_manifests(manifests);

    // Test specific event routing
    let fs_event = create_test_event("fs", "file.created", json!({"path": "/test.txt"}));
    let agents = router.route_event(&fs_event);
    assert_eq!(agents, vec!["filesystem-agent"]);

    let cmd_event = create_test_event("shell.kitty", "command.executed", json!({"cmd": "ls"}));
    let agents = router.route_event(&cmd_event);
    assert_eq!(agents, vec!["filesystem-agent"]);

    // Test wildcard routing
    let metric_event = create_test_event("system", "metric", json!({"value": 42}));
    let agents = router.route_event(&metric_event);
    assert_eq!(agents, vec!["metrics-agent"]);

    // Test unmatched event
    let unknown_event = create_test_event("unknown", "event", json!({}));
    let agents = router.route_event(&unknown_event);
    assert!(agents.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_event_scanner_basic_functionality() -> TestResult {
    let pool = setup_test_database().await?;

    // Create scanner with small batch size for testing
    let config = ScannerConfig {
        batch_size: 10,
        initial_lookback: Duration::hours(1),
        process_historical: false,
    };
    let mut scanner = EventScanner::new(config);

    // Insert test events
    let events = vec![
        create_test_event("fs", "file.created", json!({"path": "/test1.txt"})),
        create_test_event("fs", "file.modified", json!({"path": "/test2.txt"})),
        create_test_event("shell.kitty", "command.executed", json!({"cmd": "echo test"})),
    ];

    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
    }

    // Scan for new events
    let scanned_events = scanner.scan_new_events(&pool).await?;
    assert_eq!(scanned_events.len(), 3);

    // Verify state is updated
    assert_eq!(scanner.state().last_event_ids.len(), 2); // fs and shell.kitty sources
    assert!(scanner.state().last_scan_ts.is_some());

    // Second scan should find no new events
    let second_scan = scanner.scan_new_events(&pool).await?;
    assert!(second_scan.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_scanner_unqueued_events_detection() -> TestResult {
    let pool = setup_test_database().await?;

    let scanner = EventScanner::with_defaults();

    // Insert events
    let event1 = create_test_event("fs", "file.created", json!({"path": "/test1.txt"}));
    let event2 = create_test_event("fs", "file.modified", json!({"path": "/test2.txt"}));
    
    insert_event_with_validator(&pool, &event1, None).await?;
    insert_event_with_validator(&pool, &event2, None).await?;

    // Initially, all events should be unqueued
    let unqueued = scanner.get_unqueued_events(&pool, 10).await?;
    assert_eq!(unqueued.len(), 2);

    // Add one event to work queue
    add_to_work_queue(&pool, event1.id, "test-agent", 3).await?;

    // Now only one should be unqueued
    let unqueued = scanner.get_unqueued_events(&pool, 10).await?;
    assert_eq!(unqueued.len(), 1);
    assert_eq!(unqueued[0].id, event2.id);

    Ok(())
}

#[tokio::test]
async fn test_work_queue_item_claiming() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "test-agent", json!({"fs": ["file.created"]})).await?;

    // Create test events and add to work queue
    let event1 = create_test_event("fs", "file.created", json!({"path": "/test1.txt"}));
    let event2 = create_test_event("fs", "file.created", json!({"path": "/test2.txt"}));
    
    insert_event_with_validator(&pool, &event1, None).await?;
    insert_event_with_validator(&pool, &event2, None).await?;

    let _queue_id1 = add_to_work_queue(&pool, event1.id, "test-agent", 3).await?;
    let _queue_id2 = add_to_work_queue(&pool, event2.id, "test-agent", 3).await?;

    // Claim items for the agent
    let claimed_items = claim_work_queue_items(&pool, "test-agent", "worker-1", 5).await?;
    assert_eq!(claimed_items.len(), 2);

    // Verify items are in processing status
    for item in &claimed_items {
        assert_eq!(item.status, "processing");
        assert_eq!(item.processing_worker_id, Some("worker-1".to_string()));
    }

    // Try to claim again - should get no items (already claimed)
    let second_claim = claim_work_queue_items(&pool, "test-agent", "worker-2", 5).await?;
    assert!(second_claim.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_work_queue_skip_locked_behavior() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "test-agent", json!({"fs": ["file.created"]})).await?;

    // Create multiple events
    let events: Vec<_> = (0..5).map(|i| {
        create_test_event("fs", "file.created", json!({"path": format!("/test{}.txt", i)}))
    }).collect();

    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
        add_to_work_queue(&pool, event.id, "test-agent", 3).await?;
    }

    // Create barrier for synchronization
    let barrier = Arc::new(Barrier::new(3));

    // Spawn multiple workers to claim concurrently
    let pool1 = pool.clone();
    let pool2 = pool.clone();
    let barrier1 = barrier.clone();
    let barrier2 = barrier.clone();

    let worker1_task = tokio::spawn(async move {
        barrier1.wait().await;
        claim_work_queue_items(&pool1, "test-agent", "worker-1", 3).await
    });

    let worker2_task = tokio::spawn(async move {
        barrier2.wait().await;
        claim_work_queue_items(&pool2, "test-agent", "worker-2", 3).await
    });

    // Wait for all workers to start simultaneously
    barrier.wait().await;

    // Collect results
    let result1 = worker1_task.await??;
    let result2 = worker2_task.await??;

    // Total claimed should equal total available
    let total_claimed = result1.len() + result2.len();
    assert_eq!(total_claimed, 5);

    // No overlap in claimed items
    let mut all_claimed_ids = result1.iter().map(|item| item.queue_id).collect::<Vec<_>>();
    all_claimed_ids.extend(result2.iter().map(|item| item.queue_id));
    all_claimed_ids.sort();
    all_claimed_ids.dedup();
    assert_eq!(all_claimed_ids.len(), 5); // No duplicates

    Ok(())
}

#[tokio::test]
async fn test_event_processor_integration() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "test-processor", json!({"fs": ["file.created"]})).await?;

    // Create mock processor
    let processor = MockEventProcessor::new("test-processor", 2);

    // Create test events and add to work queue
    let events: Vec<_> = (0..3).map(|i| {
        create_test_event("fs", "file.created", json!({"path": format!("/test{}.txt", i)}))
    }).collect();

    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
        add_to_work_queue(&pool, event.id, "test-processor", 3).await?;
    }

    // Claim and process items manually (since Worker::process_batch is private)
    let claimed_items = claim_work_queue_items(&pool, "test-processor", "test-worker-1", 2).await?;
    assert_eq!(claimed_items.len(), 2);

    // Process claimed items
    for item in &claimed_items {
        processor.process_event(&pool, item).await?;
        complete_work_queue_item(&pool, item.queue_id).await?;
    }

    // Verify processor was called
    let calls = processor.get_processing_calls();
    assert_eq!(calls.len(), 2);

    // Verify items were completed (remaining items)
    let remaining_items = claim_work_queue_items(&pool, "test-processor", "test-worker-2", 10).await?;
    assert_eq!(remaining_items.len(), 1); // One item should remain

    Ok(())
}

#[tokio::test]
async fn test_work_queue_retry_logic() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "retry-agent", json!({"fs": ["file.created"]})).await?;

    // Create test event
    let event = create_test_event("fs", "file.created", json!({"path": "/retry-test.txt"}));
    insert_event_with_validator(&pool, &event, None).await?;
    let queue_id = add_to_work_queue(&pool, event.id, "retry-agent", 3).await?;

    // Claim the item
    let claimed_items = claim_work_queue_items(&pool, "retry-agent", "retry-worker", 1).await?;
    assert_eq!(claimed_items.len(), 1);
    let _item = &claimed_items[0];

    // Simulate failure and schedule retry
    fail_work_queue_item(&pool, queue_id, "test retry", Utc::now() - Duration::minutes(1)).await?;

    // Verify item is back in failed_retryable state
    let updated_item = get_work_item_by_id(&pool, queue_id).await?;
    assert_eq!(updated_item.status, "failed_retryable");
    assert_eq!(updated_item.attempts, 1);
    assert!(updated_item.next_retry_ts.is_some());
    assert!(updated_item.error_message_last.is_some());

    // Should be able to claim again since retry time is in the past
    let retry_claimed = claim_work_queue_items(&pool, "retry-agent", "retry-worker-2", 1).await?;
    assert_eq!(retry_claimed.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_dlq_integration() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "dlq-agent", json!({"fs": ["file.created"]})).await?;

    // Create test event
    let event = create_test_event("fs", "file.created", json!({"path": "/dlq-test.txt"}));
    insert_event_with_validator(&pool, &event, None).await?;

    // Create DLQ event
    let dlq_params = DlqEventParams {
        failed_event_id: event.id,
        agent_name: "dlq-agent".to_string(),
        source: "fs".to_string(),
        event_type: "file.created".to_string(),
        failure_reason: "Max attempts exceeded after 3 retries".to_string(),
        error_category: "permanent".to_string(),
        original_event_payload: event.payload.clone(),
        additional_metadata: Some(json!({
            "test_metadata": "test_value"
        })),
    };

    let dlq_event = insert_dlq_event(&pool, dlq_params).await?;

    // Verify DLQ event was created correctly
    assert_eq!(dlq_event.failed_event_id, event.id);
    assert_eq!(dlq_event.agent_name, "dlq-agent");
    assert_eq!(dlq_event.source, "fs");
    assert_eq!(dlq_event.event_type, "file.created");
    assert!(dlq_event.failure_reason.contains("Max attempts exceeded"));

    // Verify we can retrieve DLQ items (may have multiple from previous test runs)
    let dlq_items = get_dlq_items(&pool, "dlq-agent", 10).await?;
    assert!(!dlq_items.is_empty(), "Should have at least one DLQ item");
    
    // Find our specific DLQ item
    let our_dlq_item = dlq_items.iter().find(|item| item.failed_event_id == event.id);
    assert!(our_dlq_item.is_some(), "Should find our DLQ item");
    assert_eq!(our_dlq_item.unwrap().failed_event_id, event.id);

    Ok(())
}

#[tokio::test]
async fn test_end_to_end_scanner_router_integration() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agents in database
    register_test_agent(
        &pool, 
        "fs-processor",
        json!({"fs": ["file.created", "file.modified"]}),
    ).await?;
    
    register_test_agent(
        &pool,
        "shell-processor", 
        json!({"shell.kitty": ["command.executed"]}),
    ).await?;

    // Create test events
    let events = vec![
        create_test_event("fs", "file.created", json!({"path": "/test1.txt"})),
        create_test_event("fs", "file.modified", json!({"path": "/test2.txt"})),
        create_test_event("shell.kitty", "command.executed", json!({"cmd": "ls -la"})),
        create_test_event("unknown", "event", json!({})), // Should not be routed
    ];

    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
    }

    // Create scanner and process events
    let manifests = get_active_manifests(&pool).await?;
    let router = WorkRouter::from_manifests(manifests);
    let mut scanner = EventScanner::with_defaults();

    let scanned_events = scanner.scan_new_events(&pool).await?;
    assert_eq!(scanned_events.len(), 4);

    // Create work entries
    let work_entries_created = create_work_entries(&pool, scanned_events, &router).await?;
    assert_eq!(work_entries_created, 3); // 2 fs events + 1 shell event

    // Verify work was created for each agent
    let fs_items = claim_work_queue_items(&pool, "fs-processor", "fs-worker", 10).await?;
    let shell_items = claim_work_queue_items(&pool, "shell-processor", "shell-worker", 10).await?;

    assert_eq!(fs_items.len(), 2); // 2 filesystem events
    assert_eq!(shell_items.len(), 1); // 1 shell event

    Ok(())
}

#[tokio::test]
async fn test_concurrency_safety() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "concurrent-agent", json!({"test": ["event"]})).await?;

    // Create many test events
    let events: Vec<_> = (0..20).map(|i| {
        create_test_event("test", "event", json!({"index": i}))
    }).collect();

    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
        add_to_work_queue(&pool, event.id, "concurrent-agent", 3).await?;
    }

    // Create multiple mock processors
    let processors: Vec<_> = (0..3).map(|_i| {
        Arc::new(MockEventProcessor::new("concurrent-agent", 3))
    }).collect();

    // Process concurrently
    let tasks: Vec<_> = processors.iter().enumerate().map(|(i, processor)| {
        let pool = pool.clone();
        let processor = processor.clone();
        tokio::spawn(async move {
            let mut total_processed = 0;
            loop {
                let items = claim_work_queue_items(
                    &pool,
                    "concurrent-agent",
                    &format!("concurrent-worker-{}", i),
                    3,
                ).await.unwrap();

                if items.is_empty() {
                    break;
                }

                for item in items {
                    if processor.process_event(&pool, &item).await.is_ok() {
                        complete_work_queue_item(&pool, item.queue_id).await.unwrap();
                        total_processed += 1;
                    }
                }
            }
            total_processed
        })
    }).collect();

    let results = join_all(tasks).await;
    let total_processed: usize = results.into_iter().map(|r| r.unwrap()).sum();

    // All events should be processed exactly once
    assert_eq!(total_processed, 20);

    // Verify each processor worked
    let total_calls: u64 = processors.iter().map(|p| p.get_processed_count()).sum();
    assert_eq!(total_calls, 20);

    Ok(())
}

#[tokio::test]
async fn test_worker_crash_recovery() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "crash-agent", json!({"test": ["crash_event"]})).await?;

    // Create test events
    let events: Vec<_> = (0..5).map(|i| {
        create_test_event("test", "crash_event", json!({"crash_test": i}))
    }).collect();

    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
        add_to_work_queue(&pool, event.id, "crash-agent", 3).await?;
    }

    // Claim items with first worker
    let claimed_by_worker1 = claim_work_queue_items(&pool, "crash-agent", "worker-1", 3).await?;
    assert_eq!(claimed_by_worker1.len(), 3);

    // Simulate worker crash - items remain in processing state
    // Verify items are locked to worker-1
    for item in &claimed_by_worker1 {
        assert_eq!(item.status, "processing");
        assert_eq!(item.processing_worker_id, Some("worker-1".to_string()));
    }

    // Different worker should not be able to claim these items
    let claimed_by_worker2 = claim_work_queue_items(&pool, "crash-agent", "worker-2", 5).await?;
    assert_eq!(claimed_by_worker2.len(), 2); // Only remaining items

    // Simulate recovery: manually reset crashed worker's items
    for item in &claimed_by_worker1 {
        sqlx::query!(
            "UPDATE sinex_schemas.work_queue SET status = 'pending', processing_worker_id = NULL WHERE queue_id::uuid = $1",
            item.queue_id.to_uuid()
        )
        .execute(&pool)
        .await?;
    }

    // Now worker-2 should be able to claim the recovered items
    let recovered_items = claim_work_queue_items(&pool, "crash-agent", "worker-2", 5).await?;
    assert_eq!(recovered_items.len(), 3); // The 3 recovered items

    Ok(())
}

#[tokio::test]
async fn test_deadletter_queue_workflow() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "dlq-test-agent", json!({"test": ["dlq_event"]})).await?;

    // Create processor that fails after specific attempts
    let processor = MockEventProcessor::new("dlq-test-agent", 1);
    processor.set_fail_after_attempts(Some(2)); // Fail after 2 attempts

    // Create test event
    let event = create_test_event("test", "dlq_event", json!({"dlq_test": "data"}));
    insert_event_with_validator(&pool, &event, None).await?;
    let queue_id = add_to_work_queue(&pool, event.id, "dlq-test-agent", 2).await?;

    // First attempt - should fail
    let items = claim_work_queue_items(&pool, "dlq-test-agent", "dlq-worker", 1).await?;
    assert_eq!(items.len(), 1);
    let item = &items[0];
    
    let result = processor.process_event(&pool, item).await;
    assert!(result.is_err());
    
    fail_work_queue_item(&pool, item.queue_id, "First failure", chrono::Utc::now() - chrono::Duration::minutes(1)).await?;

    // Verify item is in failed_retryable state
    let updated_item = get_work_item_by_id(&pool, queue_id).await?;
    assert_eq!(updated_item.status, "failed_retryable");
    assert_eq!(updated_item.attempts, 1);

    // Second attempt - should also fail and move to DLQ
    let retry_items = claim_work_queue_items(&pool, "dlq-test-agent", "dlq-worker", 1).await?;
    assert_eq!(retry_items.len(), 1);
    let retry_item = &retry_items[0];
    assert_eq!(retry_item.attempts, 1); // Should show previous attempts

    let result2 = processor.process_event(&pool, retry_item).await;
    assert!(result2.is_err());

    // Now attempts should exceed max, triggering DLQ
    let new_attempts = retry_item.attempts + 1;
    assert!(new_attempts >= retry_item.max_attempts);

    // Manually create DLQ entry (simulating worker behavior)
    let dlq_params = DlqEventParams {
        failed_event_id: event.id,
        agent_name: "dlq-test-agent".to_string(),
        source: "test".to_string(),
        event_type: "dlq_event".to_string(),
        failure_reason: format!("Max attempts exceeded after {} retries", new_attempts),
        error_category: "permanent".to_string(),
        original_event_payload: event.payload.clone(),
        additional_metadata: Some(json!({
            "queue_id": retry_item.queue_id,
            "final_attempts": new_attempts,
            "worker_id": "dlq-worker"
        })),
    };

    let dlq_event = insert_dlq_event(&pool, dlq_params).await?;

    // Clean up work queue
    complete_work_queue_item(&pool, retry_item.queue_id).await?;

    // Verify DLQ entry
    assert_eq!(dlq_event.failed_event_id, event.id);
    assert_eq!(dlq_event.agent_name, "dlq-test-agent");
    assert!(dlq_event.failure_reason.contains("Max attempts exceeded"));

    // Verify we can retrieve DLQ items (may have multiple from previous test runs)
    let dlq_items = get_dlq_items(&pool, "dlq-test-agent", 10).await?;
    assert!(!dlq_items.is_empty(), "Should have at least one DLQ item");
    
    // Find our specific DLQ item
    let our_dlq_item = dlq_items.iter().find(|item| item.failed_event_id == event.id);
    assert!(our_dlq_item.is_some(), "Should find our DLQ item");
    assert_eq!(our_dlq_item.unwrap().failed_event_id, event.id);

    Ok(())
}

#[tokio::test]
async fn test_worker_metrics_tracking() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "metrics-agent", json!({"test": ["metric_event"]})).await?;

    let processor = MockEventProcessor::new("metrics-agent", 2);
    
    // Create test events - some will succeed, some will fail
    let events: Vec<_> = (0..4).map(|i| {
        create_test_event("test", "metric_event", json!({"metric_test": i}))
    }).collect();

    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
        add_to_work_queue(&pool, event.id, "metrics-agent", 3).await?;
    }

    // Process first batch successfully
    let batch1 = claim_work_queue_items(&pool, "metrics-agent", "metrics-worker", 2).await?;
    assert_eq!(batch1.len(), 2);
    
    for item in &batch1 {
        processor.process_event(&pool, item).await?;
        complete_work_queue_item(&pool, item.queue_id).await?;
    }

    // Set processor to fail for next batch
    processor.set_should_fail(true);
    
    let batch2 = claim_work_queue_items(&pool, "metrics-agent", "metrics-worker", 2).await?;
    assert_eq!(batch2.len(), 2);
    
    for item in &batch2 {
        let result = processor.process_event(&pool, item).await;
        assert!(result.is_err());
        fail_work_queue_item(&pool, item.queue_id, "Metrics test failure", chrono::Utc::now() + chrono::Duration::minutes(5)).await?;
    }

    // Verify processor tracked calls correctly
    let calls = processor.get_processing_calls();
    assert_eq!(calls.len(), 4); // All 4 events were processed (some failed)

    // Verify processing count (successful only)
    assert_eq!(processor.get_processed_count(), 2); // Only first batch succeeded

    Ok(())
}

#[tokio::test]
async fn test_agent_subscription_filtering() -> TestResult {
    let pool = setup_test_database().await?;

    // Register multiple agents with different subscriptions
    register_test_agent(
        &pool,
        "fs-agent",
        json!({"fs": ["file.created", "file.modified"]}),
    ).await?;
    
    register_test_agent(
        &pool,
        "shell-agent",
        json!({"shell.kitty": ["command.executed"]}),
    ).await?;
    
    register_test_agent(
        &pool,
        "wildcard-agent",
        json!({"*": ["heartbeat"]}),
    ).await?;

    // Create events of different types
    let fs_event = create_test_event("fs", "file.created", json!({"path": "/test.txt"}));
    let shell_event = create_test_event("shell.kitty", "command.executed", json!({"cmd": "ls"}));
    let heartbeat_event = create_test_event("system", "heartbeat", json!({"status": "ok"}));
    let unknown_event = create_test_event("unknown", "unknown_type", json!({}));

    let events = vec![&fs_event, &shell_event, &heartbeat_event, &unknown_event];
    
    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
    }

    // Get manifests and create router
    let manifests = get_active_manifests(&pool).await?;
    let router = WorkRouter::from_manifests(manifests);

    // Create work entries
    let work_created = create_work_entries(&pool, events.into_iter().cloned().collect(), &router).await?;
    assert_eq!(work_created, 3); // fs, shell, and heartbeat events should create work

    // Verify each agent gets appropriate work
    let fs_work = claim_work_queue_items(&pool, "fs-agent", "test-worker", 10).await?;
    let shell_work = claim_work_queue_items(&pool, "shell-agent", "test-worker", 10).await?;
    let wildcard_work = claim_work_queue_items(&pool, "wildcard-agent", "test-worker", 10).await?;

    assert_eq!(fs_work.len(), 1); // Only fs event
    assert_eq!(shell_work.len(), 1); // Only shell event  
    assert_eq!(wildcard_work.len(), 1); // Only heartbeat event

    // Verify correct events were routed
    assert_eq!(fs_work[0].raw_event_id, fs_event.id);
    assert_eq!(shell_work[0].raw_event_id, shell_event.id);
    assert_eq!(wildcard_work[0].raw_event_id, heartbeat_event.id);

    Ok(())
}

#[tokio::test]
async fn test_exponential_backoff_scheduling() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "backoff-agent", json!({"test": ["backoff_event"]})).await?;

    let processor = MockEventProcessor::new("backoff-agent", 1);
    processor.set_should_fail(true); // Always fail

    // Create test event
    let event = create_test_event("test", "backoff_event", json!({"backoff_test": true}));
    insert_event_with_validator(&pool, &event, None).await?;
    add_to_work_queue(&pool, event.id, "backoff-agent", 5).await?;

    // Simulate multiple failures with increasing backoff
    for attempt in 0..3 {
        let items = claim_work_queue_items(&pool, "backoff-agent", "backoff-worker", 1).await?;
        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.attempts, attempt);

        // Process and fail
        let result = processor.process_event(&pool, item).await;
        assert!(result.is_err());

        // Calculate expected backoff
        let expected_delay = sinex_worker::calculate_backoff_secs(attempt);
        let next_retry = chrono::Utc::now() + chrono::Duration::seconds(expected_delay as i64);

        fail_work_queue_item(&pool, item.queue_id, "Backoff test failure", next_retry).await?;

        // Verify backoff increases
        match attempt {
            0 => assert!((48.0..=72.0).contains(&expected_delay)), // ~60s with jitter
            1 => assert!((96.0..=144.0).contains(&expected_delay)), // ~120s with jitter  
            2 => assert!((192.0..=288.0).contains(&expected_delay)), // ~240s with jitter
            _ => {}
        }

        // Verify item is in failed_retryable state with correct attempt count
        let updated_item = get_work_item_by_id(&pool, item.queue_id).await?;
        assert_eq!(updated_item.status, "failed_retryable");
        assert_eq!(updated_item.attempts, attempt + 1);
        assert!(updated_item.next_retry_ts.is_some());
    }

    Ok(())
}

#[tokio::test]
async fn test_worker_idempotency() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "idempotent-agent", json!({"test": ["idempotent_event"]})).await?;

    let processor = MockEventProcessor::new("idempotent-agent", 1);

    // Create test event
    let event = create_test_event("test", "idempotent_event", json!({"idempotent_test": true}));
    insert_event_with_validator(&pool, &event, None).await?;
    add_to_work_queue(&pool, event.id, "idempotent-agent", 3).await?;

    // Process the same event multiple times (simulate duplicate processing)
    for i in 0..3 {
        let items = claim_work_queue_items(&pool, "idempotent-agent", &format!("worker-{}", i), 1).await?;
        
        if items.is_empty() {
            // No more items to process (expected after first completion)
            break;
        }
        
        assert_eq!(items.len(), 1);
        let item = &items[0];

        // Process successfully
        processor.process_event(&pool, item).await?;
        complete_work_queue_item(&pool, item.queue_id).await?;

        // After first completion, no more items should be available
        if i == 0 {
            let remaining_items = claim_work_queue_items(&pool, "idempotent-agent", "test-worker", 10).await?;
            assert!(remaining_items.is_empty(), "No items should remain after completion");
            break;
        }
    }

    // Verify processor was called only once
    let calls = processor.get_processing_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], event.id);

    Ok(())
}

#[tokio::test]
async fn test_batch_processing_limits() -> TestResult {
    let pool = setup_test_database().await?;

    // Register test agent
    register_test_agent(&pool, "batch-agent", json!({"test": ["batch_event"]})).await?;

    // Create many test events (more than typical batch size)
    let events: Vec<_> = (0..15).map(|i| {
        create_test_event("test", "batch_event", json!({"batch_index": i}))
    }).collect();

    for event in &events {
        insert_event_with_validator(&pool, event, None).await?;
        add_to_work_queue(&pool, event.id, "batch-agent", 3).await?;
    }

    // Claim with different batch sizes
    let batch_3 = claim_work_queue_items(&pool, "batch-agent", "worker-1", 3).await?;
    assert_eq!(batch_3.len(), 3);

    let batch_5 = claim_work_queue_items(&pool, "batch-agent", "worker-2", 5).await?;
    assert_eq!(batch_5.len(), 5);

    let batch_10 = claim_work_queue_items(&pool, "batch-agent", "worker-3", 10).await?;
    assert_eq!(batch_10.len(), 7); // Only 7 remaining items

    // Verify all items are claimed
    let remaining = claim_work_queue_items(&pool, "batch-agent", "worker-4", 10).await?;
    assert!(remaining.is_empty());

    // Verify no overlap in claimed items
    let mut all_claimed_ids = Vec::new();
    all_claimed_ids.extend(batch_3.iter().map(|item| item.queue_id));
    all_claimed_ids.extend(batch_5.iter().map(|item| item.queue_id));
    all_claimed_ids.extend(batch_10.iter().map(|item| item.queue_id));
    
    all_claimed_ids.sort();
    let unique_count = all_claimed_ids.len();
    all_claimed_ids.dedup();
    assert_eq!(all_claimed_ids.len(), unique_count); // No duplicates

    Ok(())
}