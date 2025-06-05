use anyhow::Result;
use sinex_db::{
    create_pool,
    models::{AgentManifest, PromotionQueueItem, QueueStatus, RawEvent},
    queries::{
        claim_promotion_queue_items, complete_promotion_queue_item, fail_promotion_queue_item,
        insert_raw_event, upsert_agent_manifest,
    },
};
use sinex_worker::{worker::Worker, EventProcessor};
use sqlx::{PgPool, Row};
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use tokio::time::{sleep, Duration};
use async_trait::async_trait;

/// Test processor that counts events and can fail on demand
struct TestProcessor {
    processed_count: Arc<AtomicU32>,
    should_fail: Arc<AtomicBool>,
    agent_name: String,
}

#[async_trait]
impl EventProcessor for TestProcessor {
    async fn process_event(&self, _pool: &PgPool, _item: &PromotionQueueItem) -> Result<()> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Test failure"));
        }
        self.processed_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    fn batch_size(&self) -> i32 {
        5
    }

    fn poll_interval_secs(&self) -> u64 {
        0 // Fast polling for tests
    }
}

#[sqlx::test]
async fn test_promotion_worker_end_to_end(pool: PgPool) -> Result<()> {
    // Register test agent
    let agent_name = "TestAgent_v1.0.0";
    upsert_agent_manifest(
        &pool,
        agent_name,
        "1.0.0",
        "running",
        "test",
        Some("Test agent for integration tests"),
        None,
        Some(serde_json::json!({
            "test.source": ["test_event"]
        })),
    )
    .await?;

    // Insert test events
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event = insert_raw_event(
            &pool,
            "test.source",
            "test_event",
            "test-host",
            serde_json::json!({
                "test_data": format!("Event {}", i),
                "index": i
            }),
            None,
            Some("test-ingestor-v1"),
            None,
        )
        .await?;
        event_ids.push(event.id);
    }

    // Verify events were inserted
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'test.source'"
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(count, 10);

    // Run the event router function to populate promotion queue
    for event_id in &event_ids {
        sqlx::query("SELECT sinex_router.route_raw_event_to_promotion_queue($1)")
            .bind(event_id)
            .execute(&pool)
            .await?;
    }

    // Verify promotion queue was populated
    let queue_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE target_agent_name = $1"
    )
    .bind(agent_name)
    .fetch_one(&pool)
    .await?;
    assert_eq!(queue_count, 10);

    // Create test processor
    let processed_count = Arc::new(AtomicU32::new(0));
    let should_fail = Arc::new(AtomicBool::new(false));
    let processor = Arc::new(TestProcessor {
        processed_count: processed_count.clone(),
        should_fail: should_fail.clone(),
        agent_name: agent_name.to_string(),
    });

    // Create and run worker in background
    let worker = Worker::new(pool.clone(), processor, "test-worker-1".to_string());
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run().await;
    });

    // Wait for processing
    let mut attempts = 0;
    while processed_count.load(Ordering::SeqCst) < 10 && attempts < 50 {
        sleep(Duration::from_millis(100)).await;
        attempts += 1;
    }

    // Verify all items were processed
    assert_eq!(processed_count.load(Ordering::SeqCst), 10);

    // Verify queue is empty
    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE target_agent_name = $1"
    )
    .bind(agent_name)
    .fetch_one(&pool)
    .await?;
    assert_eq!(remaining, 0);

    // Cancel worker
    worker_handle.abort();

    Ok(())
}

#[sqlx::test]
async fn test_promotion_worker_retry_logic(pool: PgPool) -> Result<()> {
    // Register test agent
    let agent_name = "TestRetryAgent_v1.0.0";
    upsert_agent_manifest(
        &pool,
        agent_name,
        "1.0.0",
        "running",
        "test",
        Some("Test agent for retry logic"),
        None,
        Some(serde_json::json!({
            "test.retry": ["retry_event"]
        })),
    )
    .await?;

    // Insert test event
    let event = insert_raw_event(
        &pool,
        "test.retry",
        "retry_event",
        "test-host",
        serde_json::json!({"test": "retry"}),
        None,
        None,
        None,
    )
    .await?;

    // Route to promotion queue
    sqlx::query("SELECT sinex_router.route_raw_event_to_promotion_queue($1)")
        .bind(&event.id)
        .execute(&pool)
        .await?;

    // Create processor that fails first 2 attempts
    let attempt_count = Arc::new(AtomicU32::new(0));
    let processed_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();
    let processed_count_clone = processed_count.clone();

    struct RetryTestProcessor {
        attempt_count: Arc<AtomicU32>,
        processed_count: Arc<AtomicU32>,
        agent_name: String,
    }

    #[async_trait]
    impl EventProcessor for RetryTestProcessor {
        async fn process_event(&self, _pool: &PgPool, _item: &PromotionQueueItem) -> Result<()> {
            let attempts = self.attempt_count.fetch_add(1, Ordering::SeqCst);
            if attempts < 2 {
                return Err(anyhow::anyhow!("Simulated failure #{}", attempts + 1));
            }
            self.processed_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn agent_name(&self) -> &str {
            &self.agent_name
        }
    }

    let processor = Arc::new(RetryTestProcessor {
        attempt_count: attempt_count_clone,
        processed_count: processed_count_clone,
        agent_name: agent_name.to_string(),
    });

    // Run worker
    let worker = Worker::new(pool.clone(), processor, "test-retry-worker".to_string());
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run().await;
    });

    // Wait for processing
    let mut total_attempts = 0;
    while processed_count.load(Ordering::SeqCst) < 1 && total_attempts < 100 {
        sleep(Duration::from_millis(100)).await;
        total_attempts += 1;
    }

    // Verify it took 3 attempts (2 failures + 1 success)
    assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
    assert_eq!(processed_count.load(Ordering::SeqCst), 1);

    // Cancel worker
    worker_handle.abort();

    Ok(())
}

#[sqlx::test]
async fn test_promotion_worker_concurrent_processing(pool: PgPool) -> Result<()> {
    // Register test agent
    let agent_name = "TestConcurrentAgent_v1.0.0";
    upsert_agent_manifest(
        &pool,
        agent_name,
        "1.0.0",
        "running",
        "test",
        Some("Test agent for concurrent processing"),
        None,
        Some(serde_json::json!({
            "test.concurrent": ["concurrent_event"]
        })),
    )
    .await?;

    // Insert many test events
    let num_events = 50;
    let mut event_ids = Vec::new();
    for i in 0..num_events {
        let event = insert_raw_event(
            &pool,
            "test.concurrent",
            "concurrent_event",
            "test-host",
            serde_json::json!({"index": i}),
            None,
            None,
            None,
        )
        .await?;
        event_ids.push(event.id);
    }

    // Route all to promotion queue
    for event_id in &event_ids {
        sqlx::query("SELECT sinex_router.route_raw_event_to_promotion_queue($1)")
            .bind(event_id)
            .execute(&pool)
            .await?;
    }

    // Create processor with processing delay
    let processed_count = Arc::new(AtomicU32::new(0));
    let processed_count_clone = processed_count.clone();

    struct SlowProcessor {
        processed_count: Arc<AtomicU32>,
        agent_name: String,
    }

    #[async_trait]
    impl EventProcessor for SlowProcessor {
        async fn process_event(&self, _pool: &PgPool, _item: &PromotionQueueItem) -> Result<()> {
            // Simulate some work
            sleep(Duration::from_millis(10)).await;
            self.processed_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn agent_name(&self) -> &str {
            &self.agent_name
        }

        fn batch_size(&self) -> i32 {
            10 // Process 10 at a time
        }
    }

    let processor = Arc::new(SlowProcessor {
        processed_count: processed_count_clone,
        agent_name: agent_name.to_string(),
    });

    // Run multiple workers concurrently
    let mut worker_handles = Vec::new();
    for i in 0..3 {
        let worker = Worker::new(
            pool.clone(),
            processor.clone(),
            format!("concurrent-worker-{}", i),
        );
        let handle = tokio::spawn(async move {
            let _ = worker.run().await;
        });
        worker_handles.push(handle);
    }

    // Wait for all processing to complete
    let start = std::time::Instant::now();
    while processed_count.load(Ordering::SeqCst) < num_events as u32 {
        if start.elapsed() > Duration::from_secs(10) {
            panic!("Timeout waiting for concurrent processing");
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Verify all processed
    assert_eq!(processed_count.load(Ordering::SeqCst), num_events as u32);

    // Cancel workers
    for handle in worker_handles {
        handle.abort();
    }

    Ok(())
}

#[sqlx::test]
async fn test_skip_locked_prevents_duplicate_processing(pool: PgPool) -> Result<()> {
    // This test verifies that SKIP LOCKED prevents multiple workers
    // from processing the same item

    let agent_name = "TestSkipLockedAgent_v1.0.0";
    upsert_agent_manifest(
        &pool,
        agent_name,
        "1.0.0",
        "running",
        "test",
        None,
        None,
        Some(serde_json::json!({
            "test.skiplock": ["skiplock_event"]
        })),
    )
    .await?;

    // Insert test event
    let event = insert_raw_event(
        &pool,
        "test.skiplock",
        "skiplock_event",
        "test-host",
        serde_json::json!({"test": "skiplock"}),
        None,
        None,
        None,
    )
    .await?;

    // Route to promotion queue
    sqlx::query("SELECT sinex_router.route_raw_event_to_promotion_queue($1)")
        .bind(&event.id)
        .execute(&pool)
        .await?;

    // Try to claim the same item from two "workers" simultaneously
    let (tx1, rx1) = tokio::sync::oneshot::channel();
    let (tx2, rx2) = tokio::sync::oneshot::channel();

    let pool1 = pool.clone();
    let pool2 = pool.clone();
    let agent_name1 = agent_name.to_string();
    let agent_name2 = agent_name.to_string();

    // Worker 1
    let handle1 = tokio::spawn(async move {
        let items = claim_promotion_queue_items(&pool1, &agent_name1, "worker-1", 10).await.unwrap();
        tx1.send(items).unwrap();
    });

    // Worker 2 (starts at same time)
    let handle2 = tokio::spawn(async move {
        let items = claim_promotion_queue_items(&pool2, &agent_name2, "worker-2", 10).await.unwrap();
        tx2.send(items).unwrap();
    });

    // Get results
    let items1 = rx1.await?;
    let items2 = rx2.await?;

    // One should get the item, the other should get nothing
    assert!(items1.len() + items2.len() == 1);
    assert!(items1.is_empty() || items2.is_empty());

    handle1.await?;
    handle2.await?;

    Ok(())
}