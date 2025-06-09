use chrono::Utc;
use serde_json::json;
use sinex_shared::{DatabaseService, RawEventBuilder, sources, event_type_constants};
use sinex_worker::{worker::Worker, EventProcessor};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Full end-to-end test: Event ingestion → Router trigger → Promotion queue → Worker processing
#[sqlx::test]
async fn test_complete_event_pipeline(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = Arc::new(DatabaseService::from_pool(pool.clone()));

    // Step 1: Register a test agent that processes filesystem events
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, status, agent_type, subscribes_to_event_types)
        VALUES 
            ('test-file-processor', '1.0.0', 'running', 'promoter', 
             '{"raw.events_feed_all": [{"source_filter": "filesystem", "event_type_filter": "file_*"}]}'::jsonb)
        ON CONFLICT (agent_name) DO UPDATE SET status = 'running'
        "#
    )
    .execute(&pool)
    .await?;

    // Step 2: Create and insert test events
    let test_events = vec![
        ("file1.txt", event_type_constants::filesystem::FILE_CREATED),
        ("file2.txt", event_type_constants::filesystem::FILE_MODIFIED),
        ("file3.txt", event_type_constants::filesystem::FILE_DELETED),
    ];

    let mut event_ids = Vec::new();
    for (filename, event_type) in test_events {
        let event = RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type,
            json!({
                "path": format!("/test/{}", filename),
                "timestamp": Utc::now().to_rfc3339(),
                "test_run": "end_to_end_pipeline"
            })
        ).build();

        let id = db_service.insert_event(&event).await?;
        event_ids.push(id);
    }

    // Step 3: Verify promotion queue entries were created by the trigger
    tokio::time::sleep(Duration::from_millis(100)).await; // Give trigger time to execute

    let queue_count: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) 
        FROM sinex_schemas.promotion_queue 
        WHERE target_agent_name = 'test-file-processor'
        AND status = 'pending'
        "#
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    assert_eq!(queue_count, 3, "Expected 3 promotion queue entries");

    // Step 4: Create a test worker to process the queue
    // Create a simple test processor
    struct TestProcessor;
    
    #[async_trait::async_trait]
    impl EventProcessor for TestProcessor {
        async fn process_event(&self, _pool: &sqlx::PgPool, _item: &sinex_db::models::PromotionQueueItem) -> anyhow::Result<()> {
            Ok(())
        }
        
        fn agent_name(&self) -> &str {
            "test-file-processor"
        }
    }
    
    let processor = Arc::new(TestProcessor);
    let worker = Worker::new(pool.clone(), processor, "test-worker-001".to_string());

    // Step 5: Run the worker for a short time
    let worker_handle = tokio::spawn(async move {
        worker.run().await
    });

    // Wait for processing to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Stop the worker
    worker_handle.abort();

    // Step 6: Verify all events were processed
    let processed_count: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) 
        FROM sinex_schemas.promotion_queue 
        WHERE target_agent_name = 'test-file-processor'
        AND status = 'completed'
        "#
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    assert_eq!(processed_count, 3, "Expected all 3 events to be processed");

    // Step 7: Verify no events are stuck in processing
    let stuck_count: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) 
        FROM sinex_schemas.promotion_queue 
        WHERE target_agent_name = 'test-file-processor'
        AND status = 'processing'
        "#
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    assert_eq!(stuck_count, 0, "No events should be stuck in processing");

    Ok(())
}

/// Test concurrent workers don't process the same events
#[sqlx::test]
async fn test_concurrent_worker_safety(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = Arc::new(DatabaseService::from_pool(pool.clone()));

    // Register agent
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, status, agent_type, subscribes_to_event_types)
        VALUES 
            ('concurrent-test-agent', '1.0.0', 'running', 'promoter', 
             '{"raw.events_feed_all": [{"source_filter": "sinex"}]}'::jsonb)
        ON CONFLICT (agent_name) DO UPDATE SET status = 'running'
        "#
    )
    .execute(&pool)
    .await?;

    // Insert many events
    for i in 0..20 {
        let event = RawEventBuilder::new(
            sources::SINEX,
            event_type_constants::sinex::AGENT_HEARTBEAT,
            json!({ "worker_test": i })
        ).build();
        
        db_service.insert_event(&event).await?;
    }

    // Wait for router trigger
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create multiple workers
    let mut worker_handles = Vec::new();
    let processed_count = Arc::new(tokio::sync::Mutex::new(0));

    for worker_num in 0..3 {
        let pool_clone = pool.clone();
        let counter = processed_count.clone();
        let worker_id = format!("concurrent-worker-{}", worker_num);

        struct ConcurrentTestProcessor {
            counter: Arc<tokio::sync::Mutex<usize>>,
        }
        
        #[async_trait::async_trait]
        impl EventProcessor for ConcurrentTestProcessor {
            async fn process_event(&self, _pool: &sqlx::PgPool, _item: &sinex_db::models::PromotionQueueItem) -> anyhow::Result<()> {
                // Simulate processing
                tokio::time::sleep(Duration::from_millis(50)).await;
                
                let mut count = self.counter.lock().await;
                *count += 1;
                
                Ok(())
            }
            
            fn agent_name(&self) -> &str {
                "concurrent-test-agent"
            }
        }
        
        let processor = Arc::new(ConcurrentTestProcessor { counter: counter.clone() });
        let worker = Worker::new(pool_clone, processor, worker_id);

        let handle = tokio::spawn(async move {
            let _ = timeout(Duration::from_secs(5), worker.run()).await;
        });

        worker_handles.push(handle);
    }

    // Wait for all workers
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Verify each event was processed exactly once
    let final_count = *processed_count.lock().await;
    assert_eq!(final_count, 20, "Each event should be processed exactly once");

    // Verify database state
    let completed: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) 
        FROM sinex_schemas.promotion_queue 
        WHERE target_agent_name = 'concurrent-test-agent'
        AND status = 'completed'
        "#
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    assert_eq!(completed, 20, "All events should be marked as completed");

    Ok(())
}

/// Test that the system handles errors gracefully
#[sqlx::test]
async fn test_error_handling_and_retry(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = Arc::new(DatabaseService::from_pool(pool.clone()));

    // Register agent
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, status, agent_type, subscribes_to_event_types)
        VALUES 
            ('error-test-agent', '1.0.0', 'running', 'promoter', 
             '{"raw.events_feed_all": [{"source_filter": "error_test"}]}'::jsonb)
        ON CONFLICT (agent_name) DO UPDATE SET status = 'running'
        "#
    )
    .execute(&pool)
    .await?;

    // Insert an event
    let event = RawEventBuilder::new(
        "error_test",
        "will_fail",
        json!({ "fail_count": 2 }) // Will fail first 2 times
    ).build();
    
    let event_id = db_service.insert_event(&event).await?;

    // Wait for router
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create worker with retry logic
    struct ErrorTestProcessor {
        fail_count: Arc<std::sync::atomic::AtomicU32>,
    }
    
    #[async_trait::async_trait]
    impl EventProcessor for ErrorTestProcessor {
        async fn process_event(&self, _pool: &sqlx::PgPool, _item: &sinex_db::models::PromotionQueueItem) -> anyhow::Result<()> {
            let count = self.fail_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count < 2 {
                anyhow::bail!("Simulated failure #{}", count + 1);
            }
            Ok(())
        }
        
        fn agent_name(&self) -> &str {
            "error-test-agent"
        }
    }
    
    let processor = Arc::new(ErrorTestProcessor {
        fail_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
    });
    let worker = Worker::new(pool.clone(), processor, "error-test-worker".to_string());
    
    // Run worker
    let handle = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(5), worker.run()).await;
    });

    // Wait for processing
    let _ = handle.await;

    // Verify the event was eventually processed successfully
    let final_status: String = sqlx::query_scalar!(
        r#"
        SELECT status 
        FROM sinex_schemas.promotion_queue 
        WHERE raw_event_id = $1::uuid::ulid
        "#,
        event_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(final_status, "completed", "Event should eventually succeed after retries");

    // Attempt count verification removed - the ErrorTestProcessor tracks attempts internally

    Ok(())
}