use chrono::Utc;
use serde_json::json;
use sinex_db::models::RawEvent;
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
    .await?;

    assert_eq!(queue_count, 3, "Expected 3 promotion queue entries");

    // Step 4: Create a test worker to process the queue
    let worker_config = WorkerConfig {
        worker_id: "test-worker-001".to_string(),
        batch_size: 10,
        poll_interval: Duration::from_millis(100),
        processing_timeout: Duration::from_secs(30),
        max_retries: 3,
        retry_delay: Duration::from_secs(1),
    };

    let worker = Worker::new(worker_config, db_service.clone());

    // Define a simple processor that marks events as processed
    let processor = |event: RawEvent| async move {
        // Simulate some processing
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // In a real processor, we'd do something with the event
        println!("Processing event: {} - {}", event.source, event.event_type);
        
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    // Step 5: Run the worker for a short time
    let worker_handle = tokio::spawn(async move {
        worker.run_with_processor(processor).await
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
    .await?;

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
    .await?;

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
        let db = db_service.clone();
        let counter = processed_count.clone();
        
        let config = WorkerConfig {
            worker_id: format!("concurrent-worker-{}", worker_num),
            batch_size: 5,
            poll_interval: Duration::from_millis(50),
            processing_timeout: Duration::from_secs(10),
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
        };

        let handle = tokio::spawn(async move {
            let worker = Worker::new(config, db);
            
            let processor = |event: RawEvent| {
                let counter = counter.clone();
                async move {
                    // Simulate processing
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    
                    let mut count = counter.lock().await;
                    *count += 1;
                    
                    Ok::<(), Box<dyn std::error::Error>>(())
                }
            };

            let _ = timeout(Duration::from_secs(5), worker.run_with_processor(processor)).await;
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
    .await?;

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
    let config = WorkerConfig {
        worker_id: "error-test-worker".to_string(),
        batch_size: 1,
        poll_interval: Duration::from_millis(100),
        processing_timeout: Duration::from_secs(5),
        max_retries: 3,
        retry_delay: Duration::from_millis(100),
    };

    let worker = Worker::new(config, db_service.clone());
    
    let attempt_counter = Arc::new(tokio::sync::Mutex::new(0));
    let counter_clone = attempt_counter.clone();
    
    let processor = move |event: RawEvent| {
        let counter = counter_clone.clone();
        async move {
            let mut attempts = counter.lock().await;
            *attempts += 1;
            
            let fail_count = event.payload["fail_count"].as_u64().unwrap_or(0);
            
            if *attempts <= fail_count {
                Err("Simulated failure".into())
            } else {
                Ok(())
            }
        }
    };

    // Run worker
    let handle = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(5), worker.run_with_processor(processor)).await;
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
        event_id
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(final_status, "completed", "Event should eventually succeed after retries");

    // Verify attempt count
    let attempt_count = *attempt_counter.lock().await;
    assert_eq!(attempt_count, 3, "Should have taken 3 attempts (2 failures + 1 success)");

    Ok(())
}