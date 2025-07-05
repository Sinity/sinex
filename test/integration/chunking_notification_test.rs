use serde_json::json;
use sinex_core::{chunking::ChunkingService, RawEventBuilder};
use sinex_db::notifications::{NotificationService, NotificationMessage, WorkQueueAction, RealtimeEventProcessor};
use crate::common::prelude::*;
use tokio::time::{timeout, Duration};

#[sinex_test]
async fn test_chunking_integration(ctx: TestContext) -> TestResult {
    let chunking_service = ChunkingService::with_default_config();
    
    // Create large payload that will be chunked
    let mut large_data = json!({
        "type": "large_event",
        "data": {}
    });
    
    // Add lots of data to trigger chunking
    let mut data_object = serde_json::Map::new();
    for i in 0..1000 {
        data_object.insert(
            format!("field_{}", i),
            json!(format!("This is a large string value for field {} that makes the payload big enough to trigger chunking behavior", i))
        );
    }
    large_data["data"] = json!(data_object);
    
    // Test chunking
    let chunks = chunking_service.chunk_string(&serde_json::to_string(&large_data)?)?;
    assert!(chunks.len() > 1, "Large payload should be chunked into multiple parts");
    
    // Verify all chunks have BLAKE3 hashes
    for chunk in &chunks {
        assert!(chunk.blake3_hash.is_some(), "Each chunk should have a BLAKE3 hash");
    }
    
    // Test deduplication
    let (_deduped_chunks, dedup_stats) = sinex_core::chunking::deduplication::deduplicate_chunks(chunks);
    println!("Deduplication stats: original={}, unique={}, duplicates={}, bytes_saved={}", 
             dedup_stats.original_chunks, dedup_stats.unique_chunks, 
             dedup_stats.duplicate_chunks, dedup_stats.bytes_saved);
    
    Ok(())
}

#[sinex_test]
async fn test_notification_system_integration(ctx: TestContext) -> TestResult {
    // Apply all migrations including notification triggers
    sqlx::migrate!("./migrations").run(ctx.pool()).await?;
    
    // Create notification service
    let (mut notification_service, mut receiver) = NotificationService::new(ctx.pool().clone()).await?;
    
    // Start listening in background
    tokio::spawn(async move {
        if let Err(e) = notification_service.start_listening().await {
            eprintln!("Notification service error: {}", e);
        }
    });
    
    // Wait a bit for listeners to be set up
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Insert an event that should trigger notification
    let event = RawEventBuilder::new("test.source", "test.event", json!({"test": "data"}))
        .with_host("test-host")
        .build();
    
    insert_event(ctx.pool(), &event).await?;
    
    // Wait for notification with timeout
    let notification = timeout(Duration::from_secs(5), receiver.recv()).await
        .expect("Should receive notification within timeout")
        .expect("Should receive a notification");
    
    match notification {
        NotificationMessage::EventInserted(notification) => {
            assert_eq!(notification.source, "test.source");
            assert_eq!(notification.event_type, "test.event");
            assert_eq!(notification.host, "test-host");
            assert!(!notification.chunked);
            assert!(notification.chunk_count.is_none());
        }
        _ => panic!("Expected EventInserted notification"),
    }
    
    Ok(())
}

#[sinex_test]
async fn test_work_queue_notifications(ctx: TestContext) -> TestResult {
    // Apply all migrations including notification triggers
    sqlx::migrate!("./migrations").run(ctx.pool()).await?;
    
    // Create notification service
    let (mut notification_service, mut receiver) = NotificationService::new(ctx.pool().clone()).await?;
    
    // Start listening in background
    tokio::spawn(async move {
        if let Err(e) = notification_service.start_listening().await {
            eprintln!("Notification service error: {}", e);
        }
    });
    
    // Wait a bit for listeners to be set up
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Insert an event first
    let event = RawEventBuilder::new("test.source", "test.event", json!({"test": "data"}))
        .build();
    
    insert_event(ctx.pool(), &event).await?;
    
    // Skip the event insertion notification
    timeout(Duration::from_secs(1), receiver.recv()).await.ok();
    
    // Add to work queue - this should trigger a notification  
    add_to_work_queue(ctx.pool(), event.id, "test-agent", 1).await?;
    
    // Wait for work queue notification
    let notification = timeout(Duration::from_secs(5), receiver.recv()).await
        .expect("Should receive notification within timeout")
        .expect("Should receive a notification");
    
    match notification {
        NotificationMessage::WorkQueueUpdated(notification) => {
            assert_eq!(notification.event_id, event.id.to_string());
            assert_eq!(notification.agent_name, "test-agent");
            assert!(matches!(notification.action, WorkQueueAction::Added));
        }
        _ => panic!("Expected WorkQueueUpdated notification, got: {:?}", notification),
    }
    
    Ok(())
}

#[sinex_test]
async fn test_realtime_processor(ctx: TestContext) -> TestResult {
    // Apply all migrations including notification triggers
    sqlx::migrate!("./migrations").run(ctx.pool()).await?;
    
    // Create real-time processor
    let mut processor = RealtimeEventProcessor::new(ctx.pool().clone()).await?;
    
    // Start processor in background (it will log processing activities)
    tokio::spawn(async move {
        if let Err(e) = processor.start().await {
            eprintln!("Real-time processor error: {}", e);
        }
    });
    
    // Wait a bit for processor to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Insert a test event
    let event = RawEventBuilder::new("critical.system", "test.event", json!({"critical": true}))
        .build();
    
    insert_event(ctx.pool(), &event).await?;
    
    // The processor should handle the notification in the background
    // We can't easily test the processing without more complex setup,
    // but this verifies the integration compiles and runs
    
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    Ok(())
}

#[sinex_test]
async fn test_chunked_event_notification(ctx: TestContext) -> TestResult {
    // Apply all migrations including notification triggers
    sqlx::migrate!("./migrations").run(ctx.pool()).await?;
    
    // Create notification service
    let (mut notification_service, mut receiver) = NotificationService::new(ctx.pool().clone()).await?;
    
    // Start listening in background
    tokio::spawn(async move {
        if let Err(e) = notification_service.start_listening().await {
            eprintln!("Notification service error: {}", e);
        }
    });
    
    // Wait a bit for listeners to be set up
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create an event with chunk_info to simulate chunked payload
    let chunked_payload = json!({
        "data": "This is chunked data",
        "chunk_info": {
            "total_chunks": 3,
            "chunk_index": 0,
            "chunk_id": "chunk_001"
        }
    });
    
    let event = RawEventBuilder::new("test.source", "chunked.event", chunked_payload)
        .build();
    
    insert_event(ctx.pool(), &event).await?;
    
    // Wait for notification
    let notification = timeout(Duration::from_secs(5), receiver.recv()).await
        .expect("Should receive notification within timeout")
        .expect("Should receive a notification");
    
    match notification {
        NotificationMessage::EventInserted(notification) => {
            assert_eq!(notification.source, "test.source");
            assert_eq!(notification.event_type, "chunked.event");
            assert!(notification.chunked, "Event should be marked as chunked");
            assert_eq!(notification.chunk_count, Some(3));
        }
        _ => panic!("Expected EventInserted notification"),
    }
    
    Ok(())
}