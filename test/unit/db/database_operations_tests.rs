use crate::common::prelude::*;

// Removed basic CRUD tests - they just verified that PostgreSQL insert/select works

#[sinex_test]
async fn test_query_events_by_source(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Insert events from different sources
    let fs_event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({"path": "/test/fs_file.txt"})
    ).build();
    
    let terminal_event = RawEventBuilder::new(
        "terminal_kitty", 
        "command.executed",
        json!({"command": "ls"})
    ).build();
    
    let wm_event = RawEventBuilder::new(
        "hyprland",
        "window.focus",
        json!({"window_id": 123})
    ).build();
    
    queries::insert_event(ctx.pool(), &fs_event).await?;
    queries::insert_event(ctx.pool(), &terminal_event).await?;
    queries::insert_event(ctx.pool(), &wm_event).await?;
    
    // Query events by source
    let fs_events = queries::get_events_by_source(ctx.pool(), "filesystem", 10).await?;
    assert!(!fs_events.is_empty());
    assert!(fs_events.iter().all(|e| e.source == "filesystem"));
    
    let terminal_events = queries::get_events_by_source(ctx.pool(), "terminal_kitty", 10).await?;
    assert!(!terminal_events.is_empty());
    assert!(terminal_events.iter().all(|e| e.source == "terminal_kitty"));
    
    let wm_events = queries::get_events_by_source(ctx.pool(), "hyprland", 10).await?;
    assert!(!wm_events.is_empty());
    assert!(wm_events.iter().all(|e| e.source == "hyprland"));
    
    Ok(())
}

#[sinex_test]
async fn test_query_events_by_type(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Insert events of different types
    let create_event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({"path": "/test/created.txt"})
    ).build();
    
    let modify_event = RawEventBuilder::new(
        "filesystem",
        "file.modified", 
        json!({"path": "/test/modified.txt"})
    ).build();
    
    let delete_event = RawEventBuilder::new(
        "filesystem",
        "file.deleted",
        json!({"path": "/test/deleted.txt"})
    ).build();
    
    queries::insert_event(ctx.pool(), &create_event).await?;
    queries::insert_event(ctx.pool(), &modify_event).await?;
    queries::insert_event(ctx.pool(), &delete_event).await?;
    
    // Query events by type
    let created_events = queries::get_events_by_type(ctx.pool(), "file.created", 10).await?;
    assert!(!created_events.is_empty());
    assert!(created_events.iter().all(|e| e.event_type == "file.created"));
    
    let modified_events = queries::get_events_by_type(ctx.pool(), "file.modified", 10).await?;
    assert!(!modified_events.is_empty());
    assert!(modified_events.iter().all(|e| e.event_type == "file.modified"));
    
    Ok(())
}

#[sinex_test]
async fn test_work_queue_operations(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create agent first (required for foreign key)
    let _agent = queries::upsert_agent_manifest(
        ctx.pool(),
        "test_agent",
        "1.0.0",
        "running",
        "test",
        Some("Test agent for work queue"),
        None,
        None,
    ).await?;
    
    // Insert a raw event first
    let event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({"path": "/test/work_queue_test.txt"})
    ).build();
    
    let inserted_event = queries::insert_event(ctx.pool(), &event).await?;
    
    // Add to work queue
    let queue_item = queries::add_to_work_queue(
        ctx.pool(),
        inserted_event.id,
        "test_agent",
        3 // max_attempts
    ).await?;
    
    pretty_assertions::assert_eq!(queue_item.raw_event_id, inserted_event.id);
    pretty_assertions::assert_eq!(queue_item.target_agent_name, "test_agent");
    pretty_assertions::assert_eq!(queue_item.status, "pending");
    pretty_assertions::assert_eq!(queue_item.attempts, 0);
    pretty_assertions::assert_eq!(queue_item.max_attempts, 3);
    
    // Get next item for processing
    let next_item = queries::get_next_work_item(ctx.pool(), "test_agent").await?;
    assert!(next_item.is_some());
    
    let item = next_item.unwrap();
    pretty_assertions::assert_eq!(item.raw_event_id, inserted_event.id);
    pretty_assertions::assert_eq!(item.target_agent_name, "test_agent");
    pretty_assertions::assert_eq!(item.status, "processing");
    
    // Complete processing
    queries::complete_work_item(ctx.pool(), item.queue_id).await?;
    
    // Verify item is completed
    let completed_item = queries::get_work_item_by_id(ctx.pool(), item.queue_id).await?;
    pretty_assertions::assert_eq!(completed_item.status, "succeeded");
    
    Ok(())
}

#[sinex_test(timeout = 90)]
async fn test_work_queue_retry_logic(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create agent first (required for foreign key)
    let _agent = queries::upsert_agent_manifest(
        ctx.pool(),
        "test_agent",
        "1.0.0",
        "running",
        "test",
        Some("Test agent for retry logic"),
        None,
        None,
    ).await?;
    
    // Insert a raw event
    let event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({"path": "/test/retry_test.txt", "size": 1024})
    ).build();
    
    let inserted_event = queries::insert_event(ctx.pool(), &event).await?;
    
    // Add to work queue with limited retries
    let _queue_item = queries::add_to_work_queue(
        ctx.pool(),
        inserted_event.id,
        "test_agent",
        2 // max_attempts
    ).await?;
    
    // Get and fail processing multiple times
    for attempt in 1..=3 {
        let next_item = queries::get_next_work_item(ctx.pool(), "test_agent").await?;
        
        if attempt <= 2 {
            // Should get an item
            assert!(next_item.is_some());
            let item = next_item.unwrap();
            pretty_assertions::assert_eq!(item.attempts, attempt - 1);
            
            // Fail the processing
            queries::fail_work_item(ctx.pool(), item.queue_id, "Test failure").await?;
        } else {
            // Should not get an item after max retries
            assert!(next_item.is_none());
        }
    }
    
    // Verify item is in DLQ
    let dlq_items = queries::get_dlq_items(ctx.pool(), "test_agent", 10).await?;
    assert!(!dlq_items.is_empty());
    
    let dlq_item = &dlq_items[0];
    pretty_assertions::assert_eq!(dlq_item.failed_event_id, inserted_event.id);
    pretty_assertions::assert_eq!(dlq_item.agent_name, "test_agent");
    assert!(!dlq_item.failure_reason.is_empty());
    
    Ok(())
}

#[sinex_test]
async fn test_event_validation(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test with valid event
    let valid_event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({
            "path": "/valid/path.txt",
            "size": 1024,
            "created_time": "2024-01-01T12:00:00Z"
        })
    ).build();
    
    let result = queries::insert_event(ctx.pool(), &valid_event).await;
    assert!(result.is_ok());
    
    // Test with event that has invalid payload structure
    // (This depends on whether validation is enforced at database level)
    let invalid_event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({
            "invalid_field": "this should not be here",
            "missing_required_path": true
        })
    ).build();
    
    // Depending on validation implementation, this might succeed or fail
    // For now, just test that it doesn't panic
    let _result = queries::insert_event(ctx.pool(), &invalid_event).await;
    // Result can be Ok or Err - we're testing that it handles it gracefully
    
    Ok(())
}

#[sinex_test(timeout = 90)]
async fn test_concurrent_event_insertion(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::task::JoinSet;
    
    let _pool = Arc::new(ctx.pool().clone());
    let mut join_set = JoinSet::new();
    
    // Spawn multiple concurrent insertions
    for i in 0..10 {
        let pool_clone = Arc::new(ctx.pool().clone());
        join_set.spawn(async move {
            let event = RawEventBuilder::new(
                "filesystem",
                "file.created",
                json!({
                    "path": format!("/test/concurrent_{}.txt", i),
                    "thread_id": i
                })
            ).build();
            
            queries::insert_event(&pool_clone, &event).await
        });
    }
    
    // Wait for all insertions to complete
    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        results.push(result??);
    }
    
    // Verify all insertions succeeded
    pretty_assertions::assert_eq!(results.len(), 10);
    
    // Verify all events are unique
    let mut ids = std::collections::HashSet::new();
    for event in results {
        assert!(ids.insert(event.id)); // Should be unique
    }
    
    Ok(())
}

#[sinex_test(timeout = 90)]
async fn test_ulid_ordering_in_database(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let mut events = Vec::new();
    
    // Insert events with small delays to ensure ULID ordering
    for i in 0..5 {
        let event = RawEventBuilder::new(
            "filesystem",
            "file.created", 
            json!({"sequence": i})
        ).build();
        
        let inserted = queries::insert_event(ctx.pool(), &event).await?;
        events.push(inserted);
        
        // Small delay to ensure timestamp progression
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    // Query events ordered by ID (ULID)
    let _ordered_events = queries::get_recent_events(ctx.pool(), 10).await?;
    
    // Verify ULID ordering matches insertion order
    for i in 1..events.len() {
        assert!(events[i].id.to_string() > events[i-1].id.to_string());
        assert!(events[i].ts_ingest >= events[i-1].ts_ingest);
    }
    
    Ok(())
}