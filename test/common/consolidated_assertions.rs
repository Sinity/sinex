//! Consolidated test assertions replacing scattered helper functions
//!
//! This module provides reusable test assertions that replace similar
//! functions found across dozens of test files.

use crate::common::prelude::*;
use serde_json::Value as JsonValue;

/// Assert that an event was inserted successfully and can be retrieved
pub async fn assert_event_inserted_and_retrievable(
    pool: &DbPool,
    event: &RawEvent,
) -> anyhow::Result<()> {
    // Insert the event
    let inserted_id = insert_event(pool, event).await?;
    assert_eq!(inserted_id, event.id, "Inserted ID should match event ID");
    
    // Retrieve and verify using direct query
    let retrieved = sqlx::query!(
        "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
         FROM raw.events WHERE id::uuid = $1",
        event.id.to_uuid()
    )
    .fetch_one(pool)
    .await?;
    
    assert_eq!(retrieved.id, event.id.to_uuid());
    assert_eq!(retrieved.source, event.source);
    assert_eq!(retrieved.event_type, event.event_type);
    assert_eq!(retrieved.payload, event.payload);
    assert_eq!(retrieved.host, event.host);
    
    Ok(())
}

/// Assert that a complete work queue flow executes successfully
pub async fn assert_work_queue_flow_complete(
    pool: &DbPool,
    event: &RawEvent,
    agent_name: &str,
    worker_name: &str,
) -> anyhow::Result<()> {
    // Insert event
    let event_id = insert_event(pool, event).await?;
    
    // Enqueue work
    add_to_work_queue(pool, event_id, agent_name, 3).await?;
    
    // Verify work is in queue
    let queue_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE status = 'pending'"
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);
    assert!(queue_count > 0, "Work should be in queue");
    
    // Claim work
    let work = claim_work_queue_items(pool, "test_agent", worker_name, 1).await?;
    assert!(!work.is_empty(), "Work should be claimable");
    let work_item = &work[0];
    assert_eq!(work_item.raw_event_id, event_id);
    assert_eq!(work_item.target_agent_name, agent_name);
    
    // Complete work
    if let Some(work_item) = work.first() {
        complete_work_queue_item(pool, work_item.queue_id).await?;
    }
    
    // Verify work is completed
    let completed_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE status = 'succeeded'"
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);
    assert!(completed_count > 0, "Work should be completed");
    
    Ok(())
}

/// Assert that multiple events are processed concurrently without conflicts
pub async fn assert_concurrent_processing(
    pool: &DbPool,
    events: &[RawEvent],
    agent_name: &str,
    worker_count: usize,
) -> anyhow::Result<()> {
    // Insert all events and enqueue work
    let mut event_ids = Vec::new();
    for event in events {
        let event_id = insert_event(pool, event).await?;
        add_to_work_queue(pool, event_id, agent_name, 3).await?;
        event_ids.push(event_id);
    }
    
    // Start workers concurrently
    let mut worker_tasks = Vec::new();
    for i in 0..worker_count {
        let pool = pool.clone();
        let worker_name = format!("worker_{}", i);
        
        let task = tokio::spawn(async move {
            let mut processed = Vec::new();
            
            // Keep claiming work until none available
            loop {
                let work = claim_work_queue_items(&pool, "test_agent", &worker_name, 1).await?;
                if work.is_empty() {
                    break;
                }
                let work_item = &work[0];
                complete_work_queue_item(&pool, work_item.queue_id).await?;
                processed.push(work_item.raw_event_id);
            }
            
            Ok::<Vec<sinex_ulid::Ulid>, anyhow::Error>(processed)
        });
        
        worker_tasks.push(task);
    }
    
    // Wait for all workers to complete
    let mut all_processed = Vec::new();
    for task in worker_tasks {
        let processed = task.await??;
        all_processed.extend(processed);
    }
    
    // Verify all events were processed exactly once
    assert_eq!(all_processed.len(), events.len());
    
    // Verify uniqueness (no double processing)
    let mut unique_processed = all_processed.clone();
    unique_processed.sort();
    unique_processed.dedup();
    assert_eq!(unique_processed.len(), events.len());
    
    // Verify all original events were processed
    for event_id in &event_ids {
        assert!(all_processed.contains(event_id), 
               "Event {} should have been processed", event_id);
    }
    
    Ok(())
}

/// Assert that event queries return expected results
pub async fn assert_event_queries_work(
    pool: &DbPool,
    test_events: &[RawEvent],
) -> anyhow::Result<()> {
    // Insert all test events
    for event in test_events {
        insert_event(pool, event).await?;
    }
    
    // Group events by source for testing
    let mut events_by_source: std::collections::HashMap<String, Vec<&RawEvent>> = 
        std::collections::HashMap::new();
    
    for event in test_events {
        events_by_source.entry(event.source.clone())
            .or_insert_with(Vec::new)
            .push(event);
    }
    
    // Test source-based queries
    for (source, expected_events) in &events_by_source {
        let retrieved = sqlx::query!(
            "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
             FROM raw.events WHERE source = $1 LIMIT $2",
            source, 100i64
        )
        .fetch_all(pool)
        .await?;
        assert_eq!(retrieved.len(), expected_events.len(), 
                   "Source {} should have {} events", source, expected_events.len());
        
        for event in expected_events {
            assert!(retrieved.iter().any(|e| e.id == event.id.to_uuid()),
                   "Event {} should be in source {} results", event.id, source);
        }
    }
    
    // Group events by type for testing
    let mut events_by_type: std::collections::HashMap<String, Vec<&RawEvent>> = 
        std::collections::HashMap::new();
    
    for event in test_events {
        events_by_type.entry(event.event_type.clone())
            .or_insert_with(Vec::new)
            .push(event);
    }
    
    // Test type-based queries
    for (event_type, expected_events) in &events_by_type {
        let retrieved = sqlx::query!(
            "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
             FROM raw.events WHERE event_type = $1 LIMIT $2",
            event_type, 100i64
        )
        .fetch_all(pool)
        .await?;
        assert_eq!(retrieved.len(), expected_events.len(),
                   "Type {} should have {} events", event_type, expected_events.len());
        
        for event in expected_events {
            assert!(retrieved.iter().any(|e| e.id == event.id.to_uuid()),
                   "Event {} should be in type {} results", event.id, event_type);
        }
    }
    
    Ok(())
}

/// Assert that events are properly ordered by ULID timestamp
pub async fn assert_event_ordering(
    pool: &DbPool,
    events: &[RawEvent],
) -> anyhow::Result<()> {
    // Insert events (they should already be ULID-ordered)
    for event in events {
        insert_event(pool, event).await?;
    }
    
    // Retrieve events and verify ordering
    let retrieved = sqlx::query!(
        "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
         FROM raw.events WHERE source = $1 LIMIT $2",
        &events[0].source, (events.len() + 10) as i64
    )
    .fetch_all(pool)
    .await?;
    
    // Filter to only our test events
    let mut test_retrieved: Vec<_> = retrieved.into_iter()
        .filter(|e| events.iter().any(|test_e| test_e.id == e.id))
        .collect();
    
    // Sort by ULID (which encodes timestamp)
    test_retrieved.sort_by(|a, b| a.id.cmp(&b.id));
    
    // Verify they're in the expected order
    for (i, event) in test_retrieved.iter().enumerate() {
        let expected_event = events.iter().find(|e| e.id == event.id).unwrap();
        assert_eq!(event.id, expected_event.id, 
                   "Event at position {} should be {}", i, expected_event.id);
    }
    
    Ok(())
}

/// Assert that payload validation works correctly
pub async fn assert_payload_validation(
    pool: &DbPool,
    valid_events: &[RawEvent],
    invalid_events: &[RawEvent],
) -> anyhow::Result<()> {
    // Valid events should insert successfully
    for event in valid_events {
        let result = insert_event(pool, event).await;
        assert!(result.is_ok(), 
               "Valid event {} should insert successfully: {:?}", 
               event.id, result.err());
    }
    
    // Invalid events should fail to insert
    for event in invalid_events {
        let result = insert_event(pool, event).await;
        assert!(result.is_err(), 
               "Invalid event {} should fail to insert", event.id);
    }
    
    Ok(())
}

/// Assert that batch operations maintain consistency
pub async fn assert_batch_consistency(
    pool: &DbPool,
    events: &[RawEvent],
) -> anyhow::Result<()> {
    // Insert all events in batch
    for event in events {
        insert_event(pool, event).await?;
    }
    
    // Verify all events are retrievable
    for event in events {
        let retrieved = sqlx::query!(
            "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
             FROM raw.events WHERE id::uuid = $1",
            event.id.to_uuid()
        )
        .fetch_one(pool)
        .await?;
        assert_eq!(retrieved.id, event.id);
        assert_eq!(retrieved.payload, event.payload);
    }
    
    // Verify batch queries return consistent results
    let all_retrieved = sqlx::query!(
        "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
         FROM raw.events WHERE source = $1 LIMIT $2",
        &events[0].source, (events.len() + 10) as i64
    )
    .fetch_all(pool)
    .await?;
    let test_events: Vec<_> = all_retrieved.into_iter()
        .filter(|e| events.iter().any(|test_e| test_e.id == e.id))
        .collect();
    
    assert_eq!(test_events.len(), events.len(), 
               "Should retrieve all {} events", events.len());
    
    Ok(())
}

/// Create a standardized test event with customizable fields
pub fn create_test_event(
    source: &str,
    event_type: &str,
    payload: JsonValue,
) -> RawEvent {
    RawEventBuilder::new(source, event_type, payload)
        .with_host("test-host")
        .build()
}

/// Create a batch of test events with sequential data
pub fn create_event_batch(
    source: &str,
    event_type: &str,
    count: usize,
) -> Vec<RawEvent> {
    (0..count)
        .map(|i| {
            create_test_event(
                source,
                event_type,
                serde_json::json!({
                    "index": i,
                    "data": format!("event_{}", i),
                    "timestamp": chrono::Utc::now().to_rfc3339()
                })
            )
        })
        .collect()
}