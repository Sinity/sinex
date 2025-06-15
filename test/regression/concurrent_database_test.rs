use sinex_db::{queries, models::RawEvent};
use crate::common::create_test_db_pool;
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test]
async fn test_concurrent_ulid_generation() {
    let pool = create_test_db_pool().await.unwrap();
    let num_tasks = 10;
    let events_per_task = 100;
    let barrier = Arc::new(Barrier::new(num_tasks));
    
    let mut handles = vec![];
    
    for task_id in 0..num_tasks {
        let pool = pool.clone();
        let barrier = barrier.clone();
        
        let handle = tokio::spawn(async move {
            // Wait for all tasks to be ready
            barrier.wait().await;
            
            let mut ulids = vec![];
            for i in 0..events_per_task {
                let event = RawEvent {
                    id: sinex_ulid::Ulid::new(),
                    source: "test".to_string(),
                    event_type: "concurrent.test".to_string(),
                    ts_ingest: chrono::Utc::now(),
                    ts_orig: None,
                    host: format!("task-{}", task_id),
                    ingestor_version: Some("test".to_string()),
                    payload_schema_id: None,
                    payload: serde_json::json!({
                        "task": task_id,
                        "event": i
                    }),
                };
                
                let result = queries::insert_event(&pool, &event).await.unwrap();
                ulids.push(result.id);
            }
            ulids
        });
        
        handles.push(handle);
    }
    
    // Collect all ULIDs
    let mut all_ulids = vec![];
    for handle in handles {
        let ulids = handle.await.unwrap();
        all_ulids.extend(ulids);
    }
    
    // Check for duplicates - this might FAIL under high concurrency
    let unique_ulids: std::collections::HashSet<_> = all_ulids.iter().collect();
    assert_eq!(
        all_ulids.len(), 
        unique_ulids.len(), 
        "Found {} duplicate ULIDs in {} total", 
        all_ulids.len() - unique_ulids.len(),
        all_ulids.len()
    );
}

#[tokio::test]
async fn test_worker_double_processing() {
    let pool = create_test_db_pool().await.unwrap();
    
    // Insert a test event
    let event = RawEvent {
        id: sinex_ulid::Ulid::new(),
        source: "test".to_string(),
        event_type: "worker.test".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test-host".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: serde_json::json!({
            "test_data": "worker processing test"
        }),
    };
    let inserted = queries::insert_event(&pool, &event).await.unwrap();
    
    // Simulate two workers trying to claim the same event
    let pool1 = pool.clone();
    let pool2 = pool.clone();
    let event_id = inserted.id;
    
    let barrier = Arc::new(Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();
    
    let worker1 = tokio::spawn(async move {
        b1.wait().await;
        // Try to claim event for processing
        sqlx::query!(
            "UPDATE raw.events SET payload = payload || '{\"processed_by\": \"worker1\"}'::jsonb 
             WHERE id::uuid = $1::uuid",
            event_id.as_uuid()
        )
        .execute(&pool1)
        .await
    });
    
    let worker2 = tokio::spawn(async move {
        b2.wait().await;
        // Try to claim same event
        sqlx::query!(
            "UPDATE raw.events SET payload = payload || '{\"processed_by\": \"worker2\"}'::jsonb 
             WHERE id::uuid = $1::uuid",
            event_id.as_uuid()
        )
        .execute(&pool2)
        .await
    });
    
    let (r1, r2) = tokio::join!(worker1, worker2);
    
    // Both should succeed because there's no proper locking!
    // This demonstrates the need for SELECT FOR UPDATE SKIP LOCKED
    assert!(r1.is_ok() && r2.is_ok(), "Both workers modified the same event!");
    
    // Check final state
    let final_event = sqlx::query!(
        "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
        event_id.as_uuid()
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    println!("Final payload: {}", final_event.payload);
    // This will show that both workers processed it - a bug!
}