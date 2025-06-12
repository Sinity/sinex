use sinex_db::{queries, models::RawEvent};
use sinex_ulid::Ulid;
use crate::common::create_test_db_pool;
use std::sync::Arc;
use tokio::sync::Semaphore;
use futures::future::join_all;

#[tokio::test]
async fn test_event_payload_approaching_1gb_limit() {
    let pool = create_test_db_pool().await.unwrap();
    
    // Create progressively larger payloads
    let sizes = vec![1_000_000, 10_000_000, 50_000_000, 100_000_000];
    
    for size in sizes {
        let huge_string = "x".repeat(size);
        let event = RawEvent {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "huge.payload".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: serde_json::json!({
                "data": huge_string,
                "size": size
            }),
        };
        
        match queries::insert_event(&pool, &event).await {
            Ok(_) => println!("Successfully inserted {}MB payload", size / 1_000_000),
            Err(e) => {
                println!("Failed at {}MB: {}", size / 1_000_000, e);
                // This is where we expect failure
                break;
            }
        }
    }
}

#[tokio::test]
async fn test_connection_pool_exhaustion() {
    let pool = create_test_db_pool().await.unwrap();
    
    // Default pool size is usually ~10-100
    let concurrent_ops = 200;
    let semaphore = Arc::new(Semaphore::new(concurrent_ops));
    let mut handles = vec![];
    
    for i in 0..concurrent_ops {
        let pool = pool.clone();
        let sem = semaphore.clone();
        
        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            
            // Try to hold connection for extended time
            let result = sqlx::query!("SELECT pg_sleep(30)")
                .execute(&pool)
                .await;
                
            match result {
                Ok(_) => println!("Connection {} completed", i),
                Err(e) => println!("Connection {} failed: {:?}", i, e),
            }
        });
        
        handles.push(handle);
    }
    
    // This should cause pool exhaustion and timeouts
    let results = join_all(handles).await;
    let failures = results.iter().filter(|r| r.is_err()).count();
    
    println!("Failed connections: {}/{}", failures, concurrent_ops);
    // Expect many failures due to pool exhaustion
}

#[tokio::test]
async fn test_events_spanning_chunk_boundary() {
    let pool = create_test_db_pool().await.unwrap();
    
    // TimescaleDB typically chunks by week
    // Insert events right at chunk boundary
    let chunk_boundary = chrono::Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0).unwrap()
        .and_utc();
    
    let before_boundary = chunk_boundary - chrono::Duration::milliseconds(1);
    let after_boundary = chunk_boundary + chrono::Duration::milliseconds(1);
    
    // Insert events
    let event_before = RawEvent {
        id: Ulid::from_datetime(before_boundary),
        source: "test".to_string(),
        event_type: "boundary.test".to_string(),
        ts_ingest: before_boundary,
        ts_orig: Some(before_boundary),
        host: "test".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: serde_json::json!({"position": "before"}),
    };
    
    let event_after = RawEvent {
        id: Ulid::from_datetime(after_boundary),
        source: "test".to_string(),
        event_type: "boundary.test".to_string(),
        ts_ingest: after_boundary,
        ts_orig: Some(after_boundary),
        host: "test".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: serde_json::json!({"position": "after"}),
    };
    
    queries::insert_event(&pool, &event_before).await.unwrap();
    queries::insert_event(&pool, &event_after).await.unwrap();
    
    // Query spanning boundary
    let result = sqlx::query!(
        r#"
        SELECT COUNT(*) as count
        FROM raw.events 
        WHERE ts_ingest >= $1 AND ts_ingest <= $2
        "#,
        before_boundary,
        after_boundary
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    println!("Events found spanning boundary: {}", result.count.unwrap());
    // This might fail if chunk boundary causes issues
    assert_eq!(result.count.unwrap(), 2, "Chunk boundary query failed");
}

#[tokio::test]
async fn test_ulid_btree_index_stress() {
    let pool = create_test_db_pool().await.unwrap();
    
    // Generate ULIDs that will cause B-tree page splits
    // These are carefully crafted to have similar prefixes
    let base_time = chrono::Utc::now();
    let mut handles = vec![];
    
    for i in 0..100 {
        let pool = pool.clone();
        let time = base_time + chrono::Duration::microseconds(i);
        
        let handle = tokio::spawn(async move {
            // Create 100 events with very similar timestamps
            for j in 0..100 {
                let event = RawEvent {
                    id: Ulid::from_datetime(time + chrono::Duration::nanoseconds(j)),
                    source: "btree".to_string(),
                    event_type: "stress.test".to_string(),
                    ts_ingest: chrono::Utc::now(),
                    ts_orig: None,
                    host: format!("worker-{}", i),
                    ingestor_version: None,
                    payload_schema_id: None,
                    payload: serde_json::json!({"i": i, "j": j}),
                };
                
                if let Err(e) = queries::insert_event(&pool, &event).await {
                    println!("Insert failed during B-tree stress: {}", e);
                }
            }
        });
        
        handles.push(handle);
    }
    
    join_all(handles).await;
    
    // Query during potential index reorganization
    let count = sqlx::query!("SELECT COUNT(*) as count FROM raw.events WHERE source = 'btree'")
        .fetch_one(&pool)
        .await
        .unwrap();
    
    println!("Total events after B-tree stress: {}", count.count.unwrap());
}

#[tokio::test]
async fn test_jsonb_size_update_overflow() {
    let pool = create_test_db_pool().await.unwrap();
    
    // Insert event with moderate payload
    let event = RawEvent {
        id: Ulid::new(),
        source: "test".to_string(),
        event_type: "grow.test".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: serde_json::json!({
            "initial": "small"
        }),
    };
    
    let inserted = queries::insert_event(&pool, &event).await.unwrap();
    
    // Try to update with increasingly large payloads
    for size_mb in [1, 10, 100, 500] {
        let huge_data = "x".repeat(size_mb * 1_000_000);
        
        let update_result = sqlx::query!(
            r#"
            UPDATE raw.events 
            SET payload = payload || jsonb_build_object('huge_data', $2::text)
            WHERE id::uuid = $1::uuid
            "#,
            inserted.id.as_uuid(),
            huge_data
        )
        .execute(&pool)
        .await;
        
        match update_result {
            Ok(_) => println!("Successfully appended {}MB to payload", size_mb),
            Err(e) => {
                println!("Failed to append {}MB: {}", size_mb, e);
                break;
            }
        }
    }
}