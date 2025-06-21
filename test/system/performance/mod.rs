//! Performance and load tests

pub mod load_testing;

use anyhow::Result;
use sinex_db::queries;
use sqlx::PgPool;
use std::time::{Duration, Instant};

#[sqlx::test]
async fn test_high_volume_ingestion(pool: PgPool) -> Result<()> {
    let start = Instant::now();
    let mut handles = vec![];
    
    // Spawn multiple tasks to insert events concurrently
    for i in 0..5 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            for j in 0..200 {
                queries::insert_raw_event(
                    &pool,
                    &format!("perf_test_{}", i),
                    &format!("test_event_{}", j),
                    "test-host",
                    serde_json::json!({
                        "task": i,
                        "event": j,
                        "data": "performance test payload"
                    }),
                    None,
                    None,
                    None
                ).await?;
            }
            Ok::<_, anyhow::Error>(())
        });
        handles.push(handle);
    }
    
    // Wait for all tasks
    for handle in handles {
        handle.await??;
    }
    
    let elapsed = start.elapsed();
    println!("Inserted 1000 events in {:?}", elapsed);
    
    // Verify count
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raw.events WHERE source LIKE 'perf_test_%'")
        .fetch_one(&pool)
        .await?;
    
    assert_eq!(count, 1000);
    assert!(elapsed < Duration::from_secs(5), "Ingestion took too long: {:?}", elapsed);
    
    Ok(())
}

#[sqlx::test]
async fn test_concurrent_processing_performance(pool: PgPool) -> Result<()> {
    // Insert test events
    for i in 0..100 {
        queries::insert_raw_event(
            &pool,
            "concurrent_test",
            "process_me",
            "test-host",
            serde_json::json!({ "id": i }),
            None,
            None,
            None
        ).await?;
    }
    
    let start = Instant::now();
    let mut handles = vec![];
    
    // Spawn workers to process events concurrently
    for worker_id in 0..4 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            let mut processed = 0;
            
            // Process events until none left
            loop {
                // Try to claim an event for processing
                let maybe_event: Option<(uuid::Uuid,)> = sqlx::query_as(
                    r#"
                    SELECT id::uuid
                    FROM raw.events
                    WHERE source = 'concurrent_test'
                      AND event_type = 'process_me'
                      AND NOT EXISTS (
                        SELECT 1 FROM raw.events processed 
                        WHERE processed.source = 'concurrent_test'
                          AND processed.event_type = 'processed'
                          AND processed.payload->>'original_id' = raw.events.id::text
                      )
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    "#
                )
                .fetch_optional(&pool)
                .await?;
                
                if let Some((event_id,)) = maybe_event {
                    // Simulate processing
                    tokio::task::yield_now().await;
                    
                    // Mark as processed
                    queries::insert_raw_event(
                        &pool,
                        "concurrent_test",
                        "processed",
                        "test-host",
                        serde_json::json!({
                            "worker_id": worker_id,
                            "original_id": event_id.to_string()
                        }),
                        None,
                        None,
                        None
                    ).await?;
                    
                    processed += 1;
                } else {
                    // No more events to process
                    break;
                }
            }
            
            Ok::<_, anyhow::Error>(processed)
        });
        handles.push(handle);
    }
    
    // Wait for all workers
    let mut total_processed = 0;
    for handle in handles {
        total_processed += handle.await??;
    }
    
    let elapsed = start.elapsed();
    println!("Processed {} events in {:?} with 4 workers", total_processed, elapsed);
    
    assert_eq!(total_processed, 100);
    assert!(elapsed < Duration::from_secs(3), "Processing took too long: {:?}", elapsed);
    
    Ok(())
}

#[sqlx::test]
async fn test_query_latency(pool: PgPool) -> Result<()> {
    // Insert test data
    for i in 0..1000 {
        queries::insert_raw_event(
            &pool,
            "latency_test",
            if i % 2 == 0 { "type_a" } else { "type_b" },
            "test-host",
            serde_json::json!({
                "value": i,
                "category": if i % 10 == 0 { "special" } else { "normal" }
            }),
            None,
            None,
            None
        ).await?;
    }
    
    // Test various query patterns
    let queries_to_test = vec![
        ("Simple count", "SELECT COUNT(*) FROM raw.events WHERE source = 'latency_test'"),
        ("Filtered count", "SELECT COUNT(*) FROM raw.events WHERE source = 'latency_test' AND event_type = 'type_a'"),
        ("JSON query", "SELECT COUNT(*) FROM raw.events WHERE source = 'latency_test' AND payload->>'category' = 'special'"),
        ("Recent events", "SELECT * FROM raw.events WHERE source = 'latency_test' ORDER BY ts_ingest DESC LIMIT 10"),
    ];
    
    for (name, query) in queries_to_test {
        let start = Instant::now();
        let _result = sqlx::query(query).fetch_all(&pool).await?;
        let elapsed = start.elapsed();
        
        println!("{}: {:?}", name, elapsed);
        assert!(elapsed < Duration::from_millis(100), "{} query too slow: {:?}", name, elapsed);
    }
    
    Ok(())
}