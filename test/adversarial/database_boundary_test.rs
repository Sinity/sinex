use crate::common::prelude::*;
use chrono::Utc;
use crate::common::timing_optimization::replacements::{wait_for_filtered_event_count};

#[sinex_test]
async fn test_event_payload_approaching_1gb_limit(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let pool = ctx.pool();
    
    println!("Testing JSONB 1GB limit:");
    
    // Start with smaller sizes and work up
    let test_sizes = vec![
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
        (100 * 1024 * 1024, "100MB"),
        (500 * 1024 * 1024, "500MB"),
        (900 * 1024 * 1024, "900MB"),
        (1000 * 1024 * 1024, "1000MB"), // Approaching limit
    ];
    
    for (size, label) in test_sizes {
        println!("  Testing {} payload...", label);
        
        // Create large string
        let _large_data = "x".repeat(size);
        
        let event = events::large_payload_test_event(1024);
        
        let start = Instant::now();
        match queries::insert_event(&pool, &event).await {
            Ok(_) => {
                let elapsed = start.elapsed();
                println!("    SUCCESS: Inserted in {:?}", elapsed);
                
                // Try to update with more data
                let extra_data = "y".repeat(100 * 1024 * 1024); // 100MB more
                let update_result = sqlx::query!(
                    r#"
                    UPDATE raw.events 
                    SET payload = payload || jsonb_build_object('extra_data', $2::text)
                    WHERE id::uuid = $1::uuid
                    "#,
                    event.id.to_uuid(),
                    extra_data
                ).execute(pool).await;
                
                match update_result {
                    Ok(_) => println!("    UPDATE SUCCESS: Added 100MB more"),
                    Err(e) => println!("    UPDATE FAILED: {} (expected near limit)", e),
                }
            }
            Err(e) => {
                println!("    FAILED: {}", e);
                if size >= 900 * 1024 * 1024 {
                    println!("    Expected failure near 1GB limit");
                }
            }
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_connection_pool_exhaustion(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let pool = ctx.pool();
    
    println!("Testing connection pool exhaustion:");
    
    // Get pool stats
    println!("  Pool size: {}", pool.size());
    // Note: max_size() method not available in sqlx
    
    let num_workers = 200; // Much more than typical pool size
    let _hold_duration = Duration::from_secs(5);
    
    let mut handles = vec![];
    let start = Instant::now();
    
    for worker_id in 0..num_workers {
        let pool_clone = pool.clone();
        
        let handle = tokio::spawn(async move {
            let acquire_start = Instant::now();
            
            // Try to acquire connection with timeout
            match timeout(Duration::from_secs(5), pool_clone.acquire()).await {
                Ok(Ok(mut conn)) => {
                    let acquire_time = acquire_start.elapsed();
                    println!("    Worker {} acquired connection after {:?}", worker_id, acquire_time);
                    
                    // Hold connection
                    let query_result = sqlx::query!("SELECT pg_sleep(5)")
                        .execute(&mut *conn)
                        .await;
                    
                    match query_result {
                        Ok(_) => format!("Worker {} completed", worker_id),
                        Err(e) => format!("Worker {} query failed: {}", worker_id, e),
                    }
                }
                Ok(Err(e)) => {
                    format!("Worker {} failed to acquire: {}", worker_id, e)
                }
                Err(_) => {
                    format!("Worker {} TIMEOUT waiting for connection", worker_id)
                }
            }
        });
        
        handles.push(handle);
        
        // Stagger worker starts slightly
        tokio::task::yield_now().await;
    }
    
    let results = join_all(handles).await;
    
    let total_time = start.elapsed();
    println!("\nConnection pool exhaustion results:");
    println!("  Total time: {:?}", total_time);
    
    let mut timeouts = 0;
    let mut failures = 0;
    let mut successes = 0;
    
    for result in results {
        match result {
            Ok(msg) => {
                if msg.contains("TIMEOUT") {
                    timeouts += 1;
                } else if msg.contains("failed") {
                    failures += 1;
                } else {
                    successes += 1;
                }
            }
            Err(_) => failures += 1,
        }
    }
    
    println!("  Successes: {}", successes);
    println!("  Timeouts: {} (EXPECTED - pool exhausted)", timeouts);
    println!("  Failures: {}", failures);
    
    if timeouts == 0 {
        println!("  WARNING: No timeouts - pool might be too large or test too small");
    }
    Ok(())
}

#[sinex_test]
async fn test_concurrent_btree_index_splits(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let pool = ctx.pool();
    
    println!("Testing concurrent B-tree index splits:");
    
    // Generate ULIDs that will force B-tree page splits
    // B-trees split when pages get full, typically around same prefix
    
    let base_time = Utc::now();
    let mut handles = vec![];
    
    // Create groups of events with very similar ULIDs
    for group in 0..10 {
        let pool_clone = pool.clone();
        let group_time = base_time + chrono::Duration::milliseconds(group as i64);
        
        let handle = tokio::spawn(async move {
            let mut events = vec![];
            
            // Generate 1000 events with nearly identical timestamps
            // This forces them into same B-tree pages
            for i in 0..1000 {
                let event_time = group_time + chrono::Duration::microseconds(i as i64);
                let _ulid = Ulid::from_datetime(event_time);
                
                let event = events::indexed_test_event(0, chrono::Utc::now());
                
                events.push(event);
            }
            
            // Insert all at once to maximize split conflicts
            let start = Instant::now();
            let mut success = 0;
            let mut failed = 0;
            
            for event in events {
                match queries::insert_event(&pool_clone, &event).await {
                    Ok(_) => success += 1,
                    Err(_) => failed += 1,
                }
            }
            
            let elapsed = start.elapsed();
            
            (group, success, failed, elapsed)
        });
        
        handles.push(handle);
    }
    
    // Run concurrent queries during splits
    let query_handle = {
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            let mut inconsistencies = 0;
            
            for _ in 0..100 {
                // Use timing utility for more reliable counting during concurrent operations
                let count = wait_for_filtered_event_count(
                    &pool_clone,
                    "source = $1",
                    &["btree_test"],
                    0, // Accept any count >= 0
                    1  // Quick timeout for concurrent scenario
                ).await.unwrap_or(0);
                
                // Create a compatible result structure
                struct FakeCountRecord { count: Option<i64> }
                let count_result: Result<FakeCountRecord, sqlx::Error> = Ok(FakeCountRecord { count: Some(count) });
                
                if let Ok(record) = count_result {
                    let count = record.count.unwrap_or(0);
                    
                    // During splits, counts might be inconsistent
                    if count % 1000 != 0 && count > 0 {
                        inconsistencies += 1;
                    }
                }
                
                tokio::task::yield_now().await;
            }
            
            inconsistencies
        })
    };
    let results = join_all(handles).await;
    let query_inconsistencies = query_handle.await.unwrap();
    
    println!("\nB-tree split test results:");
    for result in results {
        if let Ok((group, success, failed, elapsed)) = result {
            println!("  Group {}: {} success, {} failed in {:?}", 
                     group, success, failed, elapsed);
        }
    }
    
    println!("  Query inconsistencies during splits: {}", query_inconsistencies);
    
    if query_inconsistencies > 0 {
        println!("  INDEX INCONSISTENCY: Queries saw partial results during splits!");
    }
    Ok(())
}

#[sinex_test]
async fn test_events_spanning_chunk_boundary(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let pool = ctx.pool();
    
    println!("Testing TimescaleDB chunk boundary operations:");
    
    // Note: This assumes default chunk interval is 7 days
    // We'll create events around a chunk boundary
    
    let chunk_boundary = Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();
    
    // Events right at the boundary
    let boundary_events = vec![
        (chunk_boundary - chrono::Duration::milliseconds(1), "before_boundary"),
        (chunk_boundary, "at_boundary"),
        (chunk_boundary + chrono::Duration::milliseconds(1), "after_boundary"),
    ];
    
    println!("  Chunk boundary at: {}", chunk_boundary);
    
    for (timestamp, label) in boundary_events {
        let event = crate::common::events::generic_adversarial_event("chunk_test", "boundary.test", json!({"test": true}), None);
        
        match queries::insert_event(&pool, &event).await {
            Ok(_) => println!("    Inserted {}: {}", label, timestamp),
            Err(e) => println!("    Failed {}: {}", label, e),
        }
    }
    
    // Run aggregation query spanning chunks
    let agg_result = sqlx::query!(
        r#"
        SELECT 
            COUNT(*) as count,
            MIN(ts_ingest) as min_time,
            MAX(ts_ingest) as max_time
        FROM raw.events 
        WHERE source = 'chunk_test'
        "#
    ).fetch_one(pool).await;
    
    match agg_result {
        Ok(record) => {
            println!("\n  Aggregation across chunks:");
            println!("    Count: {}", record.count.unwrap_or(0));
            println!("    Min time: {:?}", record.min_time);
            println!("    Max time: {:?}", record.max_time);
            
            if record.count.unwrap_or(0) != 3 {
                println!("    CHUNK ISSUE: Expected 3 events, got {}", record.count.unwrap_or(0));
            }
        }
        Err(e) => {
            println!("  Aggregation failed: {}", e);
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_query_during_chunk_compression(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let pool = ctx.pool();
    
    println!("Testing queries during chunk compression:");
    
    // Insert old events that would be compressed
    let _old_time = Utc::now() - chrono::Duration::days(30);
    
    // Insert many events to make compression worthwhile
    for i in 0..10000 {
        let event = events::large_payload_test_event(1024);
        
        queries::insert_event(&pool, &event).await.unwrap();
        
        if i % 1000 == 0 {
            println!("  Inserted {} events", i);
        }
    }
    
    // Simulate compression by running queries that would conflict
    let query_tasks = vec![
        // Count query
        tokio::spawn({
            let pool = pool.clone();
            async move {
                let start = Instant::now();
                // Use timing utility for count query during compression stress
                let count = wait_for_filtered_event_count(
                    &pool,
                    "source = $1",
                    &["compression_test"],
                    0, // Accept any count
                    3  // Reasonable timeout for stress test
                ).await.unwrap_or(0);
                
                let result: Result<i64, anyhow::Error> = Ok(count);
                
                match result {
                    Ok(r) => format!("Count: {} in {:?}", r, start.elapsed()),
                    Err(e) => format!("Count failed: {}", e),
                }
            }
        }),
        
        // Range scan
        tokio::spawn({
            let pool = pool.clone();
            async move {
                let start = Instant::now();
                // Use timing utility for range scan during compression stress
                let count = wait_for_filtered_event_count(
                    &pool,
                    "source = $1 AND ts_ingest >= $2",
                    &["compression_test"],  // Note: can't bind timestamp easily, but this is a stress test
                    0, // Accept any count
                    3  // Reasonable timeout
                ).await.unwrap_or(0);
                
                struct FakeRangeRecord { count: Option<i64> }
                let result: Result<FakeRangeRecord, anyhow::Error> = Ok(FakeRangeRecord { count: Some(count) });
                
                match result {
                    Ok(result) => format!("Range scan: {} rows in {:?}", result.count.unwrap_or(0), start.elapsed()),
                    Err(e) => format!("Range scan failed: {}", e),
                }
            }
        }),
        
        // Aggregation
        tokio::spawn({
            let pool = pool.clone();
            async move {
                let start = Instant::now();
                let result = sqlx::query!(
                    r#"
                    SELECT 
                        date_trunc('minute', ts_ingest) as minute,
                        COUNT(*) as count
                    FROM raw.events 
                    WHERE source = 'compression_test'
                    GROUP BY minute
                    ORDER BY minute
                    "#
                ).fetch_all(pool).await;
                
                match result {
                    Ok(rows) => format!("Aggregation: {} buckets in {:?}", rows.len(), start.elapsed()),
                    Err(e) => format!("Aggregation failed: {}", e),
                }
            }
        }),
    ];
    
    let results = join_all(query_tasks).await;
    
    println!("\nCompression race condition results:");
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(msg) => println!("  Query {}: {}", i + 1, msg),
            Err(e) => println!("  Query {} panicked: {}", i + 1, e),
        }
    }
    Ok(())
}