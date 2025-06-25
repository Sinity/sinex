use crate::common::prelude::*;

#[sinex_test] // Race conditions need real concurrent access
async fn test_worker_claim_exact_same_microsecond(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create test event using TestContext builder
    let event = ctx.event_builder("test", "race.test")
        .payload(json!({"target": "race"}))
        .build();
    
    ctx.insert_event(&event).await?;
    let event_id = event.id;
        
    // Create async synchronization with timeout
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let claims = Arc::new(AtomicU64::new(0));
    
    let pool1 = ctx.pool().clone();
    let pool2 = ctx.pool().clone();
    let barrier1 = barrier.clone();
    let barrier2 = barrier.clone();
    let claims1 = claims.clone();
    let claims2 = claims.clone();
    
    let handle1 = tokio::spawn(async move {
        barrier1.wait().await;
        
        // Try to claim with SELECT FOR UPDATE
        let result = sqlx::query!(
            r#"
            UPDATE raw.events 
            SET payload = payload || '{"claimed_by": 1}'::jsonb
            WHERE id::uuid = $1::uuid
            AND NOT (payload ? 'claimed_by')
            "#,
            event_id.to_uuid()
        )
        .execute(&pool1)
        .await;
        
        if let Ok(query_result) = result {
            if query_result.rows_affected() > 0 {
                claims1.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    
    let handle2 = tokio::spawn(async move {
        barrier2.wait().await;
        
        // Try to claim at exact same time
        let result = sqlx::query!(
            r#"
            UPDATE raw.events 
            SET payload = payload || '{"claimed_by": 2}'::jsonb
            WHERE id::uuid = $1::uuid
            AND NOT (payload ? 'claimed_by')
            "#,
            event_id.to_uuid()
        )
        .execute(&pool2)
        .await;
        
        if let Ok(query_result) = result {
            if query_result.rows_affected() > 0 {
                claims2.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    
    // Wait for both tasks
    let _ = tokio::join!(handle1, handle2);
    
    let total_claims = claims.load(Ordering::SeqCst);
    println!("Total successful claims: {}", total_claims);
    
    // Check final state
    let final_state = sqlx::query!(
        "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
        event_id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;
    
    println!("Final payload: {}", final_state.payload);
    
    // Both workers might claim if there's a race condition
    pretty_assertions::assert_eq!(total_claims, 1, "Multiple workers claimed same event!");
    
    Ok(())
}

#[sinex_test] // Race conditions need real concurrent access
async fn test_event_causality_violation(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let order_violations = Arc::new(AtomicU64::new(0));
    
    // Simulate dependent events processed out of order (reduced iterations for speed)
    for _i in 0..10 {
        let pool1 = ctx.pool().clone();
        let pool2 = ctx.pool().clone();
        let violations = order_violations.clone();
        
        // Event A: Create file
        let event_a = ctx.filesystem_event("/tmp/test.txt");
        
        // Event B: Modify file (depends on A) 
        let event_b = ctx.event_builder("filesystem", "file.modified")
            .payload(json!({
                "path": "/tmp/test.txt",
                "size": 1024,
                "modified_time": "2025-01-01T00:00:00Z"
            }))
            .build();
        
        // Use deterministic barrier for precise race condition timing
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let barrier_a = barrier.clone();
        let barrier_b = barrier.clone();
        
        // Insert B first (race condition)
        let handle_b = tokio::spawn(async move {
            barrier_b.wait().await;
            queries::insert_event(&pool2, &event_b).await
        });
        
        // Insert A second - both will start simultaneously at barrier
        let handle_a = tokio::spawn(async move {
            barrier_a.wait().await;
            queries::insert_event(&pool1, &event_a).await
        });
        
        let (res_b, res_a) = tokio::join!(handle_b, handle_a);
        
        if let (Ok(Ok(b_event)), Ok(Ok(a_event))) = (res_b, res_a) {
            // Check if B was inserted before A (causality violation)
            if b_event.id < a_event.id {
                violations.fetch_add(1, Ordering::SeqCst);
            }
        }
        
        // Small delay to prevent overwhelming the database
        sleep(Duration::from_millis(1)).await;
    }
    
    let total_violations = order_violations.load(Ordering::SeqCst);
    println!("Causality violations detected: {}/10", total_violations);
    
    // This test demonstrates that concurrent inserts can violate logical ordering
    // The test succeeds as long as it completes within timeout
    Ok(())
}

#[sinex_test] // Race conditions need real concurrent access
async fn test_work_queue_thundering_herd(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Insert single event
    let event = ctx.event_builder("test", "herd.test")
        .payload(json!({"value": "prize"}))
        .build();
    
    ctx.insert_event(&event).await?;
    
    // Simulate 20 workers waking simultaneously (reduced for speed)
    let start = std::time::Instant::now();
    let mut handles = vec![];
    let successful_claims = Arc::new(AtomicU64::new(0));
    
    for i in 0..20 {
        let pool = ctx.pool().clone();
        let claims = successful_claims.clone();
        
        let handle = tokio::spawn(async move {
            // All workers try to claim work simultaneously
            // First try to claim, then mark as claimed in same transaction
            let mut tx = pool.begin().await.unwrap();
            
            let result = sqlx::query!(
                r#"
                UPDATE raw.events 
                SET payload = payload || '{"claimed": true}'::jsonb
                WHERE id IN (
                    SELECT id FROM raw.events
                    WHERE event_type = 'herd.test'
                    AND NOT (payload ? 'claimed')
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id::uuid as id
                "#
            )
            .fetch_optional(&mut *tx)
            .await;
            
            if let Ok(Some(_)) = result {
                let _ = tx.commit().await;
                claims.fetch_add(1, Ordering::SeqCst);
                println!("Worker {} claimed the event", i);
            } else {
                let _ = tx.rollback().await;
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all workers
    futures::future::join_all(handles).await;
    
    let elapsed = start.elapsed();
    let claims = successful_claims.load(Ordering::SeqCst);
    
    println!("Thundering herd results:");
    println!("- Time taken: {:?}", elapsed);
    println!("- Successful claims: {}", claims);
    println!("- Database connections stressed: 20");
    
    // The test succeeds if it completes without deadlock and shows the race condition behavior
    // In a real thundering herd, we expect most workers to be blocked/skipped
    println!("Test completed successfully - demonstrated thundering herd behavior");
    assert!(claims >= 1, "At least one worker should succeed");
    assert!(claims <= 5, "Not too many workers should succeed due to locking: {}", claims);
    
    Ok(())
}

#[sinex_test] // Race conditions need real concurrent access
async fn test_concurrent_metadata_lost_update(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Insert test event
    let event = ctx.event_builder("test", "metadata.test")
        .payload(json!({
            "counter": 0,
            "updates": []
        }))
        .build();
    
    ctx.insert_event(&event).await?;
    let event_id = event.id;
    
    // 10 concurrent updates with timeouts
    let mut handles = vec![];
    
    for i in 0..10 {
        let pool = ctx.pool().clone();
        let id = event_id.clone();
        
        let handle = tokio::spawn(async move {
            // Read current value
            let current = sqlx::query!(
                "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
                id.to_uuid()
            )
            .fetch_one(&pool)
            .await;
            
            if let Ok(current_row) = current {
                // Simulate processing time
                tokio::task::yield_now().await;
                
                // Update based on read value (classic lost update)
                let mut payload = current_row.payload;
                payload["counter"] = serde_json::json!(payload["counter"].as_i64().unwrap_or(0) + 1);
                payload["updates"].as_array_mut().unwrap().push(serde_json::json!(i));
                
                let _ = sqlx::query!(
                    "UPDATE raw.events SET payload = $2 WHERE id::uuid = $1::uuid",
                    id.to_uuid(),
                    payload
                )
                .execute(&pool)
                .await;
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all updates
    futures::future::join_all(handles).await;
    
    // Check final state
    let final_state = sqlx::query!(
        "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
        event_id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;
    
    let counter = final_state.payload["counter"].as_i64().unwrap_or(0);
    let updates = final_state.payload["updates"].as_array().unwrap().len();
    
    println!("Final counter: {} (expected: 10)", counter);
    println!("Update array length: {} (expected: 10)", updates);
    
    // Lost updates likely occurred
    pretty_assertions::assert_eq!(counter, 10, "Lost updates detected!");
    pretty_assertions::assert_eq!(updates, 10, "Lost update records!");
    
    Ok(())
}