use crate::common::{create_test_db_pool, events};
use sinex_db::{queries, models::RawEvent};
use sinex_ulid::Ulid;
use std::sync::{Arc, Barrier};
use tokio::runtime::Runtime;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[test]
fn test_worker_claim_exact_same_microsecond() {
    let rt = Runtime::new().unwrap();
    
    rt.block_on(async {
        let pool = create_test_db_pool().await.unwrap();
        
        // Insert event to be claimed
        let event = events::race_test_event("race");
        
        let inserted = queries::insert_event(&pool, &event).await.unwrap();
        let event_id = inserted.id;
        
        // Create high-precision synchronization
        let barrier = Arc::new(Barrier::new(2));
        let claims = Arc::new(AtomicU64::new(0));
        
        let pool1 = pool.clone();
        let pool2 = pool.clone();
        let barrier1 = barrier.clone();
        let barrier2 = barrier.clone();
        let claims1 = claims.clone();
        let claims2 = claims.clone();
        
        let handle1 = tokio::spawn(async move {
            barrier1.wait();
            
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
            
            if result.is_ok() && result.unwrap().rows_affected() > 0 {
                claims1.fetch_add(1, Ordering::SeqCst);
            }
        });
        
        let handle2 = tokio::spawn(async move {
            barrier2.wait();
            
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
            
            if result.is_ok() && result.unwrap().rows_affected() > 0 {
                claims2.fetch_add(1, Ordering::SeqCst);
            }
        });
        
        let _ = tokio::join!(handle1, handle2);
        
        let total_claims = claims.load(Ordering::SeqCst);
        println!("Total successful claims: {}", total_claims);
        
        // Check final state
        let final_state = sqlx::query!(
            "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
            event_id.to_uuid()
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        
        println!("Final payload: {}", final_state.payload);
        
        // Both workers might claim if there's a race condition
        assert_eq!(total_claims, 1, "Multiple workers claimed same event!");
    });
}

#[test]
fn test_event_causality_violation() {
    let rt = Runtime::new().unwrap();
    
    rt.block_on(async {
        let pool = create_test_db_pool().await.unwrap();
        let order_violations = Arc::new(AtomicU64::new(0));
        
        // Simulate dependent events processed out of order
        for _ in 0..100 {
            let pool1 = pool.clone();
            let pool2 = pool.clone();
            let violations = order_violations.clone();
            
            // Event A: Create file
            let event_a = events::file_created_event("/tmp/test.txt");
            
            // Event B: Modify file (depends on A)
            let event_b = events::file_modified_event("/tmp/test.txt");
            
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
            
            if res_b.is_ok() && res_a.is_ok() {
                // Check if B was inserted before A (causality violation)
                let b_id = res_b.unwrap().unwrap().id;
                let a_id = res_a.unwrap().unwrap().id;
                
                if b_id < a_id {
                    violations.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        
        let total_violations = order_violations.load(Ordering::SeqCst);
        println!("Causality violations detected: {}/100", total_violations);
        
        // This likely shows violations due to concurrent inserts
    });
}

#[test]
fn test_work_queue_thundering_herd() {
    let rt = Runtime::new().unwrap();
    
    rt.block_on(async {
        let pool = create_test_db_pool().await.unwrap();
        
        // Insert single event
        let event = events::adversarial_test_event("herd.test", serde_json::json!({"value": "prize"}));
        
        queries::insert_event(&pool, &event).await.unwrap();
        
        // Simulate 100 workers waking simultaneously
        let start = Instant::now();
        let mut handles = vec![];
        let successful_claims = Arc::new(AtomicU64::new(0));
        
        for i in 0..100 {
            let pool = pool.clone();
            let claims = successful_claims.clone();
            
            let handle = tokio::spawn(async move {
                // All workers try to claim work simultaneously
                let result = sqlx::query!(
                    r#"
                    SELECT id::uuid as id
                    FROM raw.events
                    WHERE event_type = 'herd.test'
                    AND NOT (payload ? 'claimed')
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    "#
                )
                .fetch_optional(&pool)
                .await;
                
                if result.is_ok() && result.unwrap().is_some() {
                    claims.fetch_add(1, Ordering::SeqCst);
                    println!("Worker {} claimed the event", i);
                }
            });
            
            handles.push(handle);
        }
        
        futures::future::join_all(handles).await;
        
        let elapsed = start.elapsed();
        let claims = successful_claims.load(Ordering::SeqCst);
        
        println!("Thundering herd results:");
        println!("- Time taken: {:?}", elapsed);
        println!("- Successful claims: {}", claims);
        println!("- Database connections stressed: 100");
        
        // Only 1 should succeed, but timing shows stress
        assert_eq!(claims, 1, "Multiple workers claimed single event");
    });
}

#[test]
fn test_concurrent_metadata_lost_update() {
    let rt = Runtime::new().unwrap();
    
    rt.block_on(async {
        let pool = create_test_db_pool().await.unwrap();
        
        // Insert test event
        let event = events::adversarial_test_event("metadata.test", serde_json::json!({
            "counter": 0,
            "updates": []
        }));
        
        let inserted = queries::insert_event(&pool, &event).await.unwrap();
        let event_id = inserted.id;
        
        // 10 concurrent updates
        let mut handles = vec![];
        
        for i in 0..10 {
            let pool = pool.clone();
            let id = event_id.clone();
            
            let handle = tokio::spawn(async move {
                // Read current value
                let current = sqlx::query!(
                    "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
                    id.to_uuid()
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                
                // Simulate processing time
                tokio::task::yield_now().await;
                
                // Update based on read value (classic lost update)
                let mut payload = current.payload;
                payload["counter"] = serde_json::json!(payload["counter"].as_i64().unwrap_or(0) + 1);
                payload["updates"].as_array_mut().unwrap().push(serde_json::json!(i));
                
                sqlx::query!(
                    "UPDATE raw.events SET payload = $2 WHERE id::uuid = $1::uuid",
                    id.to_uuid(),
                    payload
                )
                .execute(&pool)
                .await
                .unwrap();
            });
            
            handles.push(handle);
        }
        
        futures::future::join_all(handles).await;
        
        // Check final state
        let final_state = sqlx::query!(
            "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
            event_id.to_uuid()
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        
        let counter = final_state.payload["counter"].as_i64().unwrap_or(0);
        let updates = final_state.payload["updates"].as_array().unwrap().len();
        
        println!("Final counter: {} (expected: 10)", counter);
        println!("Update array length: {} (expected: 10)", updates);
        
        // Lost updates likely occurred
        assert_eq!(counter, 10, "Lost updates detected!");
        assert_eq!(updates, 10, "Lost update records!");
    });
}