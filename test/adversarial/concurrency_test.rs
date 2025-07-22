// # Concurrency Test Suite
//
// Comprehensive concurrency and race condition testing.
// This module tests system behavior under concurrent access patterns.
//
// ## Test Categories
// - **Race Conditions**: Worker claiming, event causality, data consistency
// - **Worker Coordination**: Synchronization, deadlock prevention, resource sharing
// - **Database Concurrency**: Transaction isolation, lock contention, deadlock detection
// - **Memory Concurrency**: Shared state, atomic operations, cache coherency

use crate::common::prelude::*;
use chrono::Utc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

// =============================================================================
// Race Condition Tests
// =============================================================================

/// Test worker claim race conditions at microsecond precision
#[sinex_test]
async fn test_worker_claim_exact_same_microsecond(ctx: TestContext) -> TestResult {
    // Insert event to be claimed
    let event = ctx.create_test_event(
        "race",
        "race.test",
        json!({ "test": "race_condition" }),
    );
    ctx.insert_event(&event).await?;
    let event_id = event.id;

    // Create high-precision synchronization
    let barrier = Arc::new(Barrier::new(2));
    let claims = Arc::new(AtomicU64::new(0));

    let pool1 = ctx.pool().clone();
    let pool2 = ctx.pool().clone();
    let barrier1 = barrier.clone();
    let barrier2 = barrier.clone();
    let claims1 = claims.clone();
    let claims2 = claims.clone();

    let handle1 = tokio::spawn(async move {
        barrier1.wait();

        // Try to claim with SELECT FOR UPDATE - Testing concurrent claim behavior
        let result = sqlx::query!(
            r#"
                UPDATE core.events
                SET payload = payload || '{"claimed_by": 1}'::jsonb
                WHERE event_id = $1::ulid
                AND NOT (payload ? 'claimed_by')
                "#,
            event_id
        )
        .execute(&pool1)
        .await;

        if let Ok(result) = result {
            if result.rows_affected() > 0 {
                claims1.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let handle2 = tokio::spawn(async move {
        barrier2.wait();

        // Try to claim at exact same time - Testing race condition
        let result = sqlx::query!(
            r#"
                UPDATE core.events
                SET payload = payload || '{"claimed_by": 2}'::jsonb
                WHERE event_id = $1::ulid
                AND NOT (payload ? 'claimed_by')
                "#,
            event_id
        )
        .execute(&pool2)
        .await;

        if let Ok(result) = result {
            if result.rows_affected() > 0 {
                claims2.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let _ = tokio::join!(handle1, handle2);

    let total_claims = claims.load(Ordering::SeqCst);
    println!("Total successful claims: {}", total_claims);

    // Exactly one should succeed
    assert_eq!(total_claims, 1, "Race condition: multiple workers claimed same event");

    // Verify which worker won
    let result = sqlx::query!(
        r#"SELECT payload FROM core.events WHERE event_id = $1::ulid"#,
        event_id
    )
    .fetch_one(ctx.pool())
    .await?;

    let claimed_by = result.payload.get("claimed_by")
        .and_then(|v| v.as_i64())
        .expect("Event should have claimed_by field");

    println!("Event claimed by worker: {}", claimed_by);
    Ok(())
}

/// Test high-frequency concurrent event insertion
#[sinex_test]
async fn test_high_frequency_concurrent_insertion(ctx: TestContext) -> TestResult {
    let concurrent_workers = 10;
    let events_per_worker = 100;
    let total_expected = concurrent_workers * events_per_worker;

    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = vec![];

    for worker_id in 0..concurrent_workers {
        let ctx_clone = ctx.clone();
        let counter_clone = counter.clone();

        let handle = tokio::spawn(async move {
            for event_num in 0..events_per_worker {
                let event = ctx_clone.create_test_event(
                    "concurrent",
                    "insertion",
                    json!({
                        "worker_id": worker_id,
                        "event_num": event_num,
                        "timestamp": Utc::now().to_rfc3339()
                    }),
                );

                match ctx_clone.insert_event(&event).await {
                    Ok(_) => {
                        counter_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Worker {} failed to insert event {}: {}", 
                            worker_id, event_num, e);
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    let total_inserted = counter.load(Ordering::Relaxed);
    let rate = total_inserted as f64 / elapsed.as_secs_f64();

    println!("Concurrent insertion results:");
    println!("  Workers: {}", concurrent_workers);
    println!("  Events per worker: {}", events_per_worker);
    println!("  Total inserted: {}/{}", total_inserted, total_expected);
    println!("  Time: {:?}", elapsed);
    println!("  Rate: {:.2} events/sec", rate);

    assert!(
        total_inserted >= total_expected * 95 / 100,
        "Too many failed insertions: {}/{}",
        total_inserted, total_expected
    );

    Ok(())
}

// =============================================================================
// Worker Coordination Tests
// =============================================================================

/// Test distributed lock behavior with multiple workers
#[sinex_test]
async fn test_distributed_lock_behavior(ctx: TestContext) -> TestResult {
    // Simulate distributed lock using database advisory locks
    let lock_id = 12345i64;
    let workers = 5;
    let work_duration = tokio::time::Duration::from_millis(100);

    let lock_acquisitions = Arc::new(AtomicU64::new(0));
    let concurrent_holders = Arc::new(AtomicU64::new(0));
    let max_concurrent = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for worker_id in 0..workers {
        let ctx_clone = ctx.clone();
        let acquisitions = lock_acquisitions.clone();
        let concurrent = concurrent_holders.clone();
        let max_conc = max_concurrent.clone();

        let handle = tokio::spawn(async move {
            for attempt in 0..3 {
                // Try to acquire advisory lock
                let lock_result = sqlx::query!(
                    "SELECT pg_try_advisory_lock($1) as acquired",
                    lock_id
                )
                .fetch_one(ctx_clone.pool())
                .await;

                match lock_result {
                    Ok(row) if row.acquired.unwrap_or(false) => {
                        acquisitions.fetch_add(1, Ordering::SeqCst);
                        
                        // Track concurrent holders (should always be 1)
                        let current = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                        
                        // Update max concurrent
                        let mut max = max_conc.load(Ordering::SeqCst);
                        while current > max {
                            match max_conc.compare_exchange(
                                max, current, Ordering::SeqCst, Ordering::SeqCst
                            ) {
                                Ok(_) => break,
                                Err(actual) => max = actual,
                            }
                        }

                        println!("Worker {} acquired lock (attempt {})", worker_id, attempt);
                        
                        // Simulate work
                        tokio::time::sleep(work_duration).await;
                        
                        // Release lock
                        let _ = sqlx::query!("SELECT pg_advisory_unlock($1)", lock_id)
                            .execute(ctx_clone.pool())
                            .await;
                            
                        concurrent.fetch_sub(1, Ordering::SeqCst);
                        println!("Worker {} released lock", worker_id);
                        
                        break;
                    }
                    _ => {
                        println!("Worker {} failed to acquire lock (attempt {})", 
                            worker_id, attempt);
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        handle.await?;
    }

    let total_acquisitions = lock_acquisitions.load(Ordering::SeqCst);
    let max_concurrent_holders = max_concurrent.load(Ordering::SeqCst);

    println!("Distributed lock test results:");
    println!("  Total acquisitions: {}", total_acquisitions);
    println!("  Max concurrent holders: {}", max_concurrent_holders);

    assert_eq!(max_concurrent_holders, 1, "Multiple workers held lock simultaneously!");
    assert!(total_acquisitions >= workers / 2, "Too few successful lock acquisitions");

    Ok(())
}

/// Test event causality ordering under concurrent processing
#[sinex_test]
async fn test_event_causality_concurrent_processing(ctx: TestContext) -> TestResult {
    // Create a chain of causally related events
    let chain_length = 10;
    let mut event_ids = Vec::new();

    // Insert initial event
    let initial = ctx.create_test_event(
        "causality",
        "chain.start",
        json!({ "sequence": 0 }),
    );
    ctx.insert_event(&initial).await?;
    event_ids.push(initial.id);

    // Create causal chain
    for i in 1..chain_length {
        let event = ctx.create_test_event_with_sources(
            "causality",
            "chain.link",
            json!({ "sequence": i }),
            vec![event_ids[i-1]],
        );
        ctx.insert_event(&event).await?;
        event_ids.push(event.id);
    }

    // Process events concurrently but respect causality
    let processed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = vec![];

    for (idx, event_id) in event_ids.iter().enumerate() {
        let ctx_clone = ctx.clone();
        let processed_clone = processed.clone();
        let event_id = *event_id;
        let prev_id = if idx > 0 { Some(event_ids[idx-1]) } else { None };

        let handle = tokio::spawn(async move {
            // If this event has a causal dependency, wait for it
            if let Some(prev) = prev_id {
                let mut attempts = 0;
                loop {
                    let proc = processed_clone.lock().unwrap();
                    if proc.contains(&prev) {
                        break;
                    }
                    drop(proc);
                    
                    attempts += 1;
                    if attempts > 100 {
                        panic!("Causal dependency not satisfied after 100 attempts");
                    }
                    
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            }

            // Process this event
            println!("Processing event {} (sequence {})", event_id, idx);
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            // Mark as processed
            processed_clone.lock().unwrap().push(event_id);
        });

        handles.push(handle);
    }

    // Wait for all processing
    for handle in handles {
        handle.await?;
    }

    // Verify causal order was maintained
    let final_order = processed.lock().unwrap().clone();
    println!("Processing order: {:?}", final_order);

    for i in 1..final_order.len() {
        let current_idx = event_ids.iter().position(|&id| id == final_order[i]).unwrap();
        let prev_idx = event_ids.iter().position(|&id| id == final_order[i-1]).unwrap();
        
        assert!(
            current_idx > prev_idx,
            "Causal order violated: {} processed before {}",
            current_idx, prev_idx
        );
    }

    Ok(())
}

// =============================================================================
// Database Concurrency Tests
// =============================================================================

/// Test transaction isolation levels
#[sinex_test]
async fn test_transaction_isolation_levels(ctx: TestContext) -> TestResult {
    // Test different isolation levels and their effects
    
    // Insert test event
    let event = ctx.create_test_event(
        "isolation",
        "test",
        json!({ "counter": 0 }),
    );
    ctx.insert_event(&event).await?;
    let event_id = event.id;

    // Test READ COMMITTED (PostgreSQL default)
    let pool1 = ctx.pool().clone();
    let pool2 = ctx.pool().clone();

    let handle1 = tokio::spawn(async move {
        let mut tx = pool1.begin().await?;
        
        // Read current value
        let row = sqlx::query!(
            "SELECT payload FROM core.events WHERE event_id = $1::ulid",
            event_id
        )
        .fetch_one(&mut *tx)
        .await?;
        
        let counter = row.payload.get("counter").and_then(|v| v.as_i64()).unwrap_or(0);
        println!("Transaction 1 read counter: {}", counter);
        
        // Sleep to let other transaction update
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Update based on read value
        sqlx::query!(
            r#"
            UPDATE core.events 
            SET payload = payload || jsonb_build_object('counter', $2::bigint)
            WHERE event_id = $1::ulid
            "#,
            event_id,
            counter + 1
        )
        .execute(&mut *tx)
        .await?;
        
        tx.commit().await?;
        println!("Transaction 1 committed with counter: {}", counter + 1);
        Ok::<_, anyhow::Error>(counter + 1)
    });

    let handle2 = tokio::spawn(async move {
        // Small delay to ensure T1 reads first
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        
        let mut tx = pool2.begin().await?;
        
        // Read and update
        let row = sqlx::query!(
            "SELECT payload FROM core.events WHERE event_id = $1::ulid",
            event_id
        )
        .fetch_one(&mut *tx)
        .await?;
        
        let counter = row.payload.get("counter").and_then(|v| v.as_i64()).unwrap_or(0);
        println!("Transaction 2 read counter: {}", counter);
        
        sqlx::query!(
            r#"
            UPDATE core.events 
            SET payload = payload || jsonb_build_object('counter', $2::bigint)
            WHERE event_id = $1::ulid
            "#,
            event_id,
            counter + 10
        )
        .execute(&mut *tx)
        .await?;
        
        tx.commit().await?;
        println!("Transaction 2 committed with counter: {}", counter + 10);
        Ok::<_, anyhow::Error>(counter + 10)
    });

    let (r1, r2) = tokio::join!(handle1, handle2);
    r1??;
    r2??;

    // Check final value
    let final_row = sqlx::query!(
        "SELECT payload FROM core.events WHERE event_id = $1::ulid",
        event_id
    )
    .fetch_one(ctx.pool())
    .await?;
    
    let final_counter = final_row.payload.get("counter")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    
    println!("Final counter value: {}", final_counter);
    
    // With READ COMMITTED, last write wins
    assert!(final_counter == 1 || final_counter == 10,
        "Unexpected counter value: {}", final_counter);

    Ok(())
}

/// Test deadlock detection and recovery
#[sinex_test]
async fn test_deadlock_detection_recovery(ctx: TestContext) -> TestResult {
    // Create two events for cross-locking
    let event1 = ctx.create_test_event("deadlock", "test1", json!({}));
    let event2 = ctx.create_test_event("deadlock", "test2", json!({}));
    ctx.insert_event(&event1).await?;
    ctx.insert_event(&event2).await?;

    let event1_id = event1.id;
    let event2_id = event2.id;

    let deadlock_detected = Arc::new(AtomicU64::new(0));
    let pool1 = ctx.pool().clone();
    let pool2 = ctx.pool().clone();
    let deadlock1 = deadlock_detected.clone();
    let deadlock2 = deadlock_detected.clone();

    // Transaction 1: Lock event1, then event2
    let handle1 = tokio::spawn(async move {
        let mut tx = pool1.begin().await?;
        
        // Lock event1
        sqlx::query!(
            "SELECT * FROM core.events WHERE event_id = $1::ulid FOR UPDATE",
            event1_id
        )
        .fetch_one(&mut *tx)
        .await?;
        println!("T1: Locked event1");
        
        // Wait to ensure T2 locks event2
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Try to lock event2 (will deadlock)
        match sqlx::query!(
            "SELECT * FROM core.events WHERE event_id = $1::ulid FOR UPDATE",
            event2_id
        )
        .fetch_one(&mut *tx)
        .await {
            Ok(_) => {
                println!("T1: Locked event2 (no deadlock)");
                tx.commit().await?;
            }
            Err(e) => {
                println!("T1: Deadlock detected: {}", e);
                deadlock1.fetch_add(1, Ordering::SeqCst);
                tx.rollback().await?;
            }
        }
        
        Ok::<_, anyhow::Error>(())
    });

    // Transaction 2: Lock event2, then event1
    let handle2 = tokio::spawn(async move {
        let mut tx = pool2.begin().await?;
        
        // Lock event2
        sqlx::query!(
            "SELECT * FROM core.events WHERE event_id = $1::ulid FOR UPDATE",
            event2_id
        )
        .fetch_one(&mut *tx)
        .await?;
        println!("T2: Locked event2");
        
        // Wait to ensure T1 locks event1
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Try to lock event1 (will deadlock)
        match sqlx::query!(
            "SELECT * FROM core.events WHERE event_id = $1::ulid FOR UPDATE",
            event1_id
        )
        .fetch_one(&mut *tx)
        .await {
            Ok(_) => {
                println!("T2: Locked event1 (no deadlock)");
                tx.commit().await?;
            }
            Err(e) => {
                println!("T2: Deadlock detected: {}", e);
                deadlock2.fetch_add(1, Ordering::SeqCst);
                tx.rollback().await?;
            }
        }
        
        Ok::<_, anyhow::Error>(())
    });

    let (r1, r2) = tokio::join!(handle1, handle2);
    let _ = r1?;
    let _ = r2?;

    let total_deadlocks = deadlock_detected.load(Ordering::SeqCst);
    println!("Total deadlocks detected: {}", total_deadlocks);

    // At least one transaction should detect deadlock
    assert!(total_deadlocks >= 1, "No deadlock detected");

    Ok(())
}

// =============================================================================
// Memory Concurrency Tests
// =============================================================================

/// Test atomic counter behavior under high contention
#[sinex_test]
async fn test_atomic_counter_high_contention(ctx: TestContext) -> TestResult {
    let counter = Arc::new(AtomicU64::new(0));
    let iterations = 10000;
    let workers = 20;
    
    let mut handles = vec![];
    
    for _ in 0..workers {
        let counter_clone = counter.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..iterations {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                // Minimal work to maximize contention
                tokio::task::yield_now().await;
            }
        });
        handles.push(handle);
    }
    
    // Wait for all workers
    for handle in handles {
        handle.await?;
    }
    
    let final_value = counter.load(Ordering::SeqCst);
    let expected = workers * iterations;
    
    println!("Atomic counter test:");
    println!("  Workers: {}", workers);
    println!("  Iterations per worker: {}", iterations);
    println!("  Expected: {}", expected);
    println!("  Actual: {}", final_value);
    
    assert_eq!(final_value, expected as u64, "Atomic counter lost updates");
    
    Ok(())
}