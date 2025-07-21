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
use crate::common::builders::TestEventBuilder;
use crate::common::query_helpers::TestQueries;
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
    let pool = ctx.pool().clone();

    // Insert event to be claimed
    let event = TestEventBuilder::new("race", "race.test")
        .with_field("test", json!("race_condition"))
        .build();

    let inserted = TestQueries::insert_test_event(
        &pool,
        &event.source,
        &event.event_type,
        event.payload,
    ).await?;
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

        // Try to claim with SELECT FOR UPDATE - RAW SQL: Testing concurrent claim behavior
        let result = sqlx::query!(
            r#"
                UPDATE core.events
                SET payload = payload || '{"claimed_by": 1}'::jsonb
                WHERE event_id::uuid = $1::uuid
                AND NOT (payload ? 'claimed_by')
                "#,
            event_id.to_uuid()
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

        // Try to claim at exact same time - RAW SQL: Testing race condition
        let result = sqlx::query!(
            r#"
                UPDATE core.events
                SET payload = payload || '{"claimed_by": 2}'::jsonb
                WHERE event_id::uuid = $1::uuid
                AND NOT (payload ? 'claimed_by')
                "#,
            event_id.to_uuid()
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

    // Check final state
    let final_state = TestQueries::get_event(&pool, event_id).await?;

    println!("Final payload: {}", final_state.payload);

    // Both workers might claim if there's a race condition
    assert_eq!(total_claims, 1, "Multiple workers claimed same event!");

    Ok(())
}

/// Test event causality violation under concurrent processing
#[sinex_test]
async fn test_event_causality_violation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let order_violations = Arc::new(AtomicU64::new(0));

    // Simulate dependent events processed out of order
    for test_round in 0..10 {
        let parent_event = TestEventBuilder::new("causality_test", "parent.event")
            .with_field("round", json!(test_round))
            .build();

        let parent_inserted = TestQueries::insert_test_event(
            &pool,
            &parent_event.source,
            &parent_event.event_type,
            parent_event.payload,
        ).await?;

        // Create dependent events
        let mut child_events = Vec::new();
        for i in 0..5 {
            let child = TestEventBuilder::new("causality_test", "child.event")
                .with_field("round", json!(test_round))
                .with_field("child_id", json!(i))
                .build();
            child_events.push(child);
        }

        // Process children concurrently (might violate causality)
        let mut handles = vec![];
        for child in child_events {
            let pool_clone = pool.clone();
            let violations = order_violations.clone();
            let parent_id = parent_inserted.id;

            let handle = tokio::spawn(async move {
                // Check if parent has been processed - RAW SQL: Testing causality tracking
                let parent_check = sqlx::query!(
                    "SELECT payload->>'processed' as processed FROM core.events WHERE event_id::uuid = $1::uuid",
                    parent_id.to_uuid()
                )
                .fetch_one(&pool_clone)
                .await;

                if let Ok(parent_state) = parent_check {
                    if parent_state.processed != Some("true".to_string()) {
                        violations.fetch_add(1, Ordering::SeqCst);
                        println!("CAUSALITY VIOLATION: Child processed before parent");
                    }
                }

                // Insert child event
                TestQueries::insert_test_event(
                    &pool_clone,
                    &child.source,
                    &child.event_type,
                    child.payload,
                ).await
            });

            handles.push(handle);
        }

        // Process parent after small delay
        tokio::time::sleep(Duration::from_millis(10)).await;

        // RAW SQL: Testing concurrent payload updates
        sqlx::query!(
            "UPDATE core.events SET payload = payload || '{\"processed\": \"true\"}'::jsonb WHERE event_id::uuid = $1::uuid",
            parent_inserted.id.to_uuid()
        )
        .execute(&pool)
        .await?;

        // Wait for children to complete
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    let violations = order_violations.load(Ordering::SeqCst);
    println!("Causality violations detected: {}", violations);

    // Some violations are expected in concurrent processing
    if violations > 0 {
        println!("WARNING: Causality violations detected - consider event ordering mechanisms");
    }

    Ok(())
}

/// Test concurrent event insertion race conditions
#[sinex_test]
async fn test_concurrent_event_insertion_race(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let successful_insertions = Arc::new(AtomicU64::new(0));
    let failed_insertions = Arc::new(AtomicU64::new(0));
    let duplicate_ids = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Create many concurrent insertions
    for worker_id in 0..20 {
        let pool_clone = pool.clone();
        let success_count = successful_insertions.clone();
        let fail_count = failed_insertions.clone();
        let dup_count = duplicate_ids.clone();

        let handle = tokio::spawn(async move {
            for insertion_id in 0..10 {
                let event = TestEventBuilder::new("insertion_race", "concurrent.insert")
                    .with_field("worker_id", json!(worker_id))
                    .with_field("insertion_id", json!(insertion_id))
                    .with_field("timestamp", json!(Utc::now().to_rfc3339()))
                    .build();

                match TestQueries::insert_test_event(
                    &pool_clone,
                    &event.source,
                    &event.event_type,
                    event.payload,
                ).await {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        fail_count.fetch_add(1, Ordering::SeqCst);

                        // Check if it's a duplicate key error
                        if e.to_string().contains("duplicate key") {
                            dup_count.fetch_add(1, Ordering::SeqCst);
                            println!("Duplicate ULID detected: {}", e);
                        } else {
                            println!(
                                "Worker {} insertion {} failed: {}",
                                worker_id, insertion_id, e
                            );
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successful = successful_insertions.load(Ordering::SeqCst);
    let failed = failed_insertions.load(Ordering::SeqCst);
    let duplicates = duplicate_ids.load(Ordering::SeqCst);

    println!("Concurrent insertion race results:");
    println!("  Successful insertions: {}", successful);
    println!("  Failed insertions: {}", failed);
    println!("  Duplicate ID errors: {}", duplicates);

    // Most insertions should succeed
    assert!(successful > 150, "Most insertions should succeed");

    // Duplicate IDs should be rare with proper ULID generation
    assert!(duplicates < 5, "Duplicate IDs should be rare");

    Ok(())
}

/// Test data consistency under concurrent updates
#[sinex_test]
async fn test_data_consistency_under_concurrent_updates(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create base event
    let base_event = TestEventBuilder::new("consistency_test", "base.event")
        .with_field("counter", json!(0))
        .build();

    let inserted_event = TestQueries::insert_test_event(
        &pool,
        &base_event.source,
        &base_event.event_type,
        base_event.payload,
    ).await?;
    let event_id = inserted_event.id;

    let successful_updates = Arc::new(AtomicU64::new(0));
    let failed_updates = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Create concurrent updates to same event
    for worker_id in 0..10 {
        let pool_clone = pool.clone();
        let success_count = successful_updates.clone();
        let fail_count = failed_updates.clone();

        let handle = tokio::spawn(async move {
            for update_id in 0..5 {
                // Try to increment counter atomically - RAW SQL: Testing concurrent updates
                let update_result = sqlx::query!(
                    r#"
                    UPDATE core.events 
                    SET payload = jsonb_set(
                        payload, 
                        '{counter}', 
                        ((payload->>'counter')::int + 1)::text::jsonb
                    )
                    WHERE event_id::uuid = $1::uuid
                    "#,
                    event_id.to_uuid()
                )
                .execute(&pool_clone)
                .await;

                match update_result {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} update {} succeeded", worker_id, update_id);
                    }
                    Err(e) => {
                        fail_count.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} update {} failed: {}", worker_id, update_id, e);
                    }
                }

                // Small delay to allow other workers
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successful = successful_updates.load(Ordering::SeqCst);
    let failed = failed_updates.load(Ordering::SeqCst);

    // Check final counter value
    let final_state = TestQueries::get_event(&pool, event_id).await?;

    let final_counter: i32 = final_state.payload["counter"]
        .as_i64()
        .unwrap_or(0) as i32;

    println!("Data consistency test results:");
    println!("  Successful updates: {}", successful);
    println!("  Failed updates: {}", failed);
    println!("  Final counter value: {}", final_counter);
    println!("  Expected counter value: {}", successful);

    // Counter should equal successful updates (data consistency)
    assert_eq!(
        final_counter as u64, successful,
        "Counter value should match successful updates"
    );

    Ok(())
}

// =============================================================================
// Worker Coordination Tests
// =============================================================================

/// Test worker coordination with microsecond synchronization
#[sinex_test]
async fn test_worker_coordination_microsecond_sync(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    println!("Testing microsecond-level worker claim races:");

    // Insert events to be claimed
    let mut event_ids = vec![];
    for _i in 0..10 {
        let event = TestEventBuilder::new("coordination_test", "work.item")
            .with_field("test", json!(true))
            .build();

        let inserted = TestQueries::insert_test_event(
            &pool,
            &event.source,
            &event.event_type,
            event.payload,
        )
        .await
        .unwrap();
        event_ids.push(inserted.id);
    }

    // Use barrier to synchronize workers at microsecond level
    let barrier = Arc::new(Barrier::new(5));
    let double_claims = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for worker_id in 0..5 {
        let pool_clone = ctx.pool().clone();
        let barrier_clone = barrier.clone();
        let double_claims_clone = double_claims.clone();
        let event_ids_clone = event_ids.clone();

        let handle = tokio::spawn(async move {
            let mut claimed = 0;

            for event_id in event_ids_clone {
                // Synchronize all workers to claim at same microsecond
                barrier_clone.wait();

                let claim_start = Instant::now();

                // Attempt to claim with SELECT FOR UPDATE
                let claim_result = sqlx::query!(
                    r#"
                    UPDATE core.events
                    SET payload = payload || jsonb_build_object('claimed_by', $2::text, 'claim_time', $3::text)
                    WHERE event_id::uuid = $1::uuid
                    AND NOT (payload ? 'claimed_by')
                    "#,
                    event_id.to_uuid(),
                    worker_id.to_string(),
                    Utc::now().to_rfc3339()
                ).execute(&pool_clone).await;

                let claim_duration = claim_start.elapsed();

                match claim_result {
                    Ok(result) => {
                        if result.rows_affected() > 0 {
                            claimed += 1;

                            // Check if another worker also claimed (race condition)
                            let verify_result = sqlx::query!(
                                "SELECT payload->>'claimed_by' as claimer FROM core.events WHERE event_id::uuid = $1::uuid",
                                event_id.to_uuid()
                            ).fetch_one(&pool_clone).await;

                            if let Ok(record) = verify_result {
                                if record.claimer != Some(worker_id.to_string()) {
                                    double_claims_clone.fetch_add(1, Ordering::SeqCst);
                                    println!(
                                        "DOUBLE CLAIM: Worker {} claimed but {} owns it!",
                                        worker_id,
                                        record.claimer.unwrap_or_default()
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Worker {} claim failed: {}", worker_id, e);
                    }
                }

                if claim_duration.as_micros() < 10 {
                    println!(
                        "Worker {} claim completed in {}μs",
                        worker_id,
                        claim_duration.as_micros()
                    );
                }
            }

            (worker_id, claimed)
        });

        handles.push(handle);
    }

    let results = join_all(handles).await;
    let double_claims_count = double_claims.load(Ordering::SeqCst);

    println!("\nWorker coordination results:");
    for result in results {
        if let Ok((worker_id, claimed)) = result {
            println!("  Worker {}: {} events claimed", worker_id, claimed);
        }
    }
    println!("  Double claims detected: {}", double_claims_count);

    // No double claims should occur with proper coordination
    assert_eq!(double_claims_count, 0, "No double claims should occur");

    Ok(())
}

/// Test worker deadlock prevention
#[sinex_test]
async fn test_worker_deadlock_prevention(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create two events that workers will try to claim in different orders
    let event1 = TestEventBuilder::new("deadlock_test", "resource.a")
        .with_field("resource", json!("A"))
        .build();
    let event2 = TestEventBuilder::new("deadlock_test", "resource.b")
        .with_field("resource", json!("B"))
        .build();

    let inserted1 = TestQueries::insert_test_event(
        &pool,
        &event1.source,
        &event1.event_type,
        event1.payload,
    ).await?;
    let inserted2 = TestQueries::insert_test_event(
        &pool,
        &event2.source,
        &event2.event_type,
        event2.payload,
    ).await?;

    let successful_operations = Arc::new(AtomicU64::new(0));
    let failed_operations = Arc::new(AtomicU64::new(0));
    let deadlock_errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Create workers that might deadlock
    for worker_id in 0..10 {
        let pool_clone = pool.clone();
        let success_count = successful_operations.clone();
        let fail_count = failed_operations.clone();
        let deadlock_count = deadlock_errors.clone();

        let handle = tokio::spawn(async move {
            let mut tx = pool_clone.begin().await.unwrap();

            // Worker tries to claim both events in different orders
            let (first_id, second_id) = if worker_id % 2 == 0 {
                (inserted1.id, inserted2.id)
            } else {
                (inserted2.id, inserted1.id)
            };

            // Try to claim first event - RAW SQL: Testing deadlock scenarios
            let claim1_result = sqlx::query!(
                "UPDATE core.events SET payload = payload || jsonb_build_object('claimed_by', $2::text) WHERE event_id::uuid = $1::uuid",
                first_id.to_uuid(),
                worker_id.to_string()
            )
            .execute(&mut *tx)
            .await;

            if claim1_result.is_ok() {
                // Small delay to increase chance of deadlock
                tokio::time::sleep(Duration::from_millis(10)).await;

                // Try to claim second event - RAW SQL: Testing deadlock detection
                let claim2_result = sqlx::query!(
                    "UPDATE core.events SET payload = payload || jsonb_build_object('claimed_by', $2::text) WHERE event_id::uuid = $1::uuid",
                    second_id.to_uuid(),
                    worker_id.to_string()
                )
                .execute(&mut *tx)
                .await;

                if claim2_result.is_ok() {
                    match tx.commit().await {
                        Ok(_) => {
                            success_count.fetch_add(1, Ordering::SeqCst);
                            println!("Worker {} successfully claimed both events", worker_id);
                        }
                        Err(e) => {
                            fail_count.fetch_add(1, Ordering::SeqCst);
                            if e.to_string().contains("deadlock") {
                                deadlock_count.fetch_add(1, Ordering::SeqCst);
                                println!("Worker {} encountered deadlock: {}", worker_id, e);
                            } else {
                                println!("Worker {} commit failed: {}", worker_id, e);
                            }
                        }
                    }
                } else {
                    tx.rollback().await.unwrap();
                    fail_count.fetch_add(1, Ordering::SeqCst);
                }
            } else {
                tx.rollback().await.unwrap();
                fail_count.fetch_add(1, Ordering::SeqCst);
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successful = successful_operations.load(Ordering::SeqCst);
    let failed = failed_operations.load(Ordering::SeqCst);
    let deadlocks = deadlock_errors.load(Ordering::SeqCst);

    println!("Deadlock prevention test results:");
    println!("  Successful operations: {}", successful);
    println!("  Failed operations: {}", failed);
    println!("  Deadlock errors: {}", deadlocks);

    // Some operations should succeed
    assert!(successful > 0, "Some operations should succeed");

    // Deadlocks should be handled gracefully
    if deadlocks > 0 {
        println!("WARNING: Deadlocks detected - ensure proper deadlock handling");
    }

    Ok(())
}

/// Test worker load balancing under concurrent load
#[sinex_test]
async fn test_worker_load_balancing_concurrent(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create many work items
    let work_item_count = 50;
    let mut work_items = Vec::new();

    for i in 0..work_item_count {
        let event = TestEventBuilder::new("load_balance_test", "work.item")
            .with_field("item_id", json!(i))
            .with_field("priority", json!(i % 3))
            .build();

        let inserted = TestQueries::insert_test_event(
            &pool,
            &event.source,
            &event.event_type,
            event.payload,
        ).await?;
        work_items.push(inserted.id);
    }

    let worker_counts = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let total_processed = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Create workers with different processing speeds
    for worker_id in 0..8 {
        let pool_clone = pool.clone();
        let counts = worker_counts.clone();
        let processed = total_processed.clone();

        let handle = tokio::spawn(async move {
            let mut worker_processed = 0;

            loop {
                // Try to claim next available work item - RAW SQL: Testing work distribution
                let claim_result = sqlx::query!(
                    r#"
                    UPDATE core.events 
                    SET payload = payload || jsonb_build_object('claimed_by', $1::text, 'start_time', $2::text)
                    WHERE event_id::uuid IN (
                        SELECT event_id::uuid FROM core.events 
                        WHERE source = 'load_balance_test' 
                        AND NOT (payload ? 'claimed_by')
                        ORDER BY (payload->>'priority')::int DESC
                        LIMIT 1
                    )
                    "#,
                    worker_id.to_string(),
                    Utc::now().to_rfc3339()
                )
                .execute(&pool_clone)
                .await;

                match claim_result {
                    Ok(result) if result.rows_affected() > 0 => {
                        worker_processed += 1;
                        processed.fetch_add(1, Ordering::SeqCst);

                        // Simulate processing time (varies by worker)
                        let processing_time = Duration::from_millis(10 + (worker_id * 5));
                        tokio::time::sleep(processing_time).await;

                        println!("Worker {} processed item {}", worker_id, worker_processed);
                    }
                    _ => {
                        // No more work available
                        break;
                    }
                }
            }

            // Record final count
            counts.lock().unwrap().insert(worker_id, worker_processed);
            worker_processed
        });

        handles.push(handle);
    }

    let _results = join_all(handles).await;
    let total = total_processed.load(Ordering::SeqCst);

    println!("Load balancing test results:");
    println!("  Total work items: {}", work_item_count);
    println!("  Total processed: {}", total);

    let worker_counts = worker_counts.lock().unwrap();
    let mut min_processed = u64::MAX;
    let mut max_processed = 0;

    for (worker_id, count) in worker_counts.iter() {
        println!("  Worker {}: {} items", worker_id, count);
        min_processed = min_processed.min(*count);
        max_processed = max_processed.max(*count);
    }

    let load_balance_ratio = if min_processed > 0 {
        max_processed as f64 / min_processed as f64
    } else {
        f64::INFINITY
    };

    println!(
        "  Load balance ratio: {:.2} (lower is better)",
        load_balance_ratio
    );

    // All work should be processed
    assert_eq!(total, work_item_count, "All work items should be processed");

    // Load should be reasonably balanced
    assert!(
        load_balance_ratio < 3.0,
        "Load should be reasonably balanced"
    );

    Ok(())
}

// =============================================================================
// Database Concurrency Tests
// =============================================================================

/// Test database transaction isolation levels
#[sinex_test]
async fn test_database_transaction_isolation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test event
    let test_event = TestEventBuilder::new("isolation_test", "transaction.test")
        .with_field("value", json!(100))
        .build();

    let inserted = TestQueries::insert_test_event(
        &pool,
        &test_event.source,
        &test_event.event_type,
        test_event.payload,
    ).await?;
    let event_id = inserted.id;

    let isolation_violations = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Create concurrent transactions
    for tx_id in 0..5 {
        let pool_clone = pool.clone();
        let violations = isolation_violations.clone();

        let handle = tokio::spawn(async move {
            let mut tx = pool_clone.begin().await.unwrap();

            // Read initial value - RAW SQL: Testing transaction isolation
            let initial_read = sqlx::query!(
                "SELECT payload->>'value' as value FROM core.events WHERE event_id::uuid = $1::uuid",
                event_id.to_uuid()
            )
            .fetch_one(&mut *tx)
            .await
            .unwrap();

            let initial_value: i32 = initial_read
                .value
                .unwrap_or("0".to_string())
                .parse()
                .unwrap_or(0);

            // Sleep to allow other transactions to interfere
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Update based on initial read - RAW SQL: Testing isolation levels
            let new_value = initial_value + tx_id;
            sqlx::query!(
                "UPDATE core.events SET payload = jsonb_set(payload, '{value}', $2::text::jsonb) WHERE event_id::uuid = $1::uuid",
                event_id.to_uuid(),
                new_value.to_string()
            )
            .execute(&mut *tx)
            .await
            .unwrap();

            // Read again to check consistency - RAW SQL: Verifying isolation
            let final_read = sqlx::query!(
                "SELECT payload->>'value' as value FROM core.events WHERE event_id::uuid = $1::uuid",
                event_id.to_uuid()
            )
            .fetch_one(&mut *tx)
            .await
            .unwrap();

            let final_value: i32 = final_read
                .value
                .unwrap_or("0".to_string())
                .parse()
                .unwrap_or(0);

            if final_value != new_value {
                violations.fetch_add(1, Ordering::SeqCst);
                println!(
                    "Transaction {} isolation violation: expected {}, got {}",
                    tx_id, new_value, final_value
                );
            }

            match tx.commit().await {
                Ok(_) => {
                    println!("Transaction {} committed successfully", tx_id);
                }
                Err(e) => {
                    println!("Transaction {} failed: {}", tx_id, e);
                }
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let violations = isolation_violations.load(Ordering::SeqCst);

    println!("Transaction isolation test results:");
    println!("  Isolation violations: {}", violations);

    // Check final state
    let final_state = TestQueries::get_event(&pool, event_id).await?;

    println!(
        "  Final value: {}",
        final_state.payload["value"].as_i64().unwrap_or(-1)
    );

    // Isolation should be maintained
    assert_eq!(violations, 0, "No isolation violations should occur");

    Ok(())
}

/// Test database lock contention
#[sinex_test]
async fn test_database_lock_contention(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create shared resource
    let shared_event = TestEventBuilder::new("lock_test", "shared.resource")
        .with_field("counter", json!(0))
        .with_field("lock_count", json!(0))
        .build();

    let inserted = TestQueries::insert_test_event(
        &pool,
        &shared_event.source,
        &shared_event.event_type,
        shared_event.payload,
    ).await?;
    let event_id = inserted.id;

    let lock_contentions = Arc::new(AtomicU64::new(0));
    let successful_locks = Arc::new(AtomicU64::new(0));
    let failed_locks = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Create heavy lock contention
    for worker_id in 0..15 {
        let pool_clone = pool.clone();
        let contentions = lock_contentions.clone();
        let successes = successful_locks.clone();
        let failures = failed_locks.clone();

        let handle = tokio::spawn(async move {
            for _attempt in 0..5 {
                let lock_start = Instant::now();

                // Try to acquire exclusive lock - RAW SQL: Testing lock contention
                let lock_result = sqlx::query!(
                    "SELECT payload FROM core.events WHERE event_id::uuid = $1::uuid FOR UPDATE",
                    event_id.to_uuid()
                )
                .fetch_one(&pool_clone)
                .await;

                let lock_time = lock_start.elapsed();

                match lock_result {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::SeqCst);

                        // If lock took a long time, it's likely due to contention
                        if lock_time > Duration::from_millis(50) {
                            contentions.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "Worker {} experienced lock contention: {:?}",
                                worker_id, lock_time
                            );
                        }

                        // Hold lock briefly and update
                        tokio::time::sleep(Duration::from_millis(20)).await;

                        // RAW SQL: Testing concurrent lock updates
                        sqlx::query!(
                            "UPDATE core.events SET payload = jsonb_set(payload, '{lock_count}', ((payload->>'lock_count')::int + 1)::text::jsonb) WHERE event_id::uuid = $1::uuid",
                            event_id.to_uuid()
                        )
                        .execute(&pool_clone)
                        .await
                        .unwrap();
                    }
                    Err(e) => {
                        failures.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} lock failed: {}", worker_id, e);
                    }
                }

                // Small delay between attempts
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let contentions = lock_contentions.load(Ordering::SeqCst);
    let successes = successful_locks.load(Ordering::SeqCst);
    let failures = failed_locks.load(Ordering::SeqCst);

    println!("Lock contention test results:");
    println!("  Successful locks: {}", successes);
    println!("  Failed locks: {}", failures);
    println!("  Lock contentions: {}", contentions);

    // Check final lock count
    let final_state = TestQueries::get_event(&pool, event_id).await?;

    let final_lock_count: i32 = final_state.payload["lock_count"]
        .as_i64()
        .unwrap_or(0) as i32;

    println!("  Final lock count: {}", final_lock_count);

    // Most locks should succeed
    assert!(successes > 50, "Most locks should succeed");

    // Lock count should match successful operations
    assert_eq!(
        final_lock_count as u64, successes,
        "Lock count should match successful operations"
    );

    Ok(())
}
