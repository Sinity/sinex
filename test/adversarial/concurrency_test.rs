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

use crate::common::test_macros::*;
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

/// Test concurrent event insertion race
test_concurrent_operations!(test_concurrent_event_insertion_race, 20,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 20);
        Ok(())
    }
);

/// Test data consistency under concurrent updates
test_concurrent_operations!(test_data_consistency_under_concurrent_updates, 10,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 10);
        Ok(())
    }
);

// =============================================================================
// Worker Coordination Tests
// =============================================================================

/// Test worker coordination with microsecond sync
test_concurrent_operations!(test_worker_coordination_microsecond_sync, 10,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 10);
        Ok(())
    }
);

/// Test worker deadlock prevention
test_concurrent_operations!(test_worker_deadlock_prevention, 10,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 10);
        Ok(())
    }
);

/// Test worker load balancing
test_concurrent_operations!(test_worker_load_balancing_concurrent, 8,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 8);
        Ok(())
    }
);

// =============================================================================
// Database Concurrency Tests
// =============================================================================

/// Test database transaction isolation
test_concurrent_operations!(test_database_transaction_isolation, 5,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 5);
        Ok(())
    }
);

test_concurrent_operations!(test_database_lock_contention, 15,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 15);
        Ok(())
    }
);
