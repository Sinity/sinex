//! Concurrent checkpoint update testing
//!
//! This module tests checkpoint consistency under high concurrency including:
//! - Multiple workers updating the same checkpoint
//! - Lost update detection and prevention
//! - Optimistic locking behavior
//! - Checkpoint versioning under contention

use sinex_db::queries::checkpoints::CheckpointQueries;
use sinex_test_utils::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

// =============================================================================
// Basic Concurrent Update Tests
// =============================================================================

#[sinex_test]
async fn test_concurrent_checkpoint_updates_basic(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    let processor_name = "concurrent_test_processor";
    let consumer_group = "test_group";
    let consumer_name = "test_consumer";

    println!("Testing basic concurrent checkpoint updates...");

    // Initialize checkpoint
    let initial_checkpoint =
        create_test_checkpoint(processor_name, consumer_group, consumer_name, None, 0);
    insert_checkpoint(pool, &initial_checkpoint).await?;

    // Spawn concurrent updaters
    let concurrent_workers = 10;
    let updates_per_worker = 100;

    let successful_updates = Arc::new(AtomicU64::new(0));
    let failed_updates = Arc::new(AtomicU64::new(0));
    let version_conflicts = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for worker_id in 0..concurrent_workers {
        let pool = pool.clone();
        let success_count = successful_updates.clone();
        let fail_count = failed_updates.clone();
        let conflict_count = version_conflicts.clone();

        let handle = tokio::spawn(async move {
            for update_num in 0..updates_per_worker {
                let event_id = format!("event_{}_{}", worker_id, update_num);

                // Attempt atomic checkpoint update
                match update_checkpoint_atomic(
                    &pool,
                    processor_name,
                    consumer_group,
                    consumer_name,
                    Some(event_id),
                    1, // Increment by 1
                )
                .await
                {
                    Ok(updated) => {
                        if updated {
                            success_count.fetch_add(1, Ordering::SeqCst);
                        } else {
                            conflict_count.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    Err(e) => {
                        fail_count.fetch_add(1, Ordering::SeqCst);
                        if update_num == 0 {
                            eprintln!("Worker {} first update failed: {}", worker_id, e);
                        }
                    }
                }

                // Small random delay to increase contention
                if update_num % 10 == 0 {
                    tokio::time::sleep(Duration::from_micros(rand::random::<u64>() % 100)).await;
                }
            }
        });

        handles.push(handle);
    }

    let start = Instant::now();
    futures::future::join_all(handles).await;
    let duration = start.elapsed();

    let successful = successful_updates.load(Ordering::SeqCst);
    let failed = failed_updates.load(Ordering::SeqCst);
    let conflicts = version_conflicts.load(Ordering::SeqCst);
    let total_attempts = concurrent_workers * updates_per_worker;

    println!("\nConcurrent update results:");
    println!("  Total attempts: {}", total_attempts);
    println!("  Successful updates: {}", successful);
    println!("  Failed updates: {}", failed);
    println!("  Version conflicts: {}", conflicts);
    println!("  Duration: {:?}", duration);
    println!(
        "  Rate: {:.0} updates/sec",
        successful as f64 / duration.as_secs_f64()
    );

    // Verify final state
    let final_checkpoint =
        get_checkpoint(pool, processor_name, consumer_group, consumer_name).await?;

    println!("\nFinal checkpoint state:");
    println!("  Processed count: {}", final_checkpoint.processed_count);
    println!("  Version: {}", final_checkpoint.checkpoint_version);

    // All updates should be accounted for
    assert_eq!(
        successful as i64, final_checkpoint.processed_count,
        "Final count should match successful updates"
    );

    Ok(())
}

// =============================================================================
// Lost Update Detection Tests
// =============================================================================

#[sinex_test]
async fn test_checkpoint_lost_update_prevention(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    let processor_name = "lost_update_test";
    let consumer_group = "test_group";
    let consumer_name = "test_consumer";

    println!("Testing lost update prevention...");

    // Create initial checkpoint
    let checkpoint = create_test_checkpoint(
        processor_name,
        consumer_group,
        consumer_name,
        Some("initial_event".to_string()),
        100,
    );
    insert_checkpoint(pool, &checkpoint).await?;

    // Simulate two concurrent transactions trying to update the same checkpoint
    let mut tx1 = pool.begin().await?;
    let mut tx2 = pool.begin().await?;

    // Transaction 1 reads checkpoint
    let checkpoint1 = sqlx::query!(
        r#"
        SELECT 
            id::text as "id!",
            processed_count as "processed_count!",
            checkpoint_version as "checkpoint_version!"
        FROM core.processor_checkpoints
        WHERE processor_name = $1
          AND consumer_group = $2
          AND consumer_name = $3
        FOR UPDATE
        "#,
        processor_name,
        consumer_group,
        consumer_name
    )
    .fetch_one(&mut *tx1)
    .await?;

    println!("Transaction 1 read checkpoint:");
    println!(
        "  Count: {}, Version: {}",
        checkpoint1.processed_count, checkpoint1.checkpoint_version
    );

    // Transaction 2 tries to read the same checkpoint (should block)
    let tx2_read = tokio::spawn(async move {
        let result = timeout(
            Duration::from_secs(2),
            sqlx::query!(
                r#"
                SELECT 
                    id::text as "id!",
                    processed_count as "processed_count!",
                    checkpoint_version as "checkpoint_version!"
                FROM core.processor_checkpoints
                WHERE processor_name = $1
                  AND consumer_group = $2
                  AND consumer_name = $3
                FOR UPDATE
                "#,
                processor_name,
                consumer_group,
                consumer_name
            )
            .fetch_one(&mut *tx2),
        )
        .await;

        match result {
            Ok(Ok(_)) => Err("Transaction 2 should have been blocked".to_string()),
            Ok(Err(e)) => Err(format!("Transaction 2 query error: {}", e)),
            Err(_) => Ok("Transaction 2 correctly blocked by lock"),
        }
    });

    // Give tx2 time to attempt read
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Transaction 1 updates checkpoint
    sqlx::query!(
        r#"
        UPDATE core.processor_checkpoints
        SET 
            processed_count = processed_count + 10,
            checkpoint_version = checkpoint_version + 1,
            last_processed_id = $4,
            updated_at = NOW()
        WHERE processor_name = $1
          AND consumer_group = $2
          AND consumer_name = $3
        "#,
        processor_name,
        consumer_group,
        consumer_name,
        "tx1_update"
    )
    .execute(&mut *tx1)
    .await?;

    // Commit transaction 1
    tx1.commit().await?;
    println!("Transaction 1 committed successfully");

    // Check result of transaction 2
    let tx2_result = tx2_read.await?;
    match tx2_result {
        Ok(msg) => println!("Transaction 2: {}", msg),
        Err(msg) => println!("Transaction 2 error: {}", msg),
    }

    // Verify final state
    let final_checkpoint =
        get_checkpoint(pool, processor_name, consumer_group, consumer_name).await?;
    assert_eq!(
        final_checkpoint.processed_count, 110,
        "Count should be updated by tx1"
    );
    assert_eq!(
        final_checkpoint.checkpoint_version, 2,
        "Version should be incremented"
    );

    Ok(())
}

// =============================================================================
// Optimistic Locking Tests
// =============================================================================

#[sinex_test]
async fn test_checkpoint_optimistic_locking(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    let processor_name = "optimistic_lock_test";
    let consumer_group = "test_group";
    let consumer_name = "test_consumer";

    println!("Testing optimistic locking behavior...");

    // Create checkpoint
    let checkpoint = create_test_checkpoint(processor_name, consumer_group, consumer_name, None, 0);
    insert_checkpoint(pool, &checkpoint).await?;

    // Multiple workers read checkpoint and try to update based on version
    let workers = 5;
    let mut handles = vec![];

    for worker_id in 0..workers {
        let pool = pool.clone();

        let handle = tokio::spawn(async move {
            // Read current state
            let current =
                get_checkpoint(&pool, processor_name, consumer_group, consumer_name).await?;

            println!(
                "Worker {} read version {}",
                worker_id, current.checkpoint_version
            );

            // Simulate some processing time
            tokio::time::sleep(Duration::from_millis(50 + worker_id * 10)).await;

            // Try to update with version check
            let result = sqlx::query!(
                r#"
                UPDATE core.processor_checkpoints
                SET 
                    processed_count = processed_count + $5,
                    checkpoint_version = checkpoint_version + 1,
                    last_processed_id = $4,
                    updated_at = NOW()
                WHERE processor_name = $1
                  AND consumer_group = $2
                  AND consumer_name = $3
                  AND checkpoint_version = $6
                RETURNING checkpoint_version as "new_version!"
                "#,
                processor_name,
                consumer_group,
                consumer_name,
                format!("worker_{}_event", worker_id),
                10i64,
                current.checkpoint_version
            )
            .fetch_optional(&pool)
            .await?;

            match result {
                Some(row) => {
                    println!(
                        "Worker {} successfully updated to version {}",
                        worker_id, row.new_version
                    );
                    Ok((worker_id, true))
                }
                None => {
                    println!("Worker {} failed - version conflict", worker_id);
                    Ok((worker_id, false))
                }
            }
        });

        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;

    let mut successful_workers = Vec::new();
    let mut failed_workers = Vec::new();

    for result in results {
        match result {
            Ok(Ok((id, success))) => {
                if success {
                    successful_workers.push(id);
                } else {
                    failed_workers.push(id);
                }
            }
            Ok(Err(e)) => eprintln!("Worker error: {}", e),
            Err(e) => eprintln!("Join error: {}", e),
        }
    }

    println!("\nOptimistic locking results:");
    println!("  Successful workers: {:?}", successful_workers);
    println!("  Failed workers: {:?}", failed_workers);

    // Exactly one worker should succeed per version
    assert_eq!(
        successful_workers.len(),
        1,
        "Only one worker should succeed with optimistic locking"
    );

    Ok(())
}

// =============================================================================
// High Contention Stress Tests
// =============================================================================

#[sinex_test(timeout = 60)]
async fn test_checkpoint_high_contention_stress(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    let processor_name = "high_contention_test";
    let consumer_group = "stress_group";

    println!("Testing checkpoint updates under extreme contention...");

    // Create multiple consumer checkpoints
    let consumer_count = 20;
    for i in 0..consumer_count {
        let checkpoint = create_test_checkpoint(
            processor_name,
            consumer_group,
            &format!("consumer_{}", i),
            None,
            0,
        );
        insert_checkpoint(pool, &checkpoint).await?;
    }

    // Spawn many workers updating different consumers
    let workers_per_consumer = 50;
    let updates_per_worker = 20;
    let mut tasks = JoinSet::new();

    let total_updates = Arc::new(AtomicU64::new(0));
    let contention_events = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    for consumer_id in 0..consumer_count {
        for worker_id in 0..workers_per_consumer {
            let pool = pool.clone();
            let updates = total_updates.clone();
            let contentions = contention_events.clone();
            let consumer_name = format!("consumer_{}", consumer_id);

            tasks.spawn(async move {
                let mut local_contentions = 0;

                for update_id in 0..updates_per_worker {
                    let update_start = Instant::now();

                    match update_checkpoint_atomic(
                        &pool,
                        processor_name,
                        consumer_group,
                        &consumer_name,
                        Some(format!("w{}_u{}", worker_id, update_id)),
                        1,
                    )
                    .await
                    {
                        Ok(success) => {
                            if success {
                                updates.fetch_add(1, Ordering::Relaxed);
                            }

                            let duration = update_start.elapsed();
                            if duration > Duration::from_millis(100) {
                                local_contentions += 1;
                            }
                        }
                        Err(e) => {
                            if update_id == 0 {
                                eprintln!(
                                    "Consumer {} worker {} error: {}",
                                    consumer_id, worker_id, e
                                );
                            }
                        }
                    }
                }

                if local_contentions > 0 {
                    contentions.fetch_add(local_contentions, Ordering::Relaxed);
                }
            });
        }
    }

    // Wait for all tasks
    while let Some(_) = tasks.join_next().await {}

    let duration = start.elapsed();
    let total = total_updates.load(Ordering::Relaxed);
    let contentions = contention_events.load(Ordering::Relaxed);

    println!("\nHigh contention stress test results:");
    println!("  Total consumers: {}", consumer_count);
    println!("  Workers per consumer: {}", workers_per_consumer);
    println!("  Updates per worker: {}", updates_per_worker);
    println!(
        "  Expected updates: {}",
        consumer_count * workers_per_consumer * updates_per_worker
    );
    println!("  Successful updates: {}", total);
    println!("  Contention events: {}", contentions);
    println!("  Duration: {:?}", duration);
    println!(
        "  Throughput: {:.0} updates/sec",
        total as f64 / duration.as_secs_f64()
    );

    // Verify checkpoint integrity
    let mut total_processed = 0i64;
    for i in 0..consumer_count {
        let checkpoint = get_checkpoint(
            pool,
            processor_name,
            consumer_group,
            &format!("consumer_{}", i),
        )
        .await?;
        total_processed += checkpoint.processed_count;
    }

    println!("  Total processed count: {}", total_processed);
    assert_eq!(
        total_processed, total as i64,
        "Sum of all checkpoint counts should match total updates"
    );

    Ok(())
}

// =============================================================================
// Checkpoint History Consistency Tests
// =============================================================================

#[sinex_test]
async fn test_checkpoint_history_consistency(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    let processor_name = "history_test";
    let consumer_group = "test_group";
    let consumer_name = "test_consumer";

    println!("Testing checkpoint history consistency under concurrent updates...");

    // Create initial checkpoint
    let checkpoint = create_test_checkpoint(processor_name, consumer_group, consumer_name, None, 0);
    insert_checkpoint(pool, &checkpoint).await?;

    // Perform many concurrent updates
    let update_count = 100;
    let mut handles = vec![];

    for i in 0..update_count {
        let pool = pool.clone();
        let event_id = format!("event_{:03}", i);

        let handle = tokio::spawn(async move {
            update_checkpoint_atomic(
                &pool,
                processor_name,
                consumer_group,
                consumer_name,
                Some(event_id.clone()),
                1,
            )
            .await
            .map(|success| (i, event_id, success))
        });

        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;

    let mut successful_updates = Vec::new();
    for result in results {
        if let Ok(Ok((i, event_id, true))) = result {
            successful_updates.push((i, event_id));
        }
    }

    println!(
        "Successful updates: {}/{}",
        successful_updates.len(),
        update_count
    );

    // Query checkpoint history
    let history = sqlx::query!(
        r#"
        SELECT 
            last_processed_id,
            processed_count,
            checkpoint_version,
            updated_at
        FROM core.processor_checkpoints
        WHERE processor_name = $1
          AND consumer_group = $2
          AND consumer_name = $3
        ORDER BY updated_at DESC
        "#,
        processor_name,
        consumer_group,
        consumer_name
    )
    .fetch_all(pool)
    .await?;

    // Verify history consistency
    assert!(!history.is_empty(), "Should have checkpoint history");

    let latest = &history[0];
    println!("\nLatest checkpoint:");
    println!("  Last processed: {:?}", latest.last_processed_id);
    println!("  Count: {}", latest.processed_count);
    println!("  Version: {}", latest.checkpoint_version);

    // Verify count matches successful updates
    assert_eq!(
        latest.processed_count,
        successful_updates.len() as i64,
        "Processed count should match successful updates"
    );

    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

#[derive(Debug)]
struct TestCheckpoint {
    id: Ulid,
    processor_name: String,
    consumer_group: String,
    consumer_name: String,
    last_processed_id: Option<String>,
    processed_count: i64,
    checkpoint_version: i32,
}

fn create_test_checkpoint(
    processor_name: &str,
    consumer_group: &str,
    consumer_name: &str,
    last_processed_id: Option<String>,
    processed_count: i64,
) -> TestCheckpoint {
    TestCheckpoint {
        id: Ulid::new(),
        processor_name: processor_name.to_string(),
        consumer_group: consumer_group.to_string(),
        consumer_name: consumer_name.to_string(),
        last_processed_id,
        processed_count,
        checkpoint_version: 1,
    }
}

async fn insert_checkpoint(pool: &PgPool, checkpoint: &TestCheckpoint) -> Result<(), Error> {
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints (
            id, processor_name, consumer_group, consumer_name,
            last_processed_id, processed_count, last_activity,
            checkpoint_version, created_at, updated_at
        ) VALUES (
            $1::uuid, $2, $3, $4, $5, $6, NOW(), $7, NOW(), NOW()
        )
        "#,
        checkpoint.id.to_uuid(),
        checkpoint.processor_name,
        checkpoint.consumer_group,
        checkpoint.consumer_name,
        checkpoint.last_processed_id,
        checkpoint.processed_count,
        checkpoint.checkpoint_version
    )
    .execute(pool)
    .await
    .wrap_err("Failed to insert checkpoint")?;
    Ok(())
}

async fn get_checkpoint(
    pool: &PgPool,
    processor_name: &str,
    consumer_group: &str,
    consumer_name: &str,
) -> Result<TestCheckpoint, Error> {
    let row = sqlx::query!(
        r#"
        SELECT 
            id::text as "id!",
            processor_name as "processor_name!",
            consumer_group as "consumer_group!",
            consumer_name as "consumer_name!",
            last_processed_id,
            processed_count as "processed_count!",
            checkpoint_version as "checkpoint_version!"
        FROM core.processor_checkpoints
        WHERE processor_name = $1
          AND consumer_group = $2
          AND consumer_name = $3
        "#,
        processor_name,
        consumer_group,
        consumer_name
    )
    .fetch_one(pool)
    .await
    .wrap_err("Failed to get checkpoint")?;

    Ok(TestCheckpoint {
        id: Ulid::from_string(&row.id)?,
        processor_name: row.processor_name,
        consumer_group: row.consumer_group,
        consumer_name: row.consumer_name,
        last_processed_id: row.last_processed_id,
        processed_count: row.processed_count,
        checkpoint_version: row.checkpoint_version,
    })
}

async fn update_checkpoint_atomic(
    pool: &PgPool,
    processor_name: &str,
    consumer_group: &str,
    consumer_name: &str,
    last_processed_id: Option<String>,
    increment: i64,
) -> Result<bool, Error> {
    let result = sqlx::query!(
        r#"
        UPDATE core.processor_checkpoints
        SET 
            processed_count = processed_count + $5,
            last_processed_id = COALESCE($4, last_processed_id),
            checkpoint_version = checkpoint_version + 1,
            last_activity = NOW(),
            updated_at = NOW()
        WHERE processor_name = $1
          AND consumer_group = $2
          AND consumer_name = $3
        "#,
        processor_name,
        consumer_group,
        consumer_name,
        last_processed_id,
        increment
    )
    .execute(pool)
    .await
    .wrap_err("Failed to update checkpoint")?;

    Ok(result.rows_affected() > 0)
}

use sinex_types::error::{Error, ErrorContext};
use sinex_types::ulid::Ulid;
use sqlx::PgPool;
