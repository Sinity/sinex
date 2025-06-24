//! Migrated version of concurrent_processing_tests.rs using new test infrastructure
//!
//! This demonstrates:
//! - Cleaner test setup with TestContext
//! - Timing helpers replacing arbitrary sleeps
//! - Better event and worker management

use crate::common::prelude::*;
use crate::common::worker_test_utils;
use sinex_db::queries::{claim_work_queue_items, complete_work_queue_item};
use tokio::task::JoinSet;
use std::sync::Mutex;

#[sinex_test]
async fn test_select_for_update_skip_locked_prevents_duplicate_processing(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Setup test worker with items
    let _items = worker_test_utils::setup_test_worker(ctx.pool(), "test_worker", 10).await?;
    
    let pool = Arc::new(ctx.pool().clone());
    let barrier = Arc::new(Barrier::new(3));
    let processed_count = Arc::new(tokio::sync::Mutex::new(0));
    
    let mut tasks = JoinSet::new();
    
    // Spawn 3 workers that will try to process items concurrently
    for worker_id in 0..3 {
        let pool = ctx.pool().clone();
        let barrier = barrier.clone();
        let processed_count = processed_count.clone();
        
        tasks.spawn(async move {
            // Wait for all workers to be ready
            barrier.wait().await;
            
            let mut local_processed = 0;
            
            loop {
                // Try to claim items using the production function
                let items = claim_work_queue_items(ctx.pool(),
                    "test_worker",
                    &format!("worker-{}", worker_id),
                    1
                ).await?;
                
                if items.is_empty() {
                    // No more items to process
                    break;
                }
                
                for item in items {
                    // Simulate processing - no arbitrary sleep!
                    tokio::task::yield_now().await;
                    
                    // Mark as processed by completing it
                    complete_work_queue_item(ctx.pool(), item.queue_id).await?;
                    
                    local_processed += 1;
                }
            }
            
            let mut count = processed_count.lock().await;
            *count += local_processed;
            
            Ok::<(i32, i32), anyhow::Error>((worker_id, local_processed))
        });
    }
    
    // Wait for all workers to complete
    let mut worker_results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        worker_results.push(result??);
    }
    
    // Verify results
    let total_processed = *processed_count.lock().await;
    pretty_assertions::assert_eq!(total_processed, 10, "All items should be processed exactly once");
    
    // Verify no items remain
    worker_test_utils::verify_all_items_processed_by_worker(ctx.pool(), "test_worker").await?;
    
    // Verify work distribution
    let mut workers_that_worked = 0;
    for (worker_id, count) in worker_results {
        if ctx.is_verbose() {
            println!("Worker {} processed {} items", worker_id, count);
        }
        if count > 0 {
            workers_that_worked += 1;
        }
    }
    
    // Verify that multiple workers participated
    assert!(
        workers_that_worked >= 2,
        "Expected multiple workers to participate, but only {} worked",
        workers_that_worked
    );
    
    Ok(())
}

#[sinex_test]
async fn test_skip_locked_allows_parallel_processing(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Insert test items
    let _ = worker_test_utils::insert_test_items(ctx.pool(), 20).await?;
    
    let pool = Arc::new(ctx.pool().clone());
    let start = Instant::now();
    let barrier = Arc::new(Barrier::new(4));
    
    let mut tasks = JoinSet::new();
    
    // Spawn 4 workers
    for worker_id in 0..4 {
        let pool = ctx.pool().clone();
        let barrier = barrier.clone();
        let test_config = ctx.config().clone();
        
        tasks.spawn(async move {
            barrier.wait().await;
            
            let mut processed = 0;
            
            loop {
                // Try to claim items
                let items = claim_work_queue_items(ctx.pool(),
                    "test_worker",
                    &format!("worker-{}", worker_id),
                    1
                ).await?;
                
                if items.is_empty() {
                    break;
                }
                
                for item in items {
                    // Simulate work with smart waiting instead of fixed sleep
                    tokio::task::yield_now().await;
                    
                    complete_work_queue_item(ctx.pool(), item.queue_id).await?;
                    processed += 1;
                }
            }
            
            if test_config.verbose {
                println!("Worker {} processed {} items", worker_id, processed);
            }
            
            Ok::<i32, anyhow::Error>(processed)
        });
    }
    
    // Collect results
    let mut total = 0;
    while let Some(result) = tasks.join_next().await {
        total += result??;
    }
    
    let elapsed = start.elapsed();
    
    pretty_assertions::assert_eq!(total, 20, "All items should be processed");
    
    // Run step with timing logging
    ctx.run_step("verify_processing_time", || async {
        // Reasonable timeout check
        assert!(
            elapsed < ctx.default_timeout(),
            "Processing took too long ({:?}) - possible deadlock",
            elapsed
        );
        Ok(())
    }).await?;
    
    Ok(())
}

#[sinex_test]
async fn test_concurrent_claiming_prevents_duplicates(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Insert single test item
    let _ = worker_test_utils::insert_test_items(ctx.pool(), 1).await?;
    
    // Worker 1: Claim the item
    let claimed = claim_work_queue_items(
        ctx.pool(),
        "test_worker",
        "worker-1",
        1
    ).await?;
    
    pretty_assertions::assert_eq!(claimed.len(), 1, "Should claim exactly one item");
    
    // Worker 2: Try to claim - should get nothing
    let items = claim_work_queue_items(
        ctx.pool(),
        "test_worker",
        "worker-2",
        1
    ).await?;
    
    pretty_assertions::assert_eq!(items.len(), 0, "Worker 2 should not get any items");
    
    // Complete the item
    complete_work_queue_item(&ctx.pool, claimed[0].queue_id).await?;
    
    // Wait for completion using context helper
    ctx.wait_for_work_queue(0).await?;
    
    Ok(())
}

#[sinex_test]
async fn test_high_concurrency_stress_test(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create many items for stress testing
    let item_count = 100;
    let worker_count = 10;
    
    // Insert items in batch using context helper
    let events = ctx.create_event_batch("stress_test", item_count);
    ctx.insert_events(&events).await?;
    
    // Create work queue items from events
    for event in &events {
        worker_test_utils::create_work_item(&ctx.pool, "stress_worker", event.id).await?;
    }
    
    let pool = Arc::new(ctx.pool().clone());
    let barrier = Arc::new(Barrier::new(worker_count));
    let processing_stats = Arc::new(Mutex::new(HashMap::new()));
    
    let mut tasks = JoinSet::new();
    
    // Spawn many workers
    for worker_id in 0..worker_count {
        let pool = ctx.pool().clone();
        let barrier = barrier.clone();
        let stats = processing_stats.clone();
        
        tasks.spawn(async move {
            barrier.wait().await;
            
            let mut local_stats = HashMap::new();
            let mut batch_count = 0;
            
            loop {
                // Claim in batches for efficiency
                let items = claim_work_queue_items(ctx.pool(),
                    "stress_worker",
                    &format!("worker-{}", worker_id),
                    5  // Batch size
                ).await?;
                
                if items.is_empty() {
                    break;
                }
                
                batch_count += 1;
                
                for item in items {
                    // Process without delay
                    complete_work_queue_item(ctx.pool(), item.queue_id).await?;
                    *local_stats.entry("processed").or_insert(0) += 1;
                }
            }
            
            local_stats.insert("batches", batch_count);
            
            let mut stats_guard = stats.lock().await;
            stats_guard.insert(worker_id, local_stats);
            
            Ok::<(), anyhow::Error>(())
        });
    }
    
    // Wait for completion
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    
    // Analyze results
    let stats = processing_stats.lock().await;
    let total_processed: i32 = stats.values()
        .filter_map(|s| s.get("processed"))
        .sum();
    
    pretty_assertions::assert_eq!(total_processed, item_count as i32, "All items should be processed");
    
    // Verify work was distributed
    let active_workers = stats.values()
        .filter(|s| s.get("processed").copied().unwrap_or(0) > 0)
        .count();
    
    assert!(
        active_workers >= worker_count / 2,
        "At least half the workers should have processed items"
    );
    
    // Verify no items remain
    ctx.wait_for_work_queue(0).await?;
    
    Ok(())
}

#[sinex_test]
async fn test_worker_failure_recovery(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test that items can be reclaimed after worker failure
    let _ = worker_test_utils::insert_test_items(&ctx.pool, 5).await?;
    
    // Worker 1 claims items but "crashes"
    let claimed = claim_work_queue_items(
        ctx.pool(),
        "test_worker",
        "worker-failing",
        5
    ).await?;
    
    pretty_assertions::assert_eq!(claimed.len(), 5, "Should claim all items");
    
    // Simulate worker crash by not completing items
    // In real system, these would timeout and be reclaimed
    
    // For test purposes, manually reset items
    for item in &claimed {
        sqlx::query!(
            "UPDATE sinex_schemas.work_queue 
             SET status = 'pending', processing_worker_id = NULL 
             WHERE queue_id = $1::uuid::ulid",
            item.queue_id.to_uuid()
        )
        .execute(ctx.pool())
        .await?;
    }
    
    // Worker 2 should be able to claim them
    let reclaimed = claim_work_queue_items(
        ctx.pool(),
        "test_worker",
        "worker-recovery",
        5
    ).await?;
    
    pretty_assertions::assert_eq!(reclaimed.len(), 5, "Should reclaim all items");
    
    // Complete processing
    for item in reclaimed {
        complete_work_queue_item(&ctx.pool, item.queue_id).await?;
    }
    
    // Verify all completed
    ctx.wait_for_work_queue(0).await?;
    
    Ok(())
}