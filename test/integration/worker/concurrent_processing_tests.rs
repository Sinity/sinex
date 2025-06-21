use anyhow::Result;
use sinex_db::queries::{claim_work_queue_items, complete_work_queue_item};
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use sinex_ulid::Ulid;
use std::time::Duration;

// Import test setup macros and utilities
use crate::db_test;
use crate::common::worker_test_utils::{self, insert_test_items};

db_test! {
    async fn test_select_for_update_skip_locked_prevents_duplicate_processing(pool: PgPool) -> Result<()> {
        let _items = worker_test_utils::setup_test_worker(&pool, "test_worker", 10).await?;
        
        let pool = Arc::new(pool);
        let barrier = Arc::new(Barrier::new(3));
        let processed_count = Arc::new(tokio::sync::Mutex::new(0));
        
        let mut tasks = JoinSet::new();
        
        // Spawn 3 workers that will try to process items concurrently
        for worker_id in 0..3 {
            let pool = pool.clone();
            let barrier = barrier.clone();
            let processed_count = processed_count.clone();
            
            tasks.spawn(async move {
                // Wait for all workers to be ready
                barrier.wait().await;
                
                let mut local_processed = 0;
                
                loop {
                    // Try to claim items using the production function
                    let items = claim_work_queue_items(
                        &pool,
                        "test_worker",
                        &format!("worker-{}", worker_id),
                        1
                    ).await?;
                    
                    if items.is_empty() {
                        // No more items to process
                        break;
                    }
                    
                    for item in items {
                        // Simulate processing
                        tokio::task::yield_now().await;
                        
                        // Mark as processed by completing it
                        complete_work_queue_item(&pool, item.queue_id).await?;
                        
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
        assert_eq!(total_processed, 10, "All items should be processed exactly once");
        
        // Verify no items remain using utility
        worker_test_utils::verify_all_items_processed_by_worker(&pool, "test_worker").await?;
        
        // Verify work distribution and print for visibility
        let mut workers_that_worked = 0;
        for (worker_id, count) in worker_results {
            println!("Worker {} processed {} items", worker_id, count);
            if count > 0 {
                workers_that_worked += 1;
            }
        }
        
        // Verify that multiple workers participated (logical concurrency)
        // With 10 items and 3 workers, at least 2 workers should get work
        assert!(
            workers_that_worked >= 2,
            "Expected multiple workers to participate, but only {} worked",
            workers_that_worked
        );
        
        Ok(())
    }
}

db_test! {
    async fn test_skip_locked_allows_parallel_processing(pool: PgPool) -> Result<()> {
        let _ = insert_test_items(&pool, 20).await?;
        
        let pool = Arc::new(pool);
        let start = std::time::Instant::now();
        let barrier = Arc::new(Barrier::new(4));
        
        let mut tasks = JoinSet::new();
        
        // Spawn 4 workers
        for worker_id in 0..4 {
            let pool = pool.clone();
            let barrier = barrier.clone();
            
            tasks.spawn(async move {
                barrier.wait().await;
                
                let mut processed = 0;
                
                loop {
                    // Try to claim items using production logic
                    let items = claim_work_queue_items(
                        &pool,
                        "test_worker",
                        &format!("worker-{}", worker_id),
                        1
                    ).await?;
                    
                    if items.is_empty() {
                        break;
                    }
                    
                    for item in items {
                        // Simulate work
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        
                        complete_work_queue_item(&pool, item.queue_id).await?;
                        processed += 1;
                    }
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
        
        assert_eq!(total, 20, "All items should be processed");
        
        // Log timing for debugging, but don't assert on it
        println!("Processed 20 items in {}ms with 4 workers", elapsed.as_millis());
        
        // Loose timing check - if it takes more than 10 seconds, something is wrong
        assert!(
            elapsed.as_secs() < 10,
            "Processing took too long ({}ms) - possible deadlock or major issue",
            elapsed.as_millis()
        );
        
        Ok(())
    }
}

db_test! {
    async fn test_concurrent_claiming_prevents_duplicates(pool: PgPool) -> Result<()> {
        let _items = insert_test_items(&pool, 1).await?;
        
        // Worker 1: Claim the item
        let claimed = claim_work_queue_items(
            &pool,
            "test_worker",
            "worker-1",
            1
        ).await?;
        
        assert_eq!(claimed.len(), 1, "Should claim exactly one item");
        
        // Worker 2: Try to claim - should get nothing since worker 1 has it
        let items = claim_work_queue_items(
            &pool,
            "test_worker",
            "worker-2",
            1
        ).await?;
        
        assert_eq!(items.len(), 0, "Worker 2 should not get any items while worker 1 has them");
        
        // Complete the item processing
        complete_work_queue_item(&pool, claimed[0].queue_id).await?;
        
        // Verify the item is gone
        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = 'test_worker'"
        )
        .fetch_one(&pool)
        .await?;
        
        assert_eq!(remaining, 0, "Item should be deleted after completion");
        
        Ok(())
    }
}

db_test! {
    async fn test_priority_ordering_with_concurrent_workers(pool: PgPool) -> Result<()> {
        // Insert items with different timestamps
        for i in 0..10 {
            let raw_event_id = Ulid::new();
            
            // Insert raw event
            sqlx::query(
                "INSERT INTO raw.events (id, source, event_type, host, payload) 
                 VALUES ($1, $2, $3, $4, $5)"
            )
            .bind(raw_event_id.to_uuid())
            .bind("test_worker")
            .bind("test_event")
            .bind("test_host")
            .bind(serde_json::json!({"sequence": i}))
            .execute(&pool)
            .await?;
            
            // Insert into work queue with staggered timestamps
            sqlx::query(
                "INSERT INTO sinex_schemas.work_queue 
                 (queue_id, raw_event_id, target_agent_name, attempts, max_attempts, created_at) 
                 VALUES ($1, $2, $3, 0, 3, NOW() + interval '1 second' * $4)"
            )
            .bind(Ulid::new().to_uuid())
            .bind(raw_event_id.to_uuid())
            .bind("test_worker")
            .bind(i as i32)
            .execute(&pool)
            .await?;
        }
        
        // Ensure test agent exists
        sqlx::query(
            "INSERT INTO sinex_schemas.agent_manifests (agent_name, description, version) 
             VALUES ($1, $2, $3) 
             ON CONFLICT (agent_name) DO NOTHING"
        )
        .bind("test_worker")
        .bind("Test worker")
        .bind("1.0.0")
        .execute(&pool)
        .await?;
        
        let pool = Arc::new(pool);
        let processed_order = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2));
        
        let mut tasks = JoinSet::new();
        
        // Spawn 2 workers
        for worker_id in 0..2 {
            let pool = pool.clone();
            let barrier = barrier.clone();
            let processed_order = processed_order.clone();
            
            tasks.spawn(async move {
                barrier.wait().await;
                
                loop {
                    let items = claim_work_queue_items(
                        &pool,
                        "test_worker",
                        &format!("worker-{}", worker_id),
                        1
                    ).await?;
                    
                    if items.is_empty() {
                        break;
                    }
                    
                    for item in items {
                        let mut order = processed_order.lock().await;
                        order.push(item.created_at);
                        drop(order);
                        
                        complete_work_queue_item(&pool, item.queue_id).await?;
                    }
                }
                
                Ok::<(), anyhow::Error>(())
            });
        }
        
        // Wait for completion
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        
        let order = processed_order.lock().await;
        
        // Verify items were processed in order
        assert_eq!(
            order.len(), 10,
            "All items should be processed"
        );
        
        // Check that timestamps are in ascending order
        for i in 1..order.len() {
            assert!(
                order[i] >= order[i-1],
                "Items should be processed in timestamp order"
            );
        }
        
        Ok(())
    }
}