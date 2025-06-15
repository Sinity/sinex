use anyhow::Result;
use sinex_db::{create_pool_from_env, models::promotion_queue::PromotionQueue};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use sinex_ulid::Ulid;
use std::time::Duration;

async fn setup_test_db() -> Result<PgPool> {
    let pool = create_pool_from_env(None).await?;
    
    // Clean up any existing test data
    sqlx::query("TRUNCATE TABLE sinex_schemas.promotion_queue")
        .execute(&pool)
        .await?;
    
    Ok(pool)
}

async fn insert_test_items(pool: &PgPool, count: i32) -> Result<Vec<Ulid>> {
    let mut ids = Vec::new();
    
    for i in 0..count {
        let id = Ulid::new();
        ids.push(id);
        
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue (id, event_id, event_type, priority, retry_count, created_at) 
             VALUES ($1, $2, $3, $4, 0, NOW())"
        )
        .bind(id.as_uuid())
        .bind(Ulid::new().as_uuid())
        .bind(format!("test_event_{}", i))
        .bind(1)
        .execute(pool)
        .await?;
    }
    
    Ok(ids)
}

#[tokio::test]
async fn test_select_for_update_skip_locked_prevents_duplicate_processing() -> Result<()> {
    let pool = setup_test_db().await?;
    let items = insert_test_items(&pool, 10).await?;
    
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
                // Try to claim an item using SELECT FOR UPDATE SKIP LOCKED
                let item = sqlx::query_as::<_, PromotionQueue>(
                    "SELECT * FROM sinex_schemas.promotion_queue 
                     WHERE status = 'pending'
                     ORDER BY priority DESC, created_at ASC
                     FOR UPDATE SKIP LOCKED
                     LIMIT 1"
                )
                .fetch_optional(&*pool)
                .await?;
                
                match item {
                    Some(item) => {
                        // Simulate processing
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        
                        // Mark as processed
                        sqlx::query(
                            "UPDATE sinex_schemas.promotion_queue 
                             SET status = 'completed', processed_at = NOW()
                             WHERE id = $1"
                        )
                        .bind(item.id)
                        .execute(&*pool)
                        .await?;
                        
                        local_processed += 1;
                    }
                    None => {
                        // No more items to process
                        break;
                    }
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
    
    // Verify no item was processed twice
    let completed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'completed'"
    )
    .fetch_one(&*pool)
    .await?;
    
    assert_eq!(completed_count, 10, "All items should be marked as completed");
    
    // Print worker distribution for visibility
    for (worker_id, count) in worker_results {
        println!("Worker {} processed {} items", worker_id, count);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_skip_locked_allows_parallel_processing() -> Result<()> {
    let pool = setup_test_db().await?;
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
                let mut tx = pool.begin().await?;
                
                let item = sqlx::query_as::<_, PromotionQueue>(
                    "SELECT * FROM sinex_schemas.promotion_queue 
                     WHERE status = 'pending'
                     FOR UPDATE SKIP LOCKED
                     LIMIT 1"
                )
                .fetch_optional(&mut *tx)
                .await?;
                
                match item {
                    Some(item) => {
                        // Simulate work
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        
                        sqlx::query(
                            "UPDATE sinex_schemas.promotion_queue 
                             SET status = 'completed'
                             WHERE id = $1"
                        )
                        .bind(item.id)
                        .execute(&mut *tx)
                        .await?;
                        
                        tx.commit().await?;
                        processed += 1;
                    }
                    None => {
                        tx.commit().await?;
                        break;
                    }
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
    
    // With 4 workers processing 20 items at 100ms each, should take ~500ms
    // Allow some overhead
    assert!(
        elapsed.as_millis() < 800,
        "Parallel processing should be faster than serial (took {}ms)",
        elapsed.as_millis()
    );
    
    println!("Processed 20 items in {}ms with 4 workers", elapsed.as_millis());
    
    Ok(())
}

#[tokio::test]
async fn test_transaction_rollback_releases_lock() -> Result<()> {
    let pool = setup_test_db().await?;
    let items = insert_test_items(&pool, 1).await?;
    let item_id = items[0];
    
    // Worker 1: Start transaction and acquire lock, then rollback
    let mut tx1 = pool.begin().await?;
    
    let _item = sqlx::query_as::<_, PromotionQueue>(
        "SELECT * FROM sinex_schemas.promotion_queue 
         WHERE id = $1
         FOR UPDATE"
    )
    .bind(item_id.as_uuid())
    .fetch_one(&mut *tx1)
    .await?;
    
    // Spawn worker 2 that tries to acquire the same row
    let pool2 = pool.clone();
    let worker2 = tokio::spawn(async move {
        // Small delay to ensure worker 1 has the lock
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        let start = std::time::Instant::now();
        
        // This should block until worker 1 releases the lock
        let item = sqlx::query_as::<_, PromotionQueue>(
            "SELECT * FROM sinex_schemas.promotion_queue 
             WHERE id = $1
             FOR UPDATE NOWAIT"  // Use NOWAIT to detect if locked
        )
        .bind(item_id.as_uuid())
        .fetch_optional(&pool2)
        .await;
        
        let elapsed = start.elapsed();
        
        match item {
            Ok(None) => panic!("Item should exist"),
            Ok(Some(_)) => panic!("Should not be able to acquire lock immediately"),
            Err(e) => {
                // Should get a lock not available error
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("could not obtain lock") || 
                    error_msg.contains("lock not available"),
                    "Expected lock error, got: {}",
                    error_msg
                );
            }
        }
        
        elapsed
    });
    
    // Give worker 2 time to try acquiring the lock
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Rollback transaction, releasing the lock
    tx1.rollback().await?;
    
    // Worker 2 should detect the lock
    let elapsed = worker2.await?;
    assert!(elapsed.as_millis() < 200, "Lock detection should be quick");
    
    // Now verify the lock is released by acquiring it again
    let item = sqlx::query_as::<_, PromotionQueue>(
        "SELECT * FROM sinex_schemas.promotion_queue 
         WHERE id = $1
         FOR UPDATE NOWAIT"
    )
    .bind(item_id.as_uuid())
    .fetch_optional(&pool)
    .await?;
    
    assert!(item.is_some(), "Lock should be released after rollback");
    
    Ok(())
}

#[tokio::test]
async fn test_priority_ordering_with_concurrent_workers() -> Result<()> {
    let pool = setup_test_db().await?;
    
    // Insert items with different priorities
    for i in 0..10 {
        let priority = if i < 3 { 10 } else if i < 7 { 5 } else { 1 };
        
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue 
             (id, event_id, event_type, priority, retry_count, created_at) 
             VALUES ($1, $2, $3, $4, 0, NOW())"
        )
        .bind(Ulid::new().as_uuid())
        .bind(Ulid::new().as_uuid())
        .bind(format!("test_event_{}", i))
        .bind(priority)
        .execute(&pool)
        .await?;
    }
    
    let pool = Arc::new(pool);
    let processed_priorities = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(2));
    
    let mut tasks = JoinSet::new();
    
    // Spawn 2 workers
    for _ in 0..2 {
        let pool = pool.clone();
        let barrier = barrier.clone();
        let processed_priorities = processed_priorities.clone();
        
        tasks.spawn(async move {
            barrier.wait().await;
            
            loop {
                let mut tx = pool.begin().await?;
                
                let item = sqlx::query_as::<_, PromotionQueue>(
                    "SELECT * FROM sinex_schemas.promotion_queue 
                     WHERE status = 'pending'
                     ORDER BY priority DESC, created_at ASC
                     FOR UPDATE SKIP LOCKED
                     LIMIT 1"
                )
                .fetch_optional(&mut *tx)
                .await?;
                
                match item {
                    Some(item) => {
                        let mut priorities = processed_priorities.lock().await;
                        priorities.push(item.priority);
                        
                        sqlx::query(
                            "UPDATE sinex_schemas.promotion_queue 
                             SET status = 'completed'
                             WHERE id = $1"
                        )
                        .bind(item.id)
                        .execute(&mut *tx)
                        .await?;
                        
                        tx.commit().await?;
                    }
                    None => {
                        tx.commit().await?;
                        break;
                    }
                }
            }
            
            Ok::<(), anyhow::Error>(())
        });
    }
    
    // Wait for completion
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    
    let priorities = processed_priorities.lock().await;
    
    // Verify high priority items were processed first
    let high_priority_count = priorities.iter().take(3).filter(|&&p| p == 10).count();
    assert_eq!(
        high_priority_count, 3,
        "All high priority items should be processed first"
    );
    
    println!("Processing order by priority: {:?}", *priorities);
    
    Ok(())
}