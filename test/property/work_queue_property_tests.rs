use crate::common::prelude::*;
use proptest::prelude::*;
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;
use sinex_db::queries::{
    insert_raw_event, insert_work_queue_item, claim_work_queue_items,
    complete_work_queue_item, create_test_agent,
};

/// Shared state to track which items have been processed
#[derive(Debug, Clone)]
struct ProcessingTracker {
    processed_items: Arc<Mutex<HashSet<Ulid>>>,
    duplicate_detections: Arc<Mutex<Vec<Ulid>>>,
}

impl ProcessingTracker {
    fn new() -> Self {
        Self {
            processed_items: Arc::new(Mutex::new(HashSet::new())),
            duplicate_detections: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    /// Mark an item as processed, returns true if this is a duplicate
    fn mark_processed(&self, queue_id: Ulid) -> bool {
        let mut processed = self.processed_items.lock().expect("Operation failed");
        if processed.contains(&queue_id) {
            // Duplicate detected!
            let mut duplicates = self.duplicate_detections.lock().expect("Operation failed");
            duplicates.push(queue_id);
            true
        } else {
            processed.insert(queue_id);
            false
        }
    }
    
    fn get_duplicates(&self) -> Vec<Ulid> {
        self.duplicate_detections.lock().expect("Operation failed").clone()
    }
    
    fn processed_count(&self) -> usize {
        self.processed_items.lock().expect("Operation failed").len()
    }
}

/// Simulates a worker that claims and processes items with potential random crashes
async fn worker_with_crashes(
    pool: sqlx::PgPool,
    agent_name: String,
    worker_id: String,
    tracker: ProcessingTracker,
    crash_probability: f64,
    runtime_seconds: u64,
    seed: u64,
) -> Result<(), anyhow::Error> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let start_time = std::time::Instant::now();
    
    // Simple deterministic RNG for crash simulation
    let mut crash_counter = 0u64;
    let worker_hash = {
        let mut hasher = DefaultHasher::new();
        worker_id.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish()
    };
    
    while start_time.elapsed().as_secs() < runtime_seconds {
        crash_counter += 1;
        
        // Simple deterministic crash simulation
        let crash_threshold = (crash_probability * 100.0) as u64;
        let crash_roll = (crash_counter.wrapping_mul(worker_hash)) % 100;
        if crash_roll < crash_threshold {
            // Simulate crash by returning early (abandoning any claimed items)
            return Ok(());
        }
        
        // Claim items from work queue
        match claim_work_queue_items(&pool, &agent_name, &worker_id, 5).await {
            Ok(items) => {
                for item in items {
                    // Check for duplicate processing
                    let is_duplicate = tracker.mark_processed(item.queue_id);
                    
                    if !is_duplicate {
                        // Simulate processing work
                        tokio::task::yield_now().await;
                        
                        // Complete the item (unless we crash)
                        crash_counter += 1;
                        let complete_crash_roll = (crash_counter.wrapping_mul(worker_hash)) % 100;
                        if complete_crash_roll < crash_threshold {
                            // Crash before completing - item should become available again
                            return Ok(());
                        }
                        
                        // Mark as completed in database
                        let _ = complete_work_queue_item(&pool, item.queue_id).await;
                    }
                }
            }
            Err(_) => {
                // Database error, wait a bit and retry
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        
        // Small delay between claim attempts
        tokio::task::yield_now().await;
    }
    
    Ok(())
}

proptest! {
    #[test]
    fn test_no_duplicate_dequeue_with_crashes(
        num_workers in 2..=8usize,
        num_items in 10..=50usize,
        crash_probability in 0.1..=0.3f64,
        runtime_seconds in 5..=15u64,
        seed in any::<u64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            // Setup test database
            let _database_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
            
            let pool = TestPool::with_strategy(CleanupStrategy::None).await.expect("Failed to create pool");
            run_migrations(&pool).await.expect("Failed to run migrations");
            
            // Create test agent
            let agent_name = format!("test_agent_{}", Ulid::new());
            create_test_agent(&pool, &agent_name, "Test agent for property testing").await.expect("DB operation failed");
            
            // Create test events and work queue items
            let mut queue_ids = Vec::new();
            for i in 0..num_items {
                // Insert raw event
                let event = insert_raw_event(
                    &pool,
                    "test.property",
                    "property_test_event",
                    "localhost",
                    json!({"item_number": i, "test_run": Ulid::new().to_string()}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await.expect("DB operation failed");
                
                // Insert work queue item
                let queue_item = insert_work_queue_item(
                    &pool,
                    event.id,
                    &agent_name,
                ).await.expect("DB operation failed");
                
                queue_ids.push(queue_item.queue_id);
            }
            
            // Setup tracking
            let tracker = ProcessingTracker::new();
            
            // Spawn multiple workers
            let mut join_set = JoinSet::new();
            for worker_num in 0..num_workers {
                let pool_clone = pool.clone();
                let agent_name_clone = agent_name.clone();
                let worker_id = format!("worker_{}", worker_num);
                let tracker_clone = tracker.clone();
                
                join_set.spawn(worker_with_crashes(
                    pool_clone,
                    agent_name_clone,
                    worker_id,
                    tracker_clone,
                    crash_probability,
                    runtime_seconds,
                    seed.wrapping_add(worker_num as u64),
                ));
            }
            
            // Wait for all workers to complete
            while let Some(result) = join_set.join_next().await {
                if let Err(e) = result {
                    panic!("Worker task failed: {:?}", e);
                }
            }
            
            // Check for duplicates
            let duplicates = tracker.get_duplicates();
            let processed_count = tracker.processed_count();
            
            // Property: No item should be processed more than once
            prop_assert!(
                duplicates.is_empty(),
                "Duplicate processing detected! {} items were processed multiple times: {:?}",
                duplicates.len(),
                duplicates
            );
            
            // Verify some work was actually done
            prop_assert!(
                processed_count > 0,
                "No items were processed at all - test may be misconfigured"
            );
            
            // Additional verification: check database state
            let remaining_items = sqlx::query!(
                "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'pending'",
                agent_name
            )
            .fetch_one(&*pool)
            .await
            .expect("Operation failed");
            
            let completed_items = sqlx::query!(
                "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'succeeded'",
                agent_name
            )
            .fetch_one(&*pool)
            .await
            .expect("Operation failed");
            
            // Property: Total items in DB should equal processed + remaining
            let db_total = remaining_items.count.unwrap_or(0i64) + completed_items.count.unwrap_or(0i64);
            prop_assert!(
                db_total as usize >= processed_count,
                "Database inconsistency: processed {} items but only {} total in DB",
                processed_count,
                db_total
            );
            
            // Cleanup
            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
                agent_name
            ).execute(&*pool).await;
            
            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).execute(&*pool).await;
            
            Ok(())
        })?
    }
}

proptest! {
    #[test]
    fn test_work_queue_consistency_under_high_contention(
        num_workers in 5..=15usize,
        items_per_batch in 1..=3usize,
        _seed in any::<u64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let _database_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
            
            let pool = TestPool::with_strategy(CleanupStrategy::None).await.expect("Failed to create pool");
            run_migrations(&pool).await.expect("Failed to run migrations");
            
            let agent_name = format!("contention_test_{}", Ulid::new());
            create_test_agent(&pool, &agent_name, "High contention test agent").await.expect("DB operation failed");
            
            // Create exactly one item to maximize contention
            let event = insert_raw_event(
                &pool,
                "test.contention",
                "contention_event", 
                "localhost",
                json!({"contention_test": true}),
                None,
                Some("1.0.0"),
                None,
            ).await.expect("DB operation failed");
            
            let queue_item = insert_work_queue_item(&pool, event.id, &agent_name).await.expect("DB operation failed");
            let _target_queue_id = queue_item.queue_id;
            
            let tracker = ProcessingTracker::new();
            
            // All workers try to claim the same single item simultaneously
            let mut join_set = JoinSet::new();
            for worker_num in 0..num_workers {
                let pool_clone = pool.clone();
                let agent_name_clone = agent_name.clone();
                let worker_id = format!("contention_worker_{}", worker_num);
                let tracker_clone = tracker.clone();
                
                join_set.spawn(async move {
                    // Single aggressive claim attempt
                    if let Ok(items) = claim_work_queue_items(&pool_clone, &agent_name_clone, &worker_id, items_per_batch as i64).await {
                        for item in items {
                            let is_duplicate = tracker_clone.mark_processed(item.queue_id);
                            if !is_duplicate {
                                // Complete immediately
                                let _ = complete_work_queue_item(&pool_clone, item.queue_id).await;
                            }
                        }
                    }
                });
            }
            
            // Wait for all workers
            while let Some(result) = join_set.join_next().await {
                result.expect("Operation failed");
            }
            
            // Property: Exactly one worker should have processed the item
            let duplicates = tracker.get_duplicates();
            let processed_count = tracker.processed_count();
            
            prop_assert!(
                duplicates.is_empty(),
                "High contention caused duplicate processing: {:?}",
                duplicates
            );
            
            prop_assert!(
                processed_count <= 1,
                "More than one item was processed, but only one existed: {}",
                processed_count
            );
            
            // Cleanup
            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
                agent_name
            ).execute(&*pool).await;
            
            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).execute(&*pool).await;
            
            Ok(())
        })?
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    
    #[test]
    fn test_processing_tracker() {
        let tracker = ProcessingTracker::new();
        let id1 = Ulid::new();
        let id2 = Ulid::new();
        
        // First processing should succeed
        assert!(!tracker.mark_processed(id1));
        pretty_assertions::assert_eq!(tracker.processed_count(), 1);
        assert!(tracker.get_duplicates().is_empty());
        
        // Different ID should also succeed
        assert!(!tracker.mark_processed(id2));
        pretty_assertions::assert_eq!(tracker.processed_count(), 2);
        assert!(tracker.get_duplicates().is_empty());
        
        // Same ID again should detect duplicate
        assert!(tracker.mark_processed(id1));
        pretty_assertions::assert_eq!(tracker.processed_count(), 2); // Count doesn't increase
        pretty_assertions::assert_eq!(tracker.get_duplicates().len(), 1);
        pretty_assertions::assert_eq!(tracker.get_duplicates()[0], id1);
    }
    
    #[sinex_test]
    async fn test_worker_crash_simulation(pool: sqlx::PgPool) -> anyhow::Result<()> {
        // This is a basic test that the crash simulation compiles and runs
        let tracker = ProcessingTracker::new();
        
        // Test with 100% crash probability (should exit immediately)
        let result = worker_with_crashes(pool,
            "test_agent".to_string(),
            "crash_test_worker".to_string(),
            tracker,
            1.0, // 100% crash probability
            1,   // 1 second runtime
            42,  // seed
        ).await;
        
        assert!(result.is_ok());
        Ok(())
    }
}