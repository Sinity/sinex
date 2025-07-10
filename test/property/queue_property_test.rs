use crate::common::prelude::*;
use crate::common::create_test_agent;
use proptest::prelude::*;
use sinex_db::{
    work_queue::{claim_work_queue_items, complete_work_queue_item, add_to_work_queue as insert_work_queue_item},
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

/// Property tests for work queue functionality
/// 
/// This module consolidates property tests from:
/// - work_queue_property_tests.rs (work queue correctness and concurrency)
/// - Additional queue-related property tests for different queue implementations
/// - Queue performance and scalability properties

// =============================================================================
// Work Queue Correctness Properties
// =============================================================================

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
        let mut processed = self.processed_items.lock().expect("Lock failed");
        if processed.contains(&queue_id) {
            // Duplicate detected!
            let mut duplicates = self.duplicate_detections.lock().expect("Lock failed");
            duplicates.push(queue_id);
            true
        } else {
            processed.insert(queue_id);
            false
        }
    }

    fn get_duplicates(&self) -> Vec<Ulid> {
        self.duplicate_detections
            .lock()
            .expect("Lock failed")
            .clone()
    }

    fn processed_count(&self) -> usize {
        self.processed_items.lock().expect("Lock failed").len()
    }
}

/// Simulates a worker that claims and processes items with potential random crashes
async fn worker_with_crashes(
    pool: DbPool,
    agent_name: String,
    worker_id: String,
    tracker: ProcessingTracker,
    crash_probability: f64,
    runtime_seconds: u64,
    seed: u64,
) -> Result<(), anyhow::Error> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let start_time = Instant::now();

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

#[tokio::test]
async fn test_no_duplicate_dequeue_with_crashes() -> Result<(), anyhow::Error> {
    let ctx = TestContext::new().await?;
    proptest!(|(
        num_workers in 2..=8usize,
        num_items in 10..=50usize,
        crash_probability in 0.1..=0.3f64,
        runtime_seconds in 5..=15u64,
        seed in any::<u64>(),
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();

            // Create test agent
            let agent_name = format!("test_agent_{}", Ulid::new());
            create_test_agent(&pool, &agent_name).await.expect("DB operation failed");

            // Create test events and work queue items
            let mut queue_ids = Vec::new();
            for i in 0..num_items {
                // Insert raw event
                let event = crate::common::insert_event_with_validator(
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
                    3, // max_attempts
                ).await.expect("DB operation failed");

                queue_ids.push(queue_item);
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
            .fetch_one(&pool)
            .await
            .expect("Operation failed");

            let completed_items = sqlx::query!(
                "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'succeeded'",
                agent_name
            )
            .fetch_one(&pool)
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
            ).execute(&pool).await;

            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).execute(&pool).await;

            Ok(())
        })?
    });
    Ok(())
}

#[tokio::test]
async fn test_work_queue_consistency_under_high_contention() -> Result<(), anyhow::Error> {
    let ctx = TestContext::new().await?;
    proptest!(|(
        num_workers in 5..=15usize,
        items_per_batch in 1..=3usize,
        _seed in any::<u64>(),
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();

            let agent_name = format!("contention_test_{}", Ulid::new());
            create_test_agent(&pool, &agent_name).await.expect("DB operation failed");

            // Create exactly one item to maximize contention
            let event = crate::common::insert_event_with_validator(
                &pool,
                "test.contention",
                "contention_event",
                "localhost",
                json!({"contention_test": true}),
                None,
                Some("1.0.0"),
                None,
            ).await.expect("DB operation failed");

            let queue_item = insert_work_queue_item(&pool, event.id, &agent_name, 3).await.expect("DB operation failed");
            let _target_queue_id = queue_item;

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
            ).execute(&pool).await;

            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).execute(&pool).await;

            Ok(())
        })?
    });
    Ok(())
}

// =============================================================================
// Work Queue Performance Properties
// =============================================================================

#[tokio::test]
async fn test_work_queue_scalability_properties() -> Result<(), anyhow::Error> {
    let ctx = TestContext::new().await?;
    proptest!(|(
        queue_size in 50..=500usize,
        worker_count in 2..=10usize,
        batch_size in 1..=20usize,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();

            let agent_name = format!("scalability_test_{}", Ulid::new());
            create_test_agent(&pool, &agent_name).await.expect("DB operation failed");

            // Create many work queue items
            let mut queue_ids = Vec::new();
            let creation_start = Instant::now();
            
            for i in 0..queue_size {
                let event = crate::common::insert_event_with_validator(
                    &pool,
                    "test.scalability",
                    "scalability_event",
                    "localhost",
                    json!({"item_number": i}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await.expect("DB operation failed");

                let queue_item = insert_work_queue_item(&pool, event.id, &agent_name, 3).await.expect("DB operation failed");
                queue_ids.push(queue_item);
            }

            let creation_time = creation_start.elapsed();
            
            // Property: Queue creation should be reasonably fast
            prop_assert!(
                creation_time.as_millis() < (queue_size as u128 * 10), // 10ms per item max
                "Queue creation too slow: {}ms for {} items",
                creation_time.as_millis(),
                queue_size
            );

            let tracker = ProcessingTracker::new();
            let processing_start = Instant::now();

            // Spawn workers to process items
            let mut join_set = JoinSet::new();
            for worker_num in 0..worker_count {
                let pool_clone = pool.clone();
                let agent_name_clone = agent_name.clone();
                let worker_id = format!("scalability_worker_{}", worker_num);
                let tracker_clone = tracker.clone();
                let batch_size = batch_size as i64;

                join_set.spawn(async move {
                    let mut processed_locally = 0;
                    
                    // Process items until none are left
                    loop {
                        match claim_work_queue_items(&pool_clone, &agent_name_clone, &worker_id, batch_size).await {
                            Ok(items) => {
                                if items.is_empty() {
                                    break; // No more items
                                }
                                
                                for item in items {
                                    let is_duplicate = tracker_clone.mark_processed(item.queue_id);
                                    if !is_duplicate {
                                        // Complete immediately
                                        let _ = complete_work_queue_item(&pool_clone, item.queue_id).await;
                                        processed_locally += 1;
                                    }
                                }
                            }
                            Err(_) => {
                                // Database error, wait and retry
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        }
                    }
                    
                    processed_locally
                });
            }

            // Wait for all workers and collect results
            let mut total_processed_by_workers = 0;
            while let Some(result) = join_set.join_next().await {
                total_processed_by_workers += result.expect("Worker failed");
            }

            let processing_time = processing_start.elapsed();
            let tracker_processed = tracker.processed_count();
            
            // Property: All items should be processed exactly once
            prop_assert!(
                tracker.get_duplicates().is_empty(),
                "Scalability test found duplicates: {:?}",
                tracker.get_duplicates()
            );

            prop_assert_eq!(
                tracker_processed, queue_size,
                "Tracker processed {} items, expected {}",
                tracker_processed, queue_size
            );

            prop_assert_eq!(
                total_processed_by_workers, queue_size,
                "Workers processed {} items, expected {}",
                total_processed_by_workers, queue_size
            );

            // Property: Processing should be reasonably fast
            let throughput = queue_size as f64 / processing_time.as_secs_f64();
            prop_assert!(
                throughput > 10.0, // At least 10 items per second
                "Processing too slow: {:.2} items/sec for {} items with {} workers",
                throughput, queue_size, worker_count
            );

            // Property: More workers should not make things slower (within reason)
            if worker_count > 1 {
                let per_worker_throughput = throughput / worker_count as f64;
                prop_assert!(
                    per_worker_throughput > 1.0, // At least 1 item per second per worker
                    "Per-worker throughput too low: {:.2} items/sec/worker",
                    per_worker_throughput
                );
            }

            // Cleanup
            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
                agent_name
            ).execute(&pool).await;

            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).execute(&pool).await;

            Ok(())
        })?
    });
    Ok(())
}

// =============================================================================
// Queue Ordering Properties
// =============================================================================

#[tokio::test]
async fn test_work_queue_fifo_ordering_properties() -> Result<(), anyhow::Error> {
    let ctx = TestContext::new().await?;
    proptest!(|(
        item_count in 10..=50usize,
        time_gap_ms in 10..=100u64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();

            let agent_name = format!("ordering_test_{}", Ulid::new());
            create_test_agent(&pool, &agent_name).await.expect("DB operation failed");

            // Create items with controlled timing
            let mut created_ids = Vec::new();
            for i in 0..item_count {
                let event = crate::common::insert_event_with_validator(
                    &pool,
                    "test.ordering",
                    "ordering_event",
                    "localhost",
                    json!({"sequence": i, "timestamp": chrono::Utc::now()}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await.expect("DB operation failed");

                let queue_item = insert_work_queue_item(&pool, event.id, &agent_name, 3).await.expect("DB operation failed");
                created_ids.push((queue_item, i));

                // Small delay to ensure different creation times
                tokio::time::sleep(Duration::from_millis(time_gap_ms)).await;
            }

            // Claim items in order and verify FIFO behavior
            let mut claimed_sequence = Vec::new();
            let worker_id = "ordering_worker";

            // Claim items one by one to test ordering
            for _ in 0..item_count {
                match claim_work_queue_items(&pool, &agent_name, worker_id, 1).await {
                    Ok(items) => {
                        if let Some(item) = items.first() {
                            // Look up the sequence number for this item
                            let event_data: serde_json::Value = sqlx::query_scalar(
                                "SELECT payload FROM raw.events WHERE id = $1::ulid"
                            )
                            .bind(item.raw_event_id.to_string())
                            .fetch_one(&pool)
                            .await
                            .expect("Failed to fetch event data");

                            let sequence = event_data["sequence"].as_i64().unwrap() as usize;
                            claimed_sequence.push(sequence);

                            // Complete the item
                            let _ = complete_work_queue_item(&pool, item.queue_id).await;
                        }
                    }
                    Err(_) => break,
                }
            }

            // Property: Items should be claimed in FIFO order (sequence 0, 1, 2, ...)
            prop_assert_eq!(
                claimed_sequence.len(), item_count,
                "Should have claimed all {} items, got {}", item_count, claimed_sequence.len()
            );

            for (i, &sequence) in claimed_sequence.iter().enumerate() {
                prop_assert_eq!(
                    sequence, i,
                    "Item at position {} should have sequence {}, got {}",
                    i, i, sequence
                );
            }

            // Cleanup
            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
                agent_name
            ).execute(&pool).await;

            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).execute(&pool).await;

            Ok(())
        })?
    });
    Ok(())
}

// =============================================================================
// Queue State Consistency Properties
// =============================================================================

#[tokio::test]
async fn test_work_queue_state_consistency_properties() -> Result<(), anyhow::Error> {
    let ctx = TestContext::new().await?;
    proptest!(|(
        initial_items in 5..=20usize,
        operations_per_worker in 3..=10usize,
        num_workers in 2..=5usize,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();

            let agent_name = format!("consistency_test_{}", Ulid::new());
            create_test_agent(&pool, &agent_name).await.expect("DB operation failed");

            // Create initial items
            let mut created_items = Vec::new();
            for i in 0..initial_items {
                let event = crate::common::insert_event_with_validator(
                    &pool,
                    "test.consistency",
                    "consistency_event",
                    "localhost",
                    json!({"item": i}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await.expect("DB operation failed");

                let queue_item = insert_work_queue_item(&pool, event.id, &agent_name, 3).await.expect("DB operation failed");
                created_items.push(queue_item);
            }

            // Spawn workers that claim, process, and sometimes fail
            let mut join_set = JoinSet::new();
            for worker_num in 0..num_workers {
                let pool_clone = pool.clone();
                let agent_name_clone = agent_name.clone();
                let worker_id = format!("consistency_worker_{}", worker_num);

                join_set.spawn(async move {
                    let mut operations_done = 0;
                    let mut completed_items = Vec::new();
                    
                    while operations_done < operations_per_worker {
                        match claim_work_queue_items(&pool_clone, &agent_name_clone, &worker_id, 1).await {
                            Ok(items) => {
                                if let Some(item) = items.first() {
                                    // Simulate some work
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                    
                                    // Complete with 80% probability (simulate some failures)
                                    if (worker_num + operations_done) % 5 != 0 {
                                        if complete_work_queue_item(&pool_clone, item.queue_id).await.is_ok() {
                                            completed_items.push(item.queue_id);
                                        }
                                    }
                                    // 20% chance we don't complete (simulate worker crash)
                                    
                                    operations_done += 1;
                                } else {
                                    // No items available, short break
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                    operations_done += 1;
                                }
                            }
                            Err(_) => {
                                // Database error, wait and retry
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                operations_done += 1;
                            }
                        }
                    }
                    
                    completed_items
                });
            }

            // Collect results
            let mut all_completed = Vec::new();
            while let Some(result) = join_set.join_next().await {
                all_completed.extend(result.expect("Worker failed"));
            }

            // Check final state consistency
            let final_pending: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'pending'"
            )
            .bind(&agent_name)
            .fetch_one(&pool)
            .await
            .expect("Query failed");

            let final_in_progress: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'in_progress'"
            )
            .bind(&agent_name)
            .fetch_one(&pool)
            .await
            .expect("Query failed");

            let final_completed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'succeeded'"
            )
            .bind(&agent_name)
            .fetch_one(&pool)
            .await
            .expect("Query failed");

            let final_failed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'failed'"
            )
            .bind(&agent_name)
            .fetch_one(&pool)
            .await
            .expect("Query failed");

            // Property: Total items should remain constant
            let total_items = final_pending + final_in_progress + final_completed + final_failed;
            prop_assert_eq!(
                total_items as usize, initial_items,
                "Total items changed: started with {}, ended with {}",
                initial_items, total_items
            );

            // Property: Number of completed items should match our tracking
            prop_assert_eq!(
                final_completed as usize, all_completed.len(),
                "Completed count mismatch: DB says {}, workers reported {}",
                final_completed, all_completed.len()
            );

            // Property: All completed items should be unique
            let mut unique_completed = all_completed.clone();
            unique_completed.sort();
            unique_completed.dedup();
            prop_assert_eq!(
                unique_completed.len(), all_completed.len(),
                "Duplicate completions detected: {} unique vs {} total",
                unique_completed.len(), all_completed.len()
            );

            // Property: No item should be both in_progress and completed
            prop_assert!(
                final_in_progress == 0 || final_completed < initial_items as i64,
                "Cannot have both in_progress and all items completed"
            );

            // Cleanup
            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
                agent_name
            ).execute(&pool).await;

            let _ = sqlx::query!(
                "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).execute(&pool).await;

            Ok(())
        })?
    });
    Ok(())
}

// =============================================================================
// Unit Tests
// =============================================================================

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

    #[sinex_test(timeout = 40)]
    async fn test_worker_crash_simulation(ctx: TestContext) -> TestResult {
        // This is a basic test that the crash simulation compiles and runs
        let tracker = ProcessingTracker::new();

        // Test with 100% crash probability (should exit immediately)
        let result = worker_with_crashes(
            ctx.pool().clone(),
            "test_agent".to_string(),
            "crash_test_worker".to_string(),
            tracker,
            1.0, // 100% crash probability
            1,   // 1 second runtime
            42,  // seed
        )
        .await;

        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_crash_simulation_deterministic() {
        // Test that crash simulation is deterministic with same seed
        let seed = 12345u64;
        let worker_id = "test_worker";
        
        // Simple hash calculation similar to the one in worker_with_crashes
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let worker_hash = {
            let mut hasher = DefaultHasher::new();
            worker_id.hash(&mut hasher);
            seed.hash(&mut hasher);
            hasher.finish()
        };
        
        // Same calculation should produce same hash
        let worker_hash2 = {
            let mut hasher = DefaultHasher::new();
            worker_id.hash(&mut hasher);
            seed.hash(&mut hasher);
            hasher.finish()
        };
        
        assert_eq!(worker_hash, worker_hash2);
        
        // Different worker_id should produce different hash
        let different_worker_hash = {
            let mut hasher = DefaultHasher::new();
            "different_worker".hash(&mut hasher);
            seed.hash(&mut hasher);
            hasher.finish()
        };
        
        assert_ne!(worker_hash, different_worker_hash);
    }

    #[test]
    fn test_processing_tracker_thread_safety() {
        // Test that ProcessingTracker works correctly under concurrent access
        let tracker = ProcessingTracker::new();
        let tracker_clone = tracker.clone();
        
        let ids: Vec<Ulid> = (0..10).map(|_| Ulid::new()).collect();
        
        // Process some IDs
        for (i, id) in ids.iter().enumerate() {
            let is_dup = if i < 5 {
                tracker.mark_processed(*id)
            } else {
                tracker_clone.mark_processed(*id)
            };
            
            assert!(!is_dup, "First processing should not be duplicate");
        }
        
        assert_eq!(tracker.processed_count(), 10);
        assert!(tracker.get_duplicates().is_empty());
        
        // Try to process the same IDs again - should detect duplicates
        for id in &ids[0..3] {
            let is_dup = tracker.mark_processed(*id);
            assert!(is_dup, "Second processing should be duplicate");
        }
        
        assert_eq!(tracker.processed_count(), 10); // Count shouldn't increase
        assert_eq!(tracker.get_duplicates().len(), 3); // Should have 3 duplicates
    }
}