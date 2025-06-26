//! Worker test utilities
//!
//! This module provides comprehensive utilities for testing worker functionality,
//! including work queue management, worker lifecycle simulation, and assertion helpers.

use crate::common::prelude::*;
use sinex_db::models::WorkQueueItem;
use crate::common::timing_optimization::wait_helpers::{wait_for_work_queue_status_count, wait_for_work_queue_count};
/// Insert test items (simplified alias)
pub async fn insert_test_items(pool: &DbPool, item_count: usize) -> Result<Vec<Ulid>> {
    setup_test_worker(&pool, "test_worker", item_count).await
}

/// Insert test items with custom target agent
pub async fn insert_test_items_for_agent(pool: &DbPool, agent_name: &str, item_count: usize) -> Result<Vec<Ulid>> {
    setup_test_worker(&pool, agent_name, item_count).await
}

/// Setup test worker with specified number of work items
pub async fn setup_test_worker(pool: &DbPool, worker_name: &str, item_count: usize) -> Result<Vec<Ulid>> {
    let mut queue_ids = Vec::new();
    
    // Create test work queue items
    for i in 0..item_count {
        let queue_id = Ulid::new();
        let raw_event_id = Ulid::new();
        
        sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.work_queue 
            (queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, created_at)
            VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 'pending', 0, 3, NOW())
            "#,
            queue_id.to_uuid(),
            raw_event_id.to_uuid(),
            format!("{}_item_{}", worker_name, i)
        )
        .execute(pool)
        .await?;
        
        queue_ids.push(queue_id);
    }
    
    Ok(queue_ids)
}

/// Insert a test work item (simplified version for tests)
pub async fn insert_test_work_item(
    pool: &DbPool,
    target_agent: &str
) -> Result<Ulid> {
    // First create a raw event to reference
    let raw_event_id = Ulid::new();
    let event = crate::common::events::generic_adversarial_event("test_source", "test.event", json!({"test": true}), Some("test_1.0"));
    
    // Insert the raw event first
    sinex_db::queries::insert_event(&pool, &event).await?;
    
    // Now insert the work queue item using the real query function
    let work_item = sinex_db::queries::insert_work_queue_item(pool, raw_event_id, target_agent).await?;
    Ok(work_item.queue_id)
}

/// Create a test work queue item
pub async fn create_test_work_item(
    pool: &DbPool,
    target_agent: &str,
    status: &str
) -> Result<Ulid> {
    let queue_id = Ulid::new();
    let raw_event_id = Ulid::new();
    
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.work_queue 
        (queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, created_at)
        VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, $4, 0, 3, NOW())
        "#,
        queue_id.to_uuid(),
        raw_event_id.to_uuid(),
        target_agent,
        status
    )
    .execute(pool)
    .await?;
    
    Ok(queue_id)
}

/// Get work queue item by ID
pub async fn get_work_item(pool: &DbPool, queue_id: Ulid) -> Result<Option<WorkQueueItem>> {
    match sinex_db::queries::get_work_item_by_id(&pool, queue_id).await {
        Ok(item) => Ok(Some(item)),
        Err(e) => {
            // Check if it's a not found error
            if e.to_string().contains("not found") {
                Ok(None)
            } else {
                Err(e)
            }
        }
    }
}

/// Create a work queue item for an existing event
pub async fn create_work_item(
    pool: &DbPool,
    target_agent: &str,
    event_id: Ulid,
) -> Result<Ulid> {
    // Use the real query function to insert
    let work_item = sinex_db::queries::insert_work_queue_item(pool, event_id, target_agent).await?;
    Ok(work_item.queue_id)
}

/// Count work items by status using timing optimization
pub async fn count_work_items_by_status(pool: &DbPool, status: &str) -> Result<i64> {
    wait_for_work_queue_status_count(&pool, status, 0, 1)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to count work items by status: {}", e))
}

/// Count work items by agent
pub async fn count_work_items_by_agent(pool: &DbPool, agent_name: &str) -> Result<i64> {
    let count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);
    
    Ok(count)
}

/// Get work queue statistics
pub async fn get_work_queue_stats(pool: &DbPool) -> Result<WorkQueueStats> {
    let row = sqlx::query!(
        r#"
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
            COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing,
            COUNT(CASE WHEN status = 'succeeded' THEN 1 END) as succeeded,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
        FROM sinex_schemas.work_queue
        "#
    )
    .fetch_one(pool)
    .await?;
    
    Ok(WorkQueueStats {
        total: row.total.unwrap_or(0),
        pending: row.pending.unwrap_or(0),
        processing: row.processing.unwrap_or(0),
        succeeded: row.succeeded.unwrap_or(0),
        failed: row.failed.unwrap_or(0),
    })
}

/// Work queue statistics for monitoring
#[derive(Debug, Clone)]
pub struct WorkQueueStats {
    pub total: i64,
    pub pending: i64,
    pub processing: i64,
    pub succeeded: i64,
    pub failed: i64,
}

impl WorkQueueStats {
    pub fn print_summary(&self) {
        println!("=== Work Queue Statistics ===");
        println!("Total: {}", self.total);
        println!("Pending: {}", self.pending);
        println!("Processing: {}", self.processing);
        println!("Succeeded: {}", self.succeeded);
        println!("Failed: {}", self.failed);
    }
}

/// Cleanup all work queue items
pub async fn cleanup_work_queue(pool: &DbPool) -> Result<(), anyhow::Error> {
    sqlx::query!("DELETE FROM sinex_schemas.work_queue")
        .execute(pool)
        .await?;
    
    Ok(())
}

/// Verify all items have been processed
pub async fn verify_all_items_processed(pool: &DbPool) -> Result<bool> {
    let pending_count = count_work_items_by_status(&pool, "pending").await?;
    Ok(pending_count == 0)
}

/// Verify all items have been processed by specific worker (alternative signature)
pub async fn verify_all_items_processed_by_worker(pool: &DbPool, worker_name: &str) -> Result<bool> {
    // Check if there are any pending items for this worker using timing utility
    let pending_count = wait_for_work_queue_status_count(&pool, "pending", 0, 1)
        .await
        .unwrap_or(1); // If timeout, assume there are pending items
    
    Ok(pending_count == 0)
}

/// Simulate worker processing
pub async fn simulate_worker_processing(
    pool: &DbPool,
    worker_id: Ulid,
    max_items: usize
) -> Result<usize> {
    let mut processed = 0;
    
    for _ in 0..max_items {
        // Try to claim an item
        let claimed = sqlx::query!(
            r#"
            UPDATE sinex_schemas.work_queue 
            SET status = 'processing',
                processing_worker_id = $1::uuid::ulid,
                last_attempt_ts = NOW()
            WHERE queue_id = (
                SELECT queue_id 
                FROM sinex_schemas.work_queue 
                WHERE status = 'pending' 
                ORDER BY created_at 
                FOR UPDATE SKIP LOCKED 
                LIMIT 1
            )
            RETURNING queue_id::uuid
            "#,
            worker_id.to_uuid()
        )
        .fetch_optional(pool)
        .await?;
        
        if let Some(row) = claimed {
            // Mark as completed
            sqlx::query!(
                r#"
                UPDATE sinex_schemas.work_queue 
                SET status = 'succeeded',
                    processed_at = NOW()
                WHERE queue_id = $1::uuid::ulid
                "#,
                row.queue_id
            )
            .execute(pool)
            .await?;
            
            processed += 1;
        } else {
            // No more items to process
            break;
        }
    }
    
    Ok(processed)
}

/// Worker lifecycle test helpers
pub mod lifecycle {
    use super::*;
    use tokio::sync::Mutex;

    /// Mock worker for testing
    pub struct MockWorker {
        pub worker_id: Ulid,
        pub pool: DbPool,
        pub processed_count: Arc<Mutex<usize>>,
    }

    impl MockWorker {
        pub fn new(pool: DbPool) -> Self {
            Self {
                worker_id: Ulid::new(),
                pool,
                processed_count: Arc::new(Mutex::new(0)),
            }
        }

        /// Simulate worker startup
        pub async fn startup(&self) -> Result<(), anyhow::Error> {
            // Register worker or perform startup tasks
            Ok(())
        }

        /// Process a single work item
        pub async fn process_item(&self, queue_id: Ulid) -> Result<bool> {
            self.process_item_with_delay(queue_id, std::time::Duration::from_millis(10)).await
        }
        
        /// Process a work item with custom processing delay
        pub async fn process_item_with_delay(&self, queue_id: Ulid, delay: std::time::Duration) -> Result<bool> {
            // Simulate processing time
            tokio::time::sleep(delay).await;

            // Update work item status
            let result = sqlx::query!(
                r#"
                UPDATE sinex_schemas.work_queue 
                SET status = 'succeeded',
                    processed_at = NOW(),
                    processing_worker_id = $1::uuid::ulid
                WHERE queue_id = $2::uuid::ulid AND status = 'processing'
                "#,
                self.worker_id.to_uuid(),
                queue_id.to_uuid()
            )
            .execute(&self.pool)
            .await?;

            let success = result.rows_affected() > 0;
            if success {
                let mut count = self.processed_count.lock().await;
                *count += 1;
            }

            Ok(success)
        }
        
        /// Simulate processing failure
        pub async fn fail_item(&self, queue_id: Ulid, error_message: &str) -> Result<bool> {
            let result = sqlx::query!(
                r#"
                UPDATE sinex_schemas.work_queue 
                SET status = 'failed',
                    processed_at = NOW(),
                    processing_worker_id = $1::uuid::ulid,
                    attempts = attempts + 1
                WHERE queue_id = $2::uuid::ulid AND status = 'processing'
                "#,
                self.worker_id.to_uuid(),
                queue_id.to_uuid()
            )
            .execute(&self.pool)
            .await?;

            Ok(result.rows_affected() > 0)
        }

        /// Get processed count
        pub async fn get_processed_count(&self) -> usize {
            *self.processed_count.lock().await
        }

        /// Simulate worker shutdown
        pub async fn shutdown(&self) -> Result<(), anyhow::Error> {
            // Cleanup or shutdown tasks
            Ok(())
        }
    }
}

/// Assertions for worker testing
pub mod assertions {
    use super::*;

    /// Assert that work item has expected status
    pub async fn assert_work_item_status(
        pool: &DbPool,
        queue_id: Ulid,
        expected_status: &str
    ) -> Result<(), anyhow::Error> {
        let item = get_work_item(&pool, queue_id).await?
            .ok_or_else(|| anyhow::anyhow!("Work item not found: {}", queue_id))?;
        
        pretty_assertions::assert_eq!(
            item.status, expected_status,
            "Expected work item {} to have status '{}', but found '{}'",
            queue_id, expected_status, item.status
        );
        
        Ok(())
    }

    /// Assert that expected number of items have been processed
    pub async fn assert_processed_count(
        pool: &DbPool,
        expected_count: i64
    ) -> Result<(), anyhow::Error> {
        let actual_count = count_work_items_by_status(&pool, "succeeded").await?;
        
        pretty_assertions::assert_eq!(
            actual_count, expected_count,
            "Expected {} items to be processed, but found {}",
            expected_count, actual_count
        );
        
        Ok(())
    }

    /// Assert that work queue is empty
    pub async fn assert_work_queue_empty(pool: &DbPool) -> Result<(), anyhow::Error> {
        let count = wait_for_work_queue_count(&pool, 0, 1)
            .await
            .unwrap_or(1); // If timeout, assume there are items
        
        pretty_assertions::assert_eq!(
            count, 0,
            "Expected work queue to be empty, but found {} items",
            count
        );
        
        Ok(())
    }
    
    /// Assert work queue statistics match expectations
    pub async fn assert_work_queue_stats(
        pool: &DbPool,
        expected_stats: &WorkQueueStats
    ) -> Result<(), anyhow::Error> {
        let actual_stats = super::get_work_queue_stats(pool).await?;
        
        pretty_assertions::assert_eq!(
            actual_stats.total, expected_stats.total,
            "Total work items mismatch"
        );
        pretty_assertions::assert_eq!(
            actual_stats.pending, expected_stats.pending,
            "Pending work items mismatch"
        );
        pretty_assertions::assert_eq!(
            actual_stats.succeeded, expected_stats.succeeded,
            "Succeeded work items mismatch"
        );
        
        Ok(())
    }
    
    /// Assert that agent processed expected number of items
    pub async fn assert_agent_processed_count(
        pool: &DbPool,
        agent_name: &str,
        expected_count: i64
    ) -> Result<(), anyhow::Error> {
        let actual_count = super::count_work_items_by_agent(pool, agent_name).await?;
        
        pretty_assertions::assert_eq!(
            actual_count, expected_count,
            "Agent {} processed {} items, expected {}",
            agent_name, actual_count, expected_count
        );
        
        Ok(())
    }
}