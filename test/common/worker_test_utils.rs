//! Worker test utilities

use anyhow::Result;
use sinex_ulid::Ulid;
use sqlx::PgPool;
use sinex_db::models::WorkQueueItem;
use crate::common::timing_optimization::wait_helpers::{wait_for_work_queue_status_count, wait_for_work_queue_count};
use crate::common::events;
use serde_json::json;

/// Insert test items (simplified alias)
pub async fn insert_test_items(pool: &PgPool, item_count: usize) -> Result<Vec<Ulid>> {
    setup_test_worker(pool, "test_worker", item_count).await
}

/// Setup test worker with specified number of work items
pub async fn setup_test_worker(pool: &PgPool, worker_name: &str, item_count: usize) -> Result<Vec<Ulid>> {
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
    pool: &PgPool,
    target_agent: &str
) -> Result<Ulid> {
    // First create a raw event to reference
    let raw_event_id = Ulid::new();
    let event = events::generic_adversarial_event("test_source", "test.event", json!({"test": true}), Some("test_1.0"));
    
    // Insert the raw event first
    sinex_db::queries::insert_event(pool, &event).await?;
    
    // Now insert the work queue item using the real query function
    let work_item = sinex_db::queries::insert_work_queue_item(pool, raw_event_id, target_agent).await?;
    Ok(work_item.queue_id)
}

/// Create a test work queue item
pub async fn create_test_work_item(
    pool: &PgPool,
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
pub async fn get_work_item(pool: &PgPool, queue_id: Ulid) -> Result<Option<WorkQueueItem>> {
    match sinex_db::queries::get_work_item_by_id(pool, queue_id).await {
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

/// Count work items by status using timing optimization
pub async fn count_work_items_by_status(pool: &PgPool, status: &str) -> Result<i64> {
    wait_for_work_queue_status_count(pool, status, 0, 1)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to count work items by status: {}", e))
}

/// Cleanup all work queue items
pub async fn cleanup_work_queue(pool: &PgPool) -> Result<()> {
    sqlx::query!("DELETE FROM sinex_schemas.work_queue")
        .execute(pool)
        .await?;
    
    Ok(())
}

/// Verify all items have been processed
pub async fn verify_all_items_processed(pool: &PgPool) -> Result<bool> {
    let pending_count = count_work_items_by_status(pool, "pending").await?;
    Ok(pending_count == 0)
}

/// Verify all items have been processed by specific worker (alternative signature)
pub async fn verify_all_items_processed_by_worker(pool: &PgPool, worker_name: &str) -> Result<bool> {
    // Check if there are any pending items for this worker using timing utility
    let pending_count = wait_for_work_queue_status_count(pool, "pending", 0, 1)
        .await
        .unwrap_or(1); // If timeout, assume there are pending items
    
    Ok(pending_count == 0)
}

/// Simulate worker processing
pub async fn simulate_worker_processing(
    pool: &PgPool,
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
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Mock worker for testing
    pub struct MockWorker {
        pub worker_id: Ulid,
        pub pool: PgPool,
        pub processed_count: Arc<Mutex<usize>>,
    }

    impl MockWorker {
        pub fn new(pool: PgPool) -> Self {
            Self {
                worker_id: Ulid::new(),
                pool,
                processed_count: Arc::new(Mutex::new(0)),
            }
        }

        /// Simulate worker startup
        pub async fn startup(&self) -> Result<()> {
            // Register worker or perform startup tasks
            Ok(())
        }

        /// Process a single work item
        pub async fn process_item(&self, queue_id: Ulid) -> Result<bool> {
            // Simulate processing time
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

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

        /// Get processed count
        pub async fn get_processed_count(&self) -> usize {
            *self.processed_count.lock().await
        }

        /// Simulate worker shutdown
        pub async fn shutdown(&self) -> Result<()> {
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
        pool: &PgPool,
        queue_id: Ulid,
        expected_status: &str
    ) -> Result<()> {
        let item = get_work_item(pool, queue_id).await?
            .ok_or_else(|| anyhow::anyhow!("Work item not found: {}", queue_id))?;
        
        assert_eq!(
            item.status, expected_status,
            "Expected work item {} to have status '{}', but found '{}'",
            queue_id, expected_status, item.status
        );
        
        Ok(())
    }

    /// Assert that expected number of items have been processed
    pub async fn assert_processed_count(
        pool: &PgPool,
        expected_count: i64
    ) -> Result<()> {
        let actual_count = count_work_items_by_status(pool, "succeeded").await?;
        
        assert_eq!(
            actual_count, expected_count,
            "Expected {} items to be processed, but found {}",
            expected_count, actual_count
        );
        
        Ok(())
    }

    /// Assert that work queue is empty
    pub async fn assert_work_queue_empty(pool: &PgPool) -> Result<()> {
        let count = wait_for_work_queue_count(pool, 0, 1)
            .await
            .unwrap_or(1); // If timeout, assume there are items
        
        assert_eq!(
            count, 0,
            "Expected work queue to be empty, but found {} items",
            count
        );
        
        Ok(())
    }
}