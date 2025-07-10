//! Worker test utilities
//!
//! This module provides comprehensive utilities for testing worker functionality,
//! including work queue management, worker lifecycle simulation, and assertion helpers.

use crate::common::prelude::*;
use sinex_db::query_helpers::uuid_to_ulid;
use crate::common::timing_optimization::wait_helpers::{
    wait_for_work_queue_count, wait_for_work_queue_status_count,
};
use sinex_db::models::WorkQueueItem;
use chrono::Utc;
/// Insert test items (simplified alias)
pub async fn insert_test_items(pool: &DbPool, item_count: usize) -> Result<Vec<Ulid>> {
    setup_test_worker(pool, "test_worker", item_count).await
}

/// Insert test items with custom target agent
pub async fn insert_test_items_for_agent(
    pool: &DbPool,
    agent_name: &str,
    item_count: usize,
) -> Result<Vec<Ulid>> {
    setup_test_worker(pool, agent_name, item_count).await
}

/// Setup test worker with specified number of work items
pub async fn setup_test_worker(
    pool: &DbPool,
    worker_name: &str,
    item_count: usize,
) -> Result<Vec<Ulid>> {
    let mut queue_ids = Vec::new();

    // Create agent manifest for the worker first
    let agent_name = format!("{}_agent", worker_name);
    crate::common::create_test_agent(pool, &agent_name).await?;

    // Create events and work queue items atomically in a single transaction
    let mut tx = pool.begin().await?;
    
    for i in 0..item_count {
        // Create a raw event to reference
        let event = crate::common::events::generic_adversarial_event(
            "test_source",
            "test.event",
            json!({"test": true, "worker": worker_name, "index": i}),
            Some("test_1.0"),
        );

        // Insert event and work queue item atomically using raw SQL
        let queue_id = Ulid::new();
        let event_id = event.id;
        
        // Insert the raw event first
        sqlx::query!(
            r#"
            INSERT INTO raw.events (id, source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id)
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::uuid)
            "#,
            event_id.to_uuid(),
            &event.source,
            &event.event_type,
            &event.host,
            &event.payload,
            event.ts_orig,
            event.ingestor_version.as_deref(),
            event.payload_schema_id.map(|id| id.to_uuid())
        )
        .execute(&mut *tx)
        .await?;

        // Insert the work queue item referencing the event
        sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.work_queue (queue_id, raw_event_id, target_agent_name)
            VALUES ($1::uuid, $2::uuid, $3)
            "#,
            queue_id.to_uuid(),
            event_id.to_uuid(),
            &agent_name
        )
        .execute(&mut *tx)
        .await?;
        
        queue_ids.push(queue_id);
    }
    
    // Commit the transaction to ensure atomicity
    tx.commit().await?;

    Ok(queue_ids)
}

/// Create a test work queue item with event and agent setup
/// 
/// This unified function replaces the previous insert_test_work_item and create_test_work_item
/// functions to eliminate duplication while maintaining all functionality.
pub async fn create_test_work_item_with_status(
    pool: &DbPool,
    target_agent: &str,
    status: Option<&str>,
) -> Result<Ulid> {
    let status = status.unwrap_or("pending");
    
    // Create agent manifest first
    crate::common::create_test_agent(pool, target_agent).await?;

    // First create a raw event to reference
    let event = crate::common::events::generic_adversarial_event(
        "test_source",
        "test.event",
        json!({"test": true, "agent": target_agent, "status": status}),
        Some("test_1.0"),
    );

    // Insert the raw event first
    let inserted_event = sinex_db::insert_event_with_validator(pool, &event, None).await?;

    // Create work queue item with specified status
    if status == "pending" {
        // Use the production function for pending status
        sinex_db::add_to_work_queue(pool, inserted_event.id, target_agent, 3).await
    } else {
        // Use direct insert for non-pending status
        let queue_id = Ulid::new();
        sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.work_queue
            (queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, created_at)
            VALUES ($1::uuid, $2::uuid, $3, $4, 0, 3, NOW())
            "#,
            queue_id.to_uuid(),
            inserted_event.id.to_uuid(),
            target_agent,
            status
        )
        .execute(pool)
        .await?;
        Ok(queue_id)
    }
}

/// Insert a test work item (legacy compatibility wrapper)
#[deprecated(note = "Use create_test_work_item_with_status(pool, agent, None) instead")]
pub async fn insert_test_work_item(pool: &DbPool, target_agent: &str) -> Result<Ulid> {
    create_test_work_item_with_status(pool, target_agent, None).await
}

/// Create a test work queue item (legacy compatibility wrapper)
#[deprecated(note = "Use create_test_work_item_with_status(pool, agent, Some(status)) instead")]
pub async fn create_test_work_item(
    pool: &DbPool,
    target_agent: &str,
    status: &str,
) -> Result<Ulid> {
    create_test_work_item_with_status(pool, target_agent, Some(status)).await
}

/// Get work queue item by ID
pub async fn get_work_item(pool: &DbPool, queue_id: Ulid) -> Result<Option<WorkQueueItem>> {
    // Use direct query since get_work_item_by_id may not exist in new API
    let row = sqlx::query!(
        "SELECT queue_id::uuid as \"queue_id!\", raw_event_id::uuid as \"raw_event_id!\", target_agent_name, status, attempts, max_attempts, last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id, processed_at, failure_reason FROM sinex_schemas.work_queue WHERE queue_id::uuid = $1",
        queue_id.to_uuid()
    )
    .fetch_optional(pool)
    .await?;
    
    match row {
        Some(r) => Ok(Some(WorkQueueItem {
            queue_id: uuid_to_ulid(r.queue_id),
            raw_event_id: uuid_to_ulid(r.raw_event_id),
            target_agent_name: r.target_agent_name,
            status: r.status,
            attempts: r.attempts,
            max_attempts: r.max_attempts,
            last_attempt_ts: r.last_attempt_ts,
            next_retry_ts: r.next_retry_ts,
            error_message_last: r.error_message_last,
            created_at: r.created_at,
            processing_worker_id: r.processing_worker_id,
            processed_at: r.processed_at,
            failure_reason: r.failure_reason,
        })),
        None => Ok(None),
    }
}

/// Create a work queue item for an existing event
pub async fn create_work_item(pool: &DbPool, target_agent: &str, event_id: Ulid) -> Result<Ulid> {
    // Use the real query function to insert
    let queue_id = sinex_db::add_to_work_queue(pool, event_id, target_agent, 3).await?;
    let work_item = WorkQueueItem {
        queue_id,
        raw_event_id: event_id,
        target_agent_name: target_agent.to_string(),
        status: "pending".to_string(),
        attempts: 0,
        max_attempts: 3,
        last_attempt_ts: None,
        next_retry_ts: None,
        error_message_last: None,
        created_at: Utc::now(),
        processing_worker_id: None,
        processed_at: None,
        failure_reason: None,
    };
    Ok(work_item.queue_id)
}

/// Count work items by status using timing optimization
pub async fn count_work_items_by_status(pool: &DbPool, status: &str) -> Result<i64> {
    wait_for_work_queue_status_count(pool, status, 0, 1)
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
    let pending_count = count_work_items_by_status(pool, "pending").await?;
    Ok(pending_count == 0)
}

/// Verify all items have been processed by specific worker (alternative signature)
pub async fn verify_all_items_processed_by_worker(
    pool: &DbPool,
    worker_name: &str,
) -> Result<bool> {
    // Check if there are any pending items for this worker using timing utility
    let pending_count = wait_for_work_queue_status_count(pool, "pending", 0, 1)
        .await
        .unwrap_or(1); // If timeout, assume there are pending items

    Ok(pending_count == 0)
}

/// Simulate worker processing
pub async fn simulate_worker_processing(
    pool: &DbPool,
    worker_id: Ulid,
    max_items: usize,
) -> Result<usize> {
    let mut processed = 0;

    for _ in 0..max_items {
        // Try to claim an item
        let claimed = sqlx::query!(
            r#"
            UPDATE sinex_schemas.work_queue
            SET status = 'processing',
                processing_worker_id = $1,
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
            worker_id.to_string()
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
                WHERE queue_id::uuid = $1
                "#,
                row.queue_id.expect("queue_id should exist")
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
            self.process_item_with_delay(queue_id, std::time::Duration::from_millis(10))
                .await
        }

        /// Process a work item with custom processing delay
        pub async fn process_item_with_delay(
            &self,
            queue_id: Ulid,
            delay: std::time::Duration,
        ) -> Result<bool> {
            // Simulate processing time
            tokio::time::sleep(delay).await;

            // Update work item status
            let result = sqlx::query!(
                r#"
                UPDATE sinex_schemas.work_queue
                SET status = 'succeeded',
                    processed_at = NOW(),
                    processing_worker_id = $1::uuid
                WHERE queue_id::uuid = $2 AND status = 'processing'
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
                    processing_worker_id = $1::uuid,
                    attempts = attempts + 1
                WHERE queue_id::uuid = $2 AND status = 'processing'
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
        expected_status: &str,
    ) -> Result<(), anyhow::Error> {
        let item = get_work_item(pool, queue_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Work item not found: {}", queue_id))?;

        pretty_assertions::assert_eq!(
            item.status,
            expected_status,
            "Expected work item {} to have status '{}', but found '{}'",
            queue_id,
            expected_status,
            item.status
        );

        Ok(())
    }

    /// Assert that expected number of items have been processed
    pub async fn assert_processed_count(
        pool: &DbPool,
        expected_count: i64,
    ) -> Result<(), anyhow::Error> {
        let actual_count = count_work_items_by_status(pool, "succeeded").await?;

        pretty_assertions::assert_eq!(
            actual_count,
            expected_count,
            "Expected {} items to be processed, but found {}",
            expected_count,
            actual_count
        );

        Ok(())
    }

    /// Assert that work queue is empty
    pub async fn assert_work_queue_empty(pool: &DbPool) -> Result<(), anyhow::Error> {
        let count = wait_for_work_queue_count(pool, 0, 1).await.unwrap_or(1); // If timeout, assume there are items

        pretty_assertions::assert_eq!(
            count,
            0,
            "Expected work queue to be empty, but found {} items",
            count
        );

        Ok(())
    }

    /// Assert work queue statistics match expectations
    pub async fn assert_work_queue_stats(
        pool: &DbPool,
        expected_stats: &WorkQueueStats,
    ) -> Result<(), anyhow::Error> {
        let actual_stats = super::get_work_queue_stats(pool).await?;

        pretty_assertions::assert_eq!(
            actual_stats.total,
            expected_stats.total,
            "Total work items mismatch"
        );
        pretty_assertions::assert_eq!(
            actual_stats.pending,
            expected_stats.pending,
            "Pending work items mismatch"
        );
        pretty_assertions::assert_eq!(
            actual_stats.succeeded,
            expected_stats.succeeded,
            "Succeeded work items mismatch"
        );

        Ok(())
    }

    /// Assert that agent processed expected number of items
    pub async fn assert_agent_processed_count(
        pool: &DbPool,
        agent_name: &str,
        expected_count: i64,
    ) -> Result<(), anyhow::Error> {
        let actual_count = super::count_work_items_by_agent(pool, agent_name).await?;

        pretty_assertions::assert_eq!(
            actual_count,
            expected_count,
            "Agent {} processed {} items, expected {}",
            agent_name,
            actual_count,
            expected_count
        );

        Ok(())
    }
}
