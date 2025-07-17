// Worker test utilities for work queue testing
//
// Provides utilities for testing work queue functionality including:
// - Creating work items
// - Claiming and processing work items
// - Testing worker idempotency
//
// NOTE: Most work queue operations use direct SQL due to complex locking requirements
// (FOR UPDATE SKIP LOCKED) that are not abstracted by the centralized query system.

use crate::common::prelude::*;
use serde_json::Value;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};

/// A work queue item for testing
#[derive(Debug, Clone)]
pub struct WorkQueueItem {
    pub queue_id: Ulid,
    pub agent_name: String,
    pub worker_name: String,
    pub payload: Value,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Create a work item for testing
pub async fn create_work_item(pool: &DbPool, agent_name: &str, event_id: Ulid) -> AnyhowResult<Ulid> {
    let queue_id = Ulid::new();
    let payload = json!({
        "event_id": event_id.to_string(),
        "agent_name": agent_name,
        "work_type": "test_work"
    });

    // Insert into a test work queue table using direct SQL (schema operations)
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.work_queue 
        (queue_id, agent_name, payload, status, created_at)
        VALUES ($1::uuid, $2, $3, $4, $5)
        "#,
        queue_id.to_uuid(),
        agent_name,
        payload,
        "pending",
        Utc::now()
    )
    .execute(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create work item: {}", e))?;

    Ok(queue_id)
}

/// Claim work queue items for processing
pub async fn claim_work_queue_items(
    pool: &DbPool,
    agent_name: &str,
    worker_name: &str,
    max_items: usize,
) -> AnyhowResult<Vec<WorkQueueItem>> {
    // Using direct SQL for complex work queue operations with SKIP LOCKED
    let items = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET status = 'claimed', 
            claimed_at = NOW(),
            worker_name = $3
        WHERE queue_id IN (
            SELECT queue_id 
            FROM sinex_schemas.work_queue 
            WHERE agent_name = $1 AND status = 'pending'
            ORDER BY created_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        RETURNING queue_id::uuid as "queue_id!", agent_name, payload, status, created_at, claimed_at
        "#,
        agent_name,
        max_items as i64,
        worker_name
    )
    .fetch_all(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to claim work items: {}", e))?;

    let mut work_items = Vec::new();
    for item in items {
        work_items.push(WorkQueueItem {
            queue_id: sinex_db::query_helpers::uuid_to_ulid(item.queue_id),
            agent_name: item.agent_name,
            worker_name: worker_name.to_string(),
            payload: item.payload,
            status: item.status,
            created_at: item.created_at,
            claimed_at: item.claimed_at,
            completed_at: None,
        });
    }

    Ok(work_items)
}

/// Complete a work queue item
pub async fn complete_work_queue_item(pool: &DbPool, queue_id: Ulid) -> TestResult {
    // Direct SQL for work queue completion
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET status = 'completed', 
            completed_at = NOW()
        WHERE queue_id::uuid = $1
        "#,
        queue_id.to_uuid()
    )
    .execute(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to complete work item: {}", e))?;

    Ok(())
}

/// Get work queue status
pub async fn get_work_queue_status(
    pool: &DbPool,
    agent_name: &str,
) -> AnyhowResult<(usize, usize, usize)> {
    // Direct SQL for work queue aggregation
    let result = sqlx::query!(
        r#"
        SELECT 
            COUNT(*) FILTER (WHERE status = 'pending') as "pending!",
            COUNT(*) FILTER (WHERE status = 'claimed') as "claimed!",
            COUNT(*) FILTER (WHERE status = 'completed') as "completed!"
        FROM sinex_schemas.work_queue 
        WHERE agent_name = $1
        "#,
        agent_name
    )
    .fetch_one(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to get work queue status: {}", e))?;

    Ok((
        result.pending as usize,
        result.claimed as usize,
        result.completed as usize,
    ))
}

/// Clear all work items for an agent (for test cleanup)
pub async fn clear_work_queue(pool: &DbPool, agent_name: &str) -> TestResult {
    // Direct SQL for work queue cleanup
    sqlx::query!(
        r#"
        DELETE FROM sinex_schemas.work_queue 
        WHERE agent_name = $1
        "#,
        agent_name
    )
    .execute(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to clear work queue: {}", e))?;

    Ok(())
}

/// Deprecated: work queue table removed in satellite architecture
/// This function is now a no-op for compatibility
pub async fn ensure_work_queue_table(_pool: &DbPool) -> TestResult {
    // Work queue table deprecated - using Redis Streams instead
    Ok(())
}
