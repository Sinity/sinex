//! Database helper functions and macros for test standardization
//!
//! Provides standardized patterns for database operations in tests,
//! reducing boilerplate and ensuring consistency.

use crate::common::prelude::*;
use serde_json::json;

// DEPRECATED: Use database_pool::acquire_database() instead
// This function is kept for backwards compatibility during migration

/// Get shared test pool - DEPRECATED, use database_pool instead
pub async fn get_shared_test_pool() -> Result<DbPool> {
    // For backwards compatibility, create a pooled database and return its pool
    let db = crate::common::database_pool::acquire_database().await?;
    Ok(db.pool().clone())
}

/// Create multiple test work queue items for a given agent
pub async fn create_test_work_items(
    pool: &DbPool,
    agent_name: &str,
    count: usize,
) -> Result<Vec<Ulid>> {
    let mut items = Vec::new();
    for i in 0..count {
        let queue_id = Ulid::new();
        let event_id = Ulid::new();

        // First create a raw event for the foreign key constraint
        sqlx::query!(
            "INSERT INTO raw.events (id, source, event_type, payload, ts_orig, host)
             VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6)",
            sinex_db::ulid_to_uuid(event_id),
            "test_source",
            format!("test.event.{}", i),
            serde_json::json!({"test": true, "index": i}),
            chrono::Utc::now(),
            "test_host"
        )
        .execute(pool)
        .await?;

        // Then create the work queue item
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue (queue_id, raw_event_id, target_agent_name, status)
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, $4)",
            sinex_db::ulid_to_uuid(queue_id), sinex_db::ulid_to_uuid(event_id),
            agent_name, "pending"
        ).execute(pool).await?;
        items.push(queue_id);
    }
    Ok(items)
}

/// Register a test agent with unique name
pub async fn register_test_agent(pool: &DbPool, suffix: &str) -> Result<String> {
    let agent_name = format!("test_agent_{}_{}", suffix, Ulid::new());
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, status)
         VALUES ($1, $2, $3, $4)",
        agent_name,
        "1.0.0",
        "Test agent",
        "running"
    )
    .execute(pool)
    .await?;
    Ok(agent_name)
}

/// Insert a batch of test events efficiently
pub async fn insert_test_event_batch(pool: &DbPool, events: &[RawEvent]) -> Result<Vec<Ulid>> {
    let mut event_ids = Vec::new();

    for event in events {
        let inserted = queries::insert_event(&pool, event).await?;
        event_ids.push(inserted.id);
    }

    Ok(event_ids)
}

/// Create test events and work items in a single transaction
pub async fn setup_test_workload(
    pool: &DbPool,
    agent_name: &str,
    event_count: usize,
) -> Result<(Vec<Ulid>, Vec<Ulid>)> {
    // Create test events
    let test_events: Vec<_> = (0..event_count)
        .map(|i| {
            events::adversarial_test_event(
                "workload.test",
                json!({"sequence": i, "batch": "workload"}),
            )
        })
        .collect();

    let event_ids = insert_test_event_batch(pool, &test_events).await?;
    let work_item_ids = create_test_work_items(pool, agent_name, event_count).await?;

    Ok((event_ids, work_item_ids))
}

/// Create a simple test event
pub async fn create_test_event(source: &str, event_type: &str) -> RawEvent {
    RawEventBuilder::new(source, event_type, json!({"test": true})).build()
}

/// Create a test agent with minimal fields
pub async fn create_test_agent(pool: &DbPool, agent_name: &str) -> Result<()> {
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, status)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (agent_name) DO NOTHING",
        agent_name,
        "1.0.0",
        "Test agent",
        "running"
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Purge old work queue items (TTL cleanup function)
pub async fn purge_old_work_queue_items(pool: &DbPool) -> Result<u64> {
    // Purge succeeded items older than 90 days
    let result = sqlx::query!(
        "DELETE FROM sinex_schemas.work_queue
         WHERE status = 'succeeded'
         AND processed_at < NOW() - INTERVAL '90 days'"
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

// DEPRECATED MACROS - These are replaced by #[sinex_test] with the universal pool
// Keep for backwards compatibility during migration only
