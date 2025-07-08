//! Work queue database operations with clean API
//!
//! This module provides domain-specific work queue operations following the
//! *_correct.rs pattern for clean API and proper error handling.

use crate::models::WorkQueueItem;
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::DbPoolRef;
use sinex_core::{Result, CoreError, JsonValue};
use sinex_ulid::Ulid;
use sqlx::types::Uuid;
use chrono::Utc;

/// Input for adding work to the queue
#[derive(Debug)]
pub struct AddWorkInput {
    pub raw_event_id: Ulid,
    pub target_agent_name: String,
    pub max_attempts: Option<i32>,
}

/// Add work to the queue
pub async fn add_to_work_queue(pool: DbPoolRef<'_>, input: AddWorkInput) -> Result<WorkQueueItem> {
    let max_attempts = input.max_attempts.unwrap_or(5);
    let raw_event_uuid = ulid_to_uuid(input.raw_event_id);
    
    let record = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.work_queue (
            raw_event_id, target_agent_name, max_attempts
        ) VALUES ($1::uuid, $2, $3)
        RETURNING 
            queue_id::uuid as "queue_id!",
            raw_event_id::uuid as "raw_event_id!",
            target_agent_name as "target_agent_name!",
            status as "status!",
            attempts as "attempts!",
            max_attempts as "max_attempts!",
            last_attempt_ts,
            next_retry_ts,
            error_message_last,
            created_at as "created_at!",
            processing_worker_id,
            processed_at,
            failure_reason
        "#,
        raw_event_uuid,
        input.target_agent_name,
        max_attempts
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to add work to queue")
            .with_context("raw_event_id", input.raw_event_id)
            .with_context("target_agent_name", &input.target_agent_name)
            .with_context("priority", input.priority)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(WorkQueueItem {
        queue_id: uuid_to_ulid(record.queue_id),
        raw_event_id: uuid_to_ulid(record.raw_event_id),
        target_agent_name: record.target_agent_name,
        status: record.status,
        attempts: record.attempts,
        max_attempts: record.max_attempts,
        last_attempt_ts: record.last_attempt_ts,
        next_retry_ts: record.next_retry_ts,
        error_message_last: record.error_message_last,
        created_at: record.created_at,
        processing_worker_id: record.processing_worker_id,
        processed_at: record.processed_at,
        failure_reason: record.failure_reason,
    })
}

/// Claim work queue items for processing
pub async fn claim_work_queue_items(
    pool: DbPoolRef<'_>,
    agent_name: &str,
    limit: i64,
) -> Result<Vec<WorkQueueItem>> {
    let records = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET 
            status = 'processing',
            processing_worker_id = $1
        WHERE queue_id IN (
            SELECT queue_id 
            FROM sinex_schemas.work_queue 
            WHERE status = 'pending' 
              AND target_agent_name = $1
              AND (attempts < max_attempts OR max_attempts = -1)
              AND (next_retry_ts IS NULL OR next_retry_ts <= NOW())
            ORDER BY created_at ASC 
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        RETURNING 
            queue_id::uuid as "queue_id!",
            raw_event_id::uuid as "raw_event_id!",
            target_agent_name as "target_agent_name!",
            status as "status!",
            attempts as "attempts!",
            max_attempts as "max_attempts!",
            last_attempt_ts,
            next_retry_ts,
            error_message_last,
            created_at as "created_at!",
            processing_worker_id,
            processed_at,
            failure_reason
        "#,
        agent_name,
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to claim work queue items")
            .with_context("agent_name", agent_name)
            .with_context("limit", limit)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
        .into_iter()
        .map(|record| WorkQueueItem {
            queue_id: uuid_to_ulid(record.queue_id),
            raw_event_id: uuid_to_ulid(record.raw_event_id),
            target_agent_name: record.target_agent_name,
            status: record.status,
            attempts: record.attempts,
            max_attempts: record.max_attempts,
            last_attempt_ts: record.last_attempt_ts,
            next_retry_ts: record.next_retry_ts,
            error_message_last: record.error_message_last,
            created_at: record.created_at,
            processing_worker_id: record.processing_worker_id,
            processed_at: record.processed_at,
            failure_reason: record.failure_reason,
        })
        .collect())
}

/// Get next work item for an agent
pub async fn get_next_work_item(pool: DbPoolRef<'_>, agent_name: &str) -> Result<Option<WorkQueueItem>> {
    let items = claim_work_queue_items(pool, agent_name, 1).await?;
    Ok(items.into_iter().next())
}

/// Complete a work queue item
pub async fn complete_work_queue_item(pool: DbPoolRef<'_>, queue_id: Ulid) -> Result<()> {
    let queue_uuid = ulid_to_uuid(queue_id);
    
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET 
            status = 'succeeded',
            processed_at = NOW()
        WHERE queue_id = $1::uuid
        "#,
        queue_uuid
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to complete work queue item")
            .with_context("queue_id", queue_id)
            .with_source(e.to_string())
            .build()
    })?;
    
    if result.rows_affected() == 0 {
        return Err(CoreError::not_found("Work queue item", queue_id));
    }
    
    Ok(())
}

/// Mark a work queue item as failed
pub async fn fail_work_queue_item(
    pool: DbPoolRef<'_>,
    queue_id: Ulid,
    error_message: &str,
) -> Result<()> {
    let queue_uuid = ulid_to_uuid(queue_id);
    
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET 
            status = 'failed_retryable',
            last_attempt_ts = NOW(),
            next_retry_ts = NOW() + INTERVAL '5 minutes',
            error_message_last = $2,
            attempts = attempts + 1
        WHERE queue_id = $1::uuid
        "#,
        queue_uuid,
        error_message
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to mark work queue item as failed")
            .with_context("queue_id", queue_id)
            .with_context("error_message", error_message)
            .with_source(e.to_string())
            .build()
    })?;
    
    if result.rows_affected() == 0 {
        return Err(CoreError::not_found("Work queue item", queue_id));
    }
    
    Ok(())
}

/// Mark a work queue item as permanently failed
pub async fn fail_work_queue_item_permanently(
    pool: DbPoolRef<'_>,
    queue_id: Ulid,
    error_message: &str,
) -> Result<()> {
    let queue_uuid = ulid_to_uuid(queue_id);
    
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET 
            status = 'failed',
            processed_at = NOW(),
            failure_reason = $2,
            attempts = max_attempts + 1
        WHERE queue_id = $1::uuid
        "#,
        queue_uuid,
        error_message
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to mark work queue item as permanently failed")
            .with_context("queue_id", queue_id)
            .with_context("error_message", error_message)
            .with_source(e.to_string())
            .build()
    })?;
    
    if result.rows_affected() == 0 {
        return Err(CoreError::not_found("Work queue item", queue_id));
    }
    
    Ok(())
}

/// Queue depth metrics for monitoring
#[derive(Debug, Clone)]
pub struct QueueDepthMetric {
    pub agent_name: String,
    pub queue_depth: i64,
}

/// Calculate queue depth metrics for all agents
pub async fn calculate_queue_depth_metrics(pool: DbPoolRef<'_>) -> Result<Vec<QueueDepthMetric>> {
    let metrics = sqlx::query!(
        r#"
        SELECT 
            am.agent_name,
            COALESCE(wq.queue_depth, 0) as queue_depth
        FROM sinex_schemas.agent_manifests am
        LEFT JOIN (
            SELECT 
                target_agent_name,
                COUNT(*) as queue_depth
            FROM sinex_schemas.work_queue
            WHERE status = 'pending'
            GROUP BY target_agent_name
        ) wq ON am.agent_name = wq.target_agent_name
        ORDER BY am.agent_name
        "#
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to calculate queue depth metrics")
            .with_source(e.to_string())
            .build()
    })?;

    Ok(metrics
        .into_iter()
        .map(|m| QueueDepthMetric {
            agent_name: m.agent_name,
            queue_depth: m.queue_depth.unwrap_or(0),
        })
        .collect())
}

