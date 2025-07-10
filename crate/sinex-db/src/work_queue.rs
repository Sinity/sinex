//! Work queue operations following the clean API pattern
//!
//! This module provides work queue database operations with proper error handling
//! and clean API design, following the exact same pattern as existing *_correct.rs files.

use crate::models::WorkQueueItem;
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::DbPoolRef;
use anyhow::Result;
use sinex_core::Timestamp;
use sinex_ulid::Ulid;
// use sqlx::types::Uuid;  // Not needed with correct casting
use crate::models::DlqEvent;
use crate::JsonValue;

/// Claim work queue items following the exact same pattern as existing correct functions
pub async fn claim_work_queue_items(
    pool: DbPoolRef<'_>,
    agent_name: &str,
    worker_id: &str,
    limit: i64,
) -> Result<Vec<WorkQueueItem>> {
    let records = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET 
            status = 'processing',
            processing_worker_id = $2
        WHERE queue_id IN (
            SELECT queue_id 
            FROM sinex_schemas.work_queue 
            WHERE status = 'pending' 
              AND target_agent_name = $1
              AND (attempts < max_attempts OR max_attempts = -1)
              AND (next_retry_ts IS NULL OR next_retry_ts <= NOW())
            ORDER BY created_at ASC 
            LIMIT $3
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
        worker_id,
        limit
    )
    .fetch_all(pool)
    .await?;
    
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

/// Complete a work queue item following the exact same pattern as existing correct functions
pub async fn complete_work_queue_item(pool: DbPoolRef<'_>, queue_id: Ulid) -> Result<()> {
    let queue_uuid = ulid_to_uuid(queue_id);
    
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET 
            status = 'succeeded',
            processed_at = NOW()
        WHERE queue_id::uuid = $1
        "#,
        queue_uuid
    )
    .execute(pool)
    .await?;
    
    Ok(())
}

/// Mark a work queue item as failed following the exact same pattern as existing correct functions
pub async fn fail_work_queue_item(
    pool: DbPoolRef<'_>,
    queue_id: Ulid,
    error_message: &str,
    next_retry_ts: Timestamp,
) -> Result<()> {
    let queue_uuid = ulid_to_uuid(queue_id);
    
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET 
            status = 'failed_retryable',
            last_attempt_ts = NOW(),
            next_retry_ts = $3,
            error_message_last = $2,
            attempts = attempts + 1,
            processing_worker_id = NULL
        WHERE queue_id::uuid = $1
        "#,
        queue_uuid,
        error_message,
        next_retry_ts
    )
    .execute(pool)
    .await?;
    
    Ok(())
}

/// Mark a work queue item as failed with simple error message (used by old queries API)
pub async fn fail_work_item(
    pool: DbPoolRef<'_>,
    queue_id: Ulid,
    error_message: &str,
) -> Result<()> {
    let next_retry_ts = chrono::Utc::now() + chrono::Duration::minutes(5);
    fail_work_queue_item(pool, queue_id, error_message, next_retry_ts).await
}

/// Complete a work item (alias for compatibility with old queries API)
pub async fn complete_work_item(pool: DbPoolRef<'_>, queue_id: Ulid) -> Result<()> {
    complete_work_queue_item(pool, queue_id).await
}

/// Parameters for inserting a DLQ event
pub struct DlqEventParams {
    pub failed_event_id: Ulid,
    pub agent_name: String,
    pub source: String,
    pub event_type: String,
    pub failure_reason: String,
    pub error_category: String,
    pub original_event_payload: JsonValue,
    pub additional_metadata: Option<JsonValue>,
}

/// Insert a DLQ event following the exact same pattern as existing correct functions
pub async fn insert_dlq_event(
    pool: DbPoolRef<'_>,
    params: DlqEventParams,
) -> Result<DlqEvent> {
    let failed_event_uuid = ulid_to_uuid(params.failed_event_id);
    
    let record = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.dlq_events 
            (failed_event_id, agent_name, source, event_type, failure_reason, 
             error_category, original_event_payload, additional_metadata)
        VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)
        RETURNING 
            dlq_id::uuid as "dlq_id!",
            failed_event_id::uuid as "failed_event_id!",
            agent_name as "agent_name!",
            source as "source!",
            event_type as "event_type!",
            failure_reason as "failure_reason!",
            error_category as "error_category!",
            retry_count as "retry_count!",
            failed_at as "failed_at!",
            last_retry_at,
            next_retry_at,
            original_event_payload as "original_event_payload!",
            additional_metadata,
            resolved_at,
            resolved_by
        "#,
        failed_event_uuid,
        params.agent_name,
        params.source,
        params.event_type,
        params.failure_reason,
        params.error_category,
        params.original_event_payload,
        params.additional_metadata
    )
    .fetch_one(pool)
    .await?;
    
    Ok(DlqEvent {
        dlq_id: uuid_to_ulid(record.dlq_id),
        failed_event_id: uuid_to_ulid(record.failed_event_id),
        agent_name: record.agent_name,
        source: record.source,
        event_type: record.event_type,
        failure_reason: record.failure_reason,
        error_category: record.error_category,
        retry_count: record.retry_count,
        failed_at: record.failed_at,
        last_retry_at: record.last_retry_at,
        next_retry_at: record.next_retry_at,
        original_event_payload: record.original_event_payload,
        additional_metadata: record.additional_metadata,
        resolved_at: record.resolved_at,
        resolved_by: record.resolved_by,
    })
}

/// Add an event to the work queue
pub async fn add_to_work_queue(
    pool: DbPoolRef<'_>,
    event_id: Ulid,
    agent_name: &str,
    max_attempts: i32,
) -> Result<Ulid> {
    let queue_id = Ulid::new();
    let event_uuid = ulid_to_uuid(event_id);
    let queue_uuid = ulid_to_uuid(queue_id);
    
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.work_queue 
        (queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, created_at)
        VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 'pending', 0, $4, NOW())
        "#,
        queue_uuid,
        event_uuid,
        agent_name,
        max_attempts
    )
    .execute(pool)
    .await?;
    
    Ok(queue_id)
}

/// Get the next work item for processing (used by the old queries API)
pub async fn get_next_work_item(pool: DbPoolRef<'_>, agent_name: &str) -> Result<Option<WorkQueueItem>> {
    let items = claim_work_queue_items(pool, agent_name, "worker_id", 1).await?;
    Ok(items.into_iter().next())
}

/// Get a work item by its ID
pub async fn get_work_item_by_id(pool: DbPoolRef<'_>, queue_id: Ulid) -> Result<WorkQueueItem> {
    let queue_uuid = ulid_to_uuid(queue_id);
    
    let record = sqlx::query!(
        r#"
        SELECT 
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
        FROM sinex_schemas.work_queue 
        WHERE queue_id::uuid = $1
        "#,
        queue_uuid
    )
    .fetch_one(pool)
    .await?;
    
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

/// Get DLQ items for an agent
pub async fn get_dlq_items(pool: DbPoolRef<'_>, agent_name: &str, limit: i64) -> Result<Vec<DlqEvent>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            dlq_id::uuid as "dlq_id!",
            failed_event_id::uuid as "failed_event_id!",
            agent_name as "agent_name!",
            source as "source!",
            event_type as "event_type!",
            failure_reason as "failure_reason!",
            error_category as "error_category!",
            retry_count as "retry_count!",
            failed_at as "failed_at!",
            last_retry_at,
            next_retry_at,
            original_event_payload as "original_event_payload!",
            additional_metadata,
            resolved_at,
            resolved_by
        FROM sinex_schemas.dlq_events 
        WHERE agent_name = $1
        ORDER BY failed_at DESC
        LIMIT $2
        "#,
        agent_name,
        limit
    )
    .fetch_all(pool)
    .await?;
    
    Ok(records
        .into_iter()
        .map(|record| DlqEvent {
            dlq_id: uuid_to_ulid(record.dlq_id),
            failed_event_id: uuid_to_ulid(record.failed_event_id),
            agent_name: record.agent_name,
            source: record.source,
            event_type: record.event_type,
            failure_reason: record.failure_reason,
            error_category: record.error_category,
            retry_count: record.retry_count,
            failed_at: record.failed_at,
            last_retry_at: record.last_retry_at,
            next_retry_at: record.next_retry_at,
            original_event_payload: record.original_event_payload,
            additional_metadata: record.additional_metadata,
            resolved_at: record.resolved_at,
            resolved_by: record.resolved_by,
        })
        .collect())
}

/// Add a work item to the work queue with proper types (used by old queries API)
pub async fn add_to_work_queue_detailed(
    pool: DbPoolRef<'_>,
    event_id: Ulid,
    agent_name: &str,
    max_attempts: i32,
) -> Result<WorkQueueItem> {
    let queue_id = add_to_work_queue(pool, event_id, agent_name, max_attempts).await?;
    get_work_item_by_id(pool, queue_id).await
}

/// Insert event function for compatibility with old queries API
pub async fn insert_event(pool: DbPoolRef<'_>, event: &crate::RawEvent) -> Result<crate::RawEvent> {
    crate::events::insert_event_with_validator(pool, event, None).await
}