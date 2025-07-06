//! Enhanced database operations with rich error context
//!
//! This module provides enhanced versions of database operations that use
//! the ErrorContext pattern to provide detailed error information for
//! debugging and monitoring purposes.

use crate::models::{DlqEvent, WorkQueueItem};
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::{DbPoolRef, JsonValue, Timestamp};
use sinex_core::{Result, CoreError, RawEvent};
use chrono::Utc;
use sinex_ulid::Ulid;
use sqlx::types::Uuid;
use sqlx::Row;

/// Insert a raw event with enhanced error context
pub async fn insert_event_with_context(
    pool: DbPoolRef<'_>,
    event: &RawEvent,
    operation_context: &str,
) -> Result<RawEvent> {
    let start_time = std::time::Instant::now();
    
    // Convert ULID to UUID for SQLx compatibility
    let payload_schema_uuid: Option<Uuid> = event.payload_schema_id.map(ulid_to_uuid);
    
    // Insert with enhanced error context
    let record = sqlx::query!(
        r#"
        INSERT INTO raw.events (source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7::uuid)
        RETURNING 
            id::uuid as "id!", 
            source as "source!", 
            event_type as "event_type!", 
            ts_ingest as "ts_ingest!",
            ts_orig,
            host as "host!", 
            ingestor_version, 
            payload_schema_id::uuid as "payload_schema_id", 
            payload as "payload!"
        "#,
        event.source,
        event.event_type,
        event.host,
        event.payload,
        event.ts_orig,
        event.ingestor_version,
        payload_schema_uuid
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "Event insertion failed: operation={}, source={}, event_type={}, host={}, payload_size={}, duration_ms={}, event_id={}, error={}",
            operation_context, event.source, event.event_type, event.host, 
            event.payload.to_string().len(), start_time.elapsed().as_millis(), event.id, e
        ))
    })?;

    // Map the UUID fields to ULID
    Ok(RawEvent {
        id: uuid_to_ulid(record.id),
        source: record.source,
        event_type: record.event_type,
        ts_ingest: record.ts_ingest,
        ts_orig: record.ts_orig,
        host: record.host,
        ingestor_version: record.ingestor_version,
        payload_schema_id: record.payload_schema_id.map(uuid_to_ulid),
        payload: record.payload,
    })
}

/// Get event by ID with enhanced error context
pub async fn get_event_by_id_with_context(
    pool: DbPoolRef<'_>,
    event_id: Ulid,
    operation_context: &str,
) -> Result<RawEvent> {
    let start_time = std::time::Instant::now();
    let event_uuid: Uuid = ulid_to_uuid(event_id);
    
    let record = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!", 
            source as "source!", 
            event_type as "event_type!", 
            ts_ingest as "ts_ingest!",
            ts_orig, 
            host as "host!", 
            ingestor_version, 
            payload_schema_id::uuid as "payload_schema_id", 
            payload as "payload!"
        FROM raw.events
        WHERE id::uuid = $1
        "#,
        event_uuid
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "Event retrieval failed: operation={}, event_id={}, duration_ms={}, error={}",
            operation_context, event_id, start_time.elapsed().as_millis(), e
        ))
    })?;

    Ok(RawEvent {
        id: uuid_to_ulid(record.id),
        source: record.source,
        event_type: record.event_type,
        ts_ingest: record.ts_ingest,
        ts_orig: record.ts_orig,
        host: record.host,
        ingestor_version: record.ingestor_version,
        payload_schema_id: record.payload_schema_id.map(uuid_to_ulid),
        payload: record.payload,
    })
}

/// Claim work queue items with enhanced error context
pub async fn claim_work_queue_items_with_context(
    pool: DbPoolRef<'_>,
    target_agent_name: &str,
    worker_id: &str,
    batch_size: i64,
    operation_context: &str,
) -> Result<Vec<WorkQueueItem>> {
    let start_time = std::time::Instant::now();
    
    let records = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue
        SET status = 'processing', last_attempt_ts = now(), processing_worker_id = $3
        WHERE queue_id IN (
            SELECT queue_id
            FROM sinex_schemas.work_queue
            WHERE
                status IN ('pending', 'failed_retryable')
                AND target_agent_name = $1
                AND (next_retry_ts IS NULL OR next_retry_ts <= now())
            ORDER BY
                CASE status WHEN 'failed_retryable' THEN 0 ELSE 1 END,
                next_retry_ts ASC NULLS FIRST,
                created_at ASC
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
        target_agent_name,
        batch_size,
        worker_id
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "Work queue claim failed: operation={}, target_agent={}, worker_id={}, batch_size={}, duration_ms={}, error={}",
            operation_context, target_agent_name, worker_id, batch_size, start_time.elapsed().as_millis(), e
        ))
    })?;

    // Map UUID fields to ULID
    let items: Vec<WorkQueueItem> = records
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
        .collect();

    tracing::info!(
        target_agent = target_agent_name,
        worker_id = worker_id,
        claimed_count = items.len(),
        duration_ms = start_time.elapsed().as_millis(),
        "Successfully claimed work queue items"
    );

    Ok(items)
}

/// Complete work queue item with enhanced error context
pub async fn complete_work_queue_item_with_context(
    pool: DbPoolRef<'_>,
    queue_id: Ulid,
    operation_context: &str,
) -> Result<()> {
    let start_time = std::time::Instant::now();
    let queue_uuid: Uuid = ulid_to_uuid(queue_id);
    
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET status = 'succeeded', processed_at = now(), processing_worker_id = NULL
        WHERE queue_id::uuid = $1
        "#,
        queue_uuid
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "Work queue completion failed: operation={}, queue_id={}, duration_ms={}, error={}",
            operation_context, queue_id, start_time.elapsed().as_millis(), e
        ))
    })?;

    if result.rows_affected() == 0 {
        return Err(CoreError::Database(format!(
            "Work queue item not found or already processed: operation={}, queue_id={}",
            operation_context, queue_id
        )));
    }

    tracing::debug!(
        queue_id = queue_id.to_string(),
        duration_ms = start_time.elapsed().as_millis(),
        "Work queue item completed successfully"
    );

    Ok(())
}

/// Fail work queue item with enhanced error context
pub async fn fail_work_queue_item_with_context(
    pool: DbPoolRef<'_>,
    queue_id: Ulid,
    error_message: &str,
    next_retry_ts: Timestamp,
    operation_context: &str,
) -> Result<()> {
    let start_time = std::time::Instant::now();
    let queue_uuid: Uuid = ulid_to_uuid(queue_id);
    
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue
        SET 
            attempts = attempts + 1,
            status = 'failed_retryable',
            error_message_last = $2,
            next_retry_ts = $3,
            processing_worker_id = NULL
        WHERE queue_id::uuid = $1
        "#,
        queue_uuid,
        error_message,
        next_retry_ts
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "Work queue failure update failed: operation={}, queue_id={}, error_message={}, duration_ms={}, error={}",
            operation_context, queue_id, error_message, start_time.elapsed().as_millis(), e
        ))
    })?;

    if result.rows_affected() == 0 {
        return Err(CoreError::Database(format!(
            "Work queue item not found: operation={}, queue_id={}",
            operation_context, queue_id
        )));
    }

    tracing::warn!(
        queue_id = queue_id.to_string(),
        error_message = error_message,
        next_retry = next_retry_ts.to_rfc3339(),
        duration_ms = start_time.elapsed().as_millis(),
        "Work queue item marked for retry"
    );

    Ok(())
}

/// Update agent heartbeat with enhanced error context
pub async fn update_agent_heartbeat_with_context(
    pool: DbPoolRef<'_>,
    agent_name: &str,
    operation_context: &str,
) -> Result<()> {
    let start_time = std::time::Instant::now();
    
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.agent_manifests
        SET last_heartbeat_ts = NOW()
        WHERE agent_name = $1
        "#,
        agent_name
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "Agent heartbeat update failed: operation={}, agent_name={}, duration_ms={}, error={}",
            operation_context, agent_name, start_time.elapsed().as_millis(), e
        ))
    })?;

    if result.rows_affected() == 0 {
        return Err(CoreError::Database(format!(
            "Agent not found in manifests: operation={}, agent_name={}",
            operation_context, agent_name
        )));
    }

    tracing::trace!(
        agent_name = agent_name,
        duration_ms = start_time.elapsed().as_millis(),
        "Agent heartbeat updated"
    );

    Ok(())
}

/// Insert DLQ event with enhanced error context
pub async fn insert_dlq_event_with_context(
    pool: DbPoolRef<'_>,
    failed_event_id: Ulid,
    agent_name: &str,
    source: &str,
    event_type: &str,
    failure_reason: &str,
    error_category: &str,
    original_event_payload: JsonValue,
    additional_metadata: Option<JsonValue>,
    operation_context: &str,
) -> Result<DlqEvent> {
    let start_time = std::time::Instant::now();
    let failed_event_uuid: Uuid = ulid_to_uuid(failed_event_id);
    
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
        agent_name,
        source,
        event_type,
        failure_reason,
        error_category,
        original_event_payload,
        additional_metadata
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "DLQ event insertion failed: operation={}, failed_event_id={}, agent_name={}, source={}, event_type={}, error_category={}, failure_reason={}, duration_ms={}, error={}",
            operation_context, failed_event_id, agent_name, source, event_type, error_category, failure_reason, start_time.elapsed().as_millis(), e
        ))
    })?;

    tracing::warn!(
        dlq_id = uuid_to_ulid(record.dlq_id).to_string(),
        failed_event_id = failed_event_id.to_string(),
        agent_name = agent_name,
        error_category = error_category,
        failure_reason = failure_reason,
        duration_ms = start_time.elapsed().as_millis(),
        "Event moved to DLQ"
    );

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

/// Get recent events with enhanced error context and metrics
pub async fn get_recent_events_with_context(
    pool: DbPoolRef<'_>,
    limit: i64,
    operation_context: &str,
) -> Result<Vec<RawEvent>> {
    let start_time = std::time::Instant::now();
    
    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!", 
            source as "source!", 
            event_type as "event_type!", 
            ts_ingest as "ts_ingest!",
            ts_orig, 
            host as "host!", 
            ingestor_version, 
            payload_schema_id::uuid as "payload_schema_id", 
            payload as "payload!"
        FROM raw.events
        ORDER BY ts_ingest DESC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "Recent events query failed: operation={}, limit={}, duration_ms={}, error={}",
            operation_context, limit, start_time.elapsed().as_millis(), e
        ))
    })?;

    let events: Vec<RawEvent> = records
        .into_iter()
        .map(|record| RawEvent {
            id: uuid_to_ulid(record.id),
            source: record.source,
            event_type: record.event_type,
            ts_ingest: record.ts_ingest,
            ts_orig: record.ts_orig,
            host: record.host,
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(uuid_to_ulid),
            payload: record.payload,
        })
        .collect();

    tracing::debug!(
        count = events.len(),
        limit = limit,
        duration_ms = start_time.elapsed().as_millis(),
        operation = operation_context,
        "Retrieved recent events"
    );

    Ok(events)
}

/// Database transaction wrapper with enhanced error context
pub async fn with_transaction<F, T>(
    pool: DbPoolRef<'_>,
    operation_context: &str,
    transaction_fn: F,
) -> Result<T>
where
    F: for<'c> FnOnce(&'c mut sqlx::Transaction<'_, sqlx::Postgres>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'c>>,
{
    let start_time = std::time::Instant::now();
    
    let mut tx = pool.begin()
        .await
        .map_err(|e| {
            CoreError::Database(format!(
                "Transaction begin failed: operation={}, duration_ms={}, error={}",
                operation_context, start_time.elapsed().as_millis(), e
            ))
        })?;

    let result = transaction_fn(&mut tx).await;

    match result {
        Ok(value) => {
            tx.commit()
                .await
                .map_err(|e| {
                    CoreError::Database(format!(
                        "Transaction commit failed: operation={}, duration_ms={}, error={}",
                        operation_context, start_time.elapsed().as_millis(), e
                    ))
                })?;
            
            tracing::debug!(
                operation = operation_context,
                duration_ms = start_time.elapsed().as_millis(),
                "Transaction completed successfully"
            );
            
            Ok(value)
        }
        Err(e) => {
            if let Err(rollback_err) = tx.rollback().await {
                tracing::error!(
                    operation = operation_context,
                    original_error = %e,
                    rollback_error = %rollback_err,
                    "Transaction rollback failed"
                );
                
                return Err(CoreError::Database(format!(
                    "Transaction rollback failed: operation={}, original_error={}, rollback_error={}",
                    operation_context, e, rollback_err
                )));
            }
            
            tracing::warn!(
                operation = operation_context,
                error = %e,
                duration_ms = start_time.elapsed().as_millis(),
                "Transaction rolled back due to error"
            );
            
            Err(e)
        }
    }
}

/// Enhanced database health check with detailed metrics
pub async fn database_health_check_with_context(
    pool: DbPoolRef<'_>,
    operation_context: &str,
) -> Result<JsonValue> {
    let start_time = std::time::Instant::now();
    
    // Check basic connectivity
    let connectivity_check = sqlx::query("SELECT 1 as health_check")
        .fetch_one(pool)
        .await
        .map_err(|e| {
            CoreError::Database(format!(
                "Database connectivity check failed: operation={}, duration_ms={}, error={}",
                operation_context, start_time.elapsed().as_millis(), e
            ))
        })?;

    // Get table sizes and row counts
    let table_stats = sqlx::query!(
        r#"
        SELECT 
            schemaname,
            relname as tablename,
            pg_total_relation_size(schemaname||'.'||relname)::bigint as total_size_bytes,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes
        FROM pg_stat_user_tables 
        WHERE schemaname IN ('raw', 'sinex_schemas')
        ORDER BY total_size_bytes DESC
        LIMIT 10
        "#
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::Database(format!(
            "Table statistics query failed: operation={}, error={}",
            operation_context, e
        ))
    })?;

    // Get connection pool stats
    let pool_stats = serde_json::json!({
        "size": pool.size(),
        "idle": pool.num_idle(),
    });

    let response = serde_json::json!({
        "status": "healthy",
        "check_duration_ms": start_time.elapsed().as_millis(),
        "connectivity": connectivity_check.get::<i32, _>("health_check") == 1,
        "pool_stats": pool_stats,
        "table_stats": table_stats.into_iter().map(|row| {
            serde_json::json!({
                "schema": row.schemaname,
                "table": row.tablename,
                "size_bytes": row.total_size_bytes,
                "inserts": row.inserts,
                "updates": row.updates,
                "deletes": row.deletes
            })
        }).collect::<Vec<_>>(),
        "timestamp": Utc::now().to_rfc3339()
    });

    tracing::info!(
        operation = operation_context,
        duration_ms = start_time.elapsed().as_millis(),
        pool_size = pool.size(),
        pool_idle = pool.num_idle(),
        "Database health check completed"
    );

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_core::EventFactory;

    #[tokio::test]
    #[ignore = "requires database"]
    async fn test_enhanced_error_context() {
        // This would be a real test in a full implementation
        // Testing that error context is properly propagated
        
        let factory = EventFactory::new("test");
        let event = factory.filesystem()
            .path("/test.txt")
            .created()
            .size(1024)
            .build();

        // Test would verify error context includes all relevant fields
        // when database operations fail
    }
}