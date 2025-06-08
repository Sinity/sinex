use crate::models::{PromotionQueueItem, RawEvent};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sinex_ulid::Ulid;

/// Get an event by its ULID using compile-time safe query
pub async fn get_event_by_id(pool: &PgPool, id: Ulid) -> Result<Option<RawEvent>> {
    // Use query! with manual struct construction since query_as! has issues with custom types
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
        WHERE id = $1::uuid::ulid
        "#,
        id.as_uuid()
    )
    .fetch_optional(pool)
    .await?;
    
    Ok(record.map(|r| RawEvent {
        id: r.id.into(),
        source: r.source,
        event_type: r.event_type,
        ts_ingest: r.ts_ingest,
        ts_orig: r.ts_orig,
        host: r.host,
        ingestor_version: r.ingestor_version,
        payload_schema_id: r.payload_schema_id.map(Into::into),
        payload: r.payload,
    }))
}

/// Insert a raw event with compile-time checking
pub async fn insert_raw_event_safe(
    pool: &PgPool,
    source: &str,
    event_type: &str,
    host: &str,
    payload: serde_json::Value,
    ts_orig: Option<DateTime<Utc>>,
    ingestor_version: Option<&str>,
    payload_schema_id: Option<Ulid>,
) -> Result<RawEvent> {
    let record = sqlx::query!(
        r#"
        INSERT INTO raw.events (source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7::uuid::ulid)
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
        source,
        event_type,
        host,
        payload,
        ts_orig,
        ingestor_version,
        payload_schema_id.map(|u| u.as_uuid())
    )
    .fetch_one(pool)
    .await?;
    
    Ok(RawEvent {
        id: record.id.into(),
        source: record.source,
        event_type: record.event_type,
        ts_ingest: record.ts_ingest,
        ts_orig: record.ts_orig,
        host: record.host,
        ingestor_version: record.ingestor_version,
        payload_schema_id: record.payload_schema_id.map(Into::into),
        payload: record.payload,
    })
}

/// Get recent events with compile-time safe query
pub async fn get_recent_events(
    pool: &PgPool,
    limit: i64,
    source_filter: Option<&str>,
) -> Result<Vec<RawEvent>> {
    match source_filter {
        Some(source) => {
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
                WHERE source = $1
                ORDER BY id DESC
                LIMIT $2
                "#,
                source,
                limit
            )
            .fetch_all(pool)
            .await?;
            
            Ok(records.into_iter().map(|r| RawEvent {
                id: r.id.into(),
                source: r.source,
                event_type: r.event_type,
                ts_ingest: r.ts_ingest,
                ts_orig: r.ts_orig,
                host: r.host,
                ingestor_version: r.ingestor_version,
                payload_schema_id: r.payload_schema_id.map(Into::into),
                payload: r.payload,
            }).collect())
        }
        None => {
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
                ORDER BY id DESC
                LIMIT $1
                "#,
                limit
            )
            .fetch_all(pool)
            .await?;
            
            Ok(records.into_iter().map(|r| RawEvent {
                id: r.id.into(),
                source: r.source,
                event_type: r.event_type,
                ts_ingest: r.ts_ingest,
                ts_orig: r.ts_orig,
                host: r.host,
                ingestor_version: r.ingestor_version,
                payload_schema_id: r.payload_schema_id.map(Into::into),
                payload: r.payload,
            }).collect())
        }
    }
}

/// Claim promotion queue items with compile-time safety
pub async fn claim_promotion_queue_items_safe(
    pool: &PgPool,
    target_agent_name: &str,
    worker_id: &str,
    batch_size: i64,
) -> Result<Vec<PromotionQueueItem>> {
    let records = sqlx::query!(
        r#"
        UPDATE sinex_schemas.promotion_queue
        SET status = 'processing', last_attempt_ts = now(), processing_worker_id = $3
        WHERE queue_id IN (
            SELECT queue_id
            FROM sinex_schemas.promotion_queue
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
            processing_worker_id
        "#,
        target_agent_name,
        batch_size,
        worker_id
    )
    .fetch_all(pool)
    .await?;
    
    Ok(records.into_iter().map(|r| PromotionQueueItem {
        queue_id: r.queue_id.into(),
        raw_event_id: r.raw_event_id.into(),
        target_agent_name: r.target_agent_name,
        status: r.status,
        attempts: r.attempts,
        max_attempts: r.max_attempts,
        last_attempt_ts: r.last_attempt_ts,
        next_retry_ts: r.next_retry_ts,
        error_message_last: r.error_message_last,
        created_at: r.created_at,
        processing_worker_id: r.processing_worker_id,
    }).collect())
}

/// Complete a promotion queue item
pub async fn complete_promotion_queue_item_safe(pool: &PgPool, queue_id: Ulid) -> Result<()> {
    sqlx::query!(
        "DELETE FROM sinex_schemas.promotion_queue WHERE queue_id = $1::uuid::ulid",
        queue_id.as_uuid()
    )
    .execute(pool)
    .await?;
    
    Ok(())
}

/// Fail a promotion queue item and schedule retry
pub async fn fail_promotion_queue_item_safe(
    pool: &PgPool,
    queue_id: Ulid,
    error_message: &str,
    next_retry_ts: DateTime<Utc>,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.promotion_queue
        SET 
            attempts = attempts + 1,
            status = 'failed_retryable',
            error_message_last = $2,
            next_retry_ts = $3,
            processing_worker_id = NULL
        WHERE queue_id = $1::uuid::ulid
        "#,
        queue_id.as_uuid(),
        error_message,
        next_retry_ts
    )
    .execute(pool)
    .await?;
    
    Ok(())
}

/// Helper function to demonstrate batch event fetching
pub async fn get_events_by_ids(pool: &PgPool, ids: &[Ulid]) -> Result<Vec<RawEvent>> {
    if ids.is_empty() {
        return Ok(vec![]);
    }
    
    // Convert ULIDs to UUIDs for the query
    let uuids: Vec<uuid::Uuid> = ids.iter().map(|u| u.as_uuid()).collect();
    
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
        WHERE id = ANY($1::uuid[]::ulid[])
        ORDER BY id
        "#,
        &uuids
    )
    .fetch_all(pool)
    .await?;
    
    Ok(records.into_iter().map(|r| RawEvent {
        id: r.id.into(),
        source: r.source,
        event_type: r.event_type,
        ts_ingest: r.ts_ingest,
        ts_orig: r.ts_orig,
        host: r.host,
        ingestor_version: r.ingestor_version,
        payload_schema_id: r.payload_schema_id.map(Into::into),
        payload: r.payload,
    }).collect())
}

/// Demonstrate that we can use query_as! with proper type overrides  
/// This is a helper struct that matches the database types exactly
#[derive(sqlx::FromRow)]
struct RawEventDb {
    id: uuid::Uuid,
    source: String,
    event_type: String,
    ts_ingest: DateTime<Utc>,
    ts_orig: Option<DateTime<Utc>>,
    host: String,
    ingestor_version: Option<String>,
    payload_schema_id: Option<uuid::Uuid>,
    payload: serde_json::Value,
}

/// Alternative approach using query_as! with a helper struct
pub async fn get_event_by_id_alternative(pool: &PgPool, id: Ulid) -> Result<Option<RawEvent>> {
    let record = sqlx::query_as!(
        RawEventDb,
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
        WHERE id = $1::uuid::ulid
        "#,
        id.as_uuid()
    )
    .fetch_optional(pool)
    .await?;
    
    Ok(record.map(|r| RawEvent {
        id: r.id.into(),
        source: r.source,
        event_type: r.event_type,
        ts_ingest: r.ts_ingest,
        ts_orig: r.ts_orig,
        host: r.host,
        ingestor_version: r.ingestor_version,
        payload_schema_id: r.payload_schema_id.map(Into::into),
        payload: r.payload,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[sqlx::test]
    async fn test_compile_time_safe_queries(pool: PgPool) -> Result<()> {
        // Test insert
        let event = insert_raw_event_safe(
            &pool,
            "test_source",
            "test_type",
            "test_host",
            serde_json::json!({"test": "data"}),
            None,
            Some("1.0.0"),
            None,
        )
        .await?;
        
        // Test get by ID
        let fetched = get_event_by_id(&pool, event.id).await?;
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().id, event.id);
        
        // Test get recent
        let recent = get_recent_events(&pool, 10, Some("test_source")).await?;
        assert!(!recent.is_empty());
        
        // Test alternative approach
        let alt_fetched = get_event_by_id_alternative(&pool, event.id).await?;
        assert!(alt_fetched.is_some());
        assert_eq!(alt_fetched.unwrap().id, event.id);
        
        Ok(())
    }
}