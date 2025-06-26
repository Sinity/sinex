//! Complete examples demonstrating the macro-based query system
//!
//! This module shows how to use the new macro system that preserves sqlx's 
//! compile-time verification while providing simplified APIs.

use crate::{DbPool, DbPoolRef, RawEvent};
use crate::models::{WorkQueueItem, AgentManifest};
use crate::query_helpers::{DbResult, RetryConfig, ulid_to_uuid, uuid_to_ulid};
use crate::{query_one_verified, query_many_verified, query_optional_verified, execute_verified, with_transaction, with_retry_transaction};
use sinex_ulid::Ulid;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

/// Example: Simple event retrieval with compile-time verification
pub async fn get_event_by_id_verified(pool: DbPoolRef<'_>, event_id: Ulid) -> DbResult<RawEvent> {
    // This expands to sqlx::query! with automatic error handling and ULID conversion
    let record = query_one_verified!(
        pool,
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
        WHERE id = $1::uuid
        "#,
        ulid_to_uuid(event_id);
        context = "fetching event by ID"
    )?;

    // Manual mapping from UUID record to ULID-based RawEvent
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

/// Example: Multi-parameter query with timeout
pub async fn get_events_by_source_and_time(
    pool: DbPoolRef<'_>,
    source: &str,
    after: DateTime<Utc>,
    limit: i64,
) -> DbResult<Vec<RawEvent>> {
    let records = query_many_verified!(
        pool,
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
        WHERE source = $1 AND ts_ingest > $2
        ORDER BY ts_ingest DESC 
        LIMIT $3
        "#,
        source, after, limit;
        context = "fetching events by source and time range",
        timeout = std::time::Duration::from_secs(10)
    )?;

    // Convert all records to ULID-based RawEvents
    let events = records.into_iter().map(|record| {
        RawEvent {
            id: uuid_to_ulid(record.id),
            source: record.source,
            event_type: record.event_type,
            ts_ingest: record.ts_ingest,
            ts_orig: record.ts_orig,
            host: record.host,
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(uuid_to_ulid),
            payload: record.payload,
        }
    }).collect();

    Ok(events)
}

/// Example: Insert with ULID handling and automatic RETURNING
pub async fn insert_event_verified(
    pool: DbPoolRef<'_>,
    source: &str,
    event_type: &str,
    host: &str,
    payload: JsonValue,
    ts_orig: Option<DateTime<Utc>>,
    ingestor_version: Option<&str>,
    payload_schema_id: Option<Ulid>,
) -> DbResult<RawEvent> {
    let record = query_one_verified!(
        pool,
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
        source, 
        event_type, 
        host, 
        payload, 
        ts_orig, 
        ingestor_version,
        payload_schema_id.map(ulid_to_uuid);
        context = "inserting new event"
    )?;

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

/// Example: Work queue operations with ULID handling
pub async fn claim_work_items_verified(
    pool: DbPoolRef<'_>,
    agent_name: &str,
    worker_id: &str,
    batch_size: i64,
) -> DbResult<Vec<WorkQueueItem>> {
    let records = query_many_verified!(
        pool,
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
        agent_name, batch_size, worker_id;
        context = "claiming work queue items"
    )?;

    // Convert to WorkQueueItem with ULID fields
    let items = records.into_iter().map(|record| {
        WorkQueueItem {
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
        }
    }).collect();

    Ok(items)
}

/// Example: Optional query result
pub async fn find_agent_by_name(
    pool: DbPoolRef<'_>,
    agent_name: &str,
) -> DbResult<Option<AgentManifest>> {
    query_optional_verified!(
        pool,
        r#"
        SELECT 
            agent_name as "agent_name!",
            description,
            version as "version!",
            status as "status!",
            agent_type as "agent_type!",
            config_template_json,
            produces_event_types,
            subscribes_to_event_types,
            required_capabilities,
            llm_dependencies,
            repo_url,
            last_heartbeat_ts,
            last_error_ts,
            last_error_summary,
            registered_at as "registered_at!",
            updated_at as "updated_at!"
        FROM sinex_schemas.agent_manifests
        WHERE agent_name = $1
        "#,
        agent_name;
        context = "finding agent by name"
    )
}

/// Example: Execute without results (UPDATE/DELETE)
pub async fn mark_event_processed(
    pool: DbPoolRef<'_>,
    queue_id: Ulid,
    worker_id: &str,
) -> DbResult<u64> {
    execute_verified!(
        pool,
        r#"
        UPDATE sinex_schemas.work_queue
        SET 
            status = 'succeeded',
            processed_at = now(),
            processing_worker_id = $2
        WHERE queue_id = $1::uuid
        "#,
        ulid_to_uuid(queue_id), worker_id;
        context = "marking work item as processed"
    )
}

/// Example: Transaction with automatic rollback
pub async fn transfer_event_ownership(
    pool: &DbPool,
    from_agent: &str,
    to_agent: &str,
    event_id: Ulid,
) -> DbResult<()> {
    with_transaction!(pool, |tx| {
        // Mark old assignment as cancelled
        execute_verified!(
            &mut *tx,
            r#"
            UPDATE sinex_schemas.work_queue
            SET status = 'cancelled'
            WHERE target_agent_name = $1 AND raw_event_id = $2::uuid
            "#,
            from_agent, ulid_to_uuid(event_id);
            context = "cancelling old assignment"
        )?;

        // Create new assignment
        execute_verified!(
            &mut *tx,
            r#"
            INSERT INTO sinex_schemas.work_queue 
                (raw_event_id, target_agent_name, status, attempts, max_attempts, created_at)
            VALUES ($1::uuid, $2, 'pending', 0, 3, now())
            "#,
            ulid_to_uuid(event_id), to_agent;
            context = "creating new assignment"
        )?;

        Ok(())
    })
}

/// Example: Retry transaction with exponential backoff
pub async fn update_agent_with_retry(
    pool: &DbPool,
    agent_name: &str,
    status: &str,
    version: &str,
) -> DbResult<AgentManifest> {
    let retry_config = RetryConfig::default();
    
    with_retry_transaction!(pool, retry_config, |tx| {
        query_one_verified!(
            &mut *tx,
            r#"
            UPDATE sinex_schemas.agent_manifests
            SET 
                status = $2,
                version = $3,
                updated_at = now()
            WHERE agent_name = $1
            RETURNING 
                agent_name as "agent_name!",
                description,
                version as "version!",
                status as "status!",
                agent_type as "agent_type!",
                config_template_json,
                produces_event_types,
                subscribes_to_event_types,
                required_capabilities,
                llm_dependencies,
                repo_url,
                last_heartbeat_ts,
                last_error_ts,
                last_error_summary,
                registered_at as "registered_at!",
                updated_at as "updated_at!"
            "#,
            agent_name, status, version;
            context = "updating agent with retry"
        )
    })
}

/// Example: Complex query with conditional logic
pub async fn get_event_statistics(
    pool: DbPoolRef<'_>,
    source_filter: Option<&str>,
    hours_back: i32,
) -> DbResult<(i64, String, DateTime<Utc>)> {
    match source_filter {
        Some(source) => {
            query_one_verified!(
                pool,
                r#"
                SELECT 
                    COUNT(*) as "count!",
                    source as "most_common_source!",
                    MAX(ts_ingest) as "latest_event!"
                FROM raw.events 
                WHERE ts_ingest > now() - interval '$1 hours'
                    AND source = $2
                GROUP BY source
                ORDER BY count DESC
                LIMIT 1
                "#,
                hours_back, source;
                context = "getting event statistics with source filter",
                timeout = std::time::Duration::from_secs(30)
            )
        }
        None => {
            query_one_verified!(
                pool,
                r#"
                SELECT 
                    COUNT(*) as "count!",
                    source as "most_common_source!",
                    MAX(ts_ingest) as "latest_event!"
                FROM raw.events 
                WHERE ts_ingest > now() - interval '$1 hours'
                GROUP BY source
                ORDER BY count DESC
                LIMIT 1
                "#,
                hours_back;
                context = "getting event statistics",
                timeout = std::time::Duration::from_secs(30)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_test_pool;

    // These tests demonstrate that the macros compile correctly
    // Actual functionality tests would require database setup

    #[test]
    fn test_macro_syntax_compilation() {
        // Verify that all our macro examples compile without syntax errors
        // The actual functionality testing requires integration tests with database
        assert!(true);
    }

    // Example of what integration tests would look like:
    /*
    #[tokio::test]
    async fn test_verified_query_integration() {
        let pool = create_test_pool("postgresql:///sinex_test").await.unwrap();
        
        // Insert test event
        let event = insert_event_verified(
            &pool,
            "test.source",
            "test_event",
            "localhost",
            serde_json::json!({"test": "data"}),
            None,
            Some("1.0.0"),
            None,
        ).await.unwrap();

        // Retrieve it back
        let retrieved = get_event_by_id_verified(&pool, event.id).await.unwrap();
        
        assert_eq!(event.id, retrieved.id);
        assert_eq!(event.source, retrieved.source);
        assert_eq!(event.payload, retrieved.payload);
    }
    */
}

// Advanced examples could be added here in the future
// For now, the basic macro system provides the core functionality needed