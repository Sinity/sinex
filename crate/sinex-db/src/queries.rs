use crate::models::{AgentManifest, DlqEvent, WorkQueueItem, RawEvent};
use crate::validation::EventValidator;
use anyhow::Result;
use chrono::{DateTime, Utc};
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Insert a raw event with ULID type conversion (compile-time safe with manual mapping)
pub async fn insert_raw_event(
    pool: DbPoolRef,
    source: &str,
    event_type: &str,
    host: &str,
    payload: JsonValue,
    ts_orig: OptionalTimestamp,
    ingestor_version: Option<&str>,
    payload_schema_id: Option<Ulid>,
) -> Result<RawEvent> {
    insert_raw_event_with_validator(pool, source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id, None).await
}

/// Insert a raw event with optional validation
pub async fn insert_raw_event_with_validator(
    pool: DbPoolRef,
    source: &str,
    event_type: &str,
    host: &str,
    payload: JsonValue,
    ts_orig: OptionalTimestamp,
    ingestor_version: Option<&str>,
    payload_schema_id: Option<Ulid>,
    validator: Option<&EventValidator>,
) -> Result<RawEvent> {
    // Validate if validator is provided
    if let Some(validator) = validator {
        // Create a temporary RawEvent for validation
        let temp_event = RawEvent {
            id: Ulid::new(), // Will be replaced by database
            source: source.to_string(),
            event_type: event_type.to_string(),
            ts_ingest: Utc::now(), // Will be replaced by database
            ts_orig,
            host: host.to_string(),
            ingestor_version: ingestor_version.map(|s| s.to_string()),
            payload_schema_id,
            payload: payload.clone(),
        };
        
        validator.validate(&temp_event)
            .map_err(|e| anyhow::anyhow!("Event validation failed: {}", e))?;
    }
    // Use query! for compile-time checking, then map to our ULID-based struct
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
        payload_schema_id.map(|id| id.to_uuid())
    )
    .fetch_one(pool)
    .await?;

    // Map the UUID fields to ULID with compile-time verified field access
    Ok(RawEvent {
        id: Ulid::from_uuid(record.id),
        source: record.source,
        event_type: record.event_type,
        ts_ingest: record.ts_ingest,
        ts_orig: record.ts_orig,
        host: record.host,
        ingestor_version: record.ingestor_version,
        payload_schema_id: record.payload_schema_id.map(Ulid::from_uuid),
        payload: record.payload,
    })
}

/// Insert a RawEvent struct directly with optional validation
pub async fn insert_event(pool: DbPoolRef, event: &RawEvent) -> Result<RawEvent> {
    insert_raw_event(
        pool,
        &event.source,
        &event.event_type,
        &event.host,
        event.payload.clone(),
        event.ts_orig,
        event.ingestor_version.as_deref(),
        event.payload_schema_id,
    ).await
}

/// Insert a RawEvent struct directly with validation
pub async fn insert_event_with_validator(pool: DbPoolRef, event: &RawEvent, validator: Option<&EventValidator>) -> Result<RawEvent> {
    insert_raw_event_with_validator(
        pool,
        &event.source,
        &event.event_type,
        &event.host,
        event.payload.clone(),
        event.ts_orig,
        event.ingestor_version.as_deref(),
        event.payload_schema_id,
        validator,
    ).await
}

/// Register or update an agent manifest
pub async fn upsert_agent_manifest(
    pool: DbPoolRef,
    agent_name: &str,
    version: &str,
    status: &str,
    agent_type: &str,
    description: Option<&str>,
    produces_event_types: Option<JsonValue>,
    subscribes_to_event_types: Option<JsonValue>,
) -> Result<AgentManifest> {
    // AgentManifest doesn't have ULID fields, so query_as! should work
    let record = sqlx::query_as!(
        AgentManifest,
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, status, agent_type, description, produces_event_types, subscribes_to_event_types)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (agent_name) DO UPDATE SET
            version = EXCLUDED.version,
            status = EXCLUDED.status,
            agent_type = EXCLUDED.agent_type,
            description = EXCLUDED.description,
            produces_event_types = EXCLUDED.produces_event_types,
            subscribes_to_event_types = EXCLUDED.subscribes_to_event_types,
            updated_at = NOW()
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
        agent_name,
        version,
        status,
        agent_type,
        description,
        produces_event_types,
        subscribes_to_event_types
    )
    .fetch_one(pool)
    .await?;

    Ok(record)
}

/// Claim items from the work queue for processing
pub async fn claim_work_queue_items(
    pool: DbPoolRef,
    target_agent_name: &str,
    worker_id: &str,
    batch_size: i64,
) -> Result<Vec<WorkQueueItem>> {
    // Use query! for compile-time checking, then map UUIDs to ULIDs
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
    .await?;

    // Map UUID fields to ULID with compile-time verified field access
    let items = records
        .into_iter()
        .map(|record| WorkQueueItem {
            queue_id: Ulid::from_uuid(record.queue_id),
            raw_event_id: Ulid::from_uuid(record.raw_event_id),
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

    Ok(items)
}

/// Mark a work queue item as successfully processed
pub async fn complete_work_queue_item(pool: DbPoolRef, queue_id: Ulid) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET status = 'succeeded', processed_at = now(), processing_worker_id = NULL
        WHERE queue_id = $1::uuid::ulid
        "#,
        queue_id.to_uuid()
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Legacy alias for backward compatibility
#[deprecated(note = "Use complete_work_queue_item instead")]
pub async fn complete_promotion_queue_item(pool: DbPoolRef, queue_id: Ulid) -> Result<()> {
    complete_work_queue_item(pool, queue_id).await
}

/// Mark a work queue item as failed and schedule retry
pub async fn fail_work_queue_item(
    pool: DbPoolRef,
    queue_id: Ulid,
    error_message: &str,
    next_retry_ts: Timestamp,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue
        SET 
            attempts = attempts + 1,
            status = 'failed_retryable',
            error_message_last = $2,
            next_retry_ts = $3,
            processing_worker_id = NULL
        WHERE queue_id = $1::uuid::ulid
        "#,
        queue_id.to_uuid(),
        error_message,
        next_retry_ts
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Mark a work queue item as permanently failed
pub async fn fail_work_queue_item_permanently(
    pool: DbPoolRef,
    queue_id: Ulid,
    failure_reason: &str,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue
        SET 
            status = 'failed',
            failure_reason = $2,
            processed_at = now(),
            processing_worker_id = NULL
        WHERE queue_id = $1::uuid::ulid
        "#,
        queue_id.to_uuid(),
        failure_reason
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Legacy alias for backward compatibility
#[deprecated(note = "Use fail_work_queue_item instead")]
pub async fn fail_promotion_queue_item(
    pool: DbPoolRef,
    queue_id: Ulid,
    error_message: &str,
    next_retry_ts: Timestamp,
) -> Result<()> {
    fail_work_queue_item(pool, queue_id, error_message, next_retry_ts).await
}

/// Update agent heartbeat timestamp
pub async fn update_agent_heartbeat(pool: DbPoolRef, agent_name: &str) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.agent_manifests
        SET last_heartbeat_ts = NOW()
        WHERE agent_name = $1
        "#,
        agent_name
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Insert an event into the DLQ
pub async fn insert_dlq_event(
    pool: DbPoolRef,
    failed_event_id: Ulid,
    agent_name: &str,
    source: &str,
    event_type: &str,
    failure_reason: &str,
    error_category: &str,
    original_event_payload: JsonValue,
    additional_metadata: Option<JsonValue>,
) -> Result<DlqEvent> {
    let record = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.dlq_events 
            (failed_event_id, agent_name, source, event_type, failure_reason, 
             error_category, original_event_payload, additional_metadata)
        VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7, $8)
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
        failed_event_id.to_uuid(),
        agent_name,
        source,
        event_type,
        failure_reason,
        error_category,
        original_event_payload,
        additional_metadata
    )
    .fetch_one(pool)
    .await?;

    Ok(DlqEvent {
        dlq_id: Ulid::from_uuid(record.dlq_id),
        failed_event_id: Ulid::from_uuid(record.failed_event_id),
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

/// Get retryable DLQ events that are ready for retry for a specific agent
pub async fn get_retryable_dlq_events_for_agent(
    pool: DbPoolRef,
    agent_name: &str,
    limit: i64,
) -> Result<Vec<DlqEvent>> {
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
        WHERE resolved_at IS NULL 
            AND error_category = 'retryable'
            AND agent_name = $1
            AND (next_retry_at IS NULL OR next_retry_at <= NOW())
        ORDER BY failed_at ASC
        LIMIT $2
        "#,
        agent_name,
        limit
    )
    .fetch_all(pool)
    .await?;

    let events = records
        .into_iter()
        .map(|record| DlqEvent {
            dlq_id: Ulid::from_uuid(record.dlq_id),
            failed_event_id: Ulid::from_uuid(record.failed_event_id),
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
        .collect();

    Ok(events)
}

/// Get retryable DLQ events that are ready for retry for all agents
pub async fn get_retryable_dlq_events(
    pool: DbPoolRef,
    limit: i64,
) -> Result<Vec<DlqEvent>> {
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
        WHERE resolved_at IS NULL 
            AND error_category = 'retryable'
            AND (next_retry_at IS NULL OR next_retry_at <= NOW())
        ORDER BY failed_at ASC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await?;

    let events = records
        .into_iter()
        .map(|record| DlqEvent {
            dlq_id: Ulid::from_uuid(record.dlq_id),
            failed_event_id: Ulid::from_uuid(record.failed_event_id),
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
        .collect();

    Ok(events)
}

/// Update DLQ event retry attempt
pub async fn update_dlq_retry_attempt(
    pool: DbPoolRef,
    dlq_id: Ulid,
    next_retry_at: OptionalTimestamp,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.dlq_events
        SET 
            retry_count = retry_count + 1,
            last_retry_at = NOW(),
            next_retry_at = $2
        WHERE dlq_id = $1::uuid::ulid
        "#,
        dlq_id.to_uuid(),
        next_retry_at
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Mark DLQ event as resolved
pub async fn resolve_dlq_event(
    pool: DbPoolRef,
    dlq_id: Ulid,
    resolved_by: &str,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.dlq_events
        SET 
            resolved_at = NOW(),
            resolved_by = $2
        WHERE dlq_id = $1::uuid::ulid
        "#,
        dlq_id.to_uuid(),
        resolved_by
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Get DLQ statistics
pub async fn get_dlq_stats(pool: DbPoolRef) -> Result<JsonValue> {
    let stats = sqlx::query!(
        r#"
        SELECT 
            COUNT(*) FILTER (WHERE resolved_at IS NULL) as "unresolved_count!",
            COUNT(*) FILTER (WHERE resolved_at IS NULL AND error_category = 'retryable') as "retryable_count!",
            COUNT(*) FILTER (WHERE resolved_at IS NULL AND error_category = 'permanent') as "permanent_count!",
            COUNT(*) FILTER (WHERE resolved_at IS NULL AND error_category = 'system') as "system_count!",
            COUNT(*) FILTER (WHERE resolved_at IS NULL AND error_category = 'user') as "user_count!",
            COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) as "resolved_count!",
            AVG(EXTRACT(EPOCH FROM (COALESCE(resolved_at, NOW()) - failed_at)))::double precision as "avg_resolution_time_seconds"
        FROM sinex_schemas.dlq_events
        "#
    )
    .fetch_one(pool)
    .await?;

    Ok(serde_json::json!({
        "unresolved_total": stats.unresolved_count,
        "by_category": {
            "retryable": stats.retryable_count,
            "permanent": stats.permanent_count,
            "system": stats.system_count,
            "user": stats.user_count
        },
        "resolved_total": stats.resolved_count,
        "avg_resolution_time_seconds": stats.avg_resolution_time_seconds
    }))
}

/// Get an event by its ULID
pub async fn get_event_by_id(pool: DbPoolRef, event_id: Ulid) -> Result<RawEvent> {
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
        event_id.to_uuid()
    )
    .fetch_one(pool)
    .await?;
    
    Ok(RawEvent {
        id: Ulid::from_uuid(record.id),
        source: record.source,
        event_type: record.event_type,
        ts_ingest: record.ts_ingest,
        ts_orig: record.ts_orig,
        host: record.host,
        ingestor_version: record.ingestor_version,
        payload_schema_id: record.payload_schema_id.map(Ulid::from_uuid),
        payload: record.payload,
    })
}

/// Get recent events ordered by timestamp (most recent first)
pub async fn get_recent_events(pool: DbPoolRef, limit: i64) -> Result<Vec<RawEvent>> {
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
    .await?;
    
    let events = records
        .into_iter()
        .map(|record| RawEvent {
            id: Ulid::from_uuid(record.id),
            source: record.source,
            event_type: record.event_type,
            ts_ingest: record.ts_ingest,
            ts_orig: record.ts_orig,
            host: record.host,
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(Ulid::from_uuid),
            payload: record.payload,
        })
        .collect();
    
    Ok(events)
}

/// Get events by source
pub async fn get_events_by_source(pool: DbPoolRef, source: &str, limit: i64) -> Result<Vec<RawEvent>> {
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
        ORDER BY ts_ingest DESC
        LIMIT $2
        "#,
        source,
        limit
    )
    .fetch_all(pool)
    .await?;
    
    let events = records
        .into_iter()
        .map(|record| RawEvent {
            id: Ulid::from_uuid(record.id),
            source: record.source,
            event_type: record.event_type,
            ts_ingest: record.ts_ingest,
            ts_orig: record.ts_orig,
            host: record.host,
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(Ulid::from_uuid),
            payload: record.payload,
        })
        .collect();
    
    Ok(events)
}

/// Get events by event type
pub async fn get_events_by_type(pool: DbPoolRef, event_type: &str, limit: i64) -> Result<Vec<RawEvent>> {
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
        WHERE event_type = $1
        ORDER BY ts_ingest DESC
        LIMIT $2
        "#,
        event_type,
        limit
    )
    .fetch_all(pool)
    .await?;
    
    let events = records
        .into_iter()
        .map(|record| RawEvent {
            id: Ulid::from_uuid(record.id),
            source: record.source,
            event_type: record.event_type,
            ts_ingest: record.ts_ingest,
            ts_orig: record.ts_orig,
            host: record.host,
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(Ulid::from_uuid),
            payload: record.payload,
        })
        .collect();
    
    Ok(events)
}

/// Get events within a time range
pub async fn get_events_in_time_range(
    pool: DbPoolRef, 
    start_time: Timestamp, 
    end_time: Timestamp
) -> Result<Vec<RawEvent>> {
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
        WHERE ts_ingest >= $1 AND ts_ingest <= $2
        ORDER BY ts_ingest DESC
        "#,
        start_time,
        end_time
    )
    .fetch_all(pool)
    .await?;
    
    let events = records
        .into_iter()
        .map(|record| RawEvent {
            id: Ulid::from_uuid(record.id),
            source: record.source,
            event_type: record.event_type,
            ts_ingest: record.ts_ingest,
            ts_orig: record.ts_orig,
            host: record.host,
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(Ulid::from_uuid),
            payload: record.payload,
        })
        .collect();
    
    Ok(events)
}

/// Add an event to the work queue
pub async fn add_to_work_queue(
    pool: DbPoolRef,
    raw_event_id: Ulid,
    target_agent_name: &str,
    max_attempts: i32,
) -> Result<WorkQueueItem> {
    let record = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.work_queue 
            (raw_event_id, target_agent_name, max_attempts)
        VALUES ($1::uuid::ulid, $2, $3)
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
        raw_event_id.to_uuid(),
        target_agent_name,
        max_attempts
    )
    .fetch_one(pool)
    .await?;

    Ok(WorkQueueItem {
        queue_id: Ulid::from_uuid(record.queue_id),
        raw_event_id: Ulid::from_uuid(record.raw_event_id),
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

/// Legacy alias for backward compatibility
#[deprecated(note = "Use add_to_work_queue instead")]
pub async fn add_to_promotion_queue(
    pool: DbPoolRef,
    raw_event_id: Ulid,
    target_agent_name: &str,
    max_attempts: i32,
) -> Result<WorkQueueItem> {
    add_to_work_queue(pool, raw_event_id, target_agent_name, max_attempts).await
}

/// Get the next work queue item for processing
pub async fn get_next_work_item(
    pool: DbPoolRef,
    target_agent_name: &str,
) -> Result<Option<WorkQueueItem>> {
    let record = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue
        SET status = 'processing', last_attempt_ts = now()
        WHERE queue_id = (
            SELECT queue_id
            FROM sinex_schemas.work_queue
            WHERE
                status = 'pending'
                AND target_agent_name = $1
                AND attempts < max_attempts
            ORDER BY created_at ASC
            LIMIT 1
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
        target_agent_name
    )
    .fetch_optional(pool)
    .await?;

    match record {
        Some(record) => Ok(Some(WorkQueueItem {
            queue_id: Ulid::from_uuid(record.queue_id),
            raw_event_id: Ulid::from_uuid(record.raw_event_id),
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
        })),
        None => Ok(None),
    }
}

/// Legacy alias for backward compatibility
#[deprecated(note = "Use get_next_work_item instead")]
pub async fn get_next_promotion_item(
    pool: DbPoolRef,
    target_agent_name: &str,
) -> Result<Option<WorkQueueItem>> {
    get_next_work_item(pool, target_agent_name).await
}

/// Complete a work queue item (mark as completed)
pub async fn complete_work_item(pool: DbPoolRef, queue_id: Ulid) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue
        SET status = 'succeeded', processed_at = now()
        WHERE queue_id = $1::uuid::ulid
        "#,
        queue_id.to_uuid()
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Legacy alias for backward compatibility
#[deprecated(note = "Use complete_work_item instead")]
pub async fn complete_promotion_item(pool: DbPoolRef, queue_id: Ulid) -> Result<()> {
    complete_work_item(pool, queue_id).await
}

/// Fail a work queue item and optionally retry or move to DLQ
pub async fn fail_work_item(
    pool: DbPoolRef,
    queue_id: Ulid,
    error_message: &str,
) -> Result<()> {
    let record = sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue
        SET 
            attempts = attempts + 1,
            error_message_last = $2,
            last_attempt_ts = now(),
            status = CASE 
                WHEN attempts + 1 >= max_attempts THEN 'failed'
                ELSE 'failed_retryable'
            END,
            processed_at = CASE 
                WHEN attempts + 1 >= max_attempts THEN now()
                ELSE processed_at
            END,
            failure_reason = CASE 
                WHEN attempts + 1 >= max_attempts THEN $2
                ELSE failure_reason
            END
        WHERE queue_id = $1::uuid::ulid
        RETURNING 
            queue_id::uuid as "queue_id!",
            raw_event_id::uuid as "raw_event_id!",
            target_agent_name as "target_agent_name!",
            attempts as "attempts!",
            max_attempts as "max_attempts!",
            status as "status!"
        "#,
        queue_id.to_uuid(),
        error_message
    )
    .fetch_one(pool)
    .await?;

    // If max attempts reached, move to DLQ but keep in work queue for metrics/TTL
    if record.status == "failed" {
        let event = get_event_by_id(pool, Ulid::from_uuid(record.raw_event_id)).await?;
        
        insert_dlq_event(
            pool,
            event.id,
            &record.target_agent_name,
            &event.source,
            &event.event_type,
            error_message,
            "retryable",
            event.payload,
            None,
        ).await?;
    }

    Ok(())
}

/// Legacy alias for backward compatibility
#[deprecated(note = "Use fail_work_item instead")]
pub async fn fail_promotion_item(
    pool: DbPoolRef,
    queue_id: Ulid,
    error_message: &str,
) -> Result<()> {
    fail_work_item(pool, queue_id, error_message).await
}

/// Get a work queue item by ID
pub async fn get_work_item_by_id(pool: DbPoolRef, queue_id: Ulid) -> Result<WorkQueueItem> {
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
        WHERE queue_id = $1::uuid::ulid
        "#,
        queue_id.to_uuid()
    )
    .fetch_one(pool)
    .await?;

    Ok(WorkQueueItem {
        queue_id: Ulid::from_uuid(record.queue_id),
        raw_event_id: Ulid::from_uuid(record.raw_event_id),
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

/// Legacy alias for backward compatibility
#[deprecated(note = "Use get_work_item_by_id instead")]
pub async fn get_promotion_item_by_id(pool: DbPoolRef, queue_id: Ulid) -> Result<WorkQueueItem> {
    get_work_item_by_id(pool, queue_id).await
}

/// Purge old completed work queue items based on TTL policy
/// Removes items with status 'succeeded' or 'failed' that have processed_at older than 90 days
pub async fn purge_old_work_queue_items(pool: DbPoolRef) -> Result<u64> {
    let result = sqlx::query!(
        r#"
        DELETE FROM sinex_schemas.work_queue
        WHERE status IN ('succeeded', 'failed')
        AND processed_at IS NOT NULL
        AND processed_at < now() - interval '90 days'
        "#
    )
    .execute(pool)
    .await?;
    
    Ok(result.rows_affected())
}

/// Get count of work queue items eligible for purging (for monitoring)
pub async fn count_purgeable_work_queue_items(pool: DbPoolRef) -> Result<i64> {
    let result = sqlx::query!(
        r#"
        SELECT COUNT(*) as count
        FROM sinex_schemas.work_queue
        WHERE status IN ('succeeded', 'failed')
        AND processed_at IS NOT NULL
        AND processed_at < now() - interval '90 days'
        "#
    )
    .fetch_one(pool)
    .await?;
    
    Ok(result.count.unwrap_or(0))
}

/// Get DLQ items for a specific agent
pub async fn get_dlq_items(
    pool: DbPoolRef,
    agent_name: &str,
    limit: i64,
) -> Result<Vec<DlqEvent>> {
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

    let events = records
        .into_iter()
        .map(|record| DlqEvent {
            dlq_id: Ulid::from_uuid(record.dlq_id),
            failed_event_id: Ulid::from_uuid(record.failed_event_id),
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
        .collect();

    Ok(events)
}
// ===== Test Helper Functions =====

/// Create a test agent for use in tests
pub async fn create_test_agent(
    pool: DbPoolRef,
    agent_name: &str,
    description: &str,
) -> Result<AgentManifest> {
    upsert_agent_manifest(
        pool,
        agent_name,
        "1.0.0",
        "running",
        "test",
        Some(description),
        None,
        None,
    ).await
}

/// Insert a work queue item for testing
pub async fn insert_work_queue_item(
    pool: DbPoolRef,
    raw_event_id: Ulid,
    target_agent_name: &str,
) -> Result<WorkQueueItem> {
    let record = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name)
        VALUES ($1::uuid::ulid, $2)
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
        raw_event_id.to_uuid(),
        target_agent_name
    )
    .fetch_one(pool)
    .await?;

    Ok(WorkQueueItem {
        queue_id: Ulid::from_uuid(record.queue_id),
        raw_event_id: Ulid::from_uuid(record.raw_event_id),
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

/// Refresh the routing cache materialized view
pub async fn refresh_routing_cache(pool: DbPoolRef) -> Result<()> {
    sqlx::query!("SELECT sinex_router.refresh_routing_cache()")
        .execute(pool)
        .await?;
    Ok(())
}

/// Run the batch router function to process unrouted events
pub async fn run_batch_router(pool: DbPoolRef) -> Result<i64> {
    let result = sqlx::query!(
        "SELECT sinex_router.batch_route_events() as count"
    )
    .fetch_one(pool)
    .await?;
    
    Ok(result.count.unwrap_or(0))
}

/// Calculate queue depth metrics per agent
#[derive(Debug, Clone)]
pub struct QueueDepthMetric {
    pub agent_name: String,
    pub queue_depth: i64,
}

pub async fn calculate_queue_depth_metrics(pool: DbPoolRef) -> Result<Vec<QueueDepthMetric>> {
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
    .await?;
    
    Ok(metrics.into_iter().map(|m| QueueDepthMetric {
        agent_name: m.agent_name,
        queue_depth: m.queue_depth.unwrap_or(0),
    }).collect())
}