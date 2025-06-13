use crate::models::{AgentManifest, DlqEvent, PromotionQueueItem, RawEvent};
use crate::validation::EventValidator;
use anyhow::Result;
use chrono::{DateTime, Utc};
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Insert a raw event with ULID type conversion (compile-time safe with manual mapping)
pub async fn insert_raw_event(
    pool: &PgPool,
    source: &str,
    event_type: &str,
    host: &str,
    payload: serde_json::Value,
    ts_orig: Option<DateTime<Utc>>,
    ingestor_version: Option<&str>,
    payload_schema_id: Option<Ulid>,
) -> Result<RawEvent> {
    insert_raw_event_with_validator(pool, source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id, None).await
}

/// Insert a raw event with optional validation
pub async fn insert_raw_event_with_validator(
    pool: &PgPool,
    source: &str,
    event_type: &str,
    host: &str,
    payload: serde_json::Value,
    ts_orig: Option<DateTime<Utc>>,
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
pub async fn insert_event(pool: &PgPool, event: &RawEvent) -> Result<RawEvent> {
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
pub async fn insert_event_with_validator(pool: &PgPool, event: &RawEvent, validator: Option<&EventValidator>) -> Result<RawEvent> {
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
    pool: &PgPool,
    agent_name: &str,
    version: &str,
    status: &str,
    agent_type: &str,
    description: Option<&str>,
    produces_event_types: Option<serde_json::Value>,
    subscribes_to_event_types: Option<serde_json::Value>,
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

/// Claim items from the promotion queue for processing
pub async fn claim_promotion_queue_items(
    pool: &PgPool,
    target_agent_name: &str,
    worker_id: &str,
    batch_size: i64,
) -> Result<Vec<PromotionQueueItem>> {
    // Use query! for compile-time checking, then map UUIDs to ULIDs
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

    // Map UUID fields to ULID with compile-time verified field access
    let items = records
        .into_iter()
        .map(|record| PromotionQueueItem {
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
        })
        .collect();

    Ok(items)
}

/// Mark a promotion queue item as successfully processed
pub async fn complete_promotion_queue_item(pool: &PgPool, queue_id: Ulid) -> Result<()> {
    sqlx::query!(
        "DELETE FROM sinex_schemas.promotion_queue WHERE queue_id = $1::uuid::ulid",
        queue_id.to_uuid()
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Mark a promotion queue item as failed and schedule retry
pub async fn fail_promotion_queue_item(
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
        queue_id.to_uuid(),
        error_message,
        next_retry_ts
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Update agent heartbeat timestamp
pub async fn update_agent_heartbeat(pool: &PgPool, agent_name: &str) -> Result<()> {
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
    pool: &PgPool,
    failed_event_id: Ulid,
    agent_name: &str,
    source: &str,
    event_type: &str,
    failure_reason: &str,
    error_category: &str,
    original_event_payload: serde_json::Value,
    additional_metadata: Option<serde_json::Value>,
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
    pool: &PgPool,
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
    pool: &PgPool,
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
    pool: &PgPool,
    dlq_id: Ulid,
    next_retry_at: Option<DateTime<Utc>>,
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
    pool: &PgPool,
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
pub async fn get_dlq_stats(pool: &PgPool) -> Result<serde_json::Value> {
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