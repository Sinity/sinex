use crate::models::{AgentManifest, PromotionQueueItem, RawEvent};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// Insert a raw event
pub async fn insert_raw_event(
    pool: &PgPool,
    source: &str,
    event_type: &str,
    host: &str,
    payload: serde_json::Value,
    ts_orig: Option<DateTime<Utc>>,
    ingestor_version: Option<&str>,
    payload_schema_id: Option<Uuid>,
) -> Result<RawEvent> {
    let event = sqlx::query_as!(
        RawEvent,
        r#"
        INSERT INTO raw.events (source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, source, event_type, ts_orig, host, ingestor_version, payload_schema_id, payload
        "#,
        source,
        event_type,
        host,
        payload,
        ts_orig,
        ingestor_version,
        payload_schema_id
    )
    .fetch_one(pool)
    .await?;

    Ok(event)
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
    let manifest = sqlx::query_as!(
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
        RETURNING *
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

    Ok(manifest)
}

/// Claim items from the promotion queue for processing
pub async fn claim_promotion_queue_items(
    pool: &PgPool,
    target_agent_name: &str,
    worker_id: &str,
    batch_size: i32,
) -> Result<Vec<PromotionQueueItem>> {
    let items = sqlx::query_as!(
        PromotionQueueItem,
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
        RETURNING *
        "#,
        target_agent_name,
        batch_size,
        worker_id
    )
    .fetch_all(pool)
    .await?;

    Ok(items)
}

/// Mark a promotion queue item as successfully processed
pub async fn complete_promotion_queue_item(pool: &PgPool, queue_id: Uuid) -> Result<()> {
    sqlx::query!(
        "DELETE FROM sinex_schemas.promotion_queue WHERE queue_id = $1",
        queue_id
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Mark a promotion queue item as failed and schedule retry
pub async fn fail_promotion_queue_item(
    pool: &PgPool,
    queue_id: Uuid,
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
        WHERE queue_id = $1
        "#,
        queue_id,
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