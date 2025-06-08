use crate::models::{AgentManifest, PromotionQueueItem, RawEvent};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
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
    let row = sqlx::query(
        r#"
        INSERT INTO raw.events (source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7::uuid::ulid)
        RETURNING 
            id::uuid, 
            source, 
            event_type, 
            ts_ingest,
            ts_orig,
            host, 
            ingestor_version, 
            payload_schema_id::uuid, 
            payload
        "#
    )
    .bind(source)
    .bind(event_type)
    .bind(host)
    .bind(&payload)
    .bind(ts_orig)
    .bind(ingestor_version)
    .bind(payload_schema_id)
    .fetch_one(pool)
    .await?;

    let event = RawEvent {
        id: row.try_get("id")?,
        source: row.try_get("source")?,
        event_type: row.try_get("event_type")?,
        ts_ingest: row.try_get("ts_ingest")?,
        ts_orig: row.try_get("ts_orig")?,
        host: row.try_get("host")?,
        ingestor_version: row.try_get("ingestor_version")?,
        payload_schema_id: row.try_get("payload_schema_id")?,
        payload: row.try_get("payload")?,
    };

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
    let row = sqlx::query(
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
            agent_name, 
            description, 
            version, 
            status, 
            agent_type,
            config_template_json, 
            produces_event_types, 
            subscribes_to_event_types,
            required_capabilities, 
            llm_dependencies, 
            repo_url,
            last_heartbeat_ts, 
            last_error_ts, 
            last_error_summary,
            registered_at, 
            updated_at
        "#
    )
    .bind(agent_name)
    .bind(version)
    .bind(status)
    .bind(agent_type)
    .bind(description)
    .bind(produces_event_types)
    .bind(subscribes_to_event_types)
    .fetch_one(pool)
    .await?;

    let manifest = AgentManifest {
        agent_name: row.try_get("agent_name")?,
        description: row.try_get("description")?,
        version: row.try_get("version")?,
        status: row.try_get("status")?,
        agent_type: row.try_get("agent_type")?,
        config_template_json: row.try_get("config_template_json")?,
        produces_event_types: row.try_get("produces_event_types")?,
        subscribes_to_event_types: row.try_get("subscribes_to_event_types")?,
        required_capabilities: row.try_get("required_capabilities")?,
        llm_dependencies: row.try_get("llm_dependencies")?,
        repo_url: row.try_get("repo_url")?,
        last_heartbeat_ts: row.try_get("last_heartbeat_ts")?,
        last_error_ts: row.try_get("last_error_ts")?,
        last_error_summary: row.try_get("last_error_summary")?,
        registered_at: row.try_get("registered_at")?,
        updated_at: row.try_get("updated_at")?,
    };

    Ok(manifest)
}

/// Claim items from the promotion queue for processing
pub async fn claim_promotion_queue_items(
    pool: &PgPool,
    target_agent_name: &str,
    worker_id: &str,
    batch_size: i64,
) -> Result<Vec<PromotionQueueItem>> {
    let rows = sqlx::query(
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
            queue_id::uuid,
            raw_event_id::uuid,
            target_agent_name, 
            status, 
            attempts, 
            max_attempts,
            last_attempt_ts, 
            next_retry_ts, 
            error_message_last,
            created_at, 
            processing_worker_id
        "#
    )
    .bind(target_agent_name)
    .bind(batch_size)
    .bind(worker_id)
    .fetch_all(pool)
    .await?;

    let mut items = Vec::new();
    for row in rows {
        items.push(PromotionQueueItem {
            queue_id: row.try_get("queue_id")?,
            raw_event_id: row.try_get("raw_event_id")?,
            target_agent_name: row.try_get("target_agent_name")?,
            status: row.try_get("status")?,
            attempts: row.try_get("attempts")?,
            max_attempts: row.try_get("max_attempts")?,
            last_attempt_ts: row.try_get("last_attempt_ts")?,
            next_retry_ts: row.try_get("next_retry_ts")?,
            error_message_last: row.try_get("error_message_last")?,
            created_at: row.try_get("created_at")?,
            processing_worker_id: row.try_get("processing_worker_id")?,
        });
    }

    Ok(items)
}

/// Mark a promotion queue item as successfully processed
pub async fn complete_promotion_queue_item(pool: &PgPool, queue_id: Uuid) -> Result<()> {
    sqlx::query(
        "DELETE FROM sinex_schemas.promotion_queue WHERE queue_id = $1::uuid::ulid"
    )
    .bind(queue_id)
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
    sqlx::query(
        r#"
        UPDATE sinex_schemas.promotion_queue
        SET 
            attempts = attempts + 1,
            status = 'failed_retryable',
            error_message_last = $2,
            next_retry_ts = $3,
            processing_worker_id = NULL
        WHERE queue_id = $1::uuid::ulid
        "#
    )
    .bind(queue_id)
    .bind(error_message)
    .bind(next_retry_ts)
    .execute(pool)
    .await?;

    Ok(())
}

/// Update agent heartbeat timestamp
pub async fn update_agent_heartbeat(pool: &PgPool, agent_name: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sinex_schemas.agent_manifests
        SET last_heartbeat_ts = NOW()
        WHERE agent_name = $1
        "#
    )
    .bind(agent_name)
    .execute(pool)
    .await?;

    Ok(())
}