use crate::models::{AgentManifest, PromotionQueueItem, RawEvent};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// Insert a raw event (without ts_ingest - it's embedded in the ULID)
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

/// Query recent events using the ULID timestamp
pub async fn get_recent_events(
    pool: &PgPool,
    since: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<RawEvent>> {
    // Convert the timestamp to a ULID bound
    let events = sqlx::query_as!(
        RawEvent,
        r#"
        SELECT id, source, event_type, ts_orig, host, ingestor_version, payload_schema_id, payload
        FROM raw.events
        WHERE id >= $1::timestamp::ulid
        ORDER BY id DESC
        LIMIT $2
        "#,
        since,
        limit
    )
    .fetch_all(pool)
    .await?;

    Ok(events)
}

/// Query events by time range using ULID bounds
pub async fn get_events_in_range(
    pool: &PgPool,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: Option<i64>,
) -> Result<Vec<RawEvent>> {
    let query = if let Some(limit) = limit {
        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT id, source, event_type, ts_orig, host, ingestor_version, payload_schema_id, payload
            FROM raw.events
            WHERE id >= $1::timestamp::ulid 
              AND id < $2::timestamp::ulid
            ORDER BY id DESC
            LIMIT $3
            "#,
            start,
            end,
            limit
        )
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT id, source, event_type, ts_orig, host, ingestor_version, payload_schema_id, payload
            FROM raw.events
            WHERE id >= $1::timestamp::ulid 
              AND id < $2::timestamp::ulid
            ORDER BY id DESC
            "#,
            start,
            end
        )
        .fetch_all(pool)
        .await?
    };

    Ok(query)
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
        RETURNING agent_name, description, version, status, agent_type, 
                  config_template_json, produces_event_types, subscribes_to_event_types,
                  required_capabilities, llm_dependencies, repo_url,
                  last_heartbeat_ts, last_error_ts, last_error_summary,
                  registered_at, updated_at
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

/// Add item to promotion queue
pub async fn enqueue_for_promotion(
    pool: &PgPool,
    raw_event_id: Uuid,
    target_agent_name: &str,
    max_attempts: i32,
) -> Result<PromotionQueueItem> {
    let item = sqlx::query_as!(
        PromotionQueueItem,
        r#"
        INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name, max_attempts)
        VALUES ($1, $2, $3)
        RETURNING queue_id, raw_event_id, target_agent_name, status, 
                  attempts, max_attempts, last_attempt_ts, next_retry_ts,
                  error_message_last, created_at, processing_worker_id
        "#,
        raw_event_id,
        target_agent_name,
        max_attempts
    )
    .fetch_one(pool)
    .await?;

    Ok(item)
}

/// Get pending promotion queue items for processing
pub async fn claim_promotion_items(
    pool: &PgPool,
    worker_id: &str,
    batch_size: i32,
) -> Result<Vec<PromotionQueueItem>> {
    let items = sqlx::query_as!(
        PromotionQueueItem,
        r#"
        UPDATE sinex_schemas.promotion_queue
        SET status = 'processing',
            processing_worker_id = $1,
            last_attempt_ts = NOW()
        WHERE queue_id IN (
            SELECT queue_id 
            FROM sinex_schemas.promotion_queue
            WHERE status = 'pending'
               OR (status = 'failed_retryable' AND next_retry_ts <= NOW())
            ORDER BY created_at
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        RETURNING queue_id, raw_event_id, target_agent_name, status, 
                  attempts, max_attempts, last_attempt_ts, next_retry_ts,
                  error_message_last, created_at, processing_worker_id
        "#,
        worker_id,
        batch_size
    )
    .fetch_all(pool)
    .await?;

    Ok(items)
}

/// Update promotion queue item status
pub async fn update_promotion_status(
    pool: &PgPool,
    queue_id: Uuid,
    status: &str,
    error_message: Option<&str>,
    next_retry: Option<DateTime<Utc>>,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.promotion_queue
        SET status = $2,
            attempts = attempts + 1,
            error_message_last = $3,
            next_retry_ts = $4
        WHERE queue_id = $1
        "#,
        queue_id,
        status,
        error_message,
        next_retry
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Get event by ID
pub async fn get_event_by_id(pool: &PgPool, id: Uuid) -> Result<Option<RawEvent>> {
    let event = sqlx::query_as!(
        RawEvent,
        r#"
        SELECT id, source, event_type, ts_orig, host, 
               ingestor_version, payload_schema_id, payload
        FROM raw.events
        WHERE id = $1
        "#,
        id
    )
    .fetch_optional(pool)
    .await?;

    Ok(event)
}

/// Update agent heartbeat
pub async fn update_agent_heartbeat(
    pool: &PgPool,
    agent_name: &str,
    status: &str,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.agent_manifests
        SET last_heartbeat_ts = NOW(),
            status = $2
        WHERE agent_name = $1
        "#,
        agent_name,
        status
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Report agent error
pub async fn report_agent_error(
    pool: &PgPool,
    agent_name: &str,
    error_summary: &str,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.agent_manifests
        SET last_error_ts = NOW(),
            last_error_summary = $2,
            status = 'error_state'
        WHERE agent_name = $1
        "#,
        agent_name,
        error_summary
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_insert_and_query_event() -> Result<()> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&std::env::var("TEST_DATABASE_URL")?)
            .await?;

        let event = insert_raw_event(
            &pool,
            "test.source",
            "test.event",
            "test-host",
            serde_json::json!({"test": "data"}),
            None,
            Some("1.0.0"),
            None,
        )
        .await?;

        assert_eq!(event.source, "test.source");
        assert_eq!(event.event_type, "test.event");
        
        // Verify we can extract timestamp from ULID
        let ts_ingest = event.ts_ingest()?;
        assert!(ts_ingest <= Utc::now());

        // Query by time range
        let start = Utc::now() - chrono::Duration::hours(1);
        let end = Utc::now();
        let events = get_events_in_range(&pool, start, end, Some(10)).await?;
        assert!(!events.is_empty());

        Ok(())
    }
}