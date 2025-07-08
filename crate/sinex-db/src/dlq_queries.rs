//! Dead Letter Queue database operations with clean API
//!
//! This module provides domain-specific DLQ operations following the
//! *_correct.rs pattern for clean API and proper error handling.

use crate::models::{DlqEvent, DlqErrorCategory};
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::DbPoolRef;
use sinex_core::{Result, CoreError, JsonValue};
use sinex_ulid::Ulid;
use sqlx::types::Uuid;
use chrono::Utc;

/// Input for creating a DLQ event
#[derive(Debug)]
pub struct CreateDlqEventInput {
    pub raw_event_id: Ulid,
    pub agent_name: String,
    pub error_category: DlqErrorCategory,
    pub error_message: String,
    pub error_details: Option<JsonValue>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub context: Option<JsonValue>,
}

/// Insert a DLQ event
pub async fn insert_dlq_event(pool: DbPoolRef<'_>, input: CreateDlqEventInput) -> Result<DlqEvent> {
    let raw_event_uuid = ulid_to_uuid(input.raw_event_id);
    let error_details = input.error_details.unwrap_or_else(|| serde_json::json!({}));
    let context = input.context.unwrap_or_else(|| serde_json::json!({}));
    
    let record = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.dlq_events (
            raw_event_id, agent_name, error_category, error_message, 
            error_details, retry_count, max_retries, context
        ) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)
        RETURNING 
            id::uuid as "id!",
            raw_event_id::uuid as "raw_event_id!",
            agent_name as "agent_name!",
            error_category as "error_category!",
            error_message as "error_message!",
            error_details as "error_details!",
            retry_count as "retry_count!",
            max_retries as "max_retries!",
            context as "context!",
            created_at as "created_at!",
            resolved_at,
            resolved_by,
            last_retry_at
        "#,
        raw_event_uuid,
        input.agent_name,
        input.error_category.to_string(),
        input.error_message,
        error_details,
        input.retry_count,
        input.max_retries,
        context
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to insert DLQ event")
            .with_context("raw_event_id", input.raw_event_id)
            .with_context("agent_name", &input.agent_name)
            .with_context("error_category", input.error_category.to_string())
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(DlqEvent {
        id: uuid_to_ulid(record.id),
        raw_event_id: uuid_to_ulid(record.raw_event_id),
        agent_name: record.agent_name,
        error_category: DlqErrorCategory::from_str(&record.error_category).unwrap_or(DlqErrorCategory::Unknown),
        error_message: record.error_message,
        error_details: record.error_details,
        retry_count: record.retry_count,
        max_retries: record.max_retries,
        context: record.context,
        created_at: record.created_at,
        resolved_at: record.resolved_at,
        resolved_by: record.resolved_by,
        last_retry_at: record.last_retry_at,
    })
}

/// Get retryable DLQ events for a specific agent
pub async fn get_retryable_dlq_events_for_agent(
    pool: DbPoolRef<'_>,
    agent_name: &str,
    limit: i64,
) -> Result<Vec<DlqEvent>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            raw_event_id::uuid as "raw_event_id!",
            agent_name as "agent_name!",
            error_category as "error_category!",
            error_message as "error_message!",
            error_details as "error_details!",
            retry_count as "retry_count!",
            max_retries as "max_retries!",
            context as "context!",
            created_at as "created_at!",
            resolved_at,
            resolved_by,
            last_retry_at
        FROM sinex_schemas.dlq_events
        WHERE agent_name = $1 
          AND resolved_at IS NULL
          AND retry_count < max_retries
          AND error_category NOT IN ('permanent', 'schema_validation')
        ORDER BY created_at ASC
        LIMIT $2
        "#,
        agent_name,
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get retryable DLQ events for agent")
            .with_context("agent_name", agent_name)
            .with_context("limit", limit)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
        .into_iter()
        .map(|record| DlqEvent {
            id: uuid_to_ulid(record.id),
            raw_event_id: uuid_to_ulid(record.raw_event_id),
            agent_name: record.agent_name,
            error_category: DlqErrorCategory::from_str(&record.error_category).unwrap_or(DlqErrorCategory::Unknown),
            error_message: record.error_message,
            error_details: record.error_details,
            retry_count: record.retry_count,
            max_retries: record.max_retries,
            context: record.context,
            created_at: record.created_at,
            resolved_at: record.resolved_at,
            resolved_by: record.resolved_by,
            last_retry_at: record.last_retry_at,
        })
        .collect())
}

/// Get all retryable DLQ events
pub async fn get_retryable_dlq_events(pool: DbPoolRef<'_>, limit: i64) -> Result<Vec<DlqEvent>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            raw_event_id::uuid as "raw_event_id!",
            agent_name as "agent_name!",
            error_category as "error_category!",
            error_message as "error_message!",
            error_details as "error_details!",
            retry_count as "retry_count!",
            max_retries as "max_retries!",
            context as "context!",
            created_at as "created_at!",
            resolved_at,
            resolved_by,
            last_retry_at
        FROM sinex_schemas.dlq_events
        WHERE resolved_at IS NULL
          AND retry_count < max_retries
          AND error_category NOT IN ('permanent', 'schema_validation')
        ORDER BY created_at ASC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get retryable DLQ events")
            .with_context("limit", limit)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
        .into_iter()
        .map(|record| DlqEvent {
            id: uuid_to_ulid(record.id),
            raw_event_id: uuid_to_ulid(record.raw_event_id),
            agent_name: record.agent_name,
            error_category: DlqErrorCategory::from_str(&record.error_category).unwrap_or(DlqErrorCategory::Unknown),
            error_message: record.error_message,
            error_details: record.error_details,
            retry_count: record.retry_count,
            max_retries: record.max_retries,
            context: record.context,
            created_at: record.created_at,
            resolved_at: record.resolved_at,
            resolved_by: record.resolved_by,
            last_retry_at: record.last_retry_at,
        })
        .collect())
}

/// Update DLQ retry attempt
pub async fn update_dlq_retry_attempt(
    pool: DbPoolRef<'_>,
    dlq_id: Ulid,
    error_message: &str,
) -> Result<()> {
    let dlq_uuid = ulid_to_uuid(dlq_id);
    
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.dlq_events 
        SET 
            retry_count = retry_count + 1,
            last_retry_at = NOW(),
            error_message = $2
        WHERE id = $1::uuid
        "#,
        dlq_uuid,
        error_message
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to update DLQ retry attempt")
            .with_context("dlq_id", dlq_id)
            .with_context("error_message", error_message)
            .with_source(e.to_string())
            .build()
    })?;
    
    if result.rows_affected() == 0 {
        return Err(CoreError::not_found("DLQ event", dlq_id));
    }
    
    Ok(())
}

/// Resolve a DLQ event
pub async fn resolve_dlq_event(
    pool: DbPoolRef<'_>,
    dlq_id: Ulid,
    resolved_by: &str,
) -> Result<()> {
    let dlq_uuid = ulid_to_uuid(dlq_id);
    
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.dlq_events 
        SET 
            resolved_at = NOW(),
            resolved_by = $2
        WHERE id = $1::uuid
        "#,
        dlq_uuid,
        resolved_by
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to resolve DLQ event")
            .with_context("dlq_id", dlq_id)
            .with_context("resolved_by", resolved_by)
            .with_source(e.to_string())
            .build()
    })?;
    
    if result.rows_affected() == 0 {
        return Err(CoreError::not_found("DLQ event", dlq_id));
    }
    
    Ok(())
}

/// Get DLQ statistics
pub async fn get_dlq_stats(pool: DbPoolRef<'_>) -> Result<JsonValue> {
    let stats = sqlx::query!(
        r#"
        SELECT 
            COUNT(*) as total_events,
            COUNT(CASE WHEN resolved_at IS NULL THEN 1 END) as unresolved_events,
            COUNT(CASE WHEN resolved_at IS NOT NULL THEN 1 END) as resolved_events,
            COUNT(CASE WHEN retry_count >= max_retries THEN 1 END) as exhausted_retries
        FROM sinex_schemas.dlq_events
        "#
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get DLQ statistics")
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(serde_json::json!({
        "total_events": stats.total_events,
        "unresolved_events": stats.unresolved_events,
        "resolved_events": stats.resolved_events,
        "exhausted_retries": stats.exhausted_retries
    }))
}

/// Get DLQ events by category
pub async fn get_dlq_events_by_category(
    pool: DbPoolRef<'_>,
    category: DlqErrorCategory,
    limit: i64,
) -> Result<Vec<DlqEvent>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            raw_event_id::uuid as "raw_event_id!",
            agent_name as "agent_name!",
            error_category as "error_category!",
            error_message as "error_message!",
            error_details as "error_details!",
            retry_count as "retry_count!",
            max_retries as "max_retries!",
            context as "context!",
            created_at as "created_at!",
            resolved_at,
            resolved_by,
            last_retry_at
        FROM sinex_schemas.dlq_events
        WHERE error_category = $1
        ORDER BY created_at DESC
        LIMIT $2
        "#,
        category.to_string(),
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get DLQ events by category")
            .with_context("category", category.to_string())
            .with_context("limit", limit)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
        .into_iter()
        .map(|record| DlqEvent {
            id: uuid_to_ulid(record.id),
            raw_event_id: uuid_to_ulid(record.raw_event_id),
            agent_name: record.agent_name,
            error_category: DlqErrorCategory::from_str(&record.error_category).unwrap_or(DlqErrorCategory::Unknown),
            error_message: record.error_message,
            error_details: record.error_details,
            retry_count: record.retry_count,
            max_retries: record.max_retries,
            context: record.context,
            created_at: record.created_at,
            resolved_at: record.resolved_at,
            resolved_by: record.resolved_by,
            last_retry_at: record.last_retry_at,
        })
        .collect())
}

impl DlqErrorCategory {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "transient" => Some(DlqErrorCategory::Transient),
            "permanent" => Some(DlqErrorCategory::Permanent),
            "schema_validation" => Some(DlqErrorCategory::SchemaValidation),
            "timeout" => Some(DlqErrorCategory::Timeout),
            "resource_exhaustion" => Some(DlqErrorCategory::ResourceExhaustion),
            "unknown" => Some(DlqErrorCategory::Unknown),
            _ => None,
        }
    }
    
    fn to_string(&self) -> String {
        match self {
            DlqErrorCategory::Transient => "transient".to_string(),
            DlqErrorCategory::Permanent => "permanent".to_string(),
            DlqErrorCategory::SchemaValidation => "schema_validation".to_string(),
            DlqErrorCategory::Timeout => "timeout".to_string(),
            DlqErrorCategory::ResourceExhaustion => "resource_exhaustion".to_string(),
            DlqErrorCategory::Unknown => "unknown".to_string(),
        }
    }
}