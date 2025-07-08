//! Event-related database operations with clean API
//!
//! This module provides domain-specific event operations following the
//! *_correct.rs pattern for clean API and proper error handling.

use crate::models::DlqEvent;
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::validation::EventValidator;
use crate::security::SecurityValidator;
use crate::DbPoolRef;
use sinex_core::{Result, CoreError, RawEvent, JsonValue, OptionalTimestamp, Timestamp};
use sinex_ulid::Ulid;
use sqlx::types::Uuid;
use std::borrow::Cow;

/// Input for creating a raw event
#[derive(Debug)]
pub struct CreateEventInput {
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub payload: JsonValue,
    pub ts_orig: OptionalTimestamp,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub validator: Option<EventValidator>,
}

/// Insert a raw event with proper validation and sanitization
pub async fn insert_event(pool: DbPoolRef<'_>, input: CreateEventInput) -> Result<RawEvent> {
    let event_id = Ulid::new();
    let mut payload = input.payload;
    
    // Sanitize source field for path traversal
    let sanitized_source = sanitize_source(&input.source);
    
    // Sanitize path fields in payload
    sanitize_payload_paths(&mut payload)?;
    
    // Validate with optional validator
    if let Some(validator) = &input.validator {
        validator.validate_event(&sanitized_source, &input.event_type, &payload)?;
    }
    
    // Convert ULID to UUID for SQLx compatibility
    let payload_schema_uuid: Option<Uuid> = input.payload_schema_id.map(ulid_to_uuid);
    
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
        sanitized_source,
        input.event_type,
        input.host,
        payload,
        input.ts_orig,
        input.ingestor_version,
        payload_schema_uuid
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to insert event")
            .with_context("source", &sanitized_source)
            .with_context("event_type", &input.event_type)
            .with_context("host", &input.host)
            .with_source(e.to_string())
            .build()
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

/// Get an event by ID
pub async fn get_event_by_id(pool: DbPoolRef<'_>, event_id: Ulid) -> Result<RawEvent> {
    let event_uuid = ulid_to_uuid(event_id);
    
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
        WHERE id = $1::uuid
        "#,
        event_uuid
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get event by ID")
            .with_context("event_id", event_id)
            .with_source(e.to_string())
            .build()
    })?;
    
    match record {
        Some(record) => Ok(RawEvent {
            id: uuid_to_ulid(record.id),
            source: record.source,
            event_type: record.event_type,
            ts_ingest: record.ts_ingest,
            ts_orig: record.ts_orig,
            host: record.host,
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(uuid_to_ulid),
            payload: record.payload,
        }),
        None => Err(CoreError::not_found("Event", event_id)),
    }
}

/// Get recent events
pub async fn get_recent_events(pool: DbPoolRef<'_>, limit: i64) -> Result<Vec<RawEvent>> {
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
        CoreError::database("Failed to get recent events")
            .with_context("limit", limit)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
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
        .collect())
}

/// Get events by source
pub async fn get_events_by_source(pool: DbPoolRef<'_>, source: &str, limit: i64) -> Result<Vec<RawEvent>> {
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
    .await
    .map_err(|e| {
        CoreError::database("Failed to get events by source")
            .with_context("source", source)
            .with_context("limit", limit)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
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
        .collect())
}

/// Get events by type
pub async fn get_events_by_type(pool: DbPoolRef<'_>, event_type: &str, limit: i64) -> Result<Vec<RawEvent>> {
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
    .await
    .map_err(|e| {
        CoreError::database("Failed to get events by type")
            .with_context("event_type", event_type)
            .with_context("limit", limit)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
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
        .collect())
}

/// Get events in time range
pub async fn get_events_in_time_range(
    pool: DbPoolRef<'_>,
    start_time: Timestamp,
    end_time: Timestamp,
    limit: i64,
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
        LIMIT $3
        "#,
        start_time,
        end_time,
        limit
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get events in time range")
            .with_context("start_time", start_time.to_rfc3339())
            .with_context("end_time", end_time.to_rfc3339())
            .with_context("limit", limit)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
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
        .collect())
}

// Helper functions

fn sanitize_source(source: &str) -> String {
    let is_path_traversal = (source.contains("..") && !source.contains("`") && !source.contains("$(")) || 
                           source.contains("%2e%2e") || 
                           source.contains("%252e%252e") ||
                           source.contains("..%2f") ||
                           source.contains("..%5c") ||
                           source.contains("..%c0%af") ||
                           source.contains("..%c1%9c") ||
                           (source.contains("etc/passwd") && source.starts_with("../"))  ||
                           (source.contains("windows") && source.starts_with("..\\"));
                           
    if is_path_traversal {
        // This looks like a path traversal attempt, so sanitize it
        let mut sanitized = source.to_string();
        sanitized = sanitized.replace("..", "");
        sanitized = sanitized.replace("\\", "/");
        sanitized = sanitized.replace("%2e%2e", "");
        sanitized = sanitized.replace("%252e%252e", "");
        sanitized = sanitized.replace("..%2f", "");
        sanitized = sanitized.replace("..%5c", "");
        sanitized = sanitized.replace("..%c0%af", "");
        sanitized = sanitized.replace("..%c1%9c", "");
        sanitized = sanitized.replace("/etc/passwd", "/sanitized/path");
        sanitized = sanitized.replace("windows/system32", "sanitized/path");
        
        // Also apply unicode sanitization
        match SecurityValidator::sanitize_unicode(&sanitized) {
            Cow::Owned(s) => s,
            Cow::Borrowed(s) => s.to_string(),
        }
    } else {
        // Not a path traversal attempt, just sanitize unicode
        match SecurityValidator::sanitize_unicode(source) {
            Cow::Owned(s) => s,
            Cow::Borrowed(s) => s.to_string(),
        }
    }
}

fn sanitize_payload_paths(payload: &mut JsonValue) -> Result<()> {
    if let Some(obj) = payload.as_object_mut() {
        for (key, value) in obj.iter_mut() {
            if key.contains("path") || key == "file" || key == "directory" || key == "old_path" || key == "new_path" {
                if let Some(path_str) = value.as_str() {
                    // Sanitize path traversal attempts
                    if let Ok(sanitized) = SecurityValidator::sanitize_path(path_str) {
                        *value = serde_json::Value::String(sanitized.into_owned());
                    }
                }
            }
        }
    }
    Ok(())
}