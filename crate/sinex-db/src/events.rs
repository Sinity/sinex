//! Event operations following the clean API pattern
//!
//! This module provides event-related database operations with proper error handling
//! and clean API design, following the exact same pattern as existing *_correct.rs files.
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::validation::EventValidator;
use crate::DbPoolRef;
use anyhow::Result;
use sinex_core::RawEvent;
use sinex_ulid::Ulid;
use sqlx::types::Uuid;

/// Get an event by ID following the exact same pattern as existing correct functions
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
        WHERE id::uuid = $1
        "#,
        event_uuid
    )
    .fetch_one(pool)
    .await?;

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

/// Insert an event with validation following the exact same pattern as existing correct functions
pub async fn insert_event_with_validator(
    pool: DbPoolRef<'_>,
    event: &RawEvent,
    validator: Option<&EventValidator>,
) -> Result<RawEvent> {
    // Validate if validator provided
    if let Some(validator) = validator {
        validator.validate(event)?;
    }

    // Convert ULID to UUID for SQLx compatibility
    let payload_schema_uuid: Option<Uuid> = event.payload_schema_id.map(ulid_to_uuid);

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
    .await?;

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

/// Count total number of events in the database
pub async fn count_events(pool: DbPoolRef<'_>) -> Result<i64> {
    let record = sqlx::query!("SELECT COUNT(*) as count FROM raw.events")
        .fetch_one(pool)
        .await?;

    Ok(record.count.unwrap_or(0))
}
