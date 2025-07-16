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

/// Simple insert event function for test compatibility
pub async fn insert_event(pool: DbPoolRef<'_>, event: &RawEvent) -> Result<Ulid> {
    let inserted = insert_event_with_validator(pool, event, None).await?;
    Ok(inserted.id)
}

/// Get an event by ID following the exact same pattern as existing correct functions
pub async fn get_event_by_id(pool: DbPoolRef<'_>, event_id: Ulid) -> Result<RawEvent> {
    let event_uuid = ulid_to_uuid(event_id);

    let record = sqlx::query!(
        r#"
        SELECT 
            event_id::uuid as "id!", 
            source as "source!", 
            event_type as "event_type!", 
            ts_ingest as "ts_ingest!",
            ts_orig,
            host as "host!", 
            ingestor_version, 
            payload_schema_id::uuid as "payload_schema_id", 
            payload as "payload!",
            source_event_ids::uuid[] as "source_event_ids"
        FROM core.events 
        WHERE event_id::uuid = $1
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
        source_event_ids: record.source_event_ids.map(|ids| {
            ids.into_iter().map(uuid_to_ulid).collect()
        }),
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

    // Convert source_event_ids to array of UUIDs for database storage
    let source_event_uuids: Option<Vec<Uuid>> = event.source_event_ids.as_ref().map(|ids| {
        ids.iter().map(|id| id.to_uuid()).collect()
    });

    let record = sqlx::query!(
        r#"
        INSERT INTO core.events (source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id, source_event_ids)
        VALUES ($1, $2, $3, $4, $5, $6, $7::uuid, $8::uuid[])
        RETURNING 
            event_id::uuid as "id!", 
            source as "source!", 
            event_type as "event_type!", 
            ts_ingest as "ts_ingest!",
            ts_orig,
            host as "host!", 
            ingestor_version, 
            payload_schema_id::uuid as "payload_schema_id", 
            payload as "payload!",
            source_event_ids::uuid[] as "source_event_ids"
        "#,
        event.source,
        event.event_type,
        event.host,
        event.payload,
        event.ts_orig,
        event.ingestor_version,
        payload_schema_uuid,
        source_event_uuids.as_deref()
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
        source_event_ids: record.source_event_ids.map(|ids| {
            ids.into_iter().map(uuid_to_ulid).collect()
        }),
    })
}

/// Count total number of events in the database
pub async fn count_events(pool: DbPoolRef<'_>) -> Result<i64> {
    let record = sqlx::query!("SELECT COUNT(*) as count FROM core.events")
        .fetch_one(pool)
        .await?;

    Ok(record.count.unwrap_or(0))
}

/// Insert an event with an attached blob
pub async fn insert_event_with_blob(
    pool: DbPoolRef<'_>,
    event: &RawEvent,
    blob_id: Ulid,
    validator: Option<&EventValidator>,
) -> Result<RawEvent> {
    // Validate if validator provided
    if let Some(validator) = validator {
        validator.validate(event)?;
    }

    // Convert ULID to UUID for SQLx compatibility
    let payload_schema_uuid: Option<Uuid> = event.payload_schema_id.map(ulid_to_uuid);
    let blob_uuid = ulid_to_uuid(blob_id);

    // Convert source_event_ids to array of UUIDs for database storage
    let source_event_uuids: Option<Vec<Uuid>> = event.source_event_ids.as_ref().map(|ids| {
        ids.iter().map(|id| id.to_uuid()).collect()
    });

    let record = sqlx::query!(
        r#"
        INSERT INTO core.events (source, event_type, host, payload, ts_orig, ingestor_version, payload_schema_id, associated_blob_ids, source_event_ids)
        VALUES ($1, $2, $3, $4, $5, $6, $7::uuid, ARRAY[$8::uuid], $9::uuid[])
        RETURNING 
            event_id::uuid as "id!", 
            source as "source!", 
            event_type as "event_type!", 
            ts_ingest as "ts_ingest!",
            ts_orig,
            host as "host!", 
            ingestor_version, 
            payload_schema_id::uuid as "payload_schema_id", 
            payload as "payload!",
            associated_blob_ids::uuid[] as "associated_blob_ids",
            source_event_ids::uuid[] as "source_event_ids"
        "#,
        event.source,
        event.event_type,
        event.host,
        event.payload,
        event.ts_orig,
        event.ingestor_version,
        payload_schema_uuid,
        blob_uuid,
        source_event_uuids.as_deref()
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
        source_event_ids: record.source_event_ids.map(|ids| {
            ids.into_iter().map(uuid_to_ulid).collect()
        }),
    })
}

/// Get events that have associated blobs
pub async fn get_events_with_blobs(
    pool: DbPoolRef<'_>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<(RawEvent, Ulid)>> {
    let limit = limit.unwrap_or(100);
    let offset = offset.unwrap_or(0);

    let records = sqlx::query!(
        r#"
        SELECT 
            event_id::uuid as "id!", 
            source as "source!", 
            event_type as "event_type!", 
            ts_ingest as "ts_ingest!",
            ts_orig,
            host as "host!", 
            ingestor_version, 
            payload_schema_id::uuid as "payload_schema_id", 
            payload as "payload!",
            associated_blob_ids::uuid[] as "associated_blob_ids!",
            source_event_ids::uuid[] as "source_event_ids"
        FROM core.events 
        WHERE associated_blob_ids IS NOT NULL
        ORDER BY ts_ingest DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset
    )
    .fetch_all(pool)
    .await?;

    let events_with_blobs = records
        .into_iter()
        .map(|record| {
            let event = RawEvent {
                id: uuid_to_ulid(record.id),
                source: record.source,
                event_type: record.event_type,
                ts_ingest: record.ts_ingest,
                ts_orig: record.ts_orig,
                host: record.host,
                ingestor_version: record.ingestor_version,
                payload_schema_id: record.payload_schema_id.map(uuid_to_ulid),
                payload: record.payload,
                source_event_ids: record.source_event_ids.map(|ids| {
            ids.into_iter().map(uuid_to_ulid).collect()
        }),
            };
            let blob_id = uuid_to_ulid(record.associated_blob_ids[0]);
            (event, blob_id)
        })
        .collect();

    Ok(events_with_blobs)
}

/// Update an event to attach a blob
pub async fn attach_blob_to_event(
    pool: DbPoolRef<'_>,
    event_id: Ulid,
    blob_id: Ulid,
) -> Result<()> {
    let event_uuid = ulid_to_uuid(event_id);
    let blob_uuid = ulid_to_uuid(blob_id);

    sqlx::query!(
        "UPDATE core.events SET associated_blob_ids = ARRAY[$2::uuid] WHERE event_id::uuid = $1",
        event_uuid,
        blob_uuid
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Remove blob attachment from an event
pub async fn detach_blob_from_event(pool: DbPoolRef<'_>, event_id: Ulid) -> Result<()> {
    let event_uuid = ulid_to_uuid(event_id);

    sqlx::query!(
        "UPDATE core.events SET associated_blob_ids = NULL WHERE event_id::uuid = $1",
        event_uuid
    )
    .execute(pool)
    .await?;

    Ok(())
}
