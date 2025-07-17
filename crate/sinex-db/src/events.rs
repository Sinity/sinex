//! Event operations following the clean API pattern
//!
//! This module provides event-related database operations with proper error handling
//! and clean API design, following the exact same pattern as existing *_correct.rs files.
//!
//! This module has been migrated to use the centralized query system for better
//! maintainability and reduced boilerplate.
use crate::queries::EventQueries;
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::validation::EventValidator;
use crate::DbPoolRef;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_events::RawEvent;
use sinex_ulid::Ulid;
use sqlx::types::Uuid;
use sqlx::FromRow;

/// Database record structure for events
#[derive(Debug, FromRow)]
pub struct EventRecord {
    pub id: sqlx::types::Uuid,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: DateTime<Utc>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub host: String,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<sqlx::types::Uuid>,
    pub payload: JsonValue,
    pub source_event_ids: Option<Vec<sqlx::types::Uuid>>,
    pub source_material_id: Option<sqlx::types::Uuid>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    pub anchor_byte: Option<i64>,
    pub associated_blob_ids: Option<Vec<sqlx::types::Uuid>>,
}

impl From<EventRecord> for RawEvent {
    fn from(record: EventRecord) -> Self {
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
            source_event_ids: record
                .source_event_ids
                .map(|ids| ids.into_iter().map(uuid_to_ulid).collect()),
            source_material_id: record.source_material_id.map(uuid_to_ulid),
            source_material_offset_start: record.source_material_offset_start,
            source_material_offset_end: record.source_material_offset_end,
            anchor_byte: record.anchor_byte,
            associated_blob_ids: record
                .associated_blob_ids
                .map(|ids| ids.into_iter().map(uuid_to_ulid).collect()),
        }
    }
}

/// Simple insert event function for test compatibility
#[sinex_macros::auto_db_metrics(operation = "insert_event")]
pub async fn insert_event(pool: DbPoolRef<'_>, event: &RawEvent) -> Result<Ulid> {
    let inserted = insert_event_with_validator(pool, event, None).await?;
    Ok(inserted.id)
}

/// Get an event by ID following the exact same pattern as existing correct functions
#[sinex_macros::auto_db_metrics(operation = "get_event_by_id")]
pub async fn get_event_by_id(pool: DbPoolRef<'_>, event_id: Ulid) -> Result<RawEvent> {
    let record = EventQueries::get_by_id(event_id)
        .fetch_one::<EventRecord>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get event by ID: {}", e))?;

    Ok(record.into())
}

/// Insert an event with validation following the exact same pattern as existing correct functions
#[sinex_macros::auto_db_metrics(operation = "insert_event_with_validator")]
pub async fn insert_event_with_validator(
    pool: DbPoolRef<'_>,
    event: &RawEvent,
    validator: Option<&EventValidator>,
) -> Result<RawEvent> {
    // Validate if validator provided
    if let Some(validator) = validator {
        validator.validate(event)?;
    }

    let record = EventQueries::insert_event_with_source_ids(
        event.source.clone(),
        event.event_type.clone(),
        event.host.clone(),
        event.payload.clone(),
        event.ts_orig,
        event.ingestor_version.clone(),
        event.payload_schema_id,
        event.source_event_ids.clone(),
    )
    .fetch_one::<EventRecord>(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to insert event: {}", e))?;

    Ok(record.into())
}

/// Count total number of events in the database
#[sinex_macros::auto_db_metrics(operation = "count_events")]
pub async fn count_events(pool: DbPoolRef<'_>) -> Result<i64> {
    let (count,) = EventQueries::count_all()
        .fetch_one::<(i64,)>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to count events: {}", e))?;

    Ok(count)
}

/// Insert an event with an attached blob
#[sinex_macros::auto_db_metrics(operation = "insert_event_with_blob")]
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


    let record = EventQueries::insert_event_with_blob(
        event.source.clone(),
        event.event_type.clone(),
        event.host.clone(),
        event.payload.clone(),
        event.ts_orig,
        event.ingestor_version.clone(),
        event.payload_schema_id,
        blob_id,
        event.source_event_ids.clone(),
    )
    .fetch_one::<EventRecord>(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to insert event with blob: {}", e))?;

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
        source_event_ids: record
            .source_event_ids
            .map(|ids| ids.into_iter().map(uuid_to_ulid).collect()),
        source_material_id: record.source_material_id.map(uuid_to_ulid),
        source_material_offset_start: record.source_material_offset_start,
        source_material_offset_end: record.source_material_offset_end,
        anchor_byte: record.anchor_byte,
        associated_blob_ids: record
            .associated_blob_ids
            .map(|ids| ids.into_iter().map(uuid_to_ulid).collect()),
    })
}

/// Get events that have associated blobs
#[sinex_macros::auto_db_metrics(operation = "get_events_with_blobs")]
pub async fn get_events_with_blobs(
    pool: DbPoolRef<'_>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<(RawEvent, Ulid)>> {
    let limit = limit.unwrap_or(100);
    let offset = offset.unwrap_or(0);

    let records = EventQueries::get_with_blobs(Some(limit), Some(offset))
        .fetch_all::<EventRecord>(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get events with blobs: {}", e))?;

    let events_with_blobs = records
        .into_iter()
        .filter_map(|record| {
            // Extract the first blob ID if it exists
            if let Some(ref blob_ids) = record.associated_blob_ids {
                if let Some(first_blob_uuid) = blob_ids.first() {
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
                        source_event_ids: record
                            .source_event_ids
                            .map(|ids| ids.into_iter().map(uuid_to_ulid).collect()),
                        source_material_id: record.source_material_id.map(uuid_to_ulid),
                        source_material_offset_start: record.source_material_offset_start,
                        source_material_offset_end: record.source_material_offset_end,
                        anchor_byte: record.anchor_byte,
                        associated_blob_ids: record.associated_blob_ids.map(|ids| ids.into_iter().map(uuid_to_ulid).collect()),
                    };
                    let blob_id = uuid_to_ulid(*first_blob_uuid);
                    return Some((event, blob_id));
                }
            }
            None
        })
        .collect();

    Ok(events_with_blobs)
}

/// Update an event to attach a blob
#[sinex_macros::auto_db_metrics(operation = "attach_blob_to_event")]
pub async fn attach_blob_to_event(
    pool: DbPoolRef<'_>,
    event_id: Ulid,
    blob_id: Ulid,
) -> Result<()> {
    EventQueries::attach_blob(event_id, blob_id)
        .execute(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to attach blob to event: {}", e))?;

    Ok(())
}

/// Remove blob attachment from an event
#[sinex_macros::auto_db_metrics(operation = "detach_blob_from_event")]
pub async fn detach_blob_from_event(pool: DbPoolRef<'_>, event_id: Ulid) -> Result<()> {
    EventQueries::detach_blob(event_id)
        .execute(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to detach blob from event: {}", e))?;

    Ok(())
}
