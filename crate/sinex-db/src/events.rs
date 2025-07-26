//! Event operations following the clean API pattern
//!
//! This module provides event-related database operations with proper error handling
//! and clean API design, following the exact same pattern as existing *_correct.rs files.
//!
//! This module has been migrated to use the centralized query system for better
//! maintainability and reduced boilerplate.
use crate::queries::EventQueries;
use crate::query_helpers::uuid_to_ulid;
use crate::query_helpers::{DbError, DbResult};
use crate::validation::EventValidator;
use crate::DbPoolRef;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_events::RawEvent;
use sinex_ulid::Ulid;
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
// #[sinex_macros::auto_db_metrics(operation = "insert_event")]
pub async fn insert_event(pool: DbPoolRef<'_>, event: &RawEvent) -> DbResult<Ulid> {
    let inserted = insert_event_with_validator(pool, event, None).await?;
    Ok(inserted.id)
}

/// Get an event by ID following the exact same pattern as existing correct functions
// #[sinex_macros::auto_db_metrics(operation = "get_event_by_id")]
pub async fn get_event_by_id(pool: DbPoolRef<'_>, event_id: Ulid) -> DbResult<RawEvent> {
    let record = EventQueries::get_by_id(event_id)
        .fetch_one::<EventRecord>(pool)
        .await?;

    Ok(record.into())
}

/// Insert an event with validation following the exact same pattern as existing correct functions
// #[sinex_macros::auto_db_metrics(operation = "insert_event_with_validator")]
pub async fn insert_event_with_validator(
    pool: DbPoolRef<'_>,
    event: &RawEvent,
    validator: Option<&EventValidator>,
) -> DbResult<RawEvent> {
    // Validate if validator provided
    if let Some(validator) = validator {
        validator
            .validate(event)
            .map_err(|e| DbError::Transaction(format!("Validation failed: {}", e)))?;
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
    .await?;

    Ok(record.into())
}

/// Count total number of events in the database
// #[sinex_macros::auto_db_metrics(operation = "count_events")]
pub async fn count_events(pool: DbPoolRef<'_>) -> DbResult<i64> {
    let (count,) = EventQueries::count_all().fetch_one::<(i64,)>(pool).await?;

    Ok(count)
}

/// Insert an event with an attached blob
// #[sinex_macros::auto_db_metrics(operation = "insert_event_with_blob")]
pub async fn insert_event_with_blob(
    pool: DbPoolRef<'_>,
    event: &RawEvent,
    blob_id: Ulid,
    validator: Option<&EventValidator>,
) -> DbResult<RawEvent> {
    // Validate if validator provided
    if let Some(validator) = validator {
        validator
            .validate(event)
            .map_err(|e| DbError::Transaction(format!("Validation failed: {}", e)))?;
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
// #[sinex_macros::auto_db_metrics(operation = "get_events_with_blobs")]
pub async fn get_events_with_blobs(
    pool: DbPoolRef<'_>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> DbResult<Vec<(RawEvent, Ulid)>> {
    let limit = limit.unwrap_or(100);
    let offset = offset.unwrap_or(0);

    let records = EventQueries::get_with_blobs(Some(limit), Some(offset))
        .fetch_all::<EventRecord>(pool)
        .await?;

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
                        associated_blob_ids: Some(
                            blob_ids.iter().map(|id| uuid_to_ulid(*id)).collect(),
                        ),
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
// #[sinex_macros::auto_db_metrics(operation = "attach_blob_to_event")]
pub async fn attach_blob_to_event(
    pool: DbPoolRef<'_>,
    event_id: Ulid,
    blob_id: Ulid,
) -> DbResult<()> {
    EventQueries::attach_blob(event_id, blob_id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Remove blob attachment from an event
// #[sinex_macros::auto_db_metrics(operation = "detach_blob_from_event")]
pub async fn detach_blob_from_event(pool: DbPoolRef<'_>, event_id: Ulid) -> DbResult<()> {
    EventQueries::detach_blob(event_id).execute(pool).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use sinex_test_utils::prelude::*;
    use sinex_test_utils::{database_pool, TestConfig, TestContext};

    #[sinex_test]
    async fn test_insert_event_basic(ctx: TestContext) -> TestResult {
        let event = RawEvent {
            id: Ulid::new(),
            source: "test.source".to_string(),
            event_type: "test.event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: Some("1.0.0".to_string()),
            payload_schema_id: None,
            payload: json!({"test": "data"}),
            source_event_ids: None,
            anchor_byte: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            associated_blob_ids: None,
        };

        let result = insert_event(ctx.pool(), &event).await?;
        assert_eq!(result.event_id, event.id);
        assert_eq!(result.source, event.source);
        assert_eq!(result.event_type, event.event_type);
        Ok(())
    }

    #[sinex_test]
    async fn test_get_event_by_id(ctx: TestContext) -> TestResult {
        // Insert a test event first
        let event = ctx
            .event()
            .source("get.test")
            .type_("test.get")
            .field("data", "test")
            .insert()
            .await?;

        // Retrieve it by ID
        let retrieved = get_event_by_id(ctx.pool(), event.id).await?;
        assert!(retrieved.is_some());

        let retrieved_event = retrieved.unwrap();
        assert_eq!(retrieved_event.id, event.id);
        assert_eq!(retrieved_event.source, "get.test");
        assert_eq!(retrieved_event.event_type, "test.get");
        assert_eq!(retrieved_event.payload["data"], "test");
        Ok(())
    }

    #[sinex_test]
    async fn test_count_events(ctx: TestContext) -> TestResult {
        // Insert multiple events
        for i in 0..5 {
            ctx.event()
                .source("count.test")
                .type_("test.count")
                .field("index", i)
                .insert()
                .await?;
        }

        let count = count_events(ctx.pool()).await?;
        assert!(count >= 5, "Should have at least 5 events");
        Ok(())
    }
}
