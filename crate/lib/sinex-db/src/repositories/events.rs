use crate::models::{Event, Provenance, SourceMaterial};
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::repositories::common::{
    db_error, DbResult, EnhancedRepository, EventSearchFilters, Repository, TimeBucketResult,
};
use crate::schema::Events;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_types::domain::{EventSource, EventType, HostName, SchemaName, SchemaVersion};
use sinex_types::Id;
use sqlx::{FromRow, PgPool, Postgres, Transaction};

/// Event repository for database operations
pub struct EventRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for EventRepository<'a> {
    fn pool(&self) -> &'a PgPool {
        self.pool
    }

    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }
}

impl<'a> EnhancedRepository<'a> for EventRepository<'a> {
    type Table = Events;
}

/// Database record for events table
///
/// This struct matches the database schema exactly and is used for
/// sqlx query_as! macros. It serves as an intermediate representation
/// between the database and the domain model.
#[derive(Debug, FromRow)]
pub struct EventRecord {
    pub event_id: uuid::Uuid,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub payload: JsonValue,
    pub ts_orig: Option<DateTime<Utc>>,
    pub ts_ingest: DateTime<Utc>,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<uuid::Uuid>,
    // Provenance fields - separate in database
    pub source_event_ids: Option<Vec<uuid::Uuid>>,
    pub source_material_id: Option<uuid::Uuid>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    pub anchor_byte: Option<i64>,
    pub associated_blob_ids: Option<Vec<uuid::Uuid>>,
    // Schema fields
    pub payload_schema_name: Option<String>,
    pub payload_schema_version: Option<String>,
    pub processor_manifest_id: Option<i32>,
}

impl EventRecord {
    /// Convert database record to domain Event
    pub fn to_event(self) -> Event {
        // Reconstruct provenance from separate fields
        let provenance = match (self.source_event_ids, self.source_material_id) {
            (Some(event_ids), None) if !event_ids.is_empty() => Some(Provenance::Events(
                event_ids
                    .into_iter()
                    .map(|uuid| Id::<Event>::from(uuid_to_ulid(uuid)))
                    .collect(),
            )),
            (None, Some(material_id)) => Some(Provenance::Material {
                id: Id::<SourceMaterial>::from(uuid_to_ulid(material_id)),
                offset_start: self.source_material_offset_start,
                offset_end: self.source_material_offset_end,
            }),
            _ => None,
        };

        let associated_blob_ids = self
            .associated_blob_ids
            .map(|ids| ids.into_iter().map(uuid_to_ulid).collect());

        Event {
            id: Some(Id::<Event>::from_uuid(self.event_id)),
            source: self.source.into(),
            event_type: self.event_type.into(),
            host: self.host.into(),
            payload: self.payload,
            ts_ingest: self.ts_ingest,
            ts_orig: self.ts_orig,
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id.map(uuid_to_ulid),
            provenance,
            anchor_byte: self.anchor_byte,
            associated_blob_ids,
        }
    }
}

/// Extract provenance fields from domain Event for database storage
fn extract_provenance(
    event: &Event,
) -> (
    Option<Vec<uuid::Uuid>>, // source_event_ids
    Option<uuid::Uuid>,      // source_material_id
    Option<i64>,             // source_material_offset_start
    Option<i64>,             // source_material_offset_end
) {
    match &event.provenance {
        Some(Provenance::Events(ids)) => {
            let uuids = ids.iter().map(|id| ulid_to_uuid(*id.as_ulid())).collect();
            (Some(uuids), None, None, None)
        }
        Some(Provenance::Material {
            id,
            offset_start,
            offset_end,
        }) => (
            None,
            Some(ulid_to_uuid(*id.as_ulid())),
            *offset_start,
            *offset_end,
        ),
        None => (None, None, None, None),
    }
}

/// Event payload schema record
#[derive(Debug, FromRow)]
pub struct EventPayloadSchema {
    pub id: Id<EventPayloadSchema>,
    pub schema_name: SchemaName,
    pub schema_version: SchemaVersion,
    pub schema_content: JsonValue,
    pub is_active: bool,
    pub event_types: Vec<String>,
    pub description: Option<String>,
    pub examples: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deprecated_at: Option<DateTime<Utc>>,
    pub deprecation_reason: Option<String>,
}

/// New schema input structure
#[derive(Debug)]
pub struct NewSchema {
    pub schema_name: SchemaName,
    pub schema_version: SchemaVersion,
    pub schema_content: JsonValue,
    pub is_active: bool,
    pub event_types: Vec<String>,
    pub description: Option<String>,
    pub examples: Option<JsonValue>,
}

/// Event annotation record
#[derive(Debug, FromRow)]
pub struct EventAnnotation {
    pub id: Id<EventAnnotation>,
    pub event_id: Id<Event>,
    pub annotation_type: String,
    pub content: String,
    pub metadata: JsonValue,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Invalid payload event record
#[derive(Debug)]
pub struct InvalidPayloadEvent {
    pub event_id: Id<Event>,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: DateTime<Utc>,
    pub payload: JsonValue,
}

/// Batch violation record
#[derive(Debug, FromRow)]
pub struct BatchViolation {
    pub event_id: Option<Id<Event>>,
    pub prev_event_id: Option<Id<Event>>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub prev_ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub row_num: Option<i64>,
}

/// Suspicious event record  
#[derive(Debug, FromRow)]
pub struct SuspiciousEvent {
    pub event_id: Id<Event>,
    pub source: String,
    pub event_type: String,
    pub payload: JsonValue,
    pub payload_type: Option<String>,
    pub payload_size: Option<i32>,
}

/// Invalid timestamp record
#[derive(Debug)]
pub struct InvalidTimestamp {
    pub event_id: Id<Event>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub ts_ingest: DateTime<Utc>,
}

/// Command count for analytics
#[derive(Debug)]
pub struct CommandCount {
    pub command: String,
    pub count: i64,
}

/// Source activity statistics
#[derive(Debug)]
pub struct SourceActivity {
    pub source: String,
    pub event_count: i64,
    pub first_event: Option<DateTime<Utc>>,
    pub last_event: Option<DateTime<Utc>>,
}

/// Event type count
#[derive(Debug)]
pub struct EventTypeCount {
    pub event_type: String,
    pub count: i64,
}

impl<'a> EventRepository<'a> {
    pub async fn insert(&self, mut event: Event) -> DbResult<Event> {
        let id = event.id.get_or_insert_with(Id::<Event>::new).clone();

        // Extract provenance into separate fields for database
        let (
            source_event_ids,
            source_material_id,
            source_material_offset_start,
            source_material_offset_end,
        ) = extract_provenance(&event);

        let associated_blob_ids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| ulid_to_uuid(*id)).collect::<Vec<_>>());

        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                event_id, source, event_type, host, payload,
                ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, source_material_offset_start, source_material_offset_end,
                anchor_byte, associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            ) VALUES (
                $1::uuid, $2, $3, $4, $5,
                $6, $7, $8::uuid, $9::uuid[],
                $10::uuid, $11, $12,
                $13, $14::uuid[], $15, $16, $17
            )
            RETURNING 
                event_id::uuid as "event_id!",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as payload_schema_id,
                payload as "payload!",
                source_event_ids::uuid[] as source_event_ids,
                source_material_id::uuid as source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids::uuid[] as associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            "#,
            id.to_uuid(),
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            event.ts_orig,
            event.ingestor_version,
            event.payload_schema_id.map(ulid_to_uuid),
            source_event_ids.as_deref(),
            source_material_id,
            source_material_offset_start,
            source_material_offset_end,
            event.anchor_byte,
            associated_blob_ids.as_deref(),
            None::<String>, // payload_schema_name
            None::<String>, // payload_schema_version
            None::<i32>     // processor_manifest_id
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "insert event"))?;

        Ok(record.to_event())
    }

    pub async fn get_by_id(&self, id: Id<Event>) -> DbResult<Option<Event>> {
        let record = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                event_id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            FROM core.events 
            WHERE event_id = $1
            "#,
        )
        .bind(ulid_to_uuid(*id.as_ulid()))
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get event by id"))?;

        Ok(record.map(|r| r.to_event()))
    }

    pub async fn count_all(&self) -> DbResult<i64> {
        let result = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "count all events"))?;

        Ok(result.unwrap_or(0))
    }

    pub async fn get_recent(&self, limit: i64) -> DbResult<Vec<Event>> {
        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                event_id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            FROM core.events 
            ORDER BY ts_ingest DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent events"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    pub async fn get_by_source(
        &self,
        source: &EventSource,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<Event>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                event_id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            FROM core.events 
            WHERE source = $1
            ORDER BY ts_ingest DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(source.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by source"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    pub async fn get_by_event_type(
        &self,
        event_type: &EventType,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<Event>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                event_id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            FROM core.events 
            WHERE event_type = $1
            ORDER BY ts_ingest DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(event_type.as_str())
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by type"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    pub async fn count_by_source(&self, source: &EventSource) -> DbResult<i64> {
        let result = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.events WHERE source = $1",
            source.as_str()
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by source"))?;

        Ok(result.unwrap_or(0))
    }

    pub async fn count_by_event_type(&self, event_type: &EventType) -> DbResult<i64> {
        let result = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.events WHERE event_type = $1",
            event_type.as_str()
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by type"))?;

        Ok(result.unwrap_or(0))
    }

    pub async fn get_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<Event>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                event_id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            FROM core.events 
            WHERE ts_ingest >= $1 AND ts_ingest <= $2
            ORDER BY ts_ingest DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by time range"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    pub async fn count_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<i64> {
        let result = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.events WHERE ts_ingest >= $1 AND ts_ingest <= $2",
            start,
            end
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by time range"))?;

        Ok(result.unwrap_or(0))
    }

    pub async fn count_by_source_and_time_range(
        &self,
        source: &EventSource,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<i64> {
        let result = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.events WHERE source = $1 AND ts_ingest >= $2 AND ts_ingest <= $3",
            source.as_str(),
            start,
            end
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by source and time range"))?;

        Ok(result.unwrap_or(0))
    }

    pub async fn get_events_by_type_and_time_range(
        &self,
        event_type: &EventType,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
    ) -> DbResult<Vec<Event>> {
        let limit = limit.unwrap_or(100);

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                event_id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            FROM core.events 
            WHERE event_type = $1 AND ts_ingest >= $2 AND ts_ingest <= $3
            ORDER BY ts_ingest DESC
            LIMIT $4
            "#,
        )
        .bind(event_type.as_str())
        .bind(start)
        .bind(end)
        .bind(limit)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by type and time range"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    pub async fn get_process_heartbeats(
        &self,
        source: &EventSource,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<Vec<Event>> {
        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                event_id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
                source_event_ids,
                source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            FROM core.events 
            WHERE source = $1 
              AND event_type = 'process.heartbeat'
              AND ts_ingest >= $2 
              AND ts_ingest <= $3
            ORDER BY ts_ingest ASC
            "#,
        )
        .bind(source.as_str())
        .bind(start)
        .bind(end)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get process heartbeats"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    pub async fn search(&self, filters: EventSearchFilters) -> DbResult<Vec<Event>> {
        use crate::schema::Events;
        use sea_query::{Alias, Expr, PostgresQueryBuilder, Query};

        let limit = filters.limit.unwrap_or(100) as u64;
        let offset = filters.offset.unwrap_or(0) as u64;

        // Build dynamic query with SeaQuery
        let mut query = Query::select()
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::EVENT_ID),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::SOURCE),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::EVENT_TYPE),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::TS_INGEST),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::TS_ORIG),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::HOST),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::INGESTOR_VERSION),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::PAYLOAD_SCHEMA_ID),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::PAYLOAD),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::SOURCE_EVENT_IDS),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::SOURCE_MATERIAL_ID),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::SOURCE_MATERIAL_OFFSET_START),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::SOURCE_MATERIAL_OFFSET_END),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::ANCHOR_BYTE),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::ASSOCIATED_BLOB_IDS),
            ))
            .from((Alias::new(Events::SCHEMA), Alias::new(Events::TABLE)))
            .order_by(
                (
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::TS_INGEST),
                ),
                sea_query::Order::Desc,
            )
            .limit(limit)
            .offset(offset)
            .to_owned();

        // Add dynamic filters
        if let Some(source) = &filters.source {
            query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::SOURCE),
                ))
                .eq(source.as_str()),
            );
        }

        if let Some(event_type) = &filters.event_type {
            query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::EVENT_TYPE),
                ))
                .eq(event_type.as_str()),
            );
        }

        if let Some(host) = &filters.host {
            query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::HOST),
                ))
                .eq(host.as_str()),
            );
        }

        if let Some(after) = &filters.after {
            query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::TS_INGEST),
                ))
                .gte(after.clone()),
            );
        }

        if let Some(before) = &filters.before {
            query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::TS_INGEST),
                ))
                .lte(before.clone()),
            );
        }

        // TODO: Add payload_contains filter using JSONB operators

        let (sql, _values) = query.build(PostgresQueryBuilder);

        // Use the dynamic query string with renamed columns
        let records = sqlx::query_as::<_, EventRecord>(&sql)
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "search events"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    pub async fn time_series_aggregate(
        &self,
        interval: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<Vec<TimeBucketResult>> {
        // Use SeaQuery for dynamic query building with proper escaping
        use crate::schema::Events;
        use sea_query::{Alias, Expr, Func, PostgresQueryBuilder, Query};

        let query = Query::select()
            .expr_as(
                Func::cust(Alias::new("time_bucket"))
                    .arg(Expr::val(interval))
                    .arg(Expr::col((
                        Alias::new(Events::SCHEMA),
                        Alias::new(Events::TABLE),
                        Alias::new(Events::TS_INGEST),
                    ))),
                Alias::new("bucket"),
            )
            .expr_as(
                Func::count(Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::EVENT_ID),
                ))),
                Alias::new("count"),
            )
            .from((Alias::new(Events::SCHEMA), Alias::new(Events::TABLE)))
            .and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::TS_INGEST),
                ))
                .gte(start),
            )
            .and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::TS_INGEST),
                ))
                .lte(end),
            )
            .group_by_col(Alias::new("bucket"))
            .order_by(Alias::new("bucket"), sea_query::Order::Asc)
            .build(PostgresQueryBuilder);

        let (sql, _values) = query;

        sqlx::query_as(&sql)
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "time series aggregate"))
    }

    pub async fn insert_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut event: Event,
    ) -> DbResult<Event> {
        let id = event.id.get_or_insert_with(Id::<Event>::new).clone();

        // Extract provenance into separate fields for database
        let (
            source_event_ids,
            source_material_id,
            source_material_offset_start,
            source_material_offset_end,
        ) = extract_provenance(&event);

        let associated_blob_ids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| ulid_to_uuid(*id)).collect::<Vec<_>>());

        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                event_id, source, event_type, host, payload,
                ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, source_material_offset_start, source_material_offset_end,
                anchor_byte, associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            ) VALUES (
                $1::uuid, $2, $3, $4, $5,
                $6, $7, $8::uuid, $9::uuid[],
                $10::uuid, $11, $12,
                $13, $14::uuid[], $15, $16, $17
            )
            RETURNING 
                event_id::uuid as "event_id!",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as payload_schema_id,
                payload as "payload!",
                source_event_ids::uuid[] as source_event_ids,
                source_material_id::uuid as source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids::uuid[] as associated_blob_ids,
                payload_schema_name,
                payload_schema_version,
                processor_manifest_id
            "#,
            id.to_uuid(),
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            event.ts_orig,
            event.ingestor_version,
            event.payload_schema_id.map(ulid_to_uuid),
            source_event_ids.as_deref(),
            source_material_id,
            source_material_offset_start,
            source_material_offset_end,
            event.anchor_byte,
            associated_blob_ids.as_deref(),
            None::<String>, // payload_schema_name
            None::<String>, // payload_schema_version
            None::<i32>     // processor_manifest_id
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error(e, "insert event with tx"))?;

        Ok(record.to_event())
    }

    pub async fn insert_batch(&self, events: Vec<Event>) -> DbResult<Vec<Event>> {
        // For batch inserts, we'll use a transaction and insert one by one
        // A more efficient implementation would use UNNEST or COPY
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin transaction for batch insert"))?;

        let mut results = Vec::with_capacity(events.len());

        for event in events {
            let result = self.insert_with_tx(&mut tx, event).await?;
            results.push(result);
        }

        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit batch insert"))?;

        Ok(results)
    }

    // ===== Schema Management Methods =====

    /// Register a new event payload schema
    pub async fn register_schema(&self, schema: NewSchema) -> DbResult<EventPayloadSchema> {
        let id = Id::<EventPayloadSchema>::new();

        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas (
                id, schema_name, schema_version, schema_content,
                is_active, event_types, description, examples
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                id as "id: Id<EventPayloadSchema>",
                schema_name as "schema_name!: SchemaName",
                schema_version as "schema_version!: SchemaVersion",
                schema_content as "schema_content!",
                is_active as "is_active!",
                event_types as "event_types!",
                description,
                examples,
                created_at as "created_at!",
                updated_at as "updated_at!",
                deprecated_at,
                deprecation_reason
            "#,
            *id.as_ulid() as _,
            schema.schema_name.as_str(),
            schema.schema_version.as_str(),
            schema.schema_content,
            schema.is_active,
            &schema
                .event_types
                .iter()
                .map(|et| et.as_str().to_string())
                .collect::<Vec<_>>()[..],
            schema.description,
            schema.examples
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "register schema"))
    }

    /// Get schema by ID
    pub async fn get_schema_by_id(
        &self,
        id: Id<EventPayloadSchema>,
    ) -> DbResult<Option<EventPayloadSchema>> {
        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            SELECT 
                id as "id: Id<EventPayloadSchema>",
                schema_name as "schema_name!: SchemaName",
                schema_version as "schema_version!: SchemaVersion",
                schema_content as "schema_content!",
                is_active as "is_active!",
                event_types as "event_types!",
                description,
                examples,
                created_at as "created_at!",
                updated_at as "updated_at!",
                deprecated_at,
                deprecation_reason
            FROM sinex_schemas.event_payload_schemas 
            WHERE id = $1
            "#,
            *id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema by id"))
    }

    /// Get active schema for a specific event type
    pub async fn get_schema_for_event_type(
        &self,
        event_type: &EventType,
    ) -> DbResult<Option<EventPayloadSchema>> {
        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            SELECT 
                id as "id: Id<EventPayloadSchema>",
                schema_name as "schema_name!: SchemaName",
                schema_version as "schema_version!: SchemaVersion",
                schema_content as "schema_content!",
                is_active as "is_active!",
                event_types as "event_types!",
                description,
                examples,
                created_at as "created_at!",
                updated_at as "updated_at!",
                deprecated_at,
                deprecation_reason
            FROM sinex_schemas.event_payload_schemas 
            WHERE $1 = ANY(event_types) 
              AND is_active = true
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            event_type.as_str()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema for event type"))
    }

    /// Get schema by name and version
    pub async fn get_schema_by_name_and_version(
        &self,
        schema_name: &SchemaName,
        schema_version: &SchemaVersion,
    ) -> DbResult<Option<EventPayloadSchema>> {
        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            SELECT 
                id as "id: Id<EventPayloadSchema>",
                schema_name as "schema_name!: SchemaName",
                schema_version as "schema_version!: SchemaVersion",
                schema_content as "schema_content!",
                is_active as "is_active!",
                event_types as "event_types!",
                description,
                examples,
                created_at as "created_at!",
                updated_at as "updated_at!",
                deprecated_at,
                deprecation_reason
            FROM sinex_schemas.event_payload_schemas 
            WHERE schema_name = $1 
              AND schema_version = $2
            "#,
            schema_name.as_str(),
            schema_version.as_str()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema by name and version"))
    }

    /// Set schema active status
    pub async fn set_schema_active_status(
        &self,
        id: Id<EventPayloadSchema>,
        is_active: bool,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            "UPDATE sinex_schemas.event_payload_schemas SET is_active = $2, updated_at = NOW() WHERE id = $1",
            *id.as_ulid() as _,
            is_active
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "set schema active status"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Deprecate a schema with reason
    pub async fn deprecate_schema(
        &self,
        id: Id<EventPayloadSchema>,
        deprecation_reason: &str,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE sinex_schemas.event_payload_schemas 
            SET 
                is_active = false,
                deprecated_at = NOW(),
                deprecation_reason = $2,
                updated_at = NOW()
            WHERE id = $1
            "#,
            *id.as_ulid() as _,
            deprecation_reason
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "deprecate schema"))?;

        Ok(result.rows_affected() > 0)
    }

    /// List schemas with optional filters
    pub async fn list_schemas(
        &self,
        schema_name: Option<&SchemaName>,
        event_type: Option<&EventType>,
        is_active: Option<bool>,
        limit: Option<i64>,
    ) -> DbResult<Vec<EventPayloadSchema>> {
        let limit = limit.unwrap_or(100);

        // For simplicity, using a basic query. A more sophisticated implementation
        // would build the query dynamically based on which filters are provided
        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            SELECT 
                id as "id: Id<EventPayloadSchema>",
                schema_name as "schema_name!: SchemaName",
                schema_version as "schema_version!: SchemaVersion",
                schema_content as "schema_content!",
                is_active as "is_active!",
                event_types as "event_types!",
                description,
                examples,
                created_at as "created_at!",
                updated_at as "updated_at!",
                deprecated_at,
                deprecation_reason
            FROM sinex_schemas.event_payload_schemas 
            WHERE 
                ($2::text IS NULL OR schema_name = $2) AND
                ($3::text IS NULL OR $3 = ANY(event_types)) AND
                ($4::boolean IS NULL OR is_active = $4)
            ORDER BY created_at DESC
            LIMIT $1
            "#,
            limit,
            schema_name.map(|n| n.as_str()),
            event_type.map(|t| t.as_str()),
            is_active
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list schemas"))
    }

    /// Get all versions of a schema
    pub async fn get_schema_versions(
        &self,
        schema_name: &SchemaName,
    ) -> DbResult<Vec<EventPayloadSchema>> {
        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            SELECT 
                id as "id: Id<EventPayloadSchema>",
                schema_name as "schema_name!: SchemaName",
                schema_version as "schema_version!: SchemaVersion",
                schema_content as "schema_content!",
                is_active as "is_active!",
                event_types as "event_types!",
                description,
                examples,
                created_at as "created_at!",
                updated_at as "updated_at!",
                deprecated_at,
                deprecation_reason
            FROM sinex_schemas.event_payload_schemas 
            WHERE schema_name = $1
            ORDER BY created_at DESC
            "#,
            schema_name.as_str()
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema versions"))
    }

    /// Register schema within a transaction
    pub async fn register_schema_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        schema: NewSchema,
    ) -> DbResult<EventPayloadSchema> {
        let id = Id::<EventPayloadSchema>::new();

        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas (
                id, schema_name, schema_version, schema_content,
                is_active, event_types, description, examples
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                id as "id: Id<EventPayloadSchema>",
                schema_name as "schema_name!: SchemaName",
                schema_version as "schema_version!: SchemaVersion",
                schema_content as "schema_content!",
                is_active as "is_active!",
                event_types as "event_types!",
                description,
                examples,
                created_at as "created_at!",
                updated_at as "updated_at!",
                deprecated_at,
                deprecation_reason
            "#,
            *id.as_ulid() as _,
            schema.schema_name.as_str(),
            schema.schema_version.as_str(),
            schema.schema_content,
            schema.is_active,
            &schema
                .event_types
                .iter()
                .map(|et| et.as_str().to_string())
                .collect::<Vec<_>>()[..],
            schema.description,
            schema.examples
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error(e, "register schema with tx"))
    }

    // ========== Event Annotations ==========

    /// Add an annotation to an event
    pub async fn add_annotation(
        &self,
        event_id: Id<Event>,
        annotation_type: &str,
        content: &str,
        metadata: serde_json::Value,
        created_by: &str,
    ) -> DbResult<EventAnnotation> {
        let id = Id::<EventAnnotation>::new();

        sqlx::query_as!(
            EventAnnotation,
            r#"
            INSERT INTO core.event_annotations (
                id, event_id, annotation_type, content, metadata, created_by
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            RETURNING 
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            "#,
            *id.as_ulid() as _,
            *event_id.as_ulid() as _,
            annotation_type,
            content,
            metadata,
            created_by
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "add annotation"))
    }

    /// Get annotations for an event
    pub async fn get_annotations(&self, id: Id<Event>) -> DbResult<Vec<EventAnnotation>> {
        sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT 
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            FROM core.event_annotations
            WHERE event_id = $1
            ORDER BY created_at DESC
            "#,
            *id.as_ulid() as _
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get annotations"))
    }

    /// Get annotations by type
    pub async fn get_annotations_by_type(
        &self,
        annotation_type: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<EventAnnotation>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT 
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            FROM core.event_annotations
            WHERE annotation_type = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
            annotation_type,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get annotations by type"))
    }

    /// Update annotation content
    pub async fn update_annotation(
        &self,
        annotation_id: Id<EventAnnotation>,
        content: &str,
    ) -> DbResult<EventAnnotation> {
        sqlx::query_as!(
            EventAnnotation,
            r#"
            UPDATE core.event_annotations
            SET content = $2, updated_at = CURRENT_TIMESTAMP
            WHERE id = $1
            RETURNING 
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            "#,
            *annotation_id.as_ulid() as _,
            content
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "update annotation"))
    }

    /// Delete an annotation
    pub async fn delete_annotation(&self, id: Id<EventAnnotation>) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            DELETE FROM core.event_annotations
            WHERE id = $1
            "#,
            *id.as_ulid() as _
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete annotation"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Search annotations by content
    pub async fn search_annotations(
        &self,
        query: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<EventAnnotation>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT 
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            FROM core.event_annotations
            WHERE content ILIKE $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
            format!("%{}%", query),
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "search annotations"))
    }

    // ========== Data Quality Checks (replacing validation queries) ==========

    /// Find events with null or empty payloads
    pub async fn find_invalid_payloads(&self, limit: i64) -> DbResult<Vec<InvalidPayloadEvent>> {
        sqlx::query!(
            r#"
            SELECT
                event_id::uuid as "event_id!",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                payload as "payload!"
            FROM core.events
            WHERE payload IS NULL OR payload = 'null'::jsonb OR payload = '{}'::jsonb
            ORDER BY ts_ingest DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find invalid payloads"))
        .map(|rows| {
            rows.into_iter()
                .map(|row| InvalidPayloadEvent {
                    event_id: Id::<Event>::from_uuid(row.event_id),
                    source: row.source,
                    event_type: row.event_type,
                    ts_ingest: row.ts_ingest,
                    payload: row.payload,
                })
                .collect()
        })
    }

    /// Find events with timestamp regressions
    pub async fn find_timestamp_regressions(
        &self,
        limit: i64,
    ) -> DbResult<Vec<(Id<Event>, Id<Event>, DateTime<Utc>, DateTime<Utc>)>> {
        let rows = sqlx::query!(
            r#"
            WITH ordered_events AS (
                SELECT 
                    event_id,
                    ts_orig,
                    LAG(event_id) OVER (PARTITION BY source ORDER BY event_id) as prev_id,
                    LAG(ts_orig) OVER (PARTITION BY source ORDER BY event_id) as prev_ts
                FROM core.events
                WHERE ts_orig IS NOT NULL
            )
            SELECT 
                event_id::uuid as "event_id!",
                prev_id::uuid as "prev_id!",
                ts_orig as "ts_orig!",
                prev_ts as "prev_ts!"
            FROM ordered_events
            WHERE prev_ts IS NOT NULL AND ts_orig < prev_ts
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find timestamp regressions"))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    Id::<Event>::from_uuid(r.event_id),
                    Id::<Event>::from_uuid(r.prev_id),
                    r.ts_orig,
                    r.prev_ts,
                )
            })
            .collect())
    }

    // ========== Data Integrity Checks (from old integrity module) ==========

    /// Find batch monotonicity violations
    pub async fn find_batch_violations(
        &self,
        days_back: i32,
        max_violations: i64,
    ) -> DbResult<Vec<BatchViolation>> {
        let rows = sqlx::query_as!(
            BatchViolation,
            r#"
            WITH event_batches AS (
                SELECT 
                    event_id,
                    ts_orig,
                    source,
                    ROW_NUMBER() OVER (ORDER BY event_id) as row_num,
                    LAG(event_id) OVER (ORDER BY event_id) as prev_event_id,
                    LAG(ts_orig) OVER (ORDER BY event_id) as prev_ts_orig
                FROM core.events
                WHERE ts_ingest > NOW() - INTERVAL '1 day' * $1
                ORDER BY event_id DESC
                LIMIT 10000
            )
            SELECT 
                event_id as "event_id?: Id<Event>",
                prev_event_id as "prev_event_id?: Id<Event>",
                ts_orig,
                prev_ts_orig,
                source,
                row_num
            FROM event_batches
            WHERE prev_event_id IS NOT NULL
              AND (ts_orig < prev_ts_orig OR event_id < prev_event_id)
            LIMIT $2
            "#,
            days_back as f64,
            max_violations
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find batch violations"))?;

        Ok(rows)
    }

    /// Find events with suspicious payloads
    pub async fn find_suspicious_events(
        &self,
        days_back: i32,
        size_threshold: i32,
    ) -> DbResult<Vec<SuspiciousEvent>> {
        let rows = sqlx::query_as!(
            SuspiciousEvent,
            r#"
            SELECT 
                event_id as "event_id: Id<Event>",
                source as "source!",
                event_type as "event_type!",
                payload as "payload!",
                jsonb_typeof(payload) as payload_type,
                pg_column_size(payload) as payload_size
            FROM core.events
            WHERE ts_ingest > NOW() - INTERVAL '1 day' * $1
              AND (
                jsonb_typeof(payload) NOT IN ('object', 'array')
                OR pg_column_size(payload) > $2
                OR payload @> '{}'::jsonb
                OR payload = 'null'::jsonb
              )
            ORDER BY ts_ingest DESC
            LIMIT 100
            "#,
            days_back as f64,
            size_threshold
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find suspicious events"))?;

        Ok(rows)
    }

    /// Find events with invalid timestamps (too far in future/past)
    pub async fn find_invalid_timestamps(&self, limit: i64) -> DbResult<Vec<InvalidTimestamp>> {
        let rows = sqlx::query_as!(
            InvalidTimestamp,
            r#"
            SELECT 
                event_id as "event_id: Id<Event>", 
                ts_orig, 
                ts_ingest as "ts_ingest!"
            FROM core.events
            WHERE ts_orig > NOW() + INTERVAL '1 hour'
               OR ts_orig < '2020-01-01'::timestamptz
               OR ts_ingest > NOW() + INTERVAL '1 hour'
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find invalid timestamps"))?;

        Ok(rows)
    }

    // ========== Test Support Operations ==========

    /// Insert a test event
    pub async fn insert_test_event(
        &self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> DbResult<Event> {
        let event = Event::builder()
            .source(EventSource::new(source.to_string()))
            .event_type(EventType::new(event_type.to_string()))
            .host(HostName::new("test-host".to_string()))
            .payload(payload)
            .build();

        self.insert(event).await
    }

    /// Get a test event by ID
    pub async fn get_test_event(&self, id: Id<Event>) -> DbResult<Option<Event>> {
        self.get_by_id(id).await
    }

    /// Update test event payload
    pub async fn update_test_event(
        &self,
        id: Id<Event>,
        payload: serde_json::Value,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE core.events
            SET payload = $2
            WHERE event_id = $1
            "#,
            *id.as_ulid() as _,
            payload
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update test event"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Delete a test event
    pub async fn delete_test_event(&self, id: Id<Event>) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            DELETE FROM core.events
            WHERE event_id = $1
            "#,
            *id.as_ulid() as _
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete test event"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Cleanup test events by source and type
    pub async fn cleanup_test_events(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> DbResult<u64> {
        let result = sqlx::query!(
            r#"
            DELETE FROM core.events
            WHERE source = $1 AND event_type = $2
            "#,
            source.as_ref(),
            event_type.as_ref()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "cleanup test events"))?;

        Ok(result.rows_affected())
    }

    /// Cleanup test events by source only
    pub async fn cleanup_test_events_by_source(&self, source: &EventSource) -> DbResult<u64> {
        let result = sqlx::query!(
            r#"
            DELETE FROM core.events
            WHERE source = $1
            "#,
            source.as_ref()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "cleanup test events by source"))?;

        Ok(result.rows_affected())
    }

    // ========== Analytics Queries ==========

    /// Get top terminal commands
    pub async fn top_commands(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: i64,
    ) -> DbResult<Vec<CommandCount>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                payload->>'command' as command,
                COUNT(*) as count
            FROM core.events
            WHERE event_type = 'terminal.command'
              AND ts_orig >= $1
              AND ts_orig < $2
              AND payload->>'command' IS NOT NULL
            GROUP BY payload->>'command'
            ORDER BY count DESC
            LIMIT $3
            "#,
            start,
            end,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "top commands"))?;

        Ok(rows
            .into_iter()
            .map(|r| CommandCount {
                command: r.command.unwrap_or_default(),
                count: r.count.unwrap_or(0),
            })
            .collect())
    }

    /// Get top commands all time
    pub async fn top_commands_all_time(&self, limit: i64) -> DbResult<Vec<CommandCount>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                payload->>'command' as command,
                COUNT(*) as count
            FROM core.events
            WHERE event_type = 'terminal.command'
              AND payload->>'command' IS NOT NULL
            GROUP BY payload->>'command'
            ORDER BY count DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "top commands all time"))?;

        Ok(rows
            .into_iter()
            .map(|r| CommandCount {
                command: r.command.unwrap_or_default(),
                count: r.count.unwrap_or(0),
            })
            .collect())
    }

    /// Get source activity statistics
    pub async fn get_source_activity(&self, since: DateTime<Utc>) -> DbResult<Vec<SourceActivity>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                source,
                COUNT(*) as event_count,
                MIN(ts_orig) as first_event,
                MAX(ts_orig) as last_event
            FROM core.events
            WHERE ts_orig >= $1
            GROUP BY source
            ORDER BY event_count DESC
            "#,
            since
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get source activity"))?;

        Ok(rows
            .into_iter()
            .map(|r| SourceActivity {
                source: r.source,
                event_count: r.event_count.unwrap_or(0),
                first_event: r.first_event,
                last_event: r.last_event,
            })
            .collect())
    }

    /// Count events by type in time range
    pub async fn count_by_type_in_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<Vec<EventTypeCount>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                event_type,
                COUNT(*) as count
            FROM core.events
            WHERE ts_orig >= $1 AND ts_orig < $2
            GROUP BY event_type
            ORDER BY count DESC
            "#,
            start,
            end
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "count by type in range"))?;

        Ok(rows
            .into_iter()
            .map(|r| EventTypeCount {
                event_type: r.event_type,
                count: r.count.unwrap_or(0),
            })
            .collect())
    }

    /// Count events by type all time
    pub async fn count_by_type_all_time(&self) -> DbResult<Vec<EventTypeCount>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                event_type,
                COUNT(*) as count
            FROM core.events
            GROUP BY event_type
            ORDER BY count DESC
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "count by type all time"))?;

        Ok(rows
            .into_iter()
            .map(|r| EventTypeCount {
                event_type: r.event_type,
                count: r.count.unwrap_or(0),
            })
            .collect())
    }

    /// Get events over time using TimescaleDB time buckets
    ///
    /// This uses raw SQL for TimescaleDB time_bucket function
    pub async fn get_events_over_time(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        interval: sqlx::postgres::types::PgInterval,
    ) -> DbResult<Vec<TimeBucketResult>> {
        let rows = sqlx::query_as!(
            TimeBucketResult,
            r#"
            SELECT 
                time_bucket($1::interval, ts_ingest) as "bucket!",
                COUNT(*) as "count!"
            FROM core.events
            WHERE ts_ingest >= $2 AND ts_ingest <= $3
            GROUP BY time_bucket($1::interval, ts_ingest)
            ORDER BY time_bucket($1::interval, ts_ingest) ASC
            "#,
            interval,
            start_time,
            end_time
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events over time"))?;

        Ok(rows)
    }

    /// Get activity heatmap using TimescaleDB time buckets
    ///
    /// This uses raw SQL for TimescaleDB time_bucket function
    pub async fn get_activity_heatmap(
        &self,
        interval: sqlx::postgres::types::PgInterval,
        limit: i64,
    ) -> DbResult<Vec<TimeBucketResult>> {
        let rows = sqlx::query_as!(
            TimeBucketResult,
            r#"
            SELECT 
                time_bucket($1::interval, ts_ingest) as "bucket!",
                COUNT(*) as "count!"
            FROM core.events
            GROUP BY time_bucket($1::interval, ts_ingest)
            ORDER BY COUNT(*) DESC
            LIMIT $2
            "#,
            interval,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get activity heatmap"))?;

        Ok(rows)
    }

    /// Delete all events from a specific source (useful for test cleanup)
    pub async fn delete_by_source(&self, source: &str) -> DbResult<u64> {
        let result = sqlx::query!(
            r#"
            DELETE FROM core.events
            WHERE source = $1
            "#,
            source
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete by source"))?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use serde_json::json;
    use sinex_test_utils::prelude::*;
    use sinex_types::domain::{EventSource, EventType, HostName};

    #[sinex_test]
    async fn test_event_record_insert(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let pool = &ctx.pool;

        // Create an event
        let event = crate::models::Event::builder()
            .source(EventSource::new("test.source"))
            .event_type(EventType::new("test.event"))
            .host(HostName::new("test-host"))
            .payload(json!({"test": "data"}))
            .build();

        // Insert using repository pattern with EventRecord
        let inserted = pool.events().insert(event).await?;

        // Verify the event was inserted with correct data
        assert_eq!(inserted.source.as_str(), "test.source");
        assert_eq!(inserted.event_type.as_str(), "test.event");
        assert_eq!(inserted.host.as_str(), "test-host");
        assert_eq!(inserted.payload["test"], "data");

        // ts_ingest should be set by database
        assert!(!inserted.ts_ingest.timestamp().is_negative());

        Ok(())
    }

    #[sinex_test]
    async fn test_event_record_with_provenance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let pool = &ctx.pool;

        // Create a source event first
        let source_event = crate::models::Event::builder()
            .source(EventSource::new("test.source"))
            .event_type(EventType::new("source.event"))
            .host(HostName::new("test-host"))
            .payload(json!({"original": true}))
            .build();

        let source = pool.events().insert(source_event).await?;
        let source_id = source.id.unwrap();

        // Create derived event with provenance
        let derived_event = crate::models::Event::builder()
            .source(EventSource::new("test.processor"))
            .event_type(EventType::new("derived.event"))
            .host(HostName::new("test-host"))
            .payload(json!({"derived": true}))
            .provenance(crate::models::Provenance::Events(vec![source_id.clone()]))
            .build();

        let inserted = pool.events().insert(derived_event).await?;

        // Verify provenance was preserved through EventRecord
        match inserted.provenance {
            Some(crate::models::Provenance::Events(ids)) => {
                assert_eq!(ids.len(), 1);
                assert_eq!(ids[0], source_id);
            }
            _ => panic!("Expected Events provenance"),
        }

        Ok(())
    }
}
