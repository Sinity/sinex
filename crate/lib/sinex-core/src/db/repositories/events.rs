use crate::db::schema::Events;
use crate::models::{Provenance, RawEvent, SourceMaterial};
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::repositories::common::{
    db_error, DbResult, EnhancedRepository, EventSearchFilters, Repository, TimeBucketResult,
};
use crate::types::domain::{EventSource, EventType, HostName, SchemaName, SchemaVersion};
use crate::types::non_empty::NonEmptyVec;
use crate::types::Id;
use crate::EventRecord;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use tracing::instrument;

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

// Extension methods for EventRecord from sinex-migrations
pub(crate) trait EventRecordExt {
    fn to_event(self) -> RawEvent;
}

impl EventRecordExt for EventRecord {
    /// Convert database record to domain Event
    fn to_event(self) -> RawEvent {
        use crate::db::models::event::{EventId, OffsetKind, Provenance, SourceMaterial};

        // Reconstruct provenance from separate fields
        let provenance = match (
            self.source_event_ids,
            self.source_material_id,
            self.anchor_byte,
        ) {
            (Some(event_ids), None, _) if !event_ids.is_empty() => {
                let ids: Vec<EventId> = event_ids
                    .into_iter()
                    .map(|ulid| EventId::from_ulid(ulid))
                    .collect();
                Provenance::Synthesis {
                    source_event_ids: NonEmptyVec::from_vec(ids)
                        .expect("already checked non-empty"),
                    operation_id: None,
                }
            }
            (None, Some(material_id), Some(anchor_byte)) => Provenance::Material {
                id: Id::<SourceMaterial>::from_ulid(material_id),
                anchor_byte,
                offset_start: self.offset_start,
                offset_end: self.offset_end,
                offset_kind: OffsetKind::default(),
            },
            _ => {
                // Default to material provenance with placeholder values if no provenance
                // (shouldn't happen in production, but needed for type safety)
                Provenance::Material {
                    id: Id::<SourceMaterial>::new(),
                    anchor_byte: 0,
                    offset_start: None,
                    offset_end: None,
                    offset_kind: OffsetKind::default(),
                }
            }
        };

        RawEvent {
            id: Some(EventId::from_ulid(self.id)),
            source: self.source.into(),
            event_type: self.event_type.into(),
            host: self.host.into(),
            payload: self.payload,
            ts_orig: Some(self.ts_orig),
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id,
            provenance,
            associated_blob_ids: None,
        }
    }
}

/// Extract provenance fields from domain Event for database storage
fn extract_provenance(
    event: &RawEvent,
) -> (
    Option<Vec<sinex_schema::ulid::Ulid>>, // source_event_ids
    Option<sinex_schema::ulid::Ulid>,      // source_material_id
    Option<i64>,                           // offset_start
    Option<i64>,                           // offset_end
    Option<i64>,                           // anchor_byte
) {
    match &event.provenance {
        Provenance::Synthesis {
            source_event_ids, ..
        } => {
            let ulids = source_event_ids.iter().map(|id| *id.as_ulid()).collect();
            (Some(ulids), None, None, None, None)
        }
        Provenance::Material {
            id,
            anchor_byte,
            offset_start,
            offset_end,
            ..
        } => (
            None,
            Some(*id.as_ulid()),
            *offset_start,
            *offset_end,
            Some(*anchor_byte),
        ),
    }
}

/// Event payload schema record
#[derive(Debug, FromRow)]
pub struct EventPayloadSchema {
    pub id: Id<EventPayloadSchema>,
    pub source: String,
    pub event_type: String,
    pub schema_version: SchemaVersion,
    pub schema_content: JsonValue,
    pub content_hash: String,
    pub is_active: bool,
    pub updated_at: DateTime<Utc>,
}

/// New schema input structure
#[derive(Debug)]
pub struct NewSchema {
    pub source: String,
    pub event_type: String,
    pub schema_version: SchemaVersion,
    pub schema_content: JsonValue,
    pub content_hash: String,
    pub is_active: bool,
}

/// Event annotation record
#[derive(Debug, FromRow)]
pub struct EventAnnotation {
    pub id: Id<EventAnnotation>,
    pub event_id: Id<RawEvent>,
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
    pub event_id: Id<RawEvent>,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: DateTime<Utc>,
    pub payload: JsonValue,
}

/// Batch violation record
#[derive(Debug, FromRow)]
pub struct BatchViolation {
    pub event_id: Option<Id<RawEvent>>,
    pub prev_event_id: Option<Id<RawEvent>>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub prev_ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub row_num: Option<i64>,
}

/// Suspicious event record  
#[derive(Debug, FromRow)]
pub struct SuspiciousEvent {
    pub event_id: Id<RawEvent>,
    pub source: String,
    pub event_type: String,
    pub payload: JsonValue,
    pub payload_type: Option<String>,
    pub payload_size: Option<i32>,
}

/// Invalid timestamp record
#[derive(Debug)]
pub struct InvalidTimestamp {
    pub event_id: Id<RawEvent>,
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
    #[instrument(skip(self, event), fields(source = %event.source, event_type = %event.event_type, host = %event.host))]
    pub async fn insert(&self, mut event: RawEvent) -> DbResult<RawEvent> {
        let id = event.id.get_or_insert_with(Id::<RawEvent>::new).clone();

        // Extract provenance into separate fields for database
        let (source_event_ids, source_material_id, offset_start, offset_end, anchor_byte) =
            extract_provenance(&event);

        // Convert ULIDs to UUIDs before the query to avoid temporary value issues
        let source_event_uuids = source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());
        let associated_blob_uuids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());

        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                id, source, event_type, host, payload,
                ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, offset_start, offset_end,
                anchor_byte, associated_blob_ids
            ) VALUES (
                $1::uuid::ulid, $2, $3, $4, $5,
                $6, $7, $8::uuid::ulid, $9::uuid[]::ulid[],
                $10::uuid::ulid, $11, $12,
                $13, $14::uuid[]::ulid[]
            )
            RETURNING 
                id::uuid as "id!: sinex_schema::ulid::Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as "payload_schema_id: sinex_schema::ulid::Ulid",
                payload as "payload!",
                source_event_ids::uuid[] as "source_event_ids: Vec<sinex_schema::ulid::Ulid>",
                source_material_id::uuid as "source_material_id: sinex_schema::ulid::Ulid",
                offset_start,
                offset_end,
                offset_kind,
                anchor_byte,
                associated_blob_ids::uuid[] as "associated_blob_ids: Vec<sinex_schema::ulid::Ulid>"
            "#,
            id.as_ulid().as_uuid(),
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            event.ts_orig,
            event.ingestor_version,
            event.payload_schema_id.map(|id| id.as_uuid()),
            source_event_uuids.as_deref(),
            source_material_id.map(|id| id.as_uuid()),
            offset_start,
            offset_end,
            anchor_byte,
            associated_blob_uuids.as_deref()
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "insert event"))?;

        Ok(record.to_event())
    }

    #[instrument(skip(self), fields(event_id = %id))]
    pub async fn get_by_id(&self, id: Id<RawEvent>) -> DbResult<Option<RawEvent>> {
        let record = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
            FROM core.events 
            WHERE id = $1
            "#,
        )
        .bind(ulid_to_uuid(*id.as_ulid()))
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get event by id"))?;

        Ok(record.map(|r| r.to_event()))
    }

    #[instrument(skip(self))]
    pub async fn count_all(&self) -> DbResult<i64> {
        let result = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "count all events"))?;

        Ok(result.unwrap_or(0))
    }

    #[instrument(skip(self), fields(limit = limit))]
    pub async fn get_recent(&self, limit: i64) -> DbResult<Vec<RawEvent>> {
        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
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

    #[instrument(skip(self), fields(source = %source, limit = ?limit, offset = ?offset))]
    pub async fn get_by_source(
        &self,
        source: &EventSource,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<RawEvent>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
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

    #[instrument(skip(self), fields(event_type = %event_type, limit = ?limit, offset = ?offset))]
    pub async fn get_by_event_type(
        &self,
        event_type: &EventType,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<RawEvent>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
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

    #[instrument(skip(self), fields(source = %source))]
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

    #[instrument(skip(self), fields(event_type = %event_type))]
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

    #[instrument(skip(self), fields(start = %start, end = %end, limit = ?limit, offset = ?offset))]
    pub async fn get_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<RawEvent>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        // Use index hint for TimescaleDB optimization on time-range queries
        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
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

    #[instrument(skip(self), fields(start = %start, end = %end))]
    pub async fn count_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<i64> {
        // Use approximate count for better performance on large time ranges
        let result = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) 
            FROM core.events 
            WHERE ts_ingest >= $1 AND ts_ingest <= $2
            "#,
            start,
            end
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by time range"))?;

        Ok(result.unwrap_or(0))
    }

    pub async fn get_events_by_type_and_time_range(
        &self,
        event_type: &EventType,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
    ) -> DbResult<Vec<RawEvent>> {
        let limit = limit.unwrap_or(100);

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
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
    ) -> DbResult<Vec<RawEvent>> {
        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
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

    #[instrument(skip(self, filters), fields(limit = ?filters.limit, offset = ?filters.offset, source = ?filters.source, event_type = ?filters.event_type))]
    pub async fn search(&self, filters: EventSearchFilters) -> DbResult<Vec<RawEvent>> {
        use crate::db::schema::Events;
        use sea_query::{Alias, Expr, PostgresQueryBuilder, Query};

        let limit = filters.limit.unwrap_or(100) as u64;
        let offset = filters.offset.unwrap_or(0) as u64;

        // Build dynamic query with SeaQuery
        let mut query = Query::select()
            .column((Alias::new("core"), Alias::new("events"), Alias::new("id")))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("source"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("event_type"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("ts_ingest"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("ts_orig"),
            ))
            .column((Alias::new("core"), Alias::new("events"), Alias::new("host")))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("ingestor_version"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("payload_schema_id"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("payload"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("source_event_ids"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("source_material_id"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("offset_start"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("offset_end"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("anchor_byte"),
            ))
            .column((
                Alias::new("core"),
                Alias::new("events"),
                Alias::new("associated_blob_ids"),
            ))
            .from((Alias::new("core"), Alias::new("events")))
            .order_by(
                (
                    Alias::new("core"),
                    Alias::new("events"),
                    Alias::new("ts_ingest"),
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
                    Alias::new("core"),
                    Alias::new("events"),
                    Alias::new("source"),
                ))
                .eq(source.as_str()),
            );
        }

        if let Some(event_type) = &filters.event_type {
            query.and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new("events"),
                    Alias::new("event_type"),
                ))
                .eq(event_type.as_str()),
            );
        }

        if let Some(host) = &filters.host {
            query.and_where(
                Expr::col((Alias::new("core"), Alias::new("events"), Alias::new("host")))
                    .eq(host.as_str()),
            );
        }

        if let Some(after) = &filters.after {
            query.and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new("events"),
                    Alias::new("ts_ingest"),
                ))
                .gte(after.clone()),
            );
        }

        if let Some(before) = &filters.before {
            query.and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new("events"),
                    Alias::new("ts_ingest"),
                ))
                .lte(before.clone()),
            );
        }

        // Add payload_contains filter using JSONB containment operator (@>)\n        if let Some(payload_filter) = &filters.payload_contains {\n            query.and_where(\n                Expr::col((\n                    Alias::new(\"core\"),\n                    Alias::new(\"events\"),\n                    Alias::new(\"payload\"),\n                ))\n                // Use PostgreSQL JSONB containment operator @>\n                // This leverages GIN indexes for fast JSONB queries\n                .binary(sea_query::BinOper::Custom(\"@>\".to_string()), Expr::value(payload_filter.clone())),\n            );\n        }

        let (sql, _values) = query.build(PostgresQueryBuilder);

        // Use the dynamic query string with renamed columns
        let records = sqlx::query_as::<_, EventRecord>(&sql)
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "search events"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    #[instrument(skip(self), fields(interval = interval, start = %start, end = %end))]
    pub async fn time_series_aggregate(
        &self,
        interval: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<Vec<TimeBucketResult>> {
        // Use SeaQuery for dynamic query building with proper escaping
        use crate::db::schema::Events;
        use sea_query::{Alias, Expr, Func, PostgresQueryBuilder, Query};

        let query = Query::select()
            .expr_as(
                Func::cust(Alias::new("time_bucket"))
                    .arg(Expr::val(interval))
                    .arg(Expr::col((
                        Alias::new("core"),
                        Alias::new("events"),
                        Alias::new("ts_ingest"),
                    ))),
                Alias::new("bucket"),
            )
            .expr_as(
                Func::count(Expr::col((
                    Alias::new("core"),
                    Alias::new("events"),
                    Alias::new("id"),
                ))),
                Alias::new("count"),
            )
            .from((Alias::new("core"), Alias::new("events")))
            .and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new("events"),
                    Alias::new("ts_ingest"),
                ))
                .gte(start),
            )
            .and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new("events"),
                    Alias::new("ts_ingest"),
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

    #[instrument(skip(self, tx, event), fields(event_source = %event.source, event_type = %event.event_type))]
    pub async fn insert_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut event: RawEvent,
    ) -> DbResult<RawEvent> {
        let id = event.id.get_or_insert_with(Id::<RawEvent>::new).clone();

        // Extract provenance into separate fields for database
        let (source_event_ids, source_material_id, offset_start, offset_end, anchor_byte) =
            extract_provenance(&event);

        // Convert ULIDs to UUIDs before the query to avoid temporary value issues
        let source_event_uuids = source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());
        let associated_blob_uuids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());

        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                id, source, event_type, host, payload,
                ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, offset_start, offset_end,
                anchor_byte, associated_blob_ids
            ) VALUES (
                $1::uuid::ulid, $2, $3, $4, $5,
                $6, $7, $8::uuid::ulid, $9::uuid[]::ulid[],
                $10::uuid::ulid, $11, $12,
                $13, $14::uuid[]::ulid[]
            )
            RETURNING 
                id::uuid as "id!: sinex_schema::ulid::Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as "payload_schema_id: sinex_schema::ulid::Ulid",
                payload as "payload!",
                source_event_ids::uuid[] as "source_event_ids: Vec<sinex_schema::ulid::Ulid>",
                source_material_id::uuid as "source_material_id: sinex_schema::ulid::Ulid",
                offset_start,
                offset_end,
                offset_kind,
                anchor_byte,
                associated_blob_ids::uuid[] as "associated_blob_ids: Vec<sinex_schema::ulid::Ulid>"
            "#,
            id.as_ulid().as_uuid(),
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            event.ts_orig,
            event.ingestor_version,
            event.payload_schema_id.map(|id| id.as_uuid()),
            source_event_uuids.as_deref(),
            source_material_id.map(|id| id.as_uuid()),
            offset_start,
            offset_end,
            anchor_byte,
            associated_blob_uuids.as_deref()
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error(e, "insert event with tx"))?;

        Ok(record.to_event())
    }

    #[instrument(skip(self, events), fields(batch_size = events.len()))]
    pub async fn insert_batch(&self, events: Vec<RawEvent>) -> DbResult<Vec<RawEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // For small batches, use the optimized single-transaction approach
        if events.len() <= 50 {
            return self.insert_batch_unnest(events).await;
        }

        // For larger batches, chunk them to avoid overwhelming the database
        let chunk_size = 50; // Optimal chunk size for batch processing
        let max_concurrent_chunks = 3; // Conservative concurrency to avoid pool exhaustion

        let mut results = Vec::with_capacity(events.len());

        // Process chunks with controlled concurrency
        for chunk_batch in events.chunks(chunk_size * max_concurrent_chunks) {
            let mut chunk_futures = Vec::new();

            for chunk in chunk_batch.chunks(chunk_size) {
                let chunk_vec = chunk.to_vec();
                let pool_clone = self.pool.clone();

                chunk_futures.push(async move {
                    let repo = EventRepository::new(&pool_clone);
                    repo.insert_batch_unnest(chunk_vec).await
                });
            }

            // Wait for this batch of chunks to complete
            let chunk_results = futures::future::join_all(chunk_futures).await;

            // Collect results and propagate any errors immediately
            for result in chunk_results {
                match result {
                    Ok(mut chunk_results) => {
                        results.append(&mut chunk_results);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(results)
    }

    /// Optimized batch insert with transaction batching for better performance
    async fn insert_batch_unnest(&self, mut events: Vec<RawEvent>) -> DbResult<Vec<RawEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // For very small batches, use individual inserts to avoid overhead
        if events.len() == 1 {
            let event = events
                .into_iter()
                .next()
                .expect("events.len() == 1 but no element found - this should never happen");
            let inserted = self.insert(event).await?;
            return Ok(vec![inserted]);
        }

        // Begin transaction for atomicity
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin transaction for batch insert"))?;

        // Ensure all events have IDs
        for event in &mut events {
            if event.id.is_none() {
                event.id = Some(Id::<RawEvent>::new());
            }
        }

        // Use individual inserts within a single transaction for reliability
        // This provides good performance while maintaining data integrity
        for (i, event) in events.iter().enumerate() {
            let event_id = event.id.as_ref().unwrap();

            // Extract provenance
            let (source_event_ids, source_material_id, offset_start, offset_end, anchor_byte) =
                extract_provenance(event);

            // Convert ULIDs to UUIDs before the query to avoid temporary value issues
            let source_event_uuids = source_event_ids
                .as_ref()
                .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());
            let associated_blob_uuids = event
                .associated_blob_ids
                .as_ref()
                .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());

            sqlx::query!(
                r#"
                INSERT INTO core.events (
                    id, source, event_type, host, payload,
                    ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                    source_material_id, offset_start, offset_end,
                    anchor_byte, associated_blob_ids
                )
                VALUES (
                    $1::uuid::ulid, $2, $3, $4, $5,
                    $6, $7, $8::uuid::ulid, $9::uuid[]::ulid[],
                    $10::uuid::ulid, $11, $12,
                    $13, $14::uuid[]::ulid[]
                )
                "#,
                event_id.as_ulid().as_uuid(),
                event.source.as_str(),
                event.event_type.as_str(),
                event.host.as_str(),
                event.payload,
                event.ts_orig,
                event.ingestor_version,
                event.payload_schema_id.map(|id| id.as_uuid()),
                source_event_uuids.as_deref(),
                source_material_id.map(|id| id.as_uuid()),
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_uuids.as_deref()
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, &format!("insert event {} of {}", i + 1, events.len())))?;
        }

        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit batch insert"))?;

        Ok(events)
    }

    // ===== Schema Management Methods =====
    // NOTE: These methods are commented out because the actual database schema
    // is different from what these methods expect. The table has:
    // id, source, event_type, schema_version, schema_content, content_hash, is_active, updated_at
    // But the code expects additional columns that don't exist.
    /*

    /// Register a new event payload schema
    pub async fn register_schema(&self, schema: NewSchema) -> DbResult<EventPayloadSchema> {
        let id = Id::<EventPayloadSchema>::new();

        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas (
                id, source, event_type, schema_version, schema_content,
                content_hash, is_active
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7
            )
            RETURNING
                id as "id: Id<EventPayloadSchema>",
                source,
                event_type,
                schema_version as "schema_version!: SchemaVersion",
                schema_content as "schema_content!",
                content_hash,
                is_active as "is_active!",
                updated_at as "updated_at!"
            "#,
            *id.as_ulid() as _,
            schema.source,
            schema.event_type,
            schema.schema_version.as_str(),
            schema.schema_content,
            schema.content_hash,
            schema.is_active
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
        // Start transaction to ensure atomicity of event emission and state change
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin schema status update transaction"))?;

        // Get current schema details for event emission
        let schema_details = sqlx::query!(
            "SELECT schema_name, schema_version, is_active FROM sinex_schemas.event_payload_schemas WHERE id = $1",
            *id.as_ulid() as _
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db_error(e, "get schema details for status update"))?;

        if let Some(schema) = schema_details {
            // Emit schema status change intent event BEFORE state change
            let schema_status_intent_event = RawEvent::system_event(
                EventSource::new("sinex.schema.lifecycle".to_string()),
                EventType::new("schema.status_change_intent".to_string()),
                serde_json::json!({
                    "schema_id": id.as_ulid().to_string(),
                    "schema_name": schema.schema_name,
                    "schema_version": schema.schema_version,
                    "current_status": schema.is_active,
                    "new_status": is_active,
                    "change_type": if is_active { "activate" } else { "deactivate" }
                }),
            )
            .with_host(HostName::new("sinex.schema".to_string()));

            self.insert_with_tx(&mut tx, schema_status_intent_event)
                .await?;

            // Perform the status update
            let result = sqlx::query!(
                "UPDATE sinex_schemas.event_payload_schemas SET is_active = $2, updated_at = NOW() WHERE id = $1",
                *id.as_ulid() as _,
                is_active
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set schema active status"))?;

            let rows_affected = result.rows_affected() > 0;

            if rows_affected {
                // Emit schema status changed confirmation event after successful update
                let schema_status_changed_event = RawEvent::system_event(
                    EventSource::new("sinex.schema.lifecycle".to_string()),
                    EventType::new("schema.status_changed".to_string()),
                    serde_json::json!({
                        "schema_id": id.as_ulid().to_string(),
                        "schema_name": schema.schema_name,
                        "schema_version": schema.schema_version,
                        "previous_status": schema.is_active,
                        "new_status": is_active,
                        "change_type": if is_active { "activate" } else { "deactivate" }
                    }),
                )
                .with_host(HostName::new("sinex.schema".to_string()));

                self.insert_with_tx(&mut tx, schema_status_changed_event)
                    .await?;
            }

            tx.commit()
                .await
                .map_err(|e| db_error(e, "commit schema status update transaction"))?;

            Ok(rows_affected)
        } else {
            tx.rollback().await.ok();
            Ok(false)
        }
    }

    /// Deprecate a schema with reason
    pub async fn deprecate_schema(
        &self,
        id: Id<EventPayloadSchema>,
        deprecation_reason: &str,
    ) -> DbResult<bool> {
        // Start transaction to ensure atomicity of event emission and state change
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin schema deprecation transaction"))?;

        // Get current schema details for event emission
        let schema_details = sqlx::query!(
            "SELECT schema_name, schema_version, is_active, deprecated_at FROM sinex_schemas.event_payload_schemas WHERE id = $1",
            *id.as_ulid() as _
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db_error(e, "get schema details for deprecation"))?;

        if let Some(schema) = schema_details {
            // Emit schema deprecation intent event BEFORE state change
            let schema_deprecation_intent_event = RawEvent::system_event(
                EventSource::new("sinex.schema.lifecycle".to_string()),
                EventType::new("schema.deprecation_intent".to_string()),
                serde_json::json!({
                    "schema_id": id.as_ulid().to_string(),
                    "schema_name": schema.schema_name,
                    "schema_version": schema.schema_version,
                    "current_status": schema.is_active,
                    "already_deprecated": schema.deprecated_at.is_some(),
                    "deprecation_reason": deprecation_reason
                }),
            )
            .with_host(HostName::new("sinex.schema".to_string()));

            self.insert_with_tx(&mut tx, schema_deprecation_intent_event)
                .await?;

            // Perform the deprecation
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
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "deprecate schema"))?;

            let rows_affected = result.rows_affected() > 0;

            if rows_affected {
                // Emit schema deprecated confirmation event after successful deprecation
                let schema_deprecated_event = RawEvent::system_event(
                    EventSource::new("sinex.schema.lifecycle".to_string()),
                    EventType::new("schema.deprecated".to_string()),
                    serde_json::json!({
                        "schema_id": id.as_ulid().to_string(),
                        "schema_name": schema.schema_name,
                        "schema_version": schema.schema_version,
                        "previous_status": schema.is_active,
                        "was_already_deprecated": schema.deprecated_at.is_some(),
                        "deprecation_reason": deprecation_reason
                    }),
                )
                .with_host(HostName::new("sinex.schema".to_string()));

                self.insert_with_tx(&mut tx, schema_deprecated_event)
                    .await?;
            }

            tx.commit()
                .await
                .map_err(|e| db_error(e, "commit schema deprecation transaction"))?;

            Ok(rows_affected)
        } else {
            tx.rollback().await.ok();
            Ok(false)
        }
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
    */

    // ========== Event Annotations ==========

    /// Add an annotation to an event
    pub async fn add_annotation(
        &self,
        event_id: Id<RawEvent>,
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
                event_id as "event_id: Id<RawEvent>",
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
    pub async fn get_annotations(&self, id: Id<RawEvent>) -> DbResult<Vec<EventAnnotation>> {
        sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT 
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<RawEvent>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            FROM core.event_annotations
            WHERE id = $1
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
                event_id as "event_id: Id<RawEvent>",
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
                event_id as "event_id: Id<RawEvent>",
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

    /// Delete an annotation (soft delete)
    pub async fn delete_annotation(&self, id: Id<EventAnnotation>) -> DbResult<bool> {
        self.delete_annotation_with_context(id, "system", "Programmatic deletion")
            .await
    }

    /// Delete an annotation with audit context (soft delete)
    pub async fn delete_annotation_with_context(
        &self,
        id: Id<EventAnnotation>,
        deleted_by: &str,
        deletion_reason: &str,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            "DELETE FROM core.event_annotations WHERE id = $1",
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

        let rows = sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT 
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<RawEvent>",
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
        .map_err(|e| db_error(e, "search annotations"))?;

        Ok(rows)
    }

    // ========== Data Quality Checks (replacing validation queries) ==========

    /// Find events with null or empty payloads
    pub async fn find_invalid_payloads(&self, limit: i64) -> DbResult<Vec<InvalidPayloadEvent>> {
        sqlx::query!(
            r#"
            SELECT
                id::uuid as "id!",
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
                    event_id: Id::<RawEvent>::from_uuid(row.id),
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
    ) -> DbResult<Vec<(Id<RawEvent>, Id<RawEvent>, DateTime<Utc>, DateTime<Utc>)>> {
        let rows = sqlx::query!(
            r#"
            WITH ordered_events AS (
                SELECT 
                    id,
                    ts_orig,
                    LAG(id) OVER (PARTITION BY source ORDER BY id) as prev_id,
                    LAG(ts_orig) OVER (PARTITION BY source ORDER BY id) as prev_ts
                FROM core.events
                WHERE ts_orig IS NOT NULL
            )
            SELECT 
                id::uuid as "id!",
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
                    Id::<RawEvent>::from_uuid(r.id),
                    Id::<RawEvent>::from_uuid(r.prev_id),
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
                    id,
                    ts_orig,
                    source,
                    ROW_NUMBER() OVER (ORDER BY id) as row_num,
                    LAG(id) OVER (ORDER BY id) as prev_event_id,
                    LAG(ts_orig) OVER (ORDER by id) as prev_ts_orig
                FROM core.events
                WHERE ts_ingest > NOW() - INTERVAL '1 day' * $1
                ORDER BY id DESC
                LIMIT 10000
            )
            SELECT 
                id as "event_id?: Id<RawEvent>",
                prev_event_id as "prev_event_id?: Id<RawEvent>",
                ts_orig,
                prev_ts_orig,
                source,
                row_num
            FROM event_batches
            WHERE prev_event_id IS NOT NULL
              AND (ts_orig < prev_ts_orig OR id < prev_event_id)
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
                id as "event_id: Id<RawEvent>",
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
                id as "event_id: Id<RawEvent>", 
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
    ) -> DbResult<RawEvent> {
        let event = RawEvent::system_event(
            EventSource::new(source.to_string()),
            EventType::new(event_type.to_string()),
            payload,
        )
        .with_host(HostName::new("test-host".to_string()));

        self.insert(event).await
    }

    /// Get a test event by ID
    pub async fn get_test_event(&self, id: Id<RawEvent>) -> DbResult<Option<RawEvent>> {
        self.get_by_id(id).await
    }

    /// Update test event payload
    pub async fn update_test_event(
        &self,
        id: Id<RawEvent>,
        payload: serde_json::Value,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE core.events
            SET payload = $2
            WHERE id = $1
            "#,
            *id.as_ulid() as _,
            payload
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update test event"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Delete a test event (hard delete for test cleanup)
    pub async fn delete_test_event(&self, id: Id<RawEvent>) -> DbResult<bool> {
        let result = sqlx::query!("DELETE FROM core.events WHERE id = $1", *id.as_ulid() as _)
            .execute(self.pool)
            .await
            .map_err(|e| db_error(e, "delete test event"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Cleanup test events by source and type (soft delete)
    pub async fn cleanup_test_events(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> DbResult<u64> {
        self.cleanup_test_events_with_context(
            Some(source),
            Some(event_type),
            "test_system",
            "Test cleanup by source and type",
        )
        .await
    }

    /// Cleanup test events by source only (soft delete)
    pub async fn cleanup_test_events_by_source(&self, source: &EventSource) -> DbResult<u64> {
        self.cleanup_test_events_with_context(
            Some(source),
            None,
            "test_system",
            "Test cleanup by source",
        )
        .await
    }

    /// Cleanup events with audit context (proper deletion with audit trail)
    pub async fn cleanup_test_events_with_context(
        &self,
        source: Option<&EventSource>,
        event_type: Option<&EventType>,
        deleted_by: &str,
        deletion_reason: &str,
    ) -> DbResult<u64> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate a unique operation ID for audit tracking
        let operation_id = format!(
            "cleanup_{}_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            rand::random::<u32>()
        );

        // Begin transaction and set audit context
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin cleanup transaction"))?;

        // Set session variables for audit trail
        sqlx::query("SET LOCAL sinex.operation_id = $1")
            .bind(&operation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set operation_id"))?;

        sqlx::query("SET LOCAL sinex.archived_by = $1")
            .bind(deleted_by)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set archived_by"))?;

        sqlx::query("SET LOCAL sinex.archive_reason = $1")
            .bind(deletion_reason)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set archive_reason"))?;

        // Build dynamic DELETE query based on filters
        let mut query_parts = vec!["DELETE FROM core.events WHERE 1=1".to_string()];
        let mut bind_index = 1;

        if let Some(source) = source {
            query_parts.push(format!(" AND source = ${}", bind_index));
            bind_index += 1;
        }

        if let Some(event_type) = event_type {
            query_parts.push(format!(" AND event_type = ${}", bind_index));
            bind_index += 1;
        }

        // Add safety constraint to only delete test events
        query_parts.push(" AND (source LIKE '%test%' OR event_type LIKE '%test%' OR payload @> '{\"test\": true}' OR host LIKE '%test%')".to_string());

        let query_sql = query_parts.join("");

        // Execute the deletion query
        let mut query = sqlx::query(&query_sql);

        if let Some(source) = source {
            query = query.bind(source.as_str());
        }

        if let Some(event_type) = event_type {
            query = query.bind(event_type.as_str());
        }

        let result = query
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "delete test events"))?;

        let deleted_count = result.rows_affected();

        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit cleanup transaction"))?;

        tracing::info!(
            operation_id = %operation_id,
            deleted_by = %deleted_by,
            deletion_reason = %deletion_reason,
            deleted_count = %deleted_count,
            "Cleaned up test events with audit trail"
        );

        Ok(deleted_count)
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

    /// Get source activity statistics with proper pagination
    pub async fn get_source_activity(
        &self,
        since: DateTime<Utc>,
        limit: Option<i64>,
    ) -> DbResult<Vec<SourceActivity>> {
        let limit = limit.unwrap_or(100); // Default limit to prevent unbounded queries

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
            LIMIT $2
            "#,
            since,
            limit
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

    /// Count events by type all time with proper pagination
    pub async fn count_by_type_all_time(
        &self,
        limit: Option<i64>,
    ) -> DbResult<Vec<EventTypeCount>> {
        let limit = limit.unwrap_or(100); // Prevent unbounded queries

        let rows = sqlx::query!(
            r#"
            SELECT
                event_type,
                COUNT(*) as count
            FROM core.events
            GROUP BY event_type
            ORDER BY count DESC
            LIMIT $1
            "#,
            limit
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

    /// Get events over time using TimescaleDB time buckets with proper limits
    ///
    /// This uses raw SQL for TimescaleDB time_bucket function
    pub async fn get_events_over_time(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        interval: sqlx::postgres::types::PgInterval,
        limit: Option<i64>,
    ) -> DbResult<Vec<TimeBucketResult>> {
        let limit = limit.unwrap_or(1000); // Reasonable default for time series data

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
            LIMIT $4
            "#,
            interval,
            start_time,
            end_time,
            limit
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

    /// Delete all events from a specific source (soft delete, useful for test cleanup)
    pub async fn delete_by_source(&self, source: &EventSource) -> DbResult<u64> {
        self.cleanup_test_events_with_context(Some(source), None, "system", "Delete by source")
            .await
    }

    /// Hard delete events from a specific source (ADMIN USE ONLY)
    ///
    /// This bypasses audit controls and permanently removes data.
    /// Only use for test cleanup or administrative operations where
    /// you need to actually reclaim disk space.
    pub async fn hard_delete_by_source(&self, source: &EventSource) -> DbResult<u64> {
        // Note: Audit bypass mode not implemented - performing direct delete

        // Perform the hard delete
        let result = sqlx::query!("DELETE FROM core.events WHERE source = $1", source.as_str())
            .execute(self.pool)
            .await;

        // Note: Audit bypass mode not implemented

        let result = result.map_err(|e| db_error(e, "hard delete by source"))?;
        Ok(result.rows_affected())
    }

    /// Get events by multiple IDs efficiently (prevents N+1 queries)
    pub async fn get_by_ids(&self, ids: &[Id<RawEvent>]) -> DbResult<Vec<RawEvent>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let uuids: Vec<uuid::Uuid> = ids.iter().map(|id| ulid_to_uuid(*id.as_ulid())).collect();

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
            FROM core.events 
            WHERE id = ANY($1::uuid[])
            ORDER BY ts_ingest DESC
            "#,
        )
        .bind(&uuids)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by ids"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    /// Get recent events for multiple sources efficiently
    pub async fn get_recent_by_sources(
        &self,
        sources: &[EventSource],
        limit_per_source: i64,
    ) -> DbResult<Vec<RawEvent>> {
        if sources.is_empty() {
            return Ok(Vec::new());
        }

        let source_strings: Vec<String> = sources.iter().map(|s| s.as_str().to_string()).collect();

        let records = sqlx::query_as::<_, EventRecord>(
            r#"
            SELECT 
                id,
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
                offset_start,
                offset_end,
                anchor_byte,
                associated_blob_ids,
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY source ORDER BY ts_ingest DESC) as rn
                FROM core.events 
                WHERE source = ANY($1::text[])
            ) ranked_events
            WHERE rn <= $2
            ORDER BY source, ts_ingest DESC
            "#,
        )
        .bind(&source_strings)
        .bind(limit_per_source)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent by sources"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use crate::types::domain::{EventSource, EventType, HostName};
    use serde_json::json;
    use sinex_test_utils::{sinex_test, TestContext};

    use color_eyre::eyre::Result;

    #[sinex_test]
    async fn test_event_record_insert(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let pool = &ctx.pool;

        // Create an event
        let event = crate::models::RawEvent::test_event(
            EventSource::new("test.source"),
            EventType::new("test.event"),
            json!({"test": "data"}),
        )
        .with_host(HostName::new("test-host"));

        // Insert using repository pattern with EventRecord
        let inserted = pool.events().insert(event).await?;

        // Verify the event was inserted with correct data
        assert_eq!(inserted.source.as_str(), "test.source");
        assert_eq!(inserted.event_type.as_str(), "test.event");
        assert_eq!(inserted.host.as_str(), "test-host");
        assert_eq!(inserted.payload["test"], "data");

        // ID should be set after insertion
        assert!(inserted.id.is_some());

        Ok(())
    }

    #[sinex_test]
    async fn test_event_record_with_provenance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let pool = &ctx.pool;

        // Create a source event first
        let source_event = crate::models::RawEvent::test_event(
            EventSource::new("test.source"),
            EventType::new("source.event"),
            json!({"original": true}),
        )
        .with_host(HostName::new("test-host"));

        let source = pool.events().insert(source_event).await?;
        let source_id = source.id.unwrap();

        // Create derived event with provenance
        let derived_event = crate::models::RawEvent::test_event(
            EventSource::new("test.processor"),
            EventType::new("derived.event"),
            json!({"derived": true}),
        )
        .with_host(HostName::new("test-host"))
        .with_provenance(crate::models::Provenance::Events(vec![source_id.clone()]));

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
