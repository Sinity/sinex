use crate::repositories::common::{
    db_error, DbResult, EventSearchFilters, Repository, TimeBucketResult,
};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_core_types::domain::{EventSource, EventType, HostName, SchemaName, SchemaVersion};
use sinex_core_types::ids::{AnnotationId, BlobId, EventId, MaterialId, SchemaId};
use sinex_events::RawEvent;
use sinex_ulid::Ulid;
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

/// New event input structure
#[derive(Debug, bon::Builder)]
#[builder(on(String, into))]
pub struct NewEvent {
    pub source: EventSource,
    pub event_type: EventType,
    pub host: HostName,
    pub payload: JsonValue,
    pub ts_orig: Option<DateTime<Utc>>,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<SchemaId>,
    pub source_event_ids: Option<Vec<EventId>>,
    pub source_material_id: Option<MaterialId>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    pub anchor_byte: Option<i64>,
    pub associated_blob_ids: Option<Vec<BlobId>>,
}

/// Event payload schema record
#[derive(Debug, FromRow)]
pub struct EventPayloadSchema {
    pub id: SchemaId,
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
    pub id: AnnotationId,
    pub event_id: EventId,
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
    pub event_id: EventId,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: DateTime<Utc>,
    pub payload: JsonValue,
}

/// Batch violation record
#[derive(Debug, FromRow)]
pub struct BatchViolation {
    pub event_id: Option<EventId>,
    pub prev_event_id: Option<EventId>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub prev_ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub row_num: Option<i64>,
}

/// Suspicious event record  
#[derive(Debug, FromRow)]
pub struct SuspiciousEvent {
    pub event_id: EventId,
    pub source: String,
    pub event_type: String,
    pub payload: JsonValue,
    pub payload_type: Option<String>,
    pub payload_size: Option<i32>,
}

/// Invalid timestamp record
#[derive(Debug)]
pub struct InvalidTimestamp {
    pub event_id: EventId,
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
    pub async fn insert(&self, event: NewEvent) -> DbResult<RawEvent> {
        let id = EventId::new();

        // Pre-convert vectors to avoid temporary value issues
        let source_event_ulids = event
            .source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| *id.as_ulid()).collect::<Vec<_>>());
        let associated_blob_ulids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| *id.as_ulid()).collect::<Vec<_>>());

        sqlx::query_as!(
            RawEvent,
            r#"
            INSERT INTO core.events (
                event_id, source, event_type, host, payload,
                ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, source_material_offset_start, source_material_offset_end,
                anchor_byte, associated_blob_ids
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12,
                $13, $14
            )
            RETURNING 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            "#,
            *id.as_ulid() as _,
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            event.ts_orig,
            event.ingestor_version,
            event.payload_schema_id.map(|id| *id.as_ulid()) as _,
            source_event_ulids.as_deref() as Option<&[Ulid]>,
            event.source_material_id.map(|id| *id.as_ulid()) as _,
            event.source_material_offset_start,
            event.source_material_offset_end,
            event.anchor_byte,
            associated_blob_ulids.as_deref() as Option<&[Ulid]>
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "insert event"))
    }

    pub async fn get_by_id(&self, id: EventId) -> DbResult<Option<RawEvent>> {
        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            FROM core.events 
            WHERE event_id = $1
            "#,
            *id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get event by id"))
    }

    pub async fn count_all(&self) -> DbResult<i64> {
        let result = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "count all events"))?;

        Ok(result.unwrap_or(0))
    }

    pub async fn get_recent(&self, limit: i64) -> DbResult<Vec<RawEvent>> {
        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            FROM core.events 
            ORDER BY ts_ingest DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent events"))
    }

    pub async fn get_by_source(
        &self,
        source: &EventSource,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<RawEvent>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            FROM core.events 
            WHERE source = $1
            ORDER BY ts_ingest DESC
            LIMIT $2 OFFSET $3
            "#,
            source.as_str(),
            limit,
            offset
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by source"))
    }

    pub async fn get_by_event_type(
        &self,
        event_type: &EventType,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<RawEvent>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            FROM core.events 
            WHERE event_type = $1
            ORDER BY ts_ingest DESC
            LIMIT $2 OFFSET $3
            "#,
            event_type.as_str(),
            limit,
            offset
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by type"))
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
    ) -> DbResult<Vec<RawEvent>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            FROM core.events 
            WHERE ts_ingest >= $1 AND ts_ingest <= $2
            ORDER BY ts_ingest DESC
            LIMIT $3 OFFSET $4
            "#,
            start,
            end,
            limit,
            offset
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by time range"))
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
    ) -> DbResult<Vec<RawEvent>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            FROM core.events 
            WHERE event_type = $1 AND ts_ingest >= $2 AND ts_ingest <= $3
            ORDER BY ts_ingest DESC
            LIMIT $4
            "#,
            event_type.as_str(),
            start,
            end,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get events by type and time range"))
    }

    pub async fn get_process_heartbeats(
        &self,
        source: &EventSource,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<Vec<RawEvent>> {
        sqlx::query_as!(
            RawEvent,
            r#"
            SELECT 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            FROM core.events 
            WHERE source = $1 
              AND event_type = 'process.heartbeat'
              AND ts_ingest >= $2 
              AND ts_ingest <= $3
            ORDER BY ts_ingest ASC
            "#,
            source.as_str(),
            start,
            end
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get process heartbeats"))
    }

    pub async fn search(&self, filters: EventSearchFilters) -> DbResult<Vec<RawEvent>> {
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

        let (sql, values) = query.build(PostgresQueryBuilder);

        // Use the dynamic query string with renamed columns
        sqlx::query_as::<_, RawEvent>(&sql)
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "search events"))
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
            .expr_as(Func::count(Expr::asterisk()), Alias::new("count"))
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

        let (sql, values) = query;

        sqlx::query_as(&sql)
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "time series aggregate"))
    }

    pub async fn insert_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        event: NewEvent,
    ) -> DbResult<RawEvent> {
        let id = EventId::new();

        // Pre-convert vectors to avoid temporary value issues
        let source_event_ulids = event
            .source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| *id.as_ulid()).collect::<Vec<_>>());
        let associated_blob_ulids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| *id.as_ulid()).collect::<Vec<_>>());

        sqlx::query_as!(
            RawEvent,
            r#"
            INSERT INTO core.events (
                event_id, source, event_type, host, payload,
                ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, source_material_offset_start, source_material_offset_end,
                anchor_byte, associated_blob_ids
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12,
                $13, $14
            )
            RETURNING 
                event_id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id as "payload_schema_id: Ulid",
                payload as "payload!",
                source_event_ids as "source_event_ids: Vec<Ulid>",
                source_material_id as "source_material_id: Ulid",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids: Vec<Ulid>"
            "#,
            *id.as_ulid() as _,
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            event.ts_orig,
            event.ingestor_version,
            event.payload_schema_id.map(|id| *id.as_ulid()) as _,
            source_event_ulids.as_deref() as Option<&[Ulid]>,
            event.source_material_id.map(|id| *id.as_ulid()) as _,
            event.source_material_offset_start,
            event.source_material_offset_end,
            event.anchor_byte,
            associated_blob_ulids.as_deref() as Option<&[Ulid]>
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error(e, "insert event with tx"))
    }

    pub async fn insert_batch(&self, events: Vec<NewEvent>) -> DbResult<Vec<RawEvent>> {
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
        let id = SchemaId::new();

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
                id as "id: SchemaId",
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
    pub async fn get_schema_by_id(&self, id: SchemaId) -> DbResult<Option<EventPayloadSchema>> {
        sqlx::query_as!(
            EventPayloadSchema,
            r#"
            SELECT 
                id as "id: SchemaId",
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
                id as "id: SchemaId",
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
                id as "id: SchemaId",
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
    pub async fn set_schema_active_status(&self, id: SchemaId, is_active: bool) -> DbResult<bool> {
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
    pub async fn deprecate_schema(&self, id: SchemaId, deprecation_reason: &str) -> DbResult<bool> {
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
                id as "id: SchemaId",
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
                id as "id: SchemaId",
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
        let id = SchemaId::new();

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
                id as "id: SchemaId",
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
        event_id: EventId,
        annotation_type: &str,
        content: &str,
        metadata: serde_json::Value,
        created_by: &str,
    ) -> DbResult<EventAnnotation> {
        let id = AnnotationId::new();

        sqlx::query_as!(
            EventAnnotation,
            r#"
            INSERT INTO core.event_annotations (
                id, event_id, annotation_type, content, metadata, created_by
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            RETURNING 
                id as "id: AnnotationId",
                event_id as "event_id: EventId",
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
    pub async fn get_annotations(&self, event_id: EventId) -> DbResult<Vec<EventAnnotation>> {
        sqlx::query_as!(
            EventAnnotation,
            r#"
            SELECT 
                id as "id: AnnotationId",
                event_id as "event_id: EventId",
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
            *event_id.as_ulid() as _
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
                id as "id: AnnotationId",
                event_id as "event_id: EventId",
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
        annotation_id: AnnotationId,
        content: &str,
    ) -> DbResult<EventAnnotation> {
        sqlx::query_as!(
            EventAnnotation,
            r#"
            UPDATE core.event_annotations
            SET content = $2, updated_at = CURRENT_TIMESTAMP
            WHERE id = $1
            RETURNING 
                id as "id: AnnotationId",
                event_id as "event_id: EventId",
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
    pub async fn delete_annotation(&self, annotation_id: AnnotationId) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            DELETE FROM core.event_annotations
            WHERE id = $1
            "#,
            *annotation_id.as_ulid() as _
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
                id as "id: AnnotationId",
                event_id as "event_id: EventId",
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
                    event_id: EventId::from_uuid(row.event_id),
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
    ) -> DbResult<Vec<(EventId, EventId, DateTime<Utc>, DateTime<Utc>)>> {
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
                    EventId::from_uuid(r.event_id),
                    EventId::from_uuid(r.prev_id),
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
                event_id as "event_id?: EventId",
                prev_event_id as "prev_event_id?: EventId",
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
                event_id as "event_id: EventId",
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
                event_id as "event_id: EventId", 
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
        source: &EventSource,
        event_type: &EventType,
        payload: serde_json::Value,
    ) -> DbResult<RawEvent> {
        self.insert(NewEvent {
            source: source.clone(),
            event_type: event_type.clone(),
            host: HostName::new("test-host"),
            payload,
            ts_orig: None,
            ingestor_version: None,
            payload_schema_id: None,
            source_event_ids: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            anchor_byte: None,
            associated_blob_ids: None,
        })
        .await
    }

    /// Get a test event by ID
    pub async fn get_test_event(&self, event_id: EventId) -> DbResult<Option<RawEvent>> {
        self.get_by_id(event_id).await
    }

    /// Update test event payload
    pub async fn update_test_event(
        &self,
        event_id: EventId,
        payload: serde_json::Value,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE core.events
            SET payload = $2
            WHERE event_id = $1
            "#,
            *event_id.as_ulid() as _,
            payload
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update test event"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Delete a test event
    pub async fn delete_test_event(&self, event_id: EventId) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            DELETE FROM core.events
            WHERE event_id = $1
            "#,
            *event_id.as_ulid() as _
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
