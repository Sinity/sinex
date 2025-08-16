use crate::db::schema::{Events, TableDef};
use crate::models::{Provenance, RawEvent};
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::repositories::common::{
    db_error, DbResult, EnhancedRepository, EventSearchFilters, Repository, TimeBucketResult,
};
use crate::types::domain::{EventSource, EventType, HostName, SchemaName, SchemaVersion};
use crate::types::non_empty::NonEmptyVec;
use crate::types::{Id, Ulid};
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
                    .map(|uuid| EventId::from(uuid_to_ulid(uuid)))
                    .collect();
                // SAFETY: We checked that event_ids is not empty above
                Provenance::Synthesis {
                    source_event_ids: NonEmptyVec::from_vec(ids).unwrap(),
                    operation_id: None,
                }
            }
            (None, Some(material_id), Some(anchor_byte)) => Provenance::Material {
                id: Id::<SourceMaterial>::from(uuid_to_ulid(material_id)),
                anchor_byte,
                offset_start: self.offset_start,
                offset_end: self.offset_end,
                offset_kind: self
                    .offset_kind
                    .and_then(|k| match k.as_str() {
                        "byte" => Some(OffsetKind::Byte),
                        "line" => Some(OffsetKind::Line),
                        "rowid" => Some(OffsetKind::RowId),
                        "logical" => Some(OffsetKind::Logical),
                        _ => None,
                    })
                    .unwrap_or(OffsetKind::default()),
            },
            _ => {
                // This should never happen in production - all events must have provenance
                // Create a synthetic bootstrap provenance as fallback
                Provenance::Synthesis {
                    source_event_ids: NonEmptyVec::single(EventId::from(Ulid::nil())),
                    operation_id: None,
                }
            }
        };

        RawEvent {
            id: Some(EventId::from_uuid(self.id)),
            source: self.source.into(),
            event_type: self.event_type.into(),
            host: self.host.into(),
            payload: self.payload,
            ts_orig: self.ts_orig,
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id.map(uuid_to_ulid),
            provenance,
        }
    }
}

/// Extract provenance fields from domain Event for database storage
fn extract_provenance(
    event: &RawEvent,
) -> (
    Option<Vec<uuid::Uuid>>, // source_event_ids
    Option<uuid::Uuid>,      // source_material_id
    Option<i64>,             // offset_start
    Option<i64>,             // offset_end
    Option<i64>,             // anchor_byte
    Option<String>,          // offset_kind
) {
    match &event.provenance {
        Provenance::Synthesis {
            source_event_ids, ..
        } => {
            let uuids = source_event_ids
                .iter()
                .map(|id| ulid_to_uuid(*id.as_ulid()))
                .collect();
            (Some(uuids), None, None, None, None, None)
        }
        Provenance::Material {
            id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
            ..
        } => {
            let offset_kind_str = match offset_kind {
                crate::db::models::event::OffsetKind::Byte => Some("byte".to_string()),
                crate::db::models::event::OffsetKind::Line => Some("line".to_string()),
                crate::db::models::event::OffsetKind::RowId => Some("rowid".to_string()),
                crate::db::models::event::OffsetKind::Logical => Some("logical".to_string()),
            };
            (
                None,
                Some(ulid_to_uuid(*id.as_ulid())),
                *offset_start,
                *offset_end,
                Some(*anchor_byte),
                offset_kind_str,
            )
        }
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
    pub event_id: Id<RawEvent>,
    pub annotation_type: String,
    pub content: JsonValue,
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
        let (
            source_event_ids,
            source_material_id,
            offset_start,
            offset_end,
            anchor_byte,
            offset_kind,
        ) = extract_provenance(&event);

        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                id, source, event_type, host, payload,
                ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, offset_start, offset_end, offset_kind,
                anchor_byte
            ) VALUES (
                $1::uuid, $2, $3, $4, $5,
                $6, $7, $8::uuid, $9::uuid[],
                $10::uuid, $11, $12, $13,
                $14
            )
            RETURNING 
                id::uuid as "id!",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig as "ts_orig!",
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as payload_schema_id,
                payload as "payload!",
                source_event_ids::uuid[] as source_event_ids,
                source_material_id::uuid as source_material_id,
                offset_start,
                offset_end,
                offset_kind,
                anchor_byte
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
            offset_start,
            offset_end,
            offset_kind,
            anchor_byte,
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
                offset_kind,
                anchor_byte
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
                offset_kind,
                anchor_byte
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
                offset_kind,
                anchor_byte
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
                offset_kind,
                anchor_byte
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
                offset_kind,
                anchor_byte
            FROM core.events 
            WHERE ts_orig >= $1 AND ts_orig <= $2
            ORDER BY ts_orig DESC
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

    #[instrument(skip(self, tx), fields(event_id = %id))]
    pub async fn delete_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        id: Id<RawEvent>,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            "DELETE FROM core.events WHERE id = $1",
            ulid_to_uuid(*id.as_ulid())
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "delete event"))?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(skip(self))]
    pub async fn get_event_types(&self) -> DbResult<Vec<EventTypeCount>> {
        let records = sqlx::query!(
            r#"
            SELECT event_type, COUNT(*) as count
            FROM core.events
            GROUP BY event_type
            ORDER BY count DESC
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get event types"))?;

        Ok(records
            .into_iter()
            .map(|r| EventTypeCount {
                event_type: r.event_type,
                count: r.count.unwrap_or(0),
            })
            .collect())
    }

    #[instrument(skip(self))]
    pub async fn get_source_activity(&self) -> DbResult<Vec<SourceActivity>> {
        let records = sqlx::query!(
            r#"
            SELECT 
                source,
                COUNT(*) as event_count,
                MIN(ts_orig) as first_event,
                MAX(ts_orig) as last_event
            FROM core.events
            GROUP BY source
            ORDER BY event_count DESC
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get source activity"))?;

        Ok(records
            .into_iter()
            .map(|r| SourceActivity {
                source: r.source,
                event_count: r.event_count.unwrap_or(0),
                first_event: r.first_event,
                last_event: r.last_event,
            })
            .collect())
    }

    #[instrument(skip(self), fields(limit = limit))]
    pub async fn get_recent_terminal_commands(&self, limit: i64) -> DbResult<Vec<CommandCount>> {
        let records = sqlx::query!(
            r#"
            SELECT 
                payload->>'command' as command,
                COUNT(*) as count
            FROM core.events
            WHERE event_type = 'terminal.command.executed'
                AND payload->>'command' IS NOT NULL
            GROUP BY payload->>'command'
            ORDER BY count DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent terminal commands"))?;

        Ok(records
            .into_iter()
            .filter_map(|r| {
                r.command.map(|cmd| CommandCount {
                    command: cmd,
                    count: r.count.unwrap_or(0),
                })
            })
            .collect())
    }

    #[instrument(skip(self, filters))]
    pub async fn search(&self, filters: EventSearchFilters) -> DbResult<Vec<RawEvent>> {
        // Build dynamic query based on filters
        let mut query = String::from(
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
                offset_kind,
                anchor_byte
            FROM core.events
            WHERE 1=1
            "#,
        );

        let mut conditions = Vec::new();
        let mut bind_count = 0;

        if let Some(source) = &filters.source {
            bind_count += 1;
            conditions.push(format!("AND source = ${}", bind_count));
        }

        if let Some(event_type) = &filters.event_type {
            bind_count += 1;
            conditions.push(format!("AND event_type = ${}", bind_count));
        }

        if let Some(start_time) = filters.start_time {
            bind_count += 1;
            conditions.push(format!("AND ts_orig >= ${}", bind_count));
        }

        if let Some(end_time) = filters.end_time {
            bind_count += 1;
            conditions.push(format!("AND ts_orig <= ${}", bind_count));
        }

        if let Some(host) = &filters.host {
            bind_count += 1;
            conditions.push(format!("AND host = ${}", bind_count));
        }

        for condition in conditions {
            query.push_str(&condition);
            query.push(' ');
        }

        query.push_str("ORDER BY ts_orig DESC ");

        if let Some(limit) = filters.limit {
            bind_count += 1;
            query.push_str(&format!("LIMIT ${} ", bind_count));
        }

        if let Some(offset) = filters.offset {
            bind_count += 1;
            query.push_str(&format!("OFFSET ${}", bind_count));
        }

        // Create query and bind parameters dynamically
        let mut query_builder = sqlx::query_as::<_, EventRecord>(&query);

        if let Some(source) = &filters.source {
            query_builder = query_builder.bind(source.as_str());
        }

        if let Some(event_type) = &filters.event_type {
            query_builder = query_builder.bind(event_type.as_str());
        }

        if let Some(start_time) = filters.start_time {
            query_builder = query_builder.bind(start_time);
        }

        if let Some(end_time) = filters.end_time {
            query_builder = query_builder.bind(end_time);
        }

        if let Some(host) = &filters.host {
            query_builder = query_builder.bind(host.as_str());
        }

        if let Some(limit) = filters.limit {
            query_builder = query_builder.bind(limit);
        }

        if let Some(offset) = filters.offset {
            query_builder = query_builder.bind(offset);
        }

        let records = query_builder
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "search events"))?;

        Ok(records.into_iter().map(|r| r.to_event()).collect())
    }

    /// Get time-bucketed event counts for analytics
    #[instrument(skip(self))]
    pub async fn get_time_bucketed_counts(
        &self,
        bucket_minutes: i32,
    ) -> DbResult<Vec<TimeBucketResult>> {
        let records = sqlx::query!(
            r#"
            SELECT 
                time_bucket($1::interval, ts_orig) as bucket,
                COUNT(*) as count
            FROM core.events
            WHERE ts_orig > NOW() - INTERVAL '24 hours'
            GROUP BY bucket
            ORDER BY bucket DESC
            "#,
            format!("{} minutes", bucket_minutes)
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get time bucketed counts"))?;

        Ok(records
            .into_iter()
            .filter_map(|r| {
                r.bucket.map(|b| TimeBucketResult {
                    bucket: b,
                    count: r.count.unwrap_or(0),
                })
            })
            .collect())
    }
}
