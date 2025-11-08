//! Extensions for EventRepository to add missing query methods

use crate::db::models::{Event, JsonValue};
use crate::db::repositories::common::{db_error, DbResult, Repository};
use crate::db::repositories::events::{EventRecordExt, EventRepository};
use crate::types::domain::EventSource;
use crate::types::Pagination;
use crate::EventRecord;
use chrono::{DateTime, Utc};
use tracing::instrument;

impl<'a> EventRepository<'a> {
    /// Get events by source and time range
    #[instrument(skip(self), fields(source = %source.as_str()))]
    pub async fn get_by_source_and_time_range(
        &self,
        source: &EventSource,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let pagination = Pagination::with_default(limit, offset, 100);
        let (limit, offset) = pagination.as_tuple();

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
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids,
                payload_schema_name,
                payload_schema_version
            FROM core.events 
            WHERE source = $1 
              AND ts_ingest >= $2 
              AND ts_ingest <= $3

            ORDER BY ts_ingest DESC
            LIMIT $4 OFFSET $5
            "#,
        )
        .bind(source.as_str())
        .bind(start)
        .bind(end)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool())
        .await
        .map_err(|e| db_error(e, "get events by source and time range"))?;

        records.into_iter().map(|r| r.try_to_event()).collect()
    }

    /// Count events by source and time range
    #[instrument(skip(self), fields(source = %source.as_str()))]
    pub async fn count_by_source_and_time_range(
        &self,
        source: &EventSource,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM core.events 
            WHERE source = $1 
              AND ts_ingest >= $2 
              AND ts_ingest <= $3

            "#,
            source.as_str(),
            start,
            end
        )
        .fetch_one(self.pool())
        .await
        .map_err(|e| db_error(e, "count events by source and time range"))?;

        Ok(count)
    }
}
