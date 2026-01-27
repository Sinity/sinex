//! Extensions for EventRepository to add missing query methods

use crate::db::models::{Event, JsonValue};
use crate::db::repositories::common::{db_error, DbResult, Repository};
use crate::db::repositories::events::queries::extract_plan_rows;
use crate::db::repositories::events::{event_select_columns, EventRecordExt, EventRepository};
use crate::query_helpers::ulid_to_uuid;

use crate::types::domain::EventSource;
use crate::types::Pagination;
use crate::EventRecord;
use crate::Ulid;
use chrono::{DateTime, Utc};

use sqlx::types::Json;
use tracing::instrument;

impl<'a> EventRepository<'a> {
    /// Get events by source and time range
    #[instrument(skip(self), fields(source = %source.as_str()))]
    pub async fn get_by_source_and_time_range(
        &self,
        source: &EventSource,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        pagination: Pagination,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let (limit, offset) = pagination.as_tuple();

        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE source = $1 AND ts_ingest >= $2 AND ts_ingest <= $3 \
             ORDER BY ts_ingest DESC LIMIT $4 OFFSET $5"
        ))
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

    /// Count events by source with IDs strictly before the cutoff.
    #[instrument(skip(self), fields(source = %source.as_str(), cutoff = %cutoff))]
    pub async fn count_by_source_before_id(
        &self,
        source: &EventSource,
        cutoff: Ulid,
    ) -> DbResult<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM core.events
            WHERE source = $1
              AND id::uuid < $2
            "#,
            source.as_str(),
            ulid_to_uuid(cutoff)
        )
        .fetch_one(self.pool())
        .await
        .map_err(|e| db_error(e, "count events by source before id"))?;

        Ok(count)
    }

    /// Count events by source with IDs at or after the cutoff.
    #[instrument(skip(self), fields(source = %source.as_str(), cutoff = %cutoff))]
    pub async fn count_by_source_from_id(
        &self,
        source: &EventSource,
        cutoff: Ulid,
    ) -> DbResult<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM core.events
            WHERE source = $1
              AND id::uuid >= $2
            "#,
            source.as_str(),
            ulid_to_uuid(cutoff)
        )
        .fetch_one(self.pool())
        .await
        .map_err(|e| db_error(e, "count events by source from id"))?;

        Ok(count)
    }

    /// Estimate events by source and time range using planner statistics.
    #[instrument(skip(self), fields(source = %source.as_str()))]
    pub async fn estimate_count_by_source_and_time_range(
        &self,
        source: &EventSource,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> DbResult<i64> {
        // EXPLAIN output shape is not supported by sqlx macros; use runtime query.
        let plan: Json<serde_json::Value> = sqlx::query_scalar(
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT 1
            FROM core.events
            WHERE source = $1
              AND ts_ingest >= $2
              AND ts_ingest <= $3
            "#,
        )
        .bind(source.as_str())
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await
        .map_err(|e| db_error(e, "estimate events by source and time range"))?;

        Ok(extract_plan_rows(plan.0))
    }
}
