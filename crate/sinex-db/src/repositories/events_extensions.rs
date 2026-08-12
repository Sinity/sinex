//! Extensions for `EventRepository` to add missing query methods

use crate::JsonValue;
use crate::models::Event;
use crate::repositories::common::{DbResult, Repository, db_error};
use crate::repositories::events::queries::extract_plan_rows;
use crate::repositories::events::{EventRepository, event_select_columns};

use crate::EventRecord;
use sinex_primitives::Pagination;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::EventSource;
use uuid::Uuid;

use sqlx::types::Json;
use tracing::instrument;

impl EventRepository<'_> {
    /// Get events by source and time range
    #[instrument(skip(self), fields(source = %source.as_str()))]
    pub async fn get_by_source_and_time_range(
        &self,
        source: &EventSource,
        start: Timestamp,
        end: Timestamp,
        pagination: Pagination,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let (limit, offset) = pagination.as_tuple();

        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE source = $1 AND ts_coided >= $2 AND ts_coided <= $3 \
             ORDER BY ts_coided DESC LIMIT $4 OFFSET $5"
        ))
        .bind(source.as_str())
        .bind(start)
        .bind(end)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool())
        .await
        .map_err(|e| db_error(e, "get events by source and time range"))?;

        records
            .into_iter()
            .map(super::events::conversions::EventRecordExt::try_to_event)
            .collect()
    }

    /// Fetch a bounded source/time page using a UUIDv7 keyset cursor.
    pub async fn get_by_source_and_time_range_after_id(
        &self,
        source: &EventSource,
        start: Timestamp,
        end: Timestamp,
        after_id: Option<sinex_primitives::Id<Event<JsonValue>>>,
        limit: i64,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        if limit <= 0 {
            return Err(crate::SinexError::validation(
                "keyset page limit must be positive",
            ));
        }

        let mut query = String::from(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE source = $1 AND ts_coided >= $2 AND ts_coided <= $3"
        ));
        if after_id.is_some() {
            query.push_str(" AND id < $4");
        }
        query.push_str(if after_id.is_some() {
            " ORDER BY id DESC LIMIT $5"
        } else {
            " ORDER BY id DESC LIMIT $4"
        });

        let mut request = sqlx::query_as::<_, EventRecord>(&query)
            .bind(source.as_str())
            .bind(start)
            .bind(end);
        if let Some(after_id) = after_id {
            request = request.bind(after_id.to_uuid());
        }
        request = request.bind(limit);
        request
            .fetch_all(self.pool())
            .await
            .map_err(|e| db_error(e, "get events by source and time range after id"))?
            .into_iter()
            .map(super::events::conversions::EventRecordExt::try_to_event)
            .collect()
    }

    /// Get material-root events by source and time range.
    #[instrument(skip(self), fields(source = %source.as_str()))]
    pub async fn get_material_root_events_in_range(
        &self,
        source: &EventSource,
        start: Timestamp,
        end: Timestamp,
        pagination: Pagination,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let (limit, offset) = pagination.as_tuple();

        let records = sqlx::query_as::<_, EventRecord>(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE source = $1 AND ts_coided >= $2 AND ts_coided <= $3 \
             AND source_event_ids IS NULL ORDER BY ts_coided DESC LIMIT $4 OFFSET $5"
        ))
        .bind(source.as_str())
        .bind(start)
        .bind(end)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool())
        .await
        .map_err(|e| db_error(e, "get material-root events by source and time range"))?;

        records
            .into_iter()
            .map(super::events::conversions::EventRecordExt::try_to_event)
            .collect()
    }

    /// Fetch bounded material-root events with a UUIDv7 keyset cursor.
    pub async fn get_material_root_events_in_range_after_id(
        &self,
        source: &EventSource,
        start: Timestamp,
        end: Timestamp,
        after_id: Option<sinex_primitives::Id<Event<JsonValue>>>,
        limit: i64,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        if limit <= 0 {
            return Err(crate::SinexError::validation(
                "keyset page limit must be positive",
            ));
        }

        let mut query = String::from(concat!(
            "SELECT ",
            event_select_columns!(),
            " FROM core.events WHERE source = $1 AND ts_coided >= $2 AND ts_coided <= $3 ",
            "AND source_event_ids IS NULL"
        ));
        if after_id.is_some() {
            query.push_str(" AND id < $4");
        }
        query.push_str(if after_id.is_some() {
            " ORDER BY id DESC LIMIT $5"
        } else {
            " ORDER BY id DESC LIMIT $4"
        });

        let mut request = sqlx::query_as::<_, EventRecord>(&query)
            .bind(source.as_str())
            .bind(start)
            .bind(end);
        if let Some(after_id) = after_id {
            request = request.bind(after_id.to_uuid());
        }
        request = request.bind(limit);
        request
            .fetch_all(self.pool())
            .await
            .map_err(|e| db_error(e, "get material-root events after id"))?
            .into_iter()
            .map(super::events::conversions::EventRecordExt::try_to_event)
            .collect()
    }

    /// Count events by source and time range
    #[instrument(skip(self), fields(source = %source.as_str()))]
    pub async fn count_by_source_and_time_range(
        &self,
        source: &EventSource,
        start: Timestamp,
        end: Timestamp,
    ) -> DbResult<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM core.events 
            WHERE source = $1 
              AND ts_coided >= $2 
              AND ts_coided <= $3

            "#,
            source.as_str(),
            *start,
            *end
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
        cutoff: Uuid,
    ) -> DbResult<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM core.events
            WHERE source = $1
              AND id::uuid < $2
            "#,
            source.as_str(),
            cutoff
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
        cutoff: Uuid,
    ) -> DbResult<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM core.events
            WHERE source = $1
              AND id::uuid >= $2
            "#,
            source.as_str(),
            cutoff
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
        start: Timestamp,
        end: Timestamp,
    ) -> DbResult<i64> {
        // EXPLAIN output shape is not supported by sqlx macros; use runtime query.
        let plan: Json<serde_json::Value> = sqlx::query_scalar(
            r"
            EXPLAIN (FORMAT JSON)
            SELECT 1
            FROM core.events
            WHERE source = $1
              AND ts_coided >= $2
              AND ts_coided <= $3
            ",
        )
        .bind(source.as_str())
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await
        .map_err(|e| db_error(e, "estimate events by source and time range"))?;

        Ok(extract_plan_rows(&plan.0))
    }
}
