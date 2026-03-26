//! Composable event query engine — unified read path.
//!
//! Replaces 22+ hardcoded query methods with two composable entry points:
//! - `EventRepository::query()` — filter + paginate + aggregate events
//! - `EventRepository::lineage()` — traverse provenance chains via recursive CTE

use super::conversions::EventRecordExt;
use super::queries::extract_plan_rows;
use crate::EventRecord;
use crate::JsonValue;
use crate::models::Event;
use crate::repositories::common::{DbResult, db_error};
use sinex_primitives::query::{
    AggregationMode, Cursor, EventQuery, EventQueryResult, GroupByField, GroupedCount,
    LineageDirection, LineageNode, LineageQuery, LineageResult, PathOp, PayloadFilter,
    QueryResultEvent, SortDirection, SourceStatsEntry, TimeBucketEntry, TimeSeriesOrder,
};
use sinex_primitives::{Pagination, SinexError, Timestamp};
use sqlx::postgres::types::PgInterval;
use sqlx::{FromRow, Postgres, QueryBuilder};
use tracing::instrument;

use super::persistence::EventRepository;

// ─────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────

impl EventRepository<'_> {
    /// Execute a composable event query.
    ///
    /// Depending on `query.aggregation`, returns either paginated events or
    /// aggregated statistics. All filter fields AND-combine.
    #[instrument(skip(self, query), fields(
        sources = query.sources.len(),
        event_types = query.event_types.len(),
        has_cursor = query.cursor.is_some(),
        has_aggregation = query.aggregation.is_some(),
    ))]
    pub async fn query(&self, mut query: EventQuery) -> DbResult<EventQueryResult> {
        query
            .validate()
            .map_err(|e| e.with_operation("EventRepository::query"))?;

        match query.aggregation {
            None => self.execute_event_listing(query).await,
            Some(ref agg) => match agg {
                AggregationMode::Count => self.execute_count(query).await,
                AggregationMode::CountBy { .. } => self.execute_count_by(query).await,
                AggregationMode::TimeSeries { .. } => self.execute_time_series(query).await,
                AggregationMode::SourceStats { .. } => self.execute_source_stats(query).await,
            },
        }
    }

    /// Traverse the provenance graph from a given event.
    ///
    /// Uses recursive CTEs to walk `source_event_ids` (ancestors) and
    /// events referencing this event (descendants).
    #[instrument(skip(self, query), fields(event_id = %query.event_id, direction = ?query.direction))]
    pub async fn lineage(&self, mut query: LineageQuery) -> DbResult<LineageResult> {
        query
            .validate()
            .map_err(|e| e.with_operation("EventRepository::lineage"))?;

        // Fetch root event
        let root = self.get_by_id(query.event_id).await?.ok_or_else(|| {
            SinexError::not_found("Lineage root event not found")
                .with_context("event_id", query.event_id.to_string())
        })?;

        let ancestors = if matches!(
            query.direction,
            LineageDirection::Ancestors | LineageDirection::Both
        ) {
            self.fetch_ancestors(query.event_id, query.max_depth)
                .await?
        } else {
            Vec::new()
        };

        let descendants = if matches!(
            query.direction,
            LineageDirection::Descendants | LineageDirection::Both
        ) {
            self.fetch_descendants(query.event_id, query.max_depth)
                .await?
        } else {
            Vec::new()
        };

        Ok(LineageResult {
            root,
            ancestors,
            descendants,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────
// Event listing (no aggregation)
// ─────────────────────────────────────────────────────────────────────

impl EventRepository<'_> {
    async fn execute_event_listing(&self, query: EventQuery) -> DbResult<EventQueryResult> {
        let has_text_search = matches!(&query.payload, Some(PayloadFilter::TextSearch { .. }));
        let text_for_search = if let Some(PayloadFilter::TextSearch { ref text }) = query.payload {
            Some(text.clone())
        } else {
            None
        };

        let fetch_limit = query.limit + 1; // +1 to detect "has more"

        let mut qb = QueryBuilder::<Postgres>::new(format!("SELECT {}", event_select_columns!()));

        // Text search scoring columns
        if let Some(ref text) = text_for_search {
            qb.push(
                ", ts_rank_cd(to_tsvector('simple', payload::text), websearch_to_tsquery('simple', ",
            );
            qb.push_bind(text.clone());
            qb.push("))::float8 AS relevance_score");
            qb.push(", ts_headline('simple', payload::text, websearch_to_tsquery('simple', ");
            qb.push_bind(text.clone());
            qb.push("), 'MaxFragments=2, MinWords=8, MaxWords=24') AS snippet");
        } else {
            qb.push(", NULL::float8 AS relevance_score, NULL::text AS snippet");
        }

        qb.push(" FROM core.events WHERE TRUE");

        // Apply all filters
        push_filters(&mut qb, &query);

        // Cursor pagination
        if let Some(ref cursor) = query.cursor {
            push_cursor(&mut qb, cursor, query.direction);
        }

        // ORDER BY
        if has_text_search {
            qb.push(" ORDER BY relevance_score DESC, id ");
            qb.push(direction_sql(query.direction));
        } else {
            qb.push(" ORDER BY id ");
            qb.push(direction_sql(query.direction));
        }

        qb.push(" LIMIT ");
        qb.push_bind(fetch_limit);

        // Execute
        let rows: Vec<EventListingRow> = qb
            .build_query_as::<EventListingRow>()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "composable query: event listing"))?;

        let has_more = rows.len() as i64 > query.limit;
        let rows: Vec<EventListingRow> = rows.into_iter().take(query.limit as usize).collect();

        // Convert to result events
        let next_cursor = if has_more {
            rows.last().map(|row| row.record.id.to_string())
        } else {
            None
        };

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event = row.record.try_to_event()?;
            events.push(QueryResultEvent {
                event,
                relevance_score: row.relevance_score,
                snippet: row.snippet,
            });
        }

        // Optional total estimate via EXPLAIN
        let total_estimate = if query.include_total_estimate {
            Some(self.estimate_count(&query).await?)
        } else {
            None
        };

        Ok(EventQueryResult::Events {
            events,
            next_cursor,
            total_estimate,
        })
    }

    async fn estimate_count(&self, query: &EventQuery) -> DbResult<i64> {
        let mut qb = QueryBuilder::<Postgres>::new(
            "EXPLAIN (FORMAT JSON) SELECT 1 FROM core.events WHERE TRUE",
        );
        push_filters(&mut qb, query);

        let row: (JsonValue,) = qb
            .build_query_as()
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "composable query: estimate count"))?;

        Ok(extract_plan_rows(row.0))
    }
}

// Row type for event listing with optional search columns
#[derive(FromRow)]
struct EventListingRow {
    #[sqlx(flatten)]
    record: EventRecord,
    relevance_score: Option<f64>,
    snippet: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────
// Aggregation: Count
// ─────────────────────────────────────────────────────────────────────

impl EventRepository<'_> {
    async fn execute_count(&self, query: EventQuery) -> DbResult<EventQueryResult> {
        let mut qb =
            QueryBuilder::<Postgres>::new("SELECT COUNT(*) AS count FROM core.events WHERE TRUE");
        push_filters(&mut qb, &query);

        let row: CountRow = qb
            .build_query_as()
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "composable query: count"))?;

        Ok(EventQueryResult::Count { count: row.count })
    }

    async fn execute_count_by(&self, query: EventQuery) -> DbResult<EventQueryResult> {
        let (field, limit) = match &query.aggregation {
            Some(AggregationMode::CountBy { field, limit }) => {
                (field.clone(), (*limit).clamp(1, Pagination::MAX_LIMIT))
            }
            _ => unreachable!("called execute_count_by without CountBy aggregation"),
        };

        let mut qb = QueryBuilder::<Postgres>::new("SELECT ");
        push_group_by_expr(&mut qb, &field);
        qb.push(" AS key, COUNT(*) AS count FROM core.events WHERE TRUE");
        push_filters(&mut qb, &query);

        // For PayloadPath, exclude NULL keys
        if let GroupByField::PayloadPath(path) = &field {
            qb.push(" AND payload->>");
            qb.push_bind(path.clone());
            qb.push(" IS NOT NULL");
        }

        qb.push(" GROUP BY 1 ORDER BY count DESC LIMIT ");
        qb.push_bind(limit);

        let rows: Vec<GroupedCountRow> = qb
            .build_query_as()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "composable query: count_by"))?;

        let groups = rows
            .into_iter()
            .map(|r| GroupedCount {
                key: r.key.unwrap_or_default(),
                count: r.count,
            })
            .collect();

        Ok(EventQueryResult::GroupedCounts { groups })
    }

    async fn execute_time_series(&self, query: EventQuery) -> DbResult<EventQueryResult> {
        let (interval_minutes, order) = match &query.aggregation {
            Some(AggregationMode::TimeSeries {
                interval_minutes,
                order,
            }) => (*interval_minutes, *order),
            _ => unreachable!("called execute_time_series without TimeSeries aggregation"),
        };

        if interval_minutes <= 0 {
            return Err(sinex_primitives::SinexError::validation(
                "TimeSeries interval_minutes must be positive",
            )
            .with_context("interval_minutes", interval_minutes));
        }

        let interval = minutes_to_interval(interval_minutes);

        let mut qb = QueryBuilder::<Postgres>::new("SELECT time_bucket(");
        qb.push_bind(interval);
        qb.push("::interval, ts_orig) AS bucket, COUNT(*) AS count FROM core.events WHERE TRUE");
        push_filters(&mut qb, &query);
        qb.push(" GROUP BY bucket");

        match order {
            TimeSeriesOrder::TimeAsc => qb.push(" ORDER BY bucket ASC"),
            TimeSeriesOrder::CountDesc => qb.push(" ORDER BY count DESC"),
        };

        qb.push(" LIMIT ");
        qb.push_bind(Pagination::MAX_LIMIT);

        let rows: Vec<TimeBucketRow> = qb
            .build_query_as()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "composable query: time_series"))?;

        let buckets = rows
            .into_iter()
            .map(|r| TimeBucketEntry {
                bucket: r.bucket,
                count: r.count,
            })
            .collect();

        Ok(EventQueryResult::TimeSeries { buckets })
    }

    async fn execute_source_stats(&self, query: EventQuery) -> DbResult<EventQueryResult> {
        let limit = match &query.aggregation {
            Some(AggregationMode::SourceStats { limit }) => {
                (*limit).clamp(1, Pagination::MAX_LIMIT)
            }
            _ => unreachable!("called execute_source_stats without SourceStats aggregation"),
        };

        let mut qb = QueryBuilder::<Postgres>::new(
            "SELECT \
                source, \
                COUNT(*) AS event_count, \
                COUNT(DISTINCT event_type) AS event_type_count, \
                COUNT(DISTINCT host) AS host_count, \
                MIN(ts_coided) AS first_event, \
                MAX(ts_coided) AS last_event, \
                CAST(AVG(CASE WHEN ts_orig IS NOT NULL THEN EXTRACT(EPOCH FROM (ts_coided - ts_orig)) ELSE NULL END) AS DOUBLE PRECISION) AS avg_ingest_delay_secs \
            FROM core.events WHERE TRUE",
        );
        push_filters(&mut qb, &query);
        qb.push(" GROUP BY source ORDER BY event_count DESC LIMIT ");
        qb.push_bind(limit);

        let rows: Vec<SourceStatsRow> = qb
            .build_query_as()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "composable query: source_stats"))?;

        let sources = rows
            .into_iter()
            .map(|r| SourceStatsEntry {
                source: r.source.into(),
                event_count: r.event_count,
                event_type_count: r.event_type_count,
                host_count: r.host_count,
                first_event: r.first_event,
                last_event: r.last_event,
                avg_ingest_delay_secs: r.avg_ingest_delay_secs,
            })
            .collect();

        Ok(EventQueryResult::SourceStats { sources })
    }
}

// ─────────────────────────────────────────────────────────────────────
// Lineage: recursive CTEs
// ─────────────────────────────────────────────────────────────────────

impl EventRepository<'_> {
    async fn fetch_ancestors(
        &self,
        event_id: sinex_primitives::Id<Event<JsonValue>>,
        max_depth: u32,
    ) -> DbResult<Vec<LineageNode>> {
        let event_uuid = event_id.to_uuid();

        let sql = format!(
            "WITH RECURSIVE ancestors AS ( \
                SELECT e.*, 1 AS depth FROM core.events e \
                WHERE e.id = ANY( \
                    SELECT unnest(source_event_ids) FROM core.events WHERE id = $1::uuid \
                ) \
                UNION ALL \
                SELECT e.*, a.depth + 1 FROM core.events e \
                JOIN ancestors a ON e.id = ANY(a.source_event_ids) \
                WHERE a.depth < $2 \
            ) \
            SELECT {cols}, depth FROM ancestors",
            cols = event_select_columns!()
        );

        let rows: Vec<LineageRow> = sqlx::query_as(&sql)
            .bind(event_uuid)
            .bind(max_depth as i32)
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "lineage: ancestors"))?;

        rows_to_lineage_nodes(rows)
    }

    async fn fetch_descendants(
        &self,
        event_id: sinex_primitives::Id<Event<JsonValue>>,
        max_depth: u32,
    ) -> DbResult<Vec<LineageNode>> {
        let event_uuid = event_id.to_uuid();

        let sql = format!(
            "WITH RECURSIVE descendants AS ( \
                SELECT e.*, 1 AS depth FROM core.events e \
                WHERE $1::uuid = ANY(e.source_event_ids) \
                UNION ALL \
                SELECT e.*, d.depth + 1 FROM core.events e \
                JOIN descendants d ON d.id = ANY(e.source_event_ids) \
                WHERE d.depth < $2 \
            ) \
            SELECT {cols}, depth FROM descendants",
            cols = event_select_columns!()
        );

        let rows: Vec<LineageRow> = sqlx::query_as(&sql)
            .bind(event_uuid)
            .bind(max_depth as i32)
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "lineage: descendants"))?;

        rows_to_lineage_nodes(rows)
    }
}

// ─────────────────────────────────────────────────────────────────────
// Filter clause builder (shared by all query paths)
// ─────────────────────────────────────────────────────────────────────

fn push_filters(qb: &mut QueryBuilder<'_, Postgres>, query: &EventQuery) {
    if !query.sources.is_empty() {
        let values: Vec<String> = query
            .sources
            .iter()
            .map(|s| s.as_str().to_string())
            .collect();
        qb.push(" AND source = ANY(");
        qb.push_bind(values);
        qb.push(")");
    }

    if !query.event_types.is_empty() {
        let values: Vec<String> = query
            .event_types
            .iter()
            .map(|t| t.as_str().to_string())
            .collect();
        qb.push(" AND event_type = ANY(");
        qb.push_bind(values);
        qb.push(")");
    }

    if !query.hosts.is_empty() {
        let values: Vec<String> = query.hosts.iter().map(|h| h.as_str().to_string()).collect();
        qb.push(" AND host = ANY(");
        qb.push_bind(values);
        qb.push(")");
    }

    if let Some(ref range) = query.time_range {
        if let Some(start) = range.start() {
            qb.push(" AND ts_orig >= ");
            qb.push_bind(start);
        }
        if let Some(end) = range.end() {
            qb.push(" AND ts_orig <= ");
            qb.push_bind(end);
        }
    }

    if let Some(ref payload) = query.payload {
        push_payload_filter(qb, payload);
    }

    if let Some(has_lineage) = query.has_lineage {
        if has_lineage {
            qb.push(" AND source_event_ids IS NOT NULL");
        } else {
            qb.push(" AND source_event_ids IS NULL");
        }
    }

    if let Some(ref scope_key) = query.scope_key {
        qb.push(" AND scope_key = ");
        qb.push_bind(scope_key.clone());
    }

    if let Some(ref equivalence_key) = query.equivalence_key {
        qb.push(" AND equivalence_key = ");
        qb.push_bind(equivalence_key.clone());
    }
}

fn push_payload_filter(qb: &mut QueryBuilder<'_, Postgres>, filter: &PayloadFilter) {
    match filter {
        PayloadFilter::Contains { value } => {
            qb.push(" AND payload @> ");
            qb.push_bind(value.clone());
        }
        PayloadFilter::TextSearch { text } => {
            qb.push(" AND to_tsvector('simple', payload::text) @@ websearch_to_tsquery('simple', ");
            qb.push_bind(text.clone());
            qb.push(")");
        }
        PayloadFilter::HasKey { key } => {
            qb.push(" AND payload ? ");
            qb.push_bind(key.clone());
        }
        PayloadFilter::Path { path, op } => {
            push_path_op(qb, path, op);
        }
        PayloadFilter::And { filters } => {
            qb.push(" AND (TRUE");
            for f in filters {
                push_payload_filter(qb, f);
            }
            qb.push(")");
        }
        PayloadFilter::Or { filters } => {
            qb.push(" AND (FALSE");
            for f in filters {
                // Each sub-filter adds " AND ..." but inside OR we need " OR ..."
                // So we use a different approach: build each as " OR (TRUE <filter>)"
                qb.push(" OR (TRUE");
                push_payload_filter(qb, f);
                qb.push(")");
            }
            qb.push(")");
        }
        PayloadFilter::Not { filter } => {
            qb.push(" AND NOT (TRUE");
            push_payload_filter(qb, filter);
            qb.push(")");
        }
    }
}

fn push_group_by_expr(qb: &mut QueryBuilder<'_, Postgres>, field: &GroupByField) {
    match field {
        GroupByField::Source => {
            qb.push("source");
        }
        GroupByField::EventType => {
            qb.push("event_type");
        }
        GroupByField::Host => {
            qb.push("host");
        }
        GroupByField::PayloadPath(path) => {
            qb.push("payload->>");
            qb.push_bind(path.clone());
        }
    }
}

fn push_path_op(qb: &mut QueryBuilder<'_, Postgres>, path: &str, op: &PathOp) {
    match op {
        PathOp::Eq(val) => {
            if val.is_number() {
                push_numeric_payload_path_expr(qb, path);
                qb.push(" = ");
                // Extract numeric value
                push_json_numeric(qb, val);
            } else {
                qb.push(" AND payload->>");
                qb.push_bind(path.to_string());
                qb.push(" = ");
                qb.push_bind(json_to_text(val));
            }
        }
        PathOp::Gt(val) => {
            push_numeric_payload_path_expr(qb, path);
            qb.push(" > ");
            push_json_numeric(qb, val);
        }
        PathOp::Gte(val) => {
            push_numeric_payload_path_expr(qb, path);
            qb.push(" >= ");
            push_json_numeric(qb, val);
        }
        PathOp::Lt(val) => {
            push_numeric_payload_path_expr(qb, path);
            qb.push(" < ");
            push_json_numeric(qb, val);
        }
        PathOp::Lte(val) => {
            push_numeric_payload_path_expr(qb, path);
            qb.push(" <= ");
            push_json_numeric(qb, val);
        }
        PathOp::Like(pattern) => {
            qb.push(" AND payload->>");
            qb.push_bind(path.to_string());
            qb.push(" LIKE ");
            qb.push_bind(pattern.clone());
        }
        PathOp::IsNull => {
            qb.push(" AND payload->>");
            qb.push_bind(path.to_string());
            qb.push(" IS NULL");
        }
        PathOp::IsNotNull => {
            qb.push(" AND payload->>");
            qb.push_bind(path.to_string());
            qb.push(" IS NOT NULL");
        }
    }
}

fn push_numeric_payload_path_expr(qb: &mut QueryBuilder<'_, Postgres>, path: &str) {
    qb.push(" AND jsonb_typeof(payload->");
    qb.push_bind(path.to_string());
    qb.push(") = 'number' AND (payload->>");
    qb.push_bind(path.to_string());
    qb.push(")::numeric");
}

fn push_json_numeric(qb: &mut QueryBuilder<'_, Postgres>, val: &JsonValue) {
    if let Some(n) = val.as_f64() {
        qb.push_bind(n);
    } else if let Some(n) = val.as_i64() {
        qb.push_bind(n as f64);
    } else {
        // Fallback: bind as text and let Postgres cast
        qb.push_bind(val.to_string());
    }
}

fn json_to_text(val: &JsonValue) -> String {
    match val {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────
// Cursor & direction helpers
// ─────────────────────────────────────────────────────────────────────

fn push_cursor(qb: &mut QueryBuilder<'_, Postgres>, cursor: &Cursor, direction: SortDirection) {
    if let Some(ref after) = cursor.after {
        let uuid = after.to_uuid();
        match direction {
            SortDirection::Desc => {
                qb.push(" AND id < ");
                qb.push_bind(uuid);
                qb.push("::uuid");
            }
            SortDirection::Asc => {
                qb.push(" AND id > ");
                qb.push_bind(uuid);
                qb.push("::uuid");
            }
        }
    }
    if let Some(ref before) = cursor.before {
        let uuid = before.to_uuid();
        match direction {
            SortDirection::Desc => {
                qb.push(" AND id > ");
                qb.push_bind(uuid);
                qb.push("::uuid");
            }
            SortDirection::Asc => {
                qb.push(" AND id < ");
                qb.push_bind(uuid);
                qb.push("::uuid");
            }
        }
    }
}

fn direction_sql(dir: SortDirection) -> &'static str {
    match dir {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    }
}

// ─────────────────────────────────────────────────────────────────────
// Internal row types (for sqlx::FromRow)
// ─────────────────────────────────────────────────────────────────────

#[derive(FromRow)]
struct CountRow {
    count: i64,
}

#[derive(FromRow)]
struct GroupedCountRow {
    key: Option<String>,
    count: i64,
}

#[derive(FromRow)]
struct TimeBucketRow {
    bucket: Timestamp,
    count: i64,
}

#[derive(FromRow)]
struct SourceStatsRow {
    source: String,
    event_count: i64,
    event_type_count: i64,
    host_count: i64,
    first_event: Option<Timestamp>,
    last_event: Option<Timestamp>,
    avg_ingest_delay_secs: Option<f64>,
}

#[derive(FromRow)]
struct LineageRow {
    #[sqlx(flatten)]
    record: EventRecord,
    depth: i32,
}

fn rows_to_lineage_nodes(rows: Vec<LineageRow>) -> DbResult<Vec<LineageNode>> {
    let mut nodes = Vec::with_capacity(rows.len());
    for row in rows {
        let event = row.record.try_to_event()?;
        nodes.push(LineageNode {
            event,
            depth: row.depth as u32,
        });
    }
    Ok(nodes)
}

// ─────────────────────────────────────────────────────────────────────
// Utility
// ─────────────────────────────────────────────────────────────────────

fn minutes_to_interval(minutes: i32) -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::from(minutes) * 60 * 1_000_000,
    }
}
