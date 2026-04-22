//! Composable event query engine — unified read path.
//!
//! Exposes two composable entry points:
//! - `EventRepository::query()` — filter + paginate + aggregate events
//! - `EventRepository::lineage()` — traverse provenance chains via recursive CTE

use super::conversions::EventRecordExt;
use super::queries::extract_plan_rows;
use crate::EventRecord;
use crate::JsonValue;
use crate::models::Event;
use crate::repositories::DbPoolExt;
use crate::repositories::common::{DbResult, db_error};
use sinex_primitives::query::{
    AggregationMode, Cursor, CursorAnchor, EventQuery, EventQueryResult, GroupByField,
    GroupedCount, GroupedValue, GroupedValueAggregation, LineageDirection, LineageNode,
    LineageQuery, LineageResult, NumericField, PathOp, PayloadFilter, QueryResultEvent,
    SortDirection, SourceMaterialLinkInfo, SourceStatsEntry, TimeBucketEntry, TimeSeriesOrder,
};
use sinex_primitives::{Id, Pagination, Provenance, SinexError, Timestamp, Uuid};
use sqlx::postgres::types::PgInterval;
use sqlx::{FromRow, Postgres, QueryBuilder};
use std::collections::BTreeSet;
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
                AggregationMode::SumBy { .. } | AggregationMode::AvgBy { .. } => {
                    self.execute_grouped_values(query).await
                }
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

        let material_ids = collect_lineage_material_ids(&root, &ancestors, &descendants);
        let material_links = self
            .pool
            .source_materials()
            .links_for_materials(&material_ids)
            .await?
            .into_iter()
            .map(|row| SourceMaterialLinkInfo {
                from_material_id: row.from_material_id,
                to_material_id: row.to_material_id,
                relation_type: row.relation_type,
                metadata: row.metadata,
                created_at: row.created_at,
            })
            .collect();

        Ok(LineageResult {
            root,
            ancestors,
            descendants,
            material_links,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────
// Event listing (no aggregation)
// ─────────────────────────────────────────────────────────────────────

impl EventRepository<'_> {
    async fn execute_event_listing(&self, query: EventQuery) -> DbResult<EventQueryResult> {
        let text_search_terms = query
            .payload
            .as_ref()
            .map(PayloadFilter::positive_text_search_terms)
            .unwrap_or_default();
        let has_text_search = !text_search_terms.is_empty();

        let fetch_limit = query.limit + 1; // +1 to detect "has more"

        let mut qb = QueryBuilder::<Postgres>::new(format!(
            "SELECT * FROM (SELECT {}",
            event_select_columns!()
        ));

        if has_text_search {
            push_text_search_projection(&mut qb, &text_search_terms);
        } else {
            qb.push(", NULL::float8 AS relevance_score, NULL::text AS snippet");
        }

        qb.push(" FROM core.events WHERE TRUE");
        push_filters(&mut qb, &query);
        qb.push(") AS listing WHERE TRUE");

        if let Some(ref cursor) = query.cursor {
            push_cursor(&mut qb, cursor, query.direction, has_text_search);
        }

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

        let next_cursor = if has_more {
            rows.last()
                .map(|row| event_listing_cursor(row, has_text_search))
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

        Ok(extract_plan_rows(&row.0))
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

fn event_listing_cursor(row: &EventListingRow, has_text_search: bool) -> Cursor {
    let mut anchor = CursorAnchor::from_id(Id::from_uuid(row.record.id));
    if has_text_search {
        // Truncate to 6 decimal places to match the TRUNC(...)::float8 projection in
        // push_text_search_projection. This ensures the cursor value is bit-for-bit
        // identical to the projected score, preventing float8 round-trip precision loss
        // (f64 → JSON → f64) from causing rows to be skipped or duplicated during pagination.
        let score = (row.relevance_score.unwrap_or(0.0) * 1_000_000.0).trunc() / 1_000_000.0;
        anchor = anchor.with_relevance_score(score);
    }
    Cursor::after_anchor(anchor)
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

        qb.push(" GROUP BY 1 ORDER BY count DESC, key ASC LIMIT ");
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
            TimeSeriesOrder::CountDesc => qb.push(" ORDER BY count DESC, bucket ASC"),
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

    async fn execute_grouped_values(&self, query: EventQuery) -> DbResult<EventQueryResult> {
        let (field, value_field, aggregation, limit) = match &query.aggregation {
            Some(AggregationMode::SumBy {
                field,
                value_field,
                limit,
            }) => (
                field.clone(),
                value_field.clone(),
                GroupedValueAggregation::Sum,
                (*limit).clamp(1, Pagination::MAX_LIMIT),
            ),
            Some(AggregationMode::AvgBy {
                field,
                value_field,
                limit,
            }) => (
                field.clone(),
                value_field.clone(),
                GroupedValueAggregation::Avg,
                (*limit).clamp(1, Pagination::MAX_LIMIT),
            ),
            _ => unreachable!("called execute_grouped_values without SumBy/AvgBy aggregation"),
        };

        let mut qb = QueryBuilder::<Postgres>::new("SELECT ");
        push_group_by_expr(&mut qb, &field);
        qb.push(" AS key, CAST(");
        qb.push(match aggregation {
            GroupedValueAggregation::Sum => "SUM(",
            GroupedValueAggregation::Avg => "AVG(",
        });
        push_numeric_field_expr(&mut qb, &value_field);
        qb.push(
            ") AS DOUBLE PRECISION) AS value, COUNT(*) AS sample_count FROM core.events WHERE TRUE",
        );
        push_filters(&mut qb, &query);
        qb.push(" AND ");
        push_numeric_field_expr(&mut qb, &value_field);
        qb.push(" IS NOT NULL");

        if let GroupByField::PayloadPath(path) = &field {
            qb.push(" AND payload->>");
            qb.push_bind(path.clone());
            qb.push(" IS NOT NULL");
        }

        qb.push(" GROUP BY 1 ORDER BY value DESC, key ASC LIMIT ");
        qb.push_bind(limit);

        let rows: Vec<GroupedValueRow> = qb
            .build_query_as()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "composable query: grouped_values"))?;

        let groups = rows
            .into_iter()
            .map(|row| GroupedValue {
                key: row.key.unwrap_or_default(),
                value: row.value.unwrap_or_default(),
                sample_count: row.sample_count,
            })
            .collect();

        Ok(EventQueryResult::GroupedValues {
            aggregation,
            groups,
        })
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
        qb.push(" GROUP BY source ORDER BY event_count DESC, source ASC LIMIT ");
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

fn push_numeric_field_expr(qb: &mut QueryBuilder<'_, Postgres>, field: &NumericField) {
    match field {
        NumericField::PayloadPath(path) => {
            push_numeric_payload_path_sql_expr(qb, path);
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
    qb.push(" AND ");
    push_numeric_payload_path_sql_expr(qb, path);
}

fn push_numeric_payload_path_sql_expr(qb: &mut QueryBuilder<'_, Postgres>, path: &str) {
    qb.push("CASE WHEN jsonb_typeof(payload->");
    qb.push_bind(path.to_string());
    qb.push(") = 'number' THEN (payload->>");
    qb.push_bind(path.to_string());
    qb.push(")::numeric ELSE NULL END");
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

fn push_cursor(
    qb: &mut QueryBuilder<'_, Postgres>,
    cursor: &Cursor,
    direction: SortDirection,
    has_text_search: bool,
) {
    if has_text_search {
        if let Some(ref after) = cursor.after {
            push_ranked_cursor_clause(qb, after, direction, true);
        }
        if let Some(ref before) = cursor.before {
            push_ranked_cursor_clause(qb, before, direction, false);
        }
        return;
    }

    if let Some(ref after) = cursor.after {
        let uuid = after.id.to_uuid();
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
        let uuid = before.id.to_uuid();
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

fn push_text_search_projection(qb: &mut QueryBuilder<'_, Postgres>, terms: &[String]) {
    // Known limitation: when TextSearch filters are nested in Or/And combinators,
    // relevance scoring and snippet generation use a single combined tsquery (all positive
    // terms OR'd together) regardless of combinator semantics. A row that matches only term
    // A in an Or(A, B) will be ranked and highlighted as if both A and B were relevant.
    // This is correct enough for display — the filter WHERE clause still enforces the exact
    // combinator semantics — but may produce lower-quality snippets for multi-term Or queries.
    qb.push(", TRUNC(ts_rank_cd(");
    push_text_search_vector_expr(qb);
    qb.push(", ");
    push_text_search_query_expr(qb, terms);
    // Truncate to 6 decimal places so the stored projection value exactly matches the
    // cursor value built in event_listing_cursor. Without truncation, float8 round-trip
    // through JSON serialization (f64 → JSON number → f64) can lose precision, causing
    // rows to be skipped or duplicated across pages.
    qb.push(")::numeric, 6)::float8 AS relevance_score");

    // COALESCE ensures callers always receive '' rather than NULL when ts_headline finds no
    // highlighted fragment (e.g. very short payloads, or the matched tsquery lexeme does not
    // align with any word boundary that MaxFragments/MinWords can anchor on).
    qb.push(", COALESCE(CASE WHEN ");
    push_text_search_vector_expr(qb);
    qb.push(" @@ ");
    push_text_search_query_expr(qb, terms);
    qb.push(" THEN ts_headline('simple', payload::text, ");
    push_text_search_query_expr(qb, terms);
    qb.push(", 'MaxFragments=2, MinWords=8, MaxWords=24') ELSE NULL END, '') AS snippet");
}

fn push_text_search_vector_expr(qb: &mut QueryBuilder<'_, Postgres>) {
    qb.push("to_tsvector('simple', payload::text)");
}

fn push_text_search_query_expr(qb: &mut QueryBuilder<'_, Postgres>, terms: &[String]) {
    debug_assert!(!terms.is_empty());
    qb.push("(");
    for (index, term) in terms.iter().enumerate() {
        if index > 0 {
            qb.push(" || ");
        }
        qb.push("websearch_to_tsquery('simple', ");
        qb.push_bind(term.clone());
        qb.push(")");
    }
    qb.push(")");
}

fn push_ranked_cursor_clause(
    qb: &mut QueryBuilder<'_, Postgres>,
    anchor: &CursorAnchor,
    direction: SortDirection,
    is_after: bool,
) {
    let uuid = anchor.id.to_uuid();
    let score = anchor.relevance_score.unwrap_or(0.0);
    let (score_cmp, id_cmp) = match (is_after, direction) {
        (true, SortDirection::Desc) => ("<", "<"),
        (true, SortDirection::Asc) => ("<", ">"),
        (false, SortDirection::Desc) => (">", ">"),
        (false, SortDirection::Asc) => (">", "<"),
    };

    qb.push(" AND (relevance_score ");
    qb.push(score_cmp);
    qb.push(" ");
    qb.push_bind(score);
    qb.push(" OR (relevance_score = ");
    qb.push_bind(score);
    qb.push(" AND id ");
    qb.push(id_cmp);
    qb.push(" ");
    qb.push_bind(uuid);
    qb.push("::uuid))");
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
struct GroupedValueRow {
    key: Option<String>,
    value: Option<f64>,
    sample_count: i64,
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

fn collect_lineage_material_ids(
    root: &Event<JsonValue>,
    ancestors: &[LineageNode],
    descendants: &[LineageNode],
) -> Vec<Uuid> {
    let mut ids = BTreeSet::new();
    collect_event_material_id(root, &mut ids);
    for node in ancestors {
        collect_event_material_id(&node.event, &mut ids);
    }
    for node in descendants {
        collect_event_material_id(&node.event, &mut ids);
    }
    ids.into_iter().collect()
}

fn collect_event_material_id(event: &Event<JsonValue>, ids: &mut BTreeSet<Uuid>) {
    if let Provenance::Material { id, .. } = &event.provenance {
        ids.insert(id.to_uuid());
    }
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
