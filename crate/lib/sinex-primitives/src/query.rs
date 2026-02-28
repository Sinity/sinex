use crate::domain::{EventSource, EventType, HostName};
use crate::error::SinexError;
use crate::events::Event;
use crate::ids::Id;
use crate::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Shared limit/offset helper that clamps inputs and centralizes defaults.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pagination {
    limit: i64,
    offset: i64,
}

impl Pagination {
    /// Default limit applied when callers omit or pass invalid input.
    pub const DEFAULT_LIMIT: i64 = 100;
    /// Global maximum limit enforced across services unless overridden.
    pub const MAX_LIMIT: i64 = 1000;

    /// Construct pagination using standard defaults.
    #[must_use]
    pub fn new(limit: Option<i64>, offset: Option<i64>) -> Self {
        Self::with_bounds(limit, offset, Self::DEFAULT_LIMIT, Self::MAX_LIMIT)
    }

    /// Construct pagination with a custom default limit but global max.
    #[must_use]
    pub fn with_default(limit: Option<i64>, offset: Option<i64>, default_limit: i64) -> Self {
        Self::with_bounds(limit, offset, default_limit, Self::MAX_LIMIT)
    }

    /// Construct pagination with fully custom defaults and max limit.
    #[must_use]
    pub fn with_bounds(
        limit: Option<i64>,
        offset: Option<i64>,
        default_limit: i64,
        max_limit: i64,
    ) -> Self {
        assert!(
            default_limit > 0,
            "default pagination limit must be positive"
        );
        assert!(
            max_limit >= default_limit,
            "max pagination limit must be >= default limit"
        );

        let limit = limit.unwrap_or(default_limit);
        let limit = if limit <= 0 { default_limit } else { limit };
        let limit = limit.min(max_limit);

        let offset = offset.unwrap_or(0);
        let offset = offset.max(0);

        Self { limit, offset }
    }

    /// Apply a tighter max limit post-construction (useful for endpoints with stricter caps).
    #[must_use]
    pub fn clamp_max(self, max_limit: i64) -> Self {
        assert!(max_limit > 0, "max pagination limit must be positive");
        let limit = self.limit.min(max_limit);
        Self {
            limit,
            offset: self.offset,
        }
    }

    #[must_use]
    pub fn limit(&self) -> i64 {
        self.limit
    }

    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    #[must_use]
    pub fn as_tuple(&self) -> (i64, i64) {
        (self.limit, self.offset)
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            limit: Self::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

/// Helper for validating optional `(start, end)` timestamps.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeRange {
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}

impl TimeRange {
    pub fn new(start: Option<Timestamp>, end: Option<Timestamp>) -> Result<Self, SinexError> {
        if let (Some(start), Some(end)) = (start, end) {
            if start > end {
                return Err(
                    SinexError::validation("start_time must be earlier than end_time")
                        .with_context("start_time", start)
                        .with_context("end_time", end),
                );
            }
        }

        Ok(Self { start, end })
    }

    #[must_use]
    pub fn start(&self) -> Option<Timestamp> {
        self.start
    }

    #[must_use]
    pub fn end(&self) -> Option<Timestamp> {
        self.end
    }

    #[must_use]
    pub fn contains(&self, ts: Timestamp) -> bool {
        if let Some(start) = self.start {
            if ts < start {
                return false;
            }
        }
        if let Some(end) = self.end {
            if ts > end {
                return false;
            }
        }
        true
    }
}

// ─────────────────────────────────────────────────────────────────────
// Composable Event Query Engine types
// ─────────────────────────────────────────────────────────────────────

const fn default_limit() -> i64 {
    Pagination::DEFAULT_LIMIT
}

const fn default_max_depth() -> u32 {
    10
}

const fn default_agg_limit() -> i64 {
    100
}

/// Composable event query. All filter fields AND-combine. Empty vec = no filter.
///
/// Replaces 22+ hardcoded query methods with a single composable request type.
/// Supports filtering, cursor-based pagination, text search, payload predicates,
/// and aggregation modes — all through one unified interface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventQuery {
    // ── Filters ──
    #[serde(default)]
    pub sources: Vec<EventSource>,
    #[serde(default)]
    pub event_types: Vec<EventType>,
    #[serde(default)]
    pub hosts: Vec<HostName>,
    #[serde(default)]
    pub time_range: Option<TimeRange>,
    #[serde(default)]
    pub payload: Option<PayloadFilter>,

    // ── Pagination ──
    #[serde(default)]
    pub cursor: Option<Cursor>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub direction: SortDirection,

    // ── Mode ──
    #[serde(default)]
    pub aggregation: Option<AggregationMode>,

    // ── Options ──
    #[serde(default)]
    pub include_total_estimate: bool,
}

impl EventQuery {
    /// Validate and clamp query parameters.
    pub fn validate(&mut self) -> Result<(), SinexError> {
        self.limit = self.limit.clamp(1, Pagination::MAX_LIMIT);

        if let Some(ref payload) = self.payload {
            payload.validate_depth(0)?;
        }

        Ok(())
    }
}

impl Default for EventQuery {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            event_types: Vec::new(),
            hosts: Vec::new(),
            time_range: None,
            payload: None,
            cursor: None,
            limit: default_limit(),
            direction: SortDirection::default(),
            aggregation: None,
            include_total_estimate: false,
        }
    }
}

/// ULID-based keyset pagination. O(1) seek instead of O(n) offset skip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(default)]
    pub after: Option<Id<Event<JsonValue>>>,
    #[serde(default)]
    pub before: Option<Id<Event<JsonValue>>>,
}

/// Sort direction for event listing.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    #[default]
    Desc,
}

/// Composable payload predicates. Maps directly to PostgreSQL JSONB operations.
///
/// Supports boolean composition via `And`, `Or`, `Not` — arbitrary nesting
/// up to a depth limit of 8 to prevent pathological queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PayloadFilter {
    /// `payload @> value` — JSONB containment
    Contains { value: JsonValue },
    /// Full-text search via `websearch_to_tsquery`
    TextSearch { text: String },
    /// `payload ? key` — top-level key existence
    HasKey { key: String },
    /// `payload->>path <op> value` — path-based comparison
    Path { path: String, op: PathOp },
    /// All sub-filters must match
    And { filters: Vec<PayloadFilter> },
    /// Any sub-filter must match
    Or { filters: Vec<PayloadFilter> },
    /// Negate a filter
    Not { filter: Box<PayloadFilter> },
}

/// Maximum nesting depth for PayloadFilter boolean trees.
const MAX_FILTER_DEPTH: u32 = 8;

impl PayloadFilter {
    fn validate_depth(&self, depth: u32) -> Result<(), SinexError> {
        if depth > MAX_FILTER_DEPTH {
            return Err(SinexError::validation("PayloadFilter nesting too deep")
                .with_context("max_depth", MAX_FILTER_DEPTH)
                .with_context("actual_depth", depth));
        }
        match self {
            Self::And { filters } | Self::Or { filters } => {
                for f in filters {
                    f.validate_depth(depth + 1)?;
                }
            }
            Self::Not { filter } => filter.validate_depth(depth + 1)?,
            Self::Contains { .. }
            | Self::TextSearch { .. }
            | Self::HasKey { .. }
            | Self::Path { .. } => {}
        }
        Ok(())
    }
}

/// Path-based comparison operator for JSONB fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", content = "value", rename_all = "snake_case")]
pub enum PathOp {
    Eq(JsonValue),
    Gt(JsonValue),
    Gte(JsonValue),
    Lt(JsonValue),
    Lte(JsonValue),
    Like(String),
    IsNull,
    IsNotNull,
}

/// Aggregation mode — replaces event listing with grouped/bucketed results.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum AggregationMode {
    /// Total count of matching events
    Count,
    /// Count grouped by a dimension
    CountBy {
        field: GroupByField,
        #[serde(default = "default_agg_limit")]
        limit: i64,
    },
    /// Time-bucketed counts (TimescaleDB `time_bucket`)
    TimeSeries {
        interval_minutes: i32,
        #[serde(default)]
        order: TimeSeriesOrder,
    },
    /// Per-source comprehensive statistics
    SourceStats {
        #[serde(default = "default_agg_limit")]
        limit: i64,
    },
}

/// Dimension to group counts by.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupByField {
    Source,
    EventType,
    Host,
    /// Group by a specific JSON path in the payload (e.g. `"command"`)
    PayloadPath(String),
}

/// Ordering for time-series aggregation buckets.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeSeriesOrder {
    #[default]
    TimeAsc,
    CountDesc,
}

// ─────────────────────────────────────────────────────────────────────
// Provenance lineage traversal
// ─────────────────────────────────────────────────────────────────────

/// Provenance graph traversal request.
///
/// Given a root event, traverse the provenance chain in one or both directions:
/// - **Ancestors**: follow `source_event_ids` backwards to raw materials
/// - **Descendants**: find events that reference this event as a parent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageQuery {
    pub event_id: Id<Event<JsonValue>>,
    #[serde(default)]
    pub direction: LineageDirection,
    #[serde(default = "default_max_depth")]
    pub max_depth: u32,
}

impl LineageQuery {
    /// Validate and clamp parameters.
    pub fn validate(&mut self) -> Result<(), SinexError> {
        self.max_depth = self.max_depth.clamp(1, 50);
        Ok(())
    }
}

/// Direction for lineage traversal.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LineageDirection {
    Ancestors,
    Descendants,
    #[default]
    Both,
}

// ─────────────────────────────────────────────────────────────────────
// Result types
// ─────────────────────────────────────────────────────────────────────

/// Result from `events.query` — tagged enum based on whether aggregation was requested.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventQueryResult {
    /// Event listing with cursor pagination
    Events {
        events: Vec<QueryResultEvent>,
        #[serde(skip_serializing_if = "Option::is_none")]
        next_cursor: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        total_estimate: Option<i64>,
    },
    /// Single count
    Count { count: i64 },
    /// Counts grouped by a dimension
    GroupedCounts { groups: Vec<GroupedCount> },
    /// Time-bucketed counts
    TimeSeries { buckets: Vec<TimeBucketEntry> },
    /// Per-source statistics
    SourceStats { sources: Vec<SourceStatsEntry> },
}

/// An event enriched with optional search-relevance metadata.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResultEvent {
    #[serde(flatten)]
    pub event: Event<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevance_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

/// A key/count pair from a `CountBy` aggregation.
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupedCount {
    pub key: String,
    pub count: i64,
}

/// A time-bucket/count pair from a `TimeSeries` aggregation.
#[derive(Debug, Serialize, Deserialize)]
pub struct TimeBucketEntry {
    pub bucket: Timestamp,
    pub count: i64,
}

/// Per-source statistics from `SourceStats` aggregation.
#[derive(Debug, Serialize, Deserialize)]
pub struct SourceStatsEntry {
    pub source: EventSource,
    pub event_count: i64,
    pub event_type_count: i64,
    pub host_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_event: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_ingest_delay_secs: Option<f64>,
}

/// Result from `events.lineage` — the root event plus its provenance graph.
#[derive(Debug, Serialize, Deserialize)]
pub struct LineageResult {
    pub root: Event<JsonValue>,
    pub ancestors: Vec<LineageNode>,
    pub descendants: Vec<LineageNode>,
}

/// A single node in the provenance graph with its depth from the root.
#[derive(Debug, Serialize, Deserialize)]
pub struct LineageNode {
    pub event: Event<JsonValue>,
    pub depth: u32,
}
