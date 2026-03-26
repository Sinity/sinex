use crate::Timestamp;
use crate::domain::{EventSource, EventType, HostName};
use crate::error::SinexError;
use crate::events::Event;
use crate::ids::Id;
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
        if let (Some(start), Some(end)) = (start, end)
            && start >= end
        {
            return Err(
                SinexError::validation("start_time must be strictly earlier than end_time")
                    .with_context("start_time", start)
                    .with_context("end_time", end),
            );
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
        if let Some(start) = self.start
            && ts < start
        {
            return false;
        }
        if let Some(end) = self.end
            && ts > end
        {
            return false;
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

    /// Filter to events with non-null `source_event_ids` (synthesis events).
    /// When `true`, only synthesis events are returned.
    /// When `false`, only material (non-synthesis) events are returned.
    /// When `None`, no lineage filter is applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_lineage: Option<bool>,

    /// Filter to events with a specific `scope_key`.
    ///
    /// Used by scope reconciler nodes to load the working set for a scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_key: Option<String>,

    /// Filter to events produced by a specific source (for working set queries).
    ///
    /// Combined with `scope_key`, allows loading the current working set for
    /// a scope reconciler node: all live events in that scope from that source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equivalence_key: Option<String>,
}

impl EventQuery {
    /// Validate and clamp query parameters.
    pub fn validate(&mut self) -> Result<(), SinexError> {
        self.limit = self.limit.clamp(1, Pagination::MAX_LIMIT);

        if let Some(time_range) = self.time_range {
            TimeRange::new(time_range.start(), time_range.end())?;
        }

        if let Some(ref payload) = self.payload {
            payload.validate_depth(0)?;
            if let Some(ref cursor) = self.cursor {
                cursor.validate(payload.has_positive_text_search())?;
            }
        } else if let Some(ref cursor) = self.cursor {
            cursor.validate(false)?;
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
            has_lineage: None,
            scope_key: None,
            equivalence_key: None,
        }
    }
}

/// UUIDv7-based keyset pagination. O(1) seek instead of O(n) offset skip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(default)]
    pub after: Option<CursorAnchor>,
    #[serde(default)]
    pub before: Option<CursorAnchor>,
}

impl Cursor {
    #[must_use]
    pub fn after_id(id: Id<Event<JsonValue>>) -> Self {
        Self {
            after: Some(CursorAnchor::from_id(id)),
            before: None,
        }
    }

    #[must_use]
    pub fn after_anchor(anchor: CursorAnchor) -> Self {
        Self {
            after: Some(anchor),
            before: None,
        }
    }

    fn validate(&self, requires_relevance_score: bool) -> Result<(), SinexError> {
        match (&self.after, &self.before) {
            (Some(_), Some(_)) => {
                return Err(SinexError::validation(
                    "cursor cannot specify both after and before anchors",
                ));
            }
            (None, None) => return Err(SinexError::validation("cursor anchor is missing")),
            _ => {}
        }

        for (label, anchor) in [("after", self.after.as_ref()), ("before", self.before.as_ref())] {
            let Some(anchor) = anchor else {
                continue;
            };

            if let Some(score) = anchor.relevance_score {
                if !score.is_finite() {
                    return Err(SinexError::validation("cursor relevance_score must be finite")
                        .with_context("anchor", label)
                        .with_context("relevance_score", score.to_string()));
                }
            } else if requires_relevance_score {
                return Err(
                    SinexError::validation("text-search pagination cursor requires relevance_score")
                        .with_context("anchor", label),
                );
            }
        }

        Ok(())
    }
}

/// Stable cursor anchor for ordered event listings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorAnchor {
    pub id: Id<Event<JsonValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relevance_score: Option<f64>,
}

impl CursorAnchor {
    #[must_use]
    pub fn from_id(id: Id<Event<JsonValue>>) -> Self {
        Self {
            id,
            relevance_score: None,
        }
    }

    #[must_use]
    pub fn with_relevance_score(mut self, relevance_score: f64) -> Self {
        self.relevance_score = Some(relevance_score);
        self
    }
}

/// Sort direction for event listing.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    #[default]
    Desc,
}

/// Composable payload predicates. Maps directly to `PostgreSQL` JSONB operations.
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

/// Maximum nesting depth for `PayloadFilter` boolean trees.
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

    #[must_use]
    pub fn has_positive_text_search(&self) -> bool {
        match self {
            Self::TextSearch { .. } => true,
            Self::And { filters } | Self::Or { filters } => {
                filters.iter().any(Self::has_positive_text_search)
            }
            Self::Not { .. }
            | Self::Contains { .. }
            | Self::HasKey { .. }
            | Self::Path { .. } => false,
        }
    }

    #[must_use]
    pub fn contains_text_search(&self) -> bool {
        match self {
            Self::TextSearch { .. } => true,
            Self::And { filters } | Self::Or { filters } => {
                filters.iter().any(Self::contains_text_search)
            }
            Self::Not { filter } => filter.contains_text_search(),
            Self::Contains { .. } | Self::HasKey { .. } | Self::Path { .. } => false,
        }
    }

    #[must_use]
    pub fn positive_text_search_terms(&self) -> Vec<String> {
        let mut terms = Vec::new();
        self.collect_positive_text_search_terms(false, &mut terms);
        terms
    }

    fn collect_positive_text_search_terms(&self, negated: bool, terms: &mut Vec<String>) {
        match self {
            Self::TextSearch { text } if !negated => {
                if !terms.iter().any(|existing| existing == text) {
                    terms.push(text.clone());
                }
            }
            Self::And { filters } | Self::Or { filters } => {
                for filter in filters {
                    filter.collect_positive_text_search_terms(negated, terms);
                }
            }
            Self::Not { filter } => {
                filter.collect_positive_text_search_terms(!negated, terms);
            }
            Self::Contains { .. }
            | Self::TextSearch { .. }
            | Self::HasKey { .. }
            | Self::Path { .. } => {}
        }
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
    /// Time-bucketed counts (`TimescaleDB` `time_bucket`)
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventQueryResult {
    /// Event listing with cursor pagination
    Events {
        events: Vec<QueryResultEvent>,
        #[serde(skip_serializing_if = "Option::is_none")]
        next_cursor: Option<Cursor>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultEvent {
    #[serde(flatten)]
    pub event: Event<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevance_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

/// A key/count pair from a `CountBy` aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupedCount {
    pub key: String,
    pub count: i64,
}

/// A time-bucket/count pair from a `TimeSeries` aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeBucketEntry {
    pub bucket: Timestamp,
    pub count: i64,
}

/// Per-source statistics from `SourceStats` aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

// ─────────────────────────────────────────────────────────────────────
// Real-time subscription filter (in-memory event matching for SSE)
// ─────────────────────────────────────────────────────────────────────

/// Maximum nesting depth for in-memory payload filter evaluation.
/// Tighter than SQL's `MAX_FILTER_DEPTH` (8) because we recurse on the stack.
const MAX_SUBSCRIPTION_FILTER_DEPTH: u32 = 4;

/// Filter for SSE event streams. All conditions AND-combine.
/// Empty vec = no filter on that dimension (matches all).
///
/// Deliberately excludes `time_range` — live streams deliver future events only.
/// Reuses the same field types as [`EventQuery`] for consistency.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubscriptionFilter {
    #[serde(default)]
    pub sources: Vec<EventSource>,
    #[serde(default)]
    pub event_types: Vec<EventType>,
    #[serde(default)]
    pub hosts: Vec<HostName>,
    #[serde(default)]
    pub payload: Option<PayloadFilter>,
}

impl SubscriptionFilter {
    /// Validate the filter (depth check on payload filter).
    pub fn validate(&self) -> Result<(), SinexError> {
        if let Some(ref pf) = self.payload {
            if pf.contains_text_search() {
                return Err(
                    SinexError::validation(
                        "SubscriptionFilter does not support payload text search",
                    )
                    .with_context("reason", "events.stream uses in-memory matching, not PostgreSQL full-text search"),
                );
            }
            pf.validate_depth(0)?;
            // Apply tighter depth limit for in-memory evaluation
            Self::check_depth(pf, 0)?;
        }
        Ok(())
    }

    fn check_depth(pf: &PayloadFilter, depth: u32) -> Result<(), SinexError> {
        if depth > MAX_SUBSCRIPTION_FILTER_DEPTH {
            return Err(
                SinexError::validation("SubscriptionFilter payload nesting too deep")
                    .with_context("max_depth", MAX_SUBSCRIPTION_FILTER_DEPTH)
                    .with_context("actual_depth", depth),
            );
        }
        match pf {
            PayloadFilter::And { filters } | PayloadFilter::Or { filters } => {
                for f in filters {
                    Self::check_depth(f, depth + 1)?;
                }
            }
            PayloadFilter::Not { filter } => Self::check_depth(filter, depth + 1)?,
            _ => {}
        }
        Ok(())
    }

    /// Test whether an event matches this filter. All non-empty dimensions must match (AND).
    #[must_use]
    pub fn matches(&self, event: &Event<JsonValue>) -> bool {
        if !self.sources.is_empty() && !self.sources.contains(&event.source) {
            return false;
        }
        if !self.event_types.is_empty() && !self.event_types.contains(&event.event_type) {
            return false;
        }
        if !self.hosts.is_empty() && !self.hosts.contains(&event.host) {
            return false;
        }
        if let Some(ref pf) = self.payload
            && !payload_filter_matches(pf, &event.payload)
        {
            return false;
        }
        true
    }
}

/// Evaluate a [`PayloadFilter`] against a JSON value in memory.
///
/// This mirrors the SQL-side evaluation but operates on materialized data.
fn payload_filter_matches(pf: &PayloadFilter, payload: &JsonValue) -> bool {
    match pf {
        PayloadFilter::Contains { value } => json_contains(payload, value),
        PayloadFilter::TextSearch { text } => {
            // Substring search across serialized payload
            let serialized = serde_json::to_string(payload).unwrap_or_default();
            serialized.to_lowercase().contains(&text.to_lowercase())
        }
        PayloadFilter::HasKey { key } => payload.get(key.as_str()).is_some(),
        PayloadFilter::Path { path, op } => {
            let extracted = payload.get(path.as_str());
            match op {
                PathOp::IsNull => extracted.is_none() || extracted == Some(&JsonValue::Null),
                PathOp::IsNotNull => extracted.is_some() && extracted != Some(&JsonValue::Null),
                PathOp::Eq(v) => extracted == Some(v),
                PathOp::Gt(v) => {
                    json_cmp(extracted, Some(v)).is_some_and(std::cmp::Ordering::is_gt)
                }
                PathOp::Gte(v) => {
                    json_cmp(extracted, Some(v)).is_some_and(std::cmp::Ordering::is_ge)
                }
                PathOp::Lt(v) => {
                    json_cmp(extracted, Some(v)).is_some_and(std::cmp::Ordering::is_lt)
                }
                PathOp::Lte(v) => {
                    json_cmp(extracted, Some(v)).is_some_and(std::cmp::Ordering::is_le)
                }
                PathOp::Like(pattern) => {
                    if let Some(JsonValue::String(s)) = extracted {
                        like_match(s, pattern)
                    } else {
                        false
                    }
                }
            }
        }
        PayloadFilter::And { filters } => {
            filters.iter().all(|f| payload_filter_matches(f, payload))
        }
        PayloadFilter::Or { filters } => filters.iter().any(|f| payload_filter_matches(f, payload)),
        PayloadFilter::Not { filter } => !payload_filter_matches(filter, payload),
    }
}

/// Recursive JSON containment check (mirrors `PostgreSQL` `@>`).
///
/// `a @> b` is true when every key/value in `b` exists in `a`, recursing into objects.
fn json_contains(haystack: &JsonValue, needle: &JsonValue) -> bool {
    match (haystack, needle) {
        (JsonValue::Object(h), JsonValue::Object(n)) => n
            .iter()
            .all(|(k, nv)| h.get(k).is_some_and(|hv| json_contains(hv, nv))),
        (JsonValue::Array(h), JsonValue::Array(n)) => {
            n.iter().all(|nv| h.iter().any(|hv| json_contains(hv, nv)))
        }
        _ => haystack == needle,
    }
}

/// Compare two JSON values numerically (f64) or lexicographically (string).
fn json_cmp(a: Option<&JsonValue>, b: Option<&JsonValue>) -> Option<std::cmp::Ordering> {
    let (a, b) = (a?, b?);
    match (a, b) {
        (JsonValue::Number(a), JsonValue::Number(b)) => {
            let (a, b) = (a.as_f64()?, b.as_f64()?);
            a.partial_cmp(&b)
        }
        (JsonValue::String(a), JsonValue::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Simple SQL LIKE pattern matching (`%` = any sequence, `_` = any single char).
///
/// This uses an iterative backtracking matcher so adversarial wildcard patterns
/// stay linear in the haystack size instead of recursing exponentially.
fn like_match(s: &str, pattern: &str) -> bool {
    let s = s.as_bytes();
    let pattern = pattern.as_bytes();
    let mut s_idx = 0usize;
    let mut p_idx = 0usize;
    let mut wildcard_next = None;
    let mut wildcard_match_end = 0usize;

    while s_idx < s.len() {
        if p_idx < pattern.len() {
            match pattern[p_idx] {
                b'_' => {
                    s_idx += 1;
                    p_idx += 1;
                    continue;
                }
                b'%' => {
                    while p_idx < pattern.len() && pattern[p_idx] == b'%' {
                        p_idx += 1;
                    }
                    wildcard_next = Some(p_idx);
                    wildcard_match_end = s_idx;
                    if p_idx == pattern.len() {
                        return true;
                    }
                    continue;
                }
                ch if s[s_idx] == ch => {
                    s_idx += 1;
                    p_idx += 1;
                    continue;
                }
                _ => {}
            }
        }

        let Some(wildcard_resume) = wildcard_next else {
            return false;
        };
        wildcard_match_end += 1;
        s_idx = wildcard_match_end;
        p_idx = wildcard_resume;
    }

    while p_idx < pattern.len() && pattern[p_idx] == b'%' {
        p_idx += 1;
    }

    p_idx == pattern.len()
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
