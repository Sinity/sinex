//! Shared human/agent view DTOs.

use crate::domain::{OperationKind, OperationStatus};
use crate::events::Event;
use crate::ids::Id;
use crate::query::{Cursor, QueryResultEvent};
use crate::temporal::Timestamp;
use crate::{JsonValue, Provenance};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const VIEW_ENVELOPE_SCHEMA_VERSION: &str = "sinex.view-envelope/v3";
pub const CONTEXT_SUMMARY_SCHEMA_VERSION: &str = "sinex.context-summary/v1";
pub const EVENT_CARD_LIST_SCHEMA_VERSION: &str = "sinex.event-card-list/v3";
pub const EVENT_ERROR_LIST_SCHEMA_VERSION: &str = "sinex.event-error-list/v1";
pub const EVENT_QUERY_LIST_SCHEMA_VERSION: &str = "sinex.event-query-list/v1";
pub const OPERATION_JOB_LIST_SCHEMA_VERSION: &str = "sinex.operation-job-list/v1";
pub const OPERATION_VIEW_SCHEMA_VERSION: &str = "sinex.operation-view/v1";
pub const SOURCE_COVERAGE_LIST_SCHEMA_VERSION: &str = "sinex.source-coverage-list/v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SinexObjectKind {
    Event,
    SourceDriver,
    SourceMaterial,
    MaterialAnchor,
    Document,
    DocumentChunk,
    Task,
    SemanticLane,
    SemanticEntity,
    SemanticRelation,
    Operation,
    ReplayRun,
    Snapshot,
    DlqMessage,
    ContextPack,
    MomentCandidate,
    PrivacySession,
    Caveat,
    RpcMethod,
    Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverageReadiness {
    Ready,
    Proposed,
    MissingMaterial,
    MissingEvents,
    MissingBinding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverageContinuity {
    Active,
    MaterialOnly,
    EventOnly,
    Gapped,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourcePrivacyPosture {
    pub tier: String,
    pub context: String,
    #[serde(default)]
    pub proposed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CoverageGapView {
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceCoverageView {
    pub source_id: String,
    pub namespace: String,
    pub event_types: Vec<String>,
    pub readiness: SourceCoverageReadiness,
    pub continuity: SourceCoverageContinuity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_material_at: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<Timestamp>,
    pub material_count: i64,
    pub event_count: i64,
    pub binding_count: usize,
    pub live_binding_count: usize,
    pub proposed_binding_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<CoverageGapView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    pub privacy: SourcePrivacyPosture,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceCoverageListView {
    pub schema_version: String,
    pub count: usize,
    pub sources: Vec<SourceCoverageView>,
}

impl SourceCoverageListView {
    #[must_use]
    pub fn new(sources: Vec<SourceCoverageView>) -> Self {
        let count = sources.len();
        Self {
            schema_version: SOURCE_COVERAGE_LIST_SCHEMA_VERSION.to_string(),
            count,
            sources,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SinexObjectRef {
    pub kind: SinexObjectKind,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_method: Option<String>,
}

impl SinexObjectRef {
    #[must_use]
    pub fn new(kind: SinexObjectKind, id: impl Into<String>) -> Self {
        Self {
            kind,
            id: id.into(),
            label: None,
            command_hint: None,
            rpc_method: None,
        }
    }

    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    #[must_use]
    pub fn with_command_hint(mut self, command_hint: impl Into<String>) -> Self {
        self.command_hint = Some(command_hint.into());
        self
    }

    #[must_use]
    pub fn with_rpc_method(mut self, rpc_method: impl Into<String>) -> Self {
        self.rpc_method = Some(rpc_method.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionAvailabilityState {
    Enabled,
    Disabled,
    Target,
    Loading,
    Dangerous,
    Partial,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionSideEffect {
    Read,
    Compose,
    Write,
    Admin,
    Destructive,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActionAvailability {
    pub id: String,
    pub label: String,
    pub state: ActionAvailabilityState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_method: Option<String>,
    pub side_effect: ActionSideEffect,
    #[serde(default, skip_serializing_if = "is_false")]
    pub requires_confirmation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub dry_run_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit_output_ref: Option<SinexObjectRef>,
}

impl ActionAvailability {
    #[must_use]
    pub fn read(
        id: impl Into<String>,
        label: impl Into<String>,
        state: ActionAvailabilityState,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            state,
            reason: None,
            command_hint: None,
            rpc_method: None,
            side_effect: ActionSideEffect::Read,
            requires_confirmation: false,
            dry_run_available: false,
            audit_output_ref: None,
        }
    }

    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    #[must_use]
    pub fn with_command_hint(mut self, command: impl Into<String>) -> Self {
        self.command_hint = Some(command.into());
        self
    }

    #[must_use]
    pub fn with_rpc_method(mut self, rpc_method: impl Into<String>) -> Self {
        self.rpc_method = Some(rpc_method.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyStateKind {
    RawVisible,
    MetadataOnly,
    Redacted,
    Suppressed,
    PermissionDenied,
    PolicyBlocked,
    TombstonePending,
    ExportRestricted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PrivacyStateView {
    pub state: PrivacyStateKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl PrivacyStateView {
    #[must_use]
    pub fn raw_visible() -> Self {
        Self {
            state: PrivacyStateKind::RawVisible,
            reason: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CaveatView {
    pub id: String,
    pub message: String,
    #[serde(rename = "ref")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_: Option<SinexObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FreshnessView {
    pub generated_at: Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_after_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ViewEnvelope<T> {
    pub schema_version: String,
    pub view_id: String,
    pub generated_at: Timestamp,
    pub source_surface: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<FreshnessView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_echo: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub filters: JsonValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub privacy_state: Option<PrivacyStateView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
    pub payload: T,
}

impl<T> ViewEnvelope<T> {
    #[must_use]
    pub fn new(source_surface: impl Into<String>, payload: T) -> Self {
        let generated_at = Timestamp::now();
        Self {
            schema_version: VIEW_ENVELOPE_SCHEMA_VERSION.to_string(),
            view_id: Id::<ViewEnvelopeMarker>::new().to_string(),
            generated_at,
            source_surface: source_surface.into(),
            runtime_target: None,
            freshness: Some(FreshnessView {
                generated_at,
                stale_after_secs: None,
            }),
            query_echo: None,
            filters: JsonValue::Null,
            caveats: Vec::new(),
            privacy_state: None,
            actions: Vec::new(),
            payload,
        }
    }

    #[must_use]
    pub fn with_query_echo(mut self, query_echo: JsonValue) -> Self {
        self.query_echo = Some(query_echo);
        self
    }

    #[must_use]
    pub fn with_filters(mut self, filters: JsonValue) -> Self {
        self.filters = filters;
        self
    }
}

#[derive(Debug)]
pub struct ViewEnvelopeMarker;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventTimestampView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingested: Option<Timestamp>,
    pub quality: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventSourceView {
    pub family: String,
    pub raw: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<SinexObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventCardView {
    #[serde(rename = "ref")]
    pub ref_: SinexObjectRef,
    pub timestamp: EventTimestampView,
    pub source: EventSourceView,
    pub event_type: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_preview: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub material_refs: Vec<SinexObjectRef>,
    pub privacy_state: PrivacyStateView,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trace_refs: Vec<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projection_badges: Vec<String>,
    pub actions: Vec<ActionAvailability>,
}

impl EventCardView {
    #[must_use]
    pub fn from_query_event(result: &QueryResultEvent) -> Self {
        let event = &result.event;
        let event_id = event.id.map(|id| id.to_string());
        let ref_ = event_ref(event_id.as_deref());
        let ingested = event.id.map(|id| id.timestamp());
        let quality = if event.ts_orig.is_some() {
            "original_timestamp".to_string()
        } else {
            "ingest_timestamp_fallback".to_string()
        };

        let (material_refs, trace_refs) = provenance_refs(&event.provenance);
        let mut caveats = Vec::new();
        if event.id.is_none() {
            caveats.push(CaveatView {
                id: "event.unpersisted".to_string(),
                message: "event has no stable persisted id".to_string(),
                ref_: None,
            });
        }
        if result.snippet.is_none() {
            caveats.push(CaveatView {
                id: "event.no_snippet".to_string(),
                message: "query result did not include a snippet".to_string(),
                ref_: event_id.as_deref().map(|id| event_ref(Some(id))),
            });
        }

        Self {
            ref_,
            timestamp: EventTimestampView {
                original: event.ts_orig,
                ingested,
                quality,
            },
            source: EventSourceView {
                family: source_family(event.source.as_str()),
                raw: event.source.to_string(),
                source_ref: Some(
                    SinexObjectRef::new(SinexObjectKind::SourceDriver, event.source.to_string())
                        .with_label(event.source.to_string()),
                ),
            },
            event_type: event.event_type.to_string(),
            summary: event_summary(event, result.snippet.as_deref()),
            payload_preview: payload_preview(&event.payload),
            material_refs,
            privacy_state: PrivacyStateView::raw_visible(),
            caveats,
            trace_refs,
            projection_badges: projection_badges(event),
            actions: event_actions(event_id.as_deref()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventCardListView {
    pub schema_version: String,
    pub count: usize,
    pub cards: Vec<EventCardView>,
}

impl EventCardListView {
    #[must_use]
    pub fn from_query_events(events: &[QueryResultEvent]) -> Self {
        Self {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: events.len(),
            cards: events.iter().map(EventCardView::from_query_event).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ContextSourceView {
    pub source: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_ts: Option<Timestamp>,
    pub latest_event: EventCardView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ContextSummaryView {
    pub schema_version: String,
    pub since: String,
    pub total_events: usize,
    pub source_count: usize,
    pub sources: Vec<ContextSourceView>,
}

impl ContextSummaryView {
    #[must_use]
    pub fn new(
        since: impl Into<String>,
        total_events: usize,
        sources: Vec<ContextSourceView>,
    ) -> Self {
        Self {
            schema_version: CONTEXT_SUMMARY_SCHEMA_VERSION.to_string(),
            since: since.into(),
            total_events,
            source_count: sources.len(),
            sources,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventQueryListView {
    pub schema_version: String,
    pub count: usize,
    pub cards: Vec<EventCardView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_estimate: Option<i64>,
}

impl EventQueryListView {
    #[must_use]
    pub fn from_query_events(
        events: &[QueryResultEvent],
        next_cursor: Option<Cursor>,
        total_estimate: Option<i64>,
    ) -> Self {
        Self {
            schema_version: EVENT_QUERY_LIST_SCHEMA_VERSION.to_string(),
            count: events.len(),
            cards: events.iter().map(EventCardView::from_query_event).collect(),
            next_cursor,
            total_estimate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventErrorListView {
    pub schema_version: String,
    pub since: String,
    pub count: usize,
    pub cards: Vec<EventCardView>,
}

impl EventErrorListView {
    #[must_use]
    pub fn from_query_events(since: impl Into<String>, events: &[QueryResultEvent]) -> Self {
        Self {
            schema_version: EVENT_ERROR_LIST_SCHEMA_VERSION.to_string(),
            since: since.into(),
            count: events.len(),
            cards: events.iter().map(EventCardView::from_query_event).collect(),
        }
    }
}

fn event_ref(event_id: Option<&str>) -> SinexObjectRef {
    let id = event_id.unwrap_or("unpersisted");
    let mut ref_ = SinexObjectRef::new(SinexObjectKind::Event, id).with_label(short_id(id));
    if event_id.is_some() {
        ref_ = ref_
            .with_command_hint(format!("sinexctl events trace {id}"))
            .with_rpc_method("events.lineage");
    }
    ref_
}

fn provenance_refs(provenance: &Provenance) -> (Vec<SinexObjectRef>, Vec<SinexObjectRef>) {
    match provenance {
        Provenance::Material {
            id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
        } => {
            let material = SinexObjectRef::new(SinexObjectKind::SourceMaterial, id.to_string())
                .with_label("source material");
            let anchor_label = match (offset_start, offset_end) {
                (Some(start), Some(end)) => format!("{} {start}..{end}", offset_kind.as_wire_str()),
                _ => format!("byte {anchor_byte}"),
            };
            let anchor = SinexObjectRef::new(
                SinexObjectKind::MaterialAnchor,
                format!("{id}:{anchor_byte}"),
            )
            .with_label(anchor_label);
            (vec![material, anchor], Vec::new())
        }
        Provenance::Derived {
            source_event_ids, ..
        } => {
            let trace_refs = source_event_ids
                .iter()
                .map(|id| {
                    SinexObjectRef::new(SinexObjectKind::Event, id.to_string())
                        .with_label(short_id(&id.to_string()))
                        .with_command_hint(format!("sinexctl events trace {id}"))
                        .with_rpc_method("events.lineage")
                })
                .collect();
            (Vec::new(), trace_refs)
        }
    }
}

fn event_actions(event_id: Option<&str>) -> Vec<ActionAvailability> {
    match event_id {
        Some(id) => vec![
            ActionAvailability::read("event.trace", "Trace", ActionAvailabilityState::Enabled)
                .with_command_hint(format!("sinexctl events trace {id}"))
                .with_rpc_method("events.lineage"),
            ActionAvailability::read("event.inspect", "Inspect", ActionAvailabilityState::Target)
                .with_reason("multi-pane event inspector is tracked separately")
                .with_command_hint(format!("sinexctl events inspect {id}"))
                .with_rpc_method("events.query"),
        ],
        None => vec![
            ActionAvailability::read("event.trace", "Trace", ActionAvailabilityState::Unavailable)
                .with_reason("event has no stable persisted id"),
            ActionAvailability::read(
                "event.inspect",
                "Inspect",
                ActionAvailabilityState::Unavailable,
            )
            .with_reason("event has no stable persisted id"),
        ],
    }
}

fn projection_badges(event: &Event<JsonValue>) -> Vec<String> {
    let mut badges = vec![if event.is_synthesized_event() {
        "derived".to_string()
    } else {
        "material".to_string()
    }];
    if event.temporal_policy.is_some() {
        badges.push("temporal_policy".to_string());
    }
    if event.semantics_version.is_some() {
        badges.push("semantics_versioned".to_string());
    }
    badges
}

fn event_summary(event: &Event<JsonValue>, snippet: Option<&str>) -> String {
    if let Some(snippet) = snippet
        && !snippet.trim().is_empty()
    {
        return truncate_chars(snippet.trim(), 160);
    }
    match &event.payload {
        JsonValue::Object(map) => {
            for key in ["summary", "title", "command", "path", "message", "name"] {
                if let Some(JsonValue::String(value)) = map.get(key)
                    && !value.trim().is_empty()
                {
                    return truncate_chars(value.trim(), 160);
                }
            }
            format!("{} with {} payload field(s)", event.event_type, map.len())
        }
        JsonValue::String(value) => truncate_chars(value, 160),
        JsonValue::Null => event.event_type.to_string(),
        other => truncate_chars(&other.to_string(), 160),
    }
}

fn payload_preview(payload: &JsonValue) -> Option<JsonValue> {
    match payload {
        JsonValue::Null => None,
        JsonValue::Object(map) => {
            let mut preview = serde_json::Map::new();
            for (key, value) in map.iter().take(8) {
                preview.insert(key.clone(), preview_value(value));
            }
            Some(JsonValue::Object(preview))
        }
        other => Some(json!({ "value": preview_value(other) })),
    }
}

fn preview_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::String(s) => JsonValue::String(truncate_chars(s, 240)),
        JsonValue::Array(values) => {
            let mut preview = values.iter().take(5).map(preview_value).collect::<Vec<_>>();
            if values.len() > preview.len() {
                preview.push(json!({ "truncated_items": values.len() - preview.len() }));
            }
            JsonValue::Array(preview)
        }
        JsonValue::Object(map) => {
            let preview = map
                .iter()
                .take(5)
                .map(|(key, value)| (key.clone(), preview_value(value)))
                .collect();
            JsonValue::Object(preview)
        }
        other => other.clone(),
    }
}

fn source_family(source: &str) -> String {
    source
        .split(['.', '-', '_'])
        .next()
        .filter(|part| !part.is_empty())
        .unwrap_or(source)
        .to_string()
}

fn short_id(id: &str) -> String {
    if id.len() <= 8 {
        id.to_string()
    } else {
        id[..8].to_string()
    }
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let end = input
        .char_indices()
        .nth(keep)
        .map_or(input.len(), |(index, _)| index);
    format!("{}...", &input[..end])
}

fn is_false(value: &bool) -> bool {
    !*value
}

// ─────────────────────────────────────────────────────────────────────────────
// Operation views
// ─────────────────────────────────────────────────────────────────────────────

/// Read-only view of a single `core.operations_log` row rendered for operator
/// and agent consumption.
///
/// Wraps the raw `OperationRecord` from `sinex-db`, replacing the untyped
/// `operation_type: String` with the typed [`OperationKind`] registry and
/// surfacing stable, named fields without exposing DB-internal identifiers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OperationView {
    /// Stable hex ID of this operation (UUID, opaque to callers).
    pub id: String,
    /// Typed classification of the operation.
    pub kind: OperationKind,
    /// Actor that submitted the operation (actor_id from auth context).
    pub operator: String,
    /// Terminal result status of the operation.
    pub status: OperationStatus,
    /// Wall-clock duration in milliseconds, `null` while still running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i32>,
    /// Human-readable result message set on completion or failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_message: Option<String>,
    /// JSONB scope payload that scoped this operation (e.g. event ID range).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<JsonValue>,
    /// Summary JSONB produced at completion, suitable for display.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_summary: Option<JsonValue>,
    /// Quick-access action hints for operator UIs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

impl OperationView {
    /// Construct from the RPC `Operation` type from `sinex-primitives::rpc::ops`.
    ///
    /// Accepts the raw `operation_type` string and converts it to [`OperationKind`].
    #[must_use]
    pub fn from_rpc(
        id: String,
        operation_type: &str,
        operator: String,
        status: OperationStatus,
        duration_ms: Option<i32>,
        result_message: Option<String>,
        scope: Option<JsonValue>,
        preview_summary: Option<JsonValue>,
    ) -> Self {
        let kind = OperationKind::from(operation_type);
        let actions = operation_actions(&id, &kind, &status);
        Self {
            id,
            kind,
            operator,
            status,
            duration_ms,
            result_message,
            scope,
            preview_summary,
            actions,
        }
    }
}

fn operation_actions(
    id: &str,
    kind: &OperationKind,
    status: &OperationStatus,
) -> Vec<ActionAvailability> {
    let is_terminal = matches!(
        status,
        OperationStatus::Success | OperationStatus::Failed | OperationStatus::Cancelled
    );
    let can_cancel =
        !is_terminal && matches!(status, OperationStatus::Running | OperationStatus::Pending);

    vec![
        ActionAvailability::read("ops.show", "Show", ActionAvailabilityState::Enabled)
            .with_command_hint(format!("sinexctl ops show {id}")),
        ActionAvailability {
            id: "ops.cancel".to_string(),
            label: "Cancel".to_string(),
            state: if can_cancel {
                ActionAvailabilityState::Enabled
            } else {
                ActionAvailabilityState::Disabled
            },
            reason: if is_terminal {
                Some("operation is already in a terminal state".to_string())
            } else {
                None
            },
            command_hint: Some(format!("sinexctl ops cancel {id}")),
            rpc_method: None,
            side_effect: ActionSideEffect::Write,
            requires_confirmation: false,
            dry_run_available: false,
            audit_output_ref: None,
        },
        ActionAvailability {
            id: "ops.replay".to_string(),
            label: "Replay".to_string(),
            state: if matches!(kind, OperationKind::Replay)
                && matches!(status, OperationStatus::Failed | OperationStatus::Cancelled)
            {
                ActionAvailabilityState::Enabled
            } else {
                ActionAvailabilityState::Unavailable
            },
            reason: if !matches!(kind, OperationKind::Replay) {
                Some("replay action only available for replay operations".to_string())
            } else {
                None
            },
            command_hint: Some(format!("sinexctl replay submit --ref-op {id}")),
            rpc_method: None,
            side_effect: ActionSideEffect::Write,
            requires_confirmation: true,
            dry_run_available: true,
            audit_output_ref: None,
        },
    ]
}

/// Payload carried inside a [`ViewEnvelope`] for `sinexctl ops jobs list`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OperationJobListView {
    pub schema_version: String,
    pub count: usize,
    pub jobs: Vec<OperationView>,
}

impl OperationJobListView {
    #[must_use]
    pub fn new(jobs: Vec<OperationView>) -> Self {
        let count = jobs.len();
        Self {
            schema_version: OPERATION_JOB_LIST_SCHEMA_VERSION.to_string(),
            count,
            jobs,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::events::SourceMaterial;
    use crate::events::builder::Provenance;
    use crate::non_empty::NonEmptyVec;
    use crate::{EventSource, EventType, HostName};
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn event_card_preserves_refs_actions_and_payload_preview() -> xtask::TestResult<()> {
        let event_id = Id::<Event<JsonValue>>::new();
        let material_id = Id::<SourceMaterial>::new();
        let result = QueryResultEvent {
            event: Event {
                id: Some(event_id),
                source: EventSource::new("shell.atuin")?,
                event_type: EventType::new("command.executed")?,
                payload: json!({
                    "command": "xtask test -p sinex-primitives",
                    "cwd": "/realm/project/sinex",
                    "extra": [1, 2, 3, 4, 5, 6],
                }),
                ts_orig: Some(Timestamp::now()),
                ts_quality: None,
                host: HostName::new("sinnix-prime")?,
                module_run_id: None,
                payload_schema_id: None,
                provenance: Provenance::Material {
                    id: material_id,
                    anchor_byte: 42,
                    offset_start: None,
                    offset_end: None,
                    offset_kind: crate::OffsetKind::Byte,
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                automaton_model: None,
                anchor_payload_hash: None,
            },
            relevance_score: Some(0.9),
            snippet: Some("ran a focused test".to_string()),
        };

        let card = EventCardView::from_query_event(&result);

        assert_eq!(card.ref_.kind, SinexObjectKind::Event);
        assert_eq!(card.ref_.id, event_id.to_string());
        assert_eq!(card.source.family, "shell");
        assert_eq!(card.summary, "ran a focused test");
        assert_eq!(card.material_refs.len(), 2);
        assert!(card.trace_refs.is_empty());
        assert!(
            card.actions.iter().any(|action| action.id == "event.trace"
                && action.state == ActionAvailabilityState::Enabled)
        );
        assert!(
            card.actions
                .iter()
                .any(|action| action.id == "event.inspect"
                    && action.state == ActionAvailabilityState::Target
                    && action.reason.is_some())
        );
        assert!(card.payload_preview.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn view_envelope_serializes_schema_version_and_payload() -> xtask::TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.recent",
            EventCardListView {
                schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
                count: 0,
                cards: Vec::new(),
            },
        )
        .with_query_echo(json!({ "since": "1h", "limit": 20 }));

        let value = serde_json::to_value(&envelope)?;
        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.recent");
        assert_eq!(
            value["payload"]["schema_version"],
            EVENT_CARD_LIST_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["count"], 0);
        Ok(())
    }

    #[sinex_test]
    async fn source_coverage_list_view_serializes_status_shape() -> xtask::TestResult<()> {
        let view = SourceCoverageView {
            source_id: "fixture.source".to_string(),
            namespace: "fixture".to_string(),
            event_types: vec!["fixture/fixture.event".to_string()],
            readiness: SourceCoverageReadiness::Ready,
            continuity: SourceCoverageContinuity::Active,
            last_material_at: None,
            last_event_at: None,
            material_count: 2,
            event_count: 3,
            binding_count: 1,
            live_binding_count: 1,
            proposed_binding_count: 0,
            gaps: Vec::new(),
            caveats: Vec::new(),
            privacy: SourcePrivacyPosture {
                tier: "sensitive".to_string(),
                context: "command".to_string(),
                proposed: false,
            },
            actions: vec![ActionAvailability::read(
                "sources.readiness",
                "Readiness",
                ActionAvailabilityState::Enabled,
            )],
        };
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.status",
            SourceCoverageListView::new(vec![view]),
        );

        let value = serde_json::to_value(&envelope)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(
            value["payload"]["schema_version"],
            SOURCE_COVERAGE_LIST_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["sources"][0]["readiness"], "ready");
        assert_eq!(value["payload"]["sources"][0]["continuity"], "active");
        Ok(())
    }

    #[sinex_test]
    async fn event_card_json_uses_contract_field_names() -> xtask::TestResult<()> {
        let result = QueryResultEvent {
            event: Event {
                id: None,
                source: EventSource::new("test.source")?,
                event_type: EventType::new("test.event")?,
                payload: json!({ "summary": "fixture summary" }),
                ts_orig: None,
                ts_quality: None,
                host: HostName::new("test-host")?,
                module_run_id: None,
                payload_schema_id: None,
                provenance: Provenance::Derived {
                    source_event_ids: NonEmptyVec::single(Id::<Event<JsonValue>>::new()),
                    operation_id: None,
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                automaton_model: None,
                anchor_payload_hash: None,
            },
            relevance_score: None,
            snippet: None,
        };

        let value = serde_json::to_value(EventCardView::from_query_event(&result))?;
        assert!(value.get("ref").is_some());
        assert!(value.get("ref_").is_none());
        assert_eq!(value["summary"], "fixture summary");
        assert_eq!(value["actions"][0]["state"], "unavailable");
        assert!(value["actions"][0].get("reason").is_some());
        Ok(())
    }

    #[sinex_test]
    async fn view_schema_generation_covers_card_and_envelope() -> xtask::TestResult<()> {
        let card_schema = serde_json::to_value(schemars::schema_for!(EventCardView))?;
        let context_envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<ContextSummaryView>))?;
        let envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventCardListView>))?;
        let error_envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventErrorListView>))?;
        let query_envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventQueryListView>))?;

        assert_eq!(card_schema["title"], "EventCardView");
        assert!(
            card_schema["properties"].get("ref").is_some(),
            "card schema should expose the contract `ref` field"
        );
        assert!(
            context_envelope_schema["properties"].get("payload").is_some(),
            "context envelope schema should include the typed summary payload"
        );
        assert!(envelope_schema["properties"].get("payload").is_some());
        assert!(
            envelope_schema["properties"]
                .get("source_surface")
                .is_some(),
            "envelope schema should include source surface metadata"
        );
        assert!(
            query_envelope_schema["properties"].get("payload").is_some(),
            "query envelope schema should include the typed query-list payload"
        );
        assert!(
            error_envelope_schema["properties"].get("payload").is_some(),
            "error envelope schema should include the typed error-list payload"
        );
        Ok(())
    }
}
