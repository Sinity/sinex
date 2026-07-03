use super::{
    ActionAvailability, ActionAvailabilityState, CaveatView, PrivacyStateView, SinexObjectKind,
    SinexObjectRef,
};
use crate::events::Event;
use crate::query::{Cursor, QueryResultEvent};
use crate::temporal::Timestamp;
use crate::{JsonValue, Provenance};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::common::truncate_chars;

pub const CONTEXT_SUMMARY_SCHEMA_VERSION: &str = "sinex.context-summary/v1";
pub const EVENT_CARD_LIST_SCHEMA_VERSION: &str = "sinex.event-card-list/v3";
pub const EVENT_ERROR_LIST_SCHEMA_VERSION: &str = "sinex.event-error-list/v1";
pub const EVENT_QUERY_LIST_SCHEMA_VERSION: &str = "sinex.event-query-list/v1";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventOriginKind {
    Material,
    Derived,
    Declared,
    System,
    ExternalMirror,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventTraceRelation {
    SourceMaterial,
    MaterialAnchor,
    SourceEvent,
    QueryRun,
    Proposal,
    Judgment,
    Operation,
    ExternalRef,
    Policy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventTraceLink {
    pub relation: EventTraceRelation,
    pub target: SinexObjectRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventCardView {
    #[serde(rename = "ref")]
    pub ref_: SinexObjectRef,
    pub timestamp: EventTimestampView,
    pub source: EventSourceView,
    pub event_type: String,
    pub origin_kind: EventOriginKind,
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
    pub trace_links: Vec<EventTraceLink>,
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

        let provenance = provenance_view(&event.provenance);
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
            origin_kind: provenance.origin_kind,
            summary: event_summary(event, result.snippet.as_deref()),
            payload_preview: payload_preview(&event.payload),
            material_refs: provenance.material_refs,
            privacy_state: PrivacyStateView::raw_visible(),
            caveats,
            trace_refs: provenance.trace_refs,
            trace_links: provenance.trace_links,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_estimate: Option<i64>,
}

impl EventCardListView {
    #[must_use]
    pub fn from_query_events(events: &[QueryResultEvent]) -> Self {
        Self::from_query_events_with_metadata(events, None, None)
    }

    #[must_use]
    pub fn from_query_events_with_metadata(
        events: &[QueryResultEvent],
        next_cursor: Option<Cursor>,
        total_estimate: Option<i64>,
    ) -> Self {
        Self {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: events.len(),
            cards: events.iter().map(EventCardView::from_query_event).collect(),
            next_cursor,
            total_estimate,
        }
    }

    #[must_use]
    pub fn with_query_metadata(
        mut self,
        next_cursor: Option<Cursor>,
        total_estimate: Option<i64>,
    ) -> Self {
        self.next_cursor = next_cursor;
        self.total_estimate = total_estimate;
        self
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_caveats: Vec<CaveatView>,
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
            source_caveats: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_source_caveats(mut self, source_caveats: Vec<CaveatView>) -> Self {
        self.source_caveats = source_caveats;
        self
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

struct EventProvenanceView {
    origin_kind: EventOriginKind,
    material_refs: Vec<SinexObjectRef>,
    trace_refs: Vec<SinexObjectRef>,
    trace_links: Vec<EventTraceLink>,
}

fn provenance_view(provenance: &Provenance) -> EventProvenanceView {
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
            EventProvenanceView {
                origin_kind: EventOriginKind::Material,
                material_refs: vec![material.clone(), anchor.clone()],
                trace_refs: Vec::new(),
                trace_links: vec![
                    EventTraceLink {
                        relation: EventTraceRelation::SourceMaterial,
                        target: material,
                    },
                    EventTraceLink {
                        relation: EventTraceRelation::MaterialAnchor,
                        target: anchor,
                    },
                ],
            }
        }
        Provenance::Derived {
            source_event_ids,
            operation_id,
        } => {
            let event_refs = source_event_ids
                .iter()
                .map(|id| {
                    SinexObjectRef::new(SinexObjectKind::Event, id.to_string())
                        .with_label(short_id(&id.to_string()))
                        .with_command_hint(format!("sinexctl events trace {id}"))
                        .with_rpc_method("events.lineage")
                })
                .collect::<Vec<_>>();
            let mut trace_links = event_refs
                .iter()
                .cloned()
                .map(|target| EventTraceLink {
                    relation: EventTraceRelation::SourceEvent,
                    target,
                })
                .collect::<Vec<_>>();
            if let Some(operation_id) = operation_id {
                trace_links.push(EventTraceLink {
                    relation: EventTraceRelation::Operation,
                    target: SinexObjectRef::new(
                        SinexObjectKind::Operation,
                        operation_id.to_string(),
                    )
                    .with_label(short_id(&operation_id.to_string()))
                    .with_command_hint(format!("sinexctl ops log --operation-id {operation_id}"))
                    .with_rpc_method("ops.get"),
                });
            }
            EventProvenanceView {
                origin_kind: EventOriginKind::Derived,
                material_refs: Vec::new(),
                trace_refs: event_refs,
                trace_links,
            }
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
