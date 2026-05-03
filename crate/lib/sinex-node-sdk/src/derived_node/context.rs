//! Trigger context for derived nodes.

use sinex_primitives::domain::{EventSource, EventType, ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::OperationMarker;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue, Uuid};

use crate::NodeLogicError;
use crate::NodeResult;
use crate::SinexError;

/// Rich trigger context passed to every derived-node processing call.
#[derive(Debug, Clone)]
pub struct DerivedTriggerContext {
    /// The event that triggered this processing call.
    pub trigger_event_id: Id<Event<JsonValue>>,

    /// Source of the trigger event.
    pub source: EventSource,

    /// Type of the trigger event.
    pub event_type: EventType,

    /// Original timestamp of the trigger event (from source material).
    pub ts_orig: Option<Timestamp>,

    /// Database-ordered timestamp derived from `UUIDv7` `id`.
    pub ts_coided: Timestamp,

    /// Whether this is live processing, historical replay, or backfill.
    pub processing_mode: ProcessingMode,

    /// What caused this processing invocation.
    pub trigger_kind: TriggerKind,

    /// If processing was initiated by a replay operation, its ID.
    pub created_by_operation_id: Option<Id<OperationMarker>>,
}

impl DerivedTriggerContext {
    /// Operation lineage, if this processing call belongs to a replay/operation.
    #[must_use]
    pub fn operation_id(&self) -> Option<Id<OperationMarker>> {
        self.created_by_operation_id
    }

    /// Convenience: get the trigger event ID as a raw UUID.
    #[must_use]
    pub fn trigger_uuid(&self) -> Uuid {
        *self.trigger_event_id.as_uuid()
    }

    /// Require the trigger event to carry an original source timestamp.
    pub fn require_ts_orig(&self) -> Result<Timestamp, NodeLogicError> {
        self.ts_orig.ok_or_else(|| {
            NodeLogicError::InputParsing(format!(
                "derived-node trigger event {} is missing ts_orig (source={}, event_type={}, processing_mode={})",
                self.trigger_event_id,
                self.source,
                self.event_type,
                self.processing_mode
            ))
        })
    }

    /// Create a context for live processing of a new event.
    pub fn live(event: &Event<JsonValue>) -> NodeResult<Self> {
        let trigger_event_id = event.id.ok_or_else(|| {
            SinexError::validation("derived-node trigger event is missing an id")
                .with_context("processing_mode", ProcessingMode::Live.to_string())
                .with_context("event_type", event.event_type.as_ref())
                .with_context("source", event.source.as_ref())
        })?;
        Ok(Self {
            trigger_event_id,
            source: event.source.clone(),
            event_type: event.event_type.clone(),
            ts_orig: event.ts_orig,
            ts_coided: trigger_event_id.timestamp(),
            processing_mode: ProcessingMode::Live,
            trigger_kind: TriggerKind::NewEvent,
            created_by_operation_id: event
                .created_by_operation_id
                .map(Id::<OperationMarker>::from_uuid),
        })
    }

    /// Create a context for historical replay processing.
    pub fn historical(
        event: &Event<JsonValue>,
        operation_id: Option<Id<OperationMarker>>,
    ) -> NodeResult<Self> {
        let trigger_event_id = event.id.ok_or_else(|| {
            SinexError::validation("derived-node replay event is missing an id")
                .with_context("processing_mode", ProcessingMode::Replay.to_string())
                .with_context("event_type", event.event_type.as_ref())
                .with_context("source", event.source.as_ref())
        })?;
        Ok(Self {
            trigger_event_id,
            source: event.source.clone(),
            event_type: event.event_type.clone(),
            ts_orig: event.ts_orig,
            ts_coided: trigger_event_id.timestamp(),
            processing_mode: ProcessingMode::Replay,
            trigger_kind: TriggerKind::ReplayRecompute,
            created_by_operation_id: operation_id.or_else(|| {
                event
                    .created_by_operation_id
                    .map(Id::<OperationMarker>::from_uuid)
            }),
        })
    }
}
