//! Trigger context for derived nodes — replaces `NodeEventContext`.

use sinex_primitives::domain::{EventSource, EventType, ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::Operation;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue, Uuid};

/// Rich trigger context passed to every derived-node processing call.
///
/// Extends the old `NodeEventContext` with processing mode, trigger kind,
/// and operation lineage — all required for replay-correct derived output.
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
    pub created_by_operation_id: Option<Id<Operation>>,
}

impl DerivedTriggerContext {
    /// Operation lineage, if this processing call belongs to a replay/operation.
    #[must_use]
    pub fn operation_id(&self) -> Option<Id<Operation>> {
        self.created_by_operation_id
    }

    /// Convenience: get the trigger event ID as a raw UUID.
    #[must_use]
    pub fn trigger_uuid(&self) -> Uuid {
        *self.trigger_event_id.as_uuid()
    }

    /// Create a context for live processing of a new event.
    pub fn live(event: &Event<JsonValue>) -> Self {
        Self {
            trigger_event_id: event.id.unwrap_or_default(),
            source: event.source.clone(),
            event_type: event.event_type.clone(),
            ts_orig: event.ts_orig,
            ts_coided: event.id.map_or_else(Timestamp::now, |id| id.timestamp()),
            processing_mode: ProcessingMode::Live,
            trigger_kind: TriggerKind::NewEvent,
            created_by_operation_id: event
                .created_by_operation_id
                .map(Id::<Operation>::from_uuid),
        }
    }

    /// Create a context for historical replay processing.
    pub fn historical(event: &Event<JsonValue>, operation_id: Option<Id<Operation>>) -> Self {
        Self {
            trigger_event_id: event.id.unwrap_or_default(),
            source: event.source.clone(),
            event_type: event.event_type.clone(),
            ts_orig: event.ts_orig,
            ts_coided: event.id.map_or_else(Timestamp::now, |id| id.timestamp()),
            processing_mode: ProcessingMode::Replay,
            trigger_kind: TriggerKind::ReplayRecompute,
            created_by_operation_id: operation_id.or_else(|| {
                event.created_by_operation_id.map(Id::<Operation>::from_uuid)
            }),
        }
    }
}
