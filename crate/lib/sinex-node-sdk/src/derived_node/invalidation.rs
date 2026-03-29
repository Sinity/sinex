//! Scope invalidation signal for derived nodes.
//!
//! When a persisted fact changes (insert, archive, replace), the gateway
//! publishes a `DerivedScopeInvalidation` to notify derived nodes. Scope-based
//! and windowed nodes use this to trigger recomputation of affected scopes.
//! Transducer nodes ignore it (their outputs are archived along with their inputs).

use serde::{Deserialize, Serialize};
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EventSource, EventType, InvalidationAction};

/// A typed invalidation signal for derived nodes.
///
/// Carries enough data for a derived node to decide:
/// - Which scopes need recomputation
/// - What changed (action) and why (`operation_id`)
/// - Which event identity was affected
///
/// # Delivery
///
/// Published to `sinex.derived.invalidation` via NATS. Derived node adapters
/// subscribe and dispatch to the appropriate trait method based on node model.
///
/// # Invariants
///
/// - `affected_event_ids` is never empty
/// - `event_source` and `event_type` identify the affected event's type (for filtering)
/// - `scope_keys` may be empty if the gateway doesn't know the scope (node must derive it)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedScopeInvalidation {
    /// IDs of events that were inserted/archived/replaced.
    pub affected_event_ids: Vec<Uuid>,

    /// What happened to the affected events.
    pub action: InvalidationAction,

    /// The replay operation that caused this invalidation, if any.
    pub operation_id: Option<Uuid>,

    /// Source of the affected events (for node filtering).
    pub event_source: EventSource,

    /// Type of the affected events (for node filtering).
    pub event_type: EventType,

    /// Pre-computed scope keys that are affected (if known from the archived events).
    ///
    /// May be empty — in which case the node must derive scope keys from the
    /// working set itself.
    pub affected_scope_keys: Vec<String>,
}

impl DerivedScopeInvalidation {
    /// Create an invalidation for archived events (e.g., replay cascade).
    pub fn archived(
        affected_event_ids: Vec<Uuid>,
        event_source: EventSource,
        event_type: EventType,
    ) -> Self {
        Self {
            affected_event_ids,
            action: InvalidationAction::Archived,
            operation_id: None,
            event_source,
            event_type,
            affected_scope_keys: Vec::new(),
        }
    }

    /// Create an invalidation for newly inserted events (e.g., late backfill).
    pub fn inserted(
        affected_event_ids: Vec<Uuid>,
        event_source: EventSource,
        event_type: EventType,
    ) -> Self {
        Self {
            affected_event_ids,
            action: InvalidationAction::Inserted,
            operation_id: None,
            event_source,
            event_type,
            affected_scope_keys: Vec::new(),
        }
    }

    /// Create an invalidation for replaced events (archive + re-insert).
    pub fn replaced(
        affected_event_ids: Vec<Uuid>,
        event_source: EventSource,
        event_type: EventType,
    ) -> Self {
        Self {
            affected_event_ids,
            action: InvalidationAction::Replaced,
            operation_id: None,
            event_source,
            event_type,
            affected_scope_keys: Vec::new(),
        }
    }

    /// Set the operation ID (replay operation that caused this).
    #[must_use]
    pub fn with_operation(mut self, operation_id: Uuid) -> Self {
        self.operation_id = Some(operation_id);
        self
    }

    /// Set pre-computed scope keys.
    #[must_use]
    pub fn with_scope_keys(mut self, scope_keys: Vec<String>) -> Self {
        self.affected_scope_keys = scope_keys;
        self
    }

    /// Whether this invalidation is relevant to a node that consumes the given event type.
    #[must_use]
    pub fn matches_input(&self, input_event_type: &str) -> bool {
        self.event_type.as_str() == input_event_type
    }
}

/// NATS subject for scope invalidation signals.
pub const INVALIDATION_SUBJECT: &str = "sinex.derived.invalidation";
