//! Derived output type — replaces `OutputEvent<T>`.

use sinex_primitives::Uuid;
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::temporal::Timestamp;

/// Output from a derived node's processing logic.
///
/// Carries the full synthetic metadata required for replay-correct
/// provenance chains. Replaces the old `OutputEvent<T>` which only
/// had `payload`, `ts_orig`, and `source_event_ids`.
#[derive(Debug, Clone)]
pub struct DerivedOutput<T> {
    /// The typed output payload.
    pub payload: T,

    /// Original timestamp for the derived event.
    ///
    /// - Transducers: inherit from input event
    /// - Windowed: wall-clock (report time)
    /// - Scope reconcilers: varies by domain logic
    pub ts_orig: Timestamp,

    /// IDs of all events that contributed to this output.
    pub source_event_ids: Vec<Uuid>,

    /// How `ts_orig` was determined — enables replay to reproduce the same value.
    pub temporal_policy: SyntheticTemporalPolicy,

    /// Semantics version of this node's processing logic.
    ///
    /// Bumping this signals that all events produced by this node should be
    /// recomputed during replay, even if inputs haven't changed.
    pub semantics_version: Option<String>,

    /// Scope key for scope-based reconciliation.
    ///
    /// Required for `ScopeReconcilerNode`; optional for others.
    pub scope_key: Option<String>,

    /// Equivalence key for deduplication during replay.
    ///
    /// Events with the same `equivalence_key` from the same node are considered
    /// semantically equivalent — replay can replace rather than duplicate.
    pub equivalence_key: Option<String>,
}

impl<T> DerivedOutput<T> {
    /// Create a transducer output: inherits `ts_orig` from input, single source event.
    pub fn transduced(payload: T, ts_orig: Timestamp, source_event_id: Uuid) -> Self {
        Self {
            payload,
            ts_orig,
            source_event_ids: vec![source_event_id],
            temporal_policy: SyntheticTemporalPolicy::InheritParent,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
        }
    }

    /// Create a windowed output: wall-clock `ts_orig`, multiple source events.
    pub fn windowed(payload: T, source_event_ids: Vec<Uuid>) -> Self {
        Self {
            payload,
            ts_orig: Timestamp::now(),
            source_event_ids,
            temporal_policy: SyntheticTemporalPolicy::WindowBoundary,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
        }
    }

    /// Create a scope reconciler output.
    pub fn reconciled(
        payload: T,
        ts_orig: Timestamp,
        source_event_ids: Vec<Uuid>,
        scope_key: String,
    ) -> Self {
        Self {
            payload,
            ts_orig,
            source_event_ids,
            temporal_policy: SyntheticTemporalPolicy::LatestInput,
            semantics_version: None,
            scope_key: Some(scope_key),
            equivalence_key: None,
        }
    }

    /// Set the semantics version.
    #[must_use]
    pub fn with_semantics_version(mut self, version: impl Into<String>) -> Self {
        self.semantics_version = Some(version.into());
        self
    }

    /// Set the equivalence key.
    #[must_use]
    pub fn with_equivalence_key(mut self, key: impl Into<String>) -> Self {
        self.equivalence_key = Some(key.into());
        self
    }

    /// Set the scope key.
    #[must_use]
    pub fn with_scope_key(mut self, key: impl Into<String>) -> Self {
        self.scope_key = Some(key.into());
        self
    }

    /// Override the temporal policy.
    #[must_use]
    pub fn with_temporal_policy(mut self, policy: SyntheticTemporalPolicy) -> Self {
        self.temporal_policy = policy;
        self
    }
}
