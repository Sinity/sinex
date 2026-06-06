//! Derived output type for synthetic events.

use sinex_primitives::Uuid;
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::temporal::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedAggregationMeta {
    /// Semantic aggregate kind, e.g. `activity.window` or `activity.session`.
    pub kind: String,

    /// Rollup depth from raw material events.
    pub rollup_level: u32,

    /// Logical input count represented by the output payload.
    pub total_input_count: u64,
}

impl DerivedAggregationMeta {
    #[must_use]
    pub fn new(kind: impl Into<String>, rollup_level: u32, total_input_count: u64) -> Self {
        Self {
            kind: kind.into(),
            rollup_level,
            total_input_count,
        }
    }
}

/// Output from a automaton's processing logic.
///
/// Carries the full synthetic metadata required for replay-correct provenance chains.
#[derive(Debug, Clone)]
pub struct DerivedOutput<T> {
    /// The typed output payload.
    pub payload: T,

    /// Original timestamp for the derived event.
    ///
    /// - Transducers: inherit from input event
    /// - Windowed: derived from latest input event (deterministic across replays)
    /// - Scope reconcilers: varies by domain logic
    pub ts_orig: Timestamp,

    /// IDs of all events that contributed to this output.
    pub source_event_ids: Vec<Uuid>,

    /// How `ts_orig` was determined — enables replay to reproduce the same value.
    pub temporal_policy: SyntheticTemporalPolicy,

    /// Semantics version of this automaton's processing logic.
    ///
    /// Bumping this signals that all events produced by this automaton should be
    /// recomputed during replay, even if inputs haven't changed.
    pub semantics_version: Option<String>,

    /// Scope key for scope-based reconciliation.
    ///
    /// Required for `ScopeReconciler`; optional for others.
    pub scope_key: Option<String>,

    /// Equivalence key for deduplication during replay.
    ///
    /// Events with the same `equivalence_key` from the same automaton are considered
    /// semantically equivalent — replay can replace rather than duplicate.
    pub equivalence_key: Option<String>,

    /// Aggregate semantics for bounded rollups.
    ///
    /// This stays in runtime metadata rather than the event row so the adapter
    /// can expose truthful fan-in metrics without widening core provenance.
    pub aggregation: Option<DerivedAggregationMeta>,

    /// Per-output event type override.
    ///
    /// When `Some`, the adapter stamps this event type on the emitted event
    /// instead of `Automaton::output_event_type()`. Used by
    /// `MultiOutputTransducer` to emit events of different types from
    /// a single processing call.
    pub event_type: Option<&'static str>,
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
            aggregation: None,
            event_type: None,
        }
    }

    /// Create a windowed output with an explicit `ts_orig`.
    ///
    /// The `ts_orig` should typically be derived from the latest event in the
    /// accumulation window (e.g. `state.recent_events.back().map(|e| e.timestamp)`).
    /// This ensures temporal determinism: replaying the same inputs produces the
    /// same timestamp on the derived output.
    ///
    /// Use [`windowed_now`](Self::windowed_now) only when wall-clock semantics
    /// are the genuine domain requirement.
    pub fn windowed(payload: T, ts_orig: Timestamp, source_event_ids: Vec<Uuid>) -> Self {
        Self {
            payload,
            ts_orig,
            source_event_ids,
            temporal_policy: SyntheticTemporalPolicy::LatestInput,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            aggregation: None,
            event_type: None,
        }
    }

    /// Create a windowed output using wall-clock `Timestamp::now()`.
    ///
    /// Only use this when the output genuinely represents an observation at the
    /// current wall-clock time. For most automatons, prefer [`windowed`](Self::windowed)
    /// with a timestamp derived from input events.
    pub fn windowed_now(payload: T, source_event_ids: Vec<Uuid>) -> Self {
        Self {
            payload,
            ts_orig: Timestamp::now(),
            source_event_ids,
            temporal_policy: SyntheticTemporalPolicy::WindowBoundary,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            aggregation: None,
            event_type: None,
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
            aggregation: None,
            event_type: None,
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

    /// Set the per-output event type for multi-output automata.
    ///
    /// When set, the adapter stamps this event type on the emitted event instead
    /// of falling back to `Automaton::output_event_type()`.
    #[must_use]
    pub fn with_event_type(mut self, event_type: &'static str) -> Self {
        self.event_type = Some(event_type);
        self
    }

    /// Attach aggregate semantics so the adapter can observe bounded fan-in truthfully.
    #[must_use]
    pub fn with_aggregation(mut self, aggregation: DerivedAggregationMeta) -> Self {
        self.aggregation = Some(aggregation);
        self
    }
}
