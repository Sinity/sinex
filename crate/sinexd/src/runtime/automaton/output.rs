//! Derived output type for synthetic events.

use sinex_primitives::Uuid;
use sinex_primitives::derivation::{ClaimSupport, DerivationDeclarationId, DerivedProductClass};
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

    /// Derivation control-plane declaration this output claims to satisfy
    /// (sinex-0vx.2). When `Some`, the adapter looks up
    /// `Automaton::OUTPUT_DECLARATIONS` for a matching `declaration_id` and
    /// rejects the output if none exists, or if `product_class` /
    /// the resolved `(output_source, output_event_type)` disagree with the
    /// declaration. `None` is accepted unconditionally during the
    /// transition period before sinex-0vx.3 stamps every call site —
    /// automata that have not been migrated yet emit exactly as before.
    pub declaration_id: Option<DerivationDeclarationId>,

    /// Product class this output claims (sinex-0vx.2). Must match the
    /// looked-up declaration's `product_class` when `declaration_id` is
    /// `Some`. Not yet persisted to `core.events` — the wire column and
    /// `Event` struct field land in sinex-8cr.2; this stage validates at
    /// the adapter boundary only.
    pub product_class: Option<DerivedProductClass>,

    /// Claim-support vector this output claims (sinex-0vx.2). If its
    /// adjudication status is `Accepted`/`Rejected`/`Superseded`, the
    /// adapter rejects the output unless `ClaimSupport::adjudication_event_id`
    /// is set — re-asserting `ClaimSupport::is_shape_valid()` at the
    /// emission boundary as a defense against any construction path that
    /// bypasses the compile-time constructors. Not yet persisted; see
    /// `product_class` doc above.
    pub claim_support: Option<ClaimSupport>,

    /// Non-canonical lane epoch this output was produced under (sinex-0vx.2).
    /// `None` means the implicit canonical epoch for `declaration_id`'s
    /// current `semantics_version` — the hot live path stays lookup-free.
    /// Only shadow/experiment/replay-into-lane runs stamp this explicitly.
    pub derivation_epoch_id: Option<Uuid>,

    /// Non-canonical lane this output was produced under (sinex-0vx.2).
    /// `None` means the implicit canonical lane. See `derivation_epoch_id`.
    pub derivation_lane_id: Option<Uuid>,
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
            declaration_id: None,
            product_class: None,
            claim_support: None,
            derivation_epoch_id: None,
            derivation_lane_id: None,
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
            declaration_id: None,
            product_class: None,
            claim_support: None,
            derivation_epoch_id: None,
            derivation_lane_id: None,
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
            declaration_id: None,
            product_class: None,
            claim_support: None,
            derivation_epoch_id: None,
            derivation_lane_id: None,
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
            declaration_id: None,
            product_class: None,
            claim_support: None,
            derivation_epoch_id: None,
            derivation_lane_id: None,
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

    /// Claim a derivation control-plane declaration for this output
    /// (sinex-0vx.2). The adapter rejects the output at emission time if no
    /// declaration with this id exists on `Automaton::OUTPUT_DECLARATIONS`.
    #[must_use]
    pub fn with_declaration_id(mut self, declaration_id: DerivationDeclarationId) -> Self {
        self.declaration_id = Some(declaration_id);
        self
    }

    /// Set the product class this output claims (sinex-0vx.2). Must match
    /// the declaration named by `with_declaration_id` when both are set.
    #[must_use]
    pub fn with_product_class(mut self, product_class: DerivedProductClass) -> Self {
        self.product_class = Some(product_class);
        self
    }

    /// Attach the claim-support vector for this output (sinex-0vx.2).
    #[must_use]
    pub fn with_claim_support(mut self, claim_support: ClaimSupport) -> Self {
        self.claim_support = Some(claim_support);
        self
    }

    /// Stamp the non-canonical lane epoch this output was produced under
    /// (sinex-0vx.2). Leave unset for the canonical live path.
    #[must_use]
    pub fn with_derivation_epoch_id(mut self, epoch_id: Uuid) -> Self {
        self.derivation_epoch_id = Some(epoch_id);
        self
    }

    /// Stamp the non-canonical lane this output was produced under
    /// (sinex-0vx.2). Leave unset for the canonical live path.
    #[must_use]
    pub fn with_derivation_lane_id(mut self, lane_id: Uuid) -> Self {
        self.derivation_lane_id = Some(lane_id);
        self
    }
}
