//! Relation extractor — [`ScopeReconciler`] implementation.
//!
//! Model classification: **`ScopeReconciler`** — maintains a sliding co-occurrence
//! window of resolved entities. When the window closes (gap > 300 s or entity
//! count reaches 2000), pairwise `entity.related` events are emitted.
//!
//! # Design note
//!
//! The spec originally called for a [`Windowed`], but that model can only emit
//! *one* output event per window completion. A co-occurrence window of N entities
//! produces O(N²) pairwise relations. Using [`ScopeReconciler`] is the
//! correct fit: its `reconcile()` returns `Vec<DerivedOutput>`, supporting
//! multi-emission per trigger while still checkpointing persistent state.
//!
//! The scope key is fixed at `"co-occurrence-window"` — a singleton scope so
//! all entities share one sliding window. If richer scoping is desired later
//! (e.g., per-source co-occurrence), the scope key can be partitioned.

use serde::{Deserialize, Serialize};
use crate::node_sdk::derived_node::{AutomatonContext, DerivedOutput, ScopeReconcilerNodeAdapter};
use crate::node_sdk::{InputProvenanceFilter, NodeLogicError, ScopeReconciler};
use sinex_primitives::Uuid;
use sinex_primitives::domain::{RelationType, SyntheticTemporalPolicy};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{EntityRelatedPayload, EntityResolvedPayload};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::{Duration, Timestamp};

/// Co-occurrence window configuration constants.
///
/// `MAX_WINDOW_ENTITIES` was 2000 historically. At that size a single
/// capacity-bound close emits N*(N-1)/2 = ~2M relation events. Each event
/// also cloned the full Vec of `source_event_ids` (one per window entry).
/// Memory cost: 2M outputs × 2000 × 16-byte UUIDs = 64 GB allocated per
/// close, with the kernel's `MemoryHigh` pressure throttling it down to a
/// "mere" 4 GB RSS leak before OOM. That's the bug fixed here.
///
/// 50 entries gives 50*49/2 = 1225 pairs per close — manageable. Combined
/// with the trimmed `source_event_ids` (only the 2 contributing entries
/// per pair, not the whole window), per-close emission cost drops from
/// ~64 GB to ~60 KB.
const MAX_WINDOW_ENTITIES: usize = 50;

/// Inter-event gap that triggers a window close. 300 s = "5 minutes of
/// quiet" — natural-rest boundary in the user's activity stream.
const WINDOW_GAP_SECS: i64 = 300;

/// Force-emit interval. Even under continuous activity (no 300 s gap),
/// the window closes periodically so accumulated co-occurrences flow
/// downstream. Without this, only `MAX_WINDOW_ENTITIES` triggers
/// emission — works, but produces large bursts at irregular intervals.
/// 60 s gives steady downstream pressure and bounds in-memory window
/// growth to ~60 s of arrivals at typical activity rates.
const WINDOW_FORCE_EMIT_SECS: i64 = 60;

const CO_OCCURRENCE_CONFIDENCE: f64 = 0.5;

/// Persistent window state: the current co-occurrence window.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelationExtractorState {
    /// Entities currently in the co-occurrence window, with their arrival times.
    pub window: Vec<WindowEntry>,

    /// Time of the most recent entity added to the window.
    pub last_seen: Option<Timestamp>,

    /// Time the current window was opened (first entry's arrival time).
    /// Used together with `WINDOW_FORCE_EMIT_SECS` to bound window age
    /// even when arrivals are continuous (no 300 s inter-event gap).
    pub window_started_at: Option<Timestamp>,

    /// Total relations emitted (for observability).
    pub relations_emitted: u64,
}

/// A single entity entry in the co-occurrence window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowEntry {
    pub entity_id: Uuid,
    pub canonical_name: String,
    pub entity_type: String,
    pub arrived_at: Timestamp,
    pub trigger_uuid: Uuid,
}

#[derive(Default)]
pub struct RelationExtractor;

/// Fixed scope key — all entities share one co-occurrence window.
const CO_OCCURRENCE_SCOPE: &str = "co-occurrence-window";

impl ScopeReconciler for RelationExtractor {
    type State = RelationExtractorState;
    type Input = EntityResolvedPayload;
    type Output = EntityRelatedPayload;

    fn name(&self) -> &'static str {
        "relation-extractor"
    }

    fn input_event_type(&self) -> &'static str {
        EntityResolvedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        EntityRelatedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        EntityRelatedPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::SynthesizedOnly
    }

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
    }

    fn scope_keys(&self, _input: &Self::Input, _context: &AutomatonContext) -> Vec<String> {
        vec![CO_OCCURRENCE_SCOPE.to_string()]
    }

    async fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
        debug_assert_eq!(
            scope_key, CO_OCCURRENCE_SCOPE,
            "relation extractor only supports fixed scope 'co-occurrence-window'"
        );

        let now = context.require_ts_orig()?;
        let trigger_uuid = context.trigger_uuid();

        // ── Window-close detection ───────────────────────────────────────
        // Three independent triggers, any of which closes the window:
        //   1. Gap: 300 s since the last arrival (natural quiescence).
        //   2. Age: 60 s since the window opened (force-emit even under
        //      continuous activity so co-occurrences flow steadily).
        //   3. Capacity: MAX_WINDOW_ENTITIES (defensive bound on burst).
        let gap_triggered = state
            .last_seen
            .is_some_and(|last| now - last >= Duration::seconds(WINDOW_GAP_SECS));
        let age_triggered = state
            .window_started_at
            .is_some_and(|opened| now - opened >= Duration::seconds(WINDOW_FORCE_EMIT_SECS));
        let capacity_triggered = state.window.len() >= MAX_WINDOW_ENTITIES;
        let should_close =
            (gap_triggered || age_triggered || capacity_triggered) && state.window.len() >= 2;

        let mut outputs = Vec::new();
        if should_close {
            outputs = drain_and_emit_pairs(state, now);
        }

        // ── Add current entity to window ─────────────────────────────────
        if state.window.is_empty() {
            state.window_started_at = Some(now);
        }
        state.window.push(WindowEntry {
            entity_id: input.entity_id,
            canonical_name: input.canonical_name,
            entity_type: input.entity_type.into_string(),
            arrived_at: now,
            trigger_uuid,
        });
        state.last_seen = Some(now);

        Ok(outputs)
    }
}

/// Drain the current window and produce one `entity.related` event per
/// pair. Each emitted event's `source_event_ids` carries ONLY the two
/// contributing entries' trigger UUIDs — not the whole window. That
/// trim is the load-bearing memory fix: with the full window cloned per
/// pair, a window of N produced O(N²) outputs × N source IDs each =
/// O(N³) memory. With the trim it's O(N²) outputs × 2 source IDs = O(N²).
fn drain_and_emit_pairs(
    state: &mut RelationExtractorState,
    now: Timestamp,
) -> Vec<DerivedOutput<EntityRelatedPayload>> {
    let entries = std::mem::take(&mut state.window);
    state.window_started_at = None;
    let ts_orig = entries.last().map_or(now, |e| e.arrived_at);

    let mut outputs = Vec::with_capacity(entries.len() * (entries.len().saturating_sub(1)) / 2);
    for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            let payload = EntityRelatedPayload {
                source_entity_id: entries[i].entity_id,
                target_entity_id: entries[j].entity_id,
                relation_type: RelationType::new("co_occurs_with"),
                confidence: CO_OCCURRENCE_CONFIDENCE,
            };
            // Only the two contributing entries' triggers — see drain_and_emit_pairs doc.
            let source_event_ids = vec![entries[i].trigger_uuid, entries[j].trigger_uuid];
            let output = DerivedOutput::reconciled(
                payload,
                ts_orig,
                source_event_ids,
                CO_OCCURRENCE_SCOPE.to_string(),
            )
            .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version("1.0.0")
            .with_equivalence_key(format!(
                "relation:{}:{}:co_occurs_with",
                entries[i].entity_id, entries[j].entity_id
            ));
            outputs.push(output);
            state.relations_emitted += 1;
        }
    }
    outputs
}

/// Node type alias registered via `AutomatonSpec` in `automata::registry`.
pub type RelationExtractorNode = ScopeReconcilerNodeAdapter<RelationExtractor>;

// ── Source-unit descriptor (issue #690 / #734) ──────────────────────────────

use sinex_primitives::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitBinding,
    SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

register_source_unit! {
    SourceUnitDescriptor {
        id: "relation-extractor",
        namespace: "derived",
        event_types: &[
            ("relation-extractor", "entity.related"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_entity_id, target_entity_id, relation_type)",
        ),
        access_policy: "event_stream_read",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:relation-extractor"),
        "relation-extractor",
        "derived",
    )
    .implementation("sinex-process")
    .adapter("AutomatonRuntime")
    .output_event_type("entity.related")
    .privacy_context("inherits_from_parents")
    .material_policy("derived_parents")
    .checkpoint_policy("append_stream")
    .resource_shape("event_stream_consumer")
    .source_unit_id("relation-extractor")
    .runner_pack("process")
    .checkpoint_family(SuCheckpointFamily::AppendStream)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:process")
    .build_impact(sinex_primitives::proof::SourceUnitBuildImpact::ZERO)
    .build()
}
