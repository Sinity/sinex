//! Relation extractor — [`ScopeReconcilerNode`] implementation.
//!
//! Model classification: **ScopeReconciler** — maintains a sliding co-occurrence
//! window of resolved entities. When the window closes (gap > 300 s or entity
//! count reaches 2000), pairwise `entity.related` events are emitted.
//!
//! # Design note
//!
//! The spec originally called for a [`WindowedNode`], but that model can only emit
//! *one* output event per window completion. A co-occurrence window of N entities
//! produces O(N²) pairwise relations. Using [`ScopeReconcilerNode`] is the
//! correct fit: its `reconcile()` returns `Vec<DerivedOutput>`, supporting
//! multi-emission per trigger while still checkpointing persistent state.
//!
//! The scope key is fixed at `"co-occurrence-window"` — a singleton scope so
//! all entities share one sliding window. If richer scoping is desired later
//! (e.g., per-source co-occurrence), the scope key can be partitioned.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{
    DerivedOutput, DerivedTriggerContext, ScopeReconcilerNodeAdapter,
};
use sinex_node_sdk::{InputProvenanceFilter, NodeLogicError, ScopeReconcilerNode};
use sinex_primitives::Uuid;
use sinex_primitives::domain::{RelationType, SyntheticTemporalPolicy};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{EntityRelatedPayload, EntityResolvedPayload};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::{Duration, Timestamp};

/// Co-occurrence window configuration constants.
const MAX_WINDOW_ENTITIES: usize = 2000;
const WINDOW_GAP_SECS: i64 = 300;
const CO_OCCURRENCE_CONFIDENCE: f64 = 0.5;

/// Persistent window state: the current co-occurrence window.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelationExtractorState {
    /// Entities currently in the co-occurrence window, with their arrival times.
    pub window: Vec<WindowEntry>,

    /// Time of the most recent entity added to the window.
    pub last_seen: Option<Timestamp>,

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

impl ScopeReconcilerNode for RelationExtractor {
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

    fn scope_keys(&self, _input: &Self::Input, _context: &DerivedTriggerContext) -> Vec<String> {
        vec![CO_OCCURRENCE_SCOPE.to_string()]
    }

    async fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
        debug_assert_eq!(
            scope_key, CO_OCCURRENCE_SCOPE,
            "relation extractor only supports fixed scope 'co-occurrence-window'"
        );

        let now = context.require_ts_orig()?;
        let trigger_uuid = context.trigger_uuid();

        // ── Gap detection: close window if gap > 300s ────────────────────
        let should_close = state.last_seen.is_some_and(|last| {
            let gap = now - last;
            gap >= Duration::seconds(WINDOW_GAP_SECS)
        });

        let mut outputs = Vec::new();

        if should_close && state.window.len() >= 2 {
            // ── Emit pairwise relations ──────────────────────────────────
            let entries = std::mem::take(&mut state.window);
            let source_event_ids: Vec<Uuid> = entries.iter().map(|e| e.trigger_uuid).collect();
            let ts_orig = entries.last().map(|e| e.arrived_at).unwrap_or(now);

            for i in 0..entries.len() {
                for j in (i + 1)..entries.len() {
                    let payload = EntityRelatedPayload {
                        source_entity_id: entries[i].entity_id,
                        target_entity_id: entries[j].entity_id,
                        relation_type: RelationType::new("co_occurs_with"),
                        confidence: CO_OCCURRENCE_CONFIDENCE,
                    };

                    let output = DerivedOutput::reconciled(
                        payload,
                        ts_orig,
                        source_event_ids.clone(),
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
        } else if state.window.len() >= MAX_WINDOW_ENTITIES {
            // ── Capacity-bound close ─────────────────────────────────────
            let entries = std::mem::take(&mut state.window);
            let source_event_ids: Vec<Uuid> = entries.iter().map(|e| e.trigger_uuid).collect();
            let ts_orig = entries.last().map(|e| e.arrived_at).unwrap_or(now);

            for i in 0..entries.len() {
                for j in (i + 1)..entries.len() {
                    let payload = EntityRelatedPayload {
                        source_entity_id: entries[i].entity_id,
                        target_entity_id: entries[j].entity_id,
                        relation_type: RelationType::new("co_occurs_with"),
                        confidence: CO_OCCURRENCE_CONFIDENCE,
                    };

                    let output = DerivedOutput::reconciled(
                        payload,
                        ts_orig,
                        source_event_ids.clone(),
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
        }

        // ── Add current entity to window ─────────────────────────────────
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

/// Node type alias for use with `node_entrypoint!`.
pub type RelationExtractorNode = ScopeReconcilerNodeAdapter<RelationExtractor>;

// ── Source-unit descriptor (issue #690 / #734) ──────────────────────────────

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};

register_source_unit! {
    SourceUnitDescriptor {
        id: "relation-extractor",
        namespace: "derived",
        runner_pack: "process",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("relation-extractor", "entity.related"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_entity_id, target_entity_id, relation_type)",
        ),
        access_policy: "event_stream_read",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:process",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}
