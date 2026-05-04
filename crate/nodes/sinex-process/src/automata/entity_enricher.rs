//! Entity enricher — [`ScopeReconcilerNode`] implementation.
//!
//! Model classification: **ScopeReconciler** — each resolved entity is its own
//! scope. On every `entity.resolved` event, the per-entity state is updated with
//! temporal statistics (first/last seen, occurrence count, active-hours histogram).
//! A periodic sweep (default 5 min) emits enriched snapshots for entities that
//! have received new observations since the last emission.
//!
//! Category refinement maps `entity_type` to a coarse `EntityCategory`:
//! `tool` → Tool, `url`/`website` → Website, `file` → Document, etc.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{
    DerivedOutput, DerivedTriggerContext, ScopeReconcilerNodeAdapter,
};
use sinex_node_sdk::{InputProvenanceFilter, NodeLogicError, ScopeReconcilerNode};
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EntityTypeName, SyntheticTemporalPolicy};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    EntityCategory, EntityEnrichedPayload, EntityResolvedPayload,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::{Duration, Timestamp};
use std::collections::{BTreeMap, HashMap};

/// Default reconciliation interval: emit enriched snapshots every 5 minutes.
const DEFAULT_RECONCILE_INTERVAL_SECS: i64 = 300;

/// Persistent enricher state: per-entity tracking plus global sweep timer.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EnricherState {
    /// Per-entity temporal statistics, keyed by `entity_id` (hex string).
    pub entities: HashMap<String, EntityStats>,

    /// Time of the last periodic sweep emission.
    pub last_sweep: Option<Timestamp>,

    /// Entities that have been updated since the last sweep.
    pub dirty_entities: Vec<Uuid>,
}

/// Accumulated temporal statistics for a single entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityStats {
    pub entity_id: Uuid,
    pub canonical_name: String,
    pub entity_type: String,
    pub first_seen: Timestamp,
    pub last_seen: Timestamp,
    pub occurrence_count: u64,
    /// Active-hours histogram: maps hour-of-day (0-23) to occurrence count.
    pub active_hours: BTreeMap<u8, u64>,
    /// Whether an enriched snapshot has been emitted (true after first emission).
    pub snapshot_emitted: bool,
    /// The time the last enriched snapshot was emitted.
    pub last_snapshot_at: Option<Timestamp>,
}

/// Configuration for the entity enricher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnricherConfig {
    /// Interval between periodic enrichment sweeps (seconds).
    #[serde(default = "default_reconcile_interval")]
    pub reconcile_interval_secs: i64,
}

fn default_reconcile_interval() -> i64 {
    DEFAULT_RECONCILE_INTERVAL_SECS
}

impl Default for EnricherConfig {
    fn default() -> Self {
        Self {
            reconcile_interval_secs: DEFAULT_RECONCILE_INTERVAL_SECS,
        }
    }
}

#[derive(Default)]
pub struct EntityEnricher {
    pub config: EnricherConfig,
}

impl ScopeReconcilerNode for EntityEnricher {
    type State = EnricherState;
    type Input = EntityResolvedPayload;
    type Output = EntityEnrichedPayload;

    fn name(&self) -> &'static str {
        "entity-enricher"
    }

    fn input_event_type(&self) -> &'static str {
        EntityResolvedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        EntityEnrichedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        EntityEnrichedPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::SynthesizedOnly
    }

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
    }

    fn scope_keys(&self, input: &Self::Input, _context: &DerivedTriggerContext) -> Vec<String> {
        // Each entity is its own scope.
        vec![input.entity_id.to_string()]
    }

    async fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
        let now = context.require_ts_orig()?;
        let entity_key = scope_key.to_string();

        // ── Update per-entity statistics ─────────────────────────────────
        let hour = hour_of_day(now);

        let stats = state
            .entities
            .entry(entity_key.clone())
            .or_insert_with(|| EntityStats {
                entity_id: input.entity_id,
                canonical_name: input.canonical_name.clone(),
                entity_type: input.entity_type.as_str().to_string(),
                first_seen: now,
                last_seen: now,
                occurrence_count: 0,
                active_hours: BTreeMap::new(),
                snapshot_emitted: false,
                last_snapshot_at: None,
            });

        if now < stats.first_seen {
            stats.first_seen = now;
        }
        if now > stats.last_seen {
            stats.last_seen = now;
        }
        stats.occurrence_count += 1;
        *stats.active_hours.entry(hour).or_insert(0) += 1;

        // Track dirty entities for periodic sweep.
        if !state.dirty_entities.contains(&input.entity_id) {
            state.dirty_entities.push(input.entity_id);
        }

        // ── Periodic sweep: emit enriched snapshots ──────────────────────
        let interval = Duration::seconds(self.config.reconcile_interval_secs);
        let should_sweep = state.last_sweep.is_none_or(|last| now - last >= interval);

        if !should_sweep {
            return Ok(Vec::new());
        }

        state.last_sweep = Some(now);
        let dirty = std::mem::take(&mut state.dirty_entities);

        let mut outputs = Vec::with_capacity(dirty.len());
        for entity_id in &dirty {
            let key = entity_id.to_string();
            let Some(stats) = state.entities.get(&key) else {
                continue;
            };

            let category = refine_category(&stats.entity_type);

            let payload = EntityEnrichedPayload {
                entity_id: *entity_id,
                canonical_name: stats.canonical_name.clone(),
                entity_type: EntityTypeName::new(&stats.entity_type),
                refined_category: category,
                first_seen: stats.first_seen,
                last_seen: stats.last_seen,
                occurrence_count: stats.occurrence_count,
                active_hours: stats.active_hours.clone(),
            };

            let output =
                DerivedOutput::reconciled(payload, now, vec![*entity_id], entity_key.clone())
                    .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
                    .with_semantics_version("1.0.0")
                    .with_equivalence_key(format!("entity-enricher:{}:{}", entity_id, now));

            outputs.push(output);
        }

        Ok(outputs)
    }
}

/// Node type alias for use with `node_entrypoint!`.
pub type EntityEnricherNode = ScopeReconcilerNodeAdapter<EntityEnricher>;

// ── Helper functions ────────────────────────────────────────────────────────

/// Extract the hour-of-day (0-23) from a timestamp.
fn hour_of_day(ts: Timestamp) -> u8 {
    // Use the raw seconds-since-epoch to compute the hour.
    let total_seconds = ts.unix_timestamp();
    let hours = (total_seconds / 3600) % 24;
    hours as u8
}

/// Map entity_type to a coarse EntityCategory.
fn refine_category(entity_type: &str) -> EntityCategory {
    match entity_type {
        "tool" | "binary" | "cli" => EntityCategory::Tool,
        "project" | "repo" | "repository" => EntityCategory::Project,
        "url" | "website" | "domain" => EntityCategory::Website,
        "file" | "document" | "doc" => EntityCategory::Document,
        "person" | "user" | "author" | "identity" => EntityCategory::Person,
        _ => EntityCategory::Document, // conservative default
    }
}

// ── Source-unit descriptor (issue #690 / #734) ──────────────────────────────

use sinex_primitives::register_source_unit;
use sinex_primitives::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};

register_source_unit! {
    SourceUnitDescriptor {
        id: "entity-enricher",
        namespace: "derived",
        runner_pack: "process",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("entity-enricher", "entity.enriched"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(entity_id, observation_window)",
        ),
        access_policy: "event_stream_read",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:process",
        build_impact: sinex_primitives::proof::SourceUnitBuildImpact::ZERO,
    }
}
