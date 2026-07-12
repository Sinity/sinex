//! Entity enricher — [`ScopeReconciler`] implementation.
//!
//! Model classification: **`ScopeReconciler`** — each resolved entity is its own
//! scope. On every `entity.resolved` event, the per-entity state is updated with
//! temporal statistics (first/last seen, occurrence count, active-hours histogram).
//! A periodic sweep (default 5 min) emits enriched snapshots for entities that
//! have received new observations since the last emission.
//!
//! Category refinement maps `entity_type` to a coarse `EntityCategory`:
//! `tool` → Tool, `url`/`website` → Website, `file` → Document, etc.

use crate::runtime::automaton::{AutomatonContext, DerivedOutput, ScopeReconcilerAdapter};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, ScopeReconciler};
use serde::{Deserialize, Serialize};
use sinex_primitives::Uuid;
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::{EntityTypeName, SyntheticTemporalPolicy};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    EntityCategory, EntityEnrichedPayload, EntityResolvedPayload,
};
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
    #[serde(default)]
    pub source_event_ids: Vec<Uuid>,
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

/// Derivation control-plane declaration for `entity-enricher` (sinex-0vx.1/0vx.3).
pub const ENTITY_ENRICHER_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "entity-enricher.entity.enriched",
        owner: "entity-enricher",
        product_class: DerivedProductClass::SemanticCandidate,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("entity-enricher"),
        output_event_type: Some("entity.enriched"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::ExplicitOnly,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Heuristic,
            SourceCoverage::Partial,
            ClaimTemporalQuality::DeclaredEffective,
        ),
        verification_command: "xtask test -p sinexd -E 'test(entity_enricher)'",
    }];

#[derive(Default)]
pub struct EntityEnricher {
    pub config: EnricherConfig,
}

impl ScopeReconciler for EntityEnricher {
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

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        ENTITY_ENRICHER_OUTPUT_DECLARATIONS;

    fn scope_keys(&self, input: &Self::Input, _context: &AutomatonContext) -> Vec<String> {
        // Each entity is its own scope.
        vec![input.entity_id.to_string()]
    }

    async fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
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
                source_event_ids: Vec::new(),
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
        let trigger_uuid = context.trigger_uuid();
        if !stats.source_event_ids.contains(&trigger_uuid) {
            stats.source_event_ids.push(trigger_uuid);
        }

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
            let Some(stats) = state.entities.get_mut(&key) else {
                continue;
            };

            let category = refine_category(&stats.entity_type);
            let source_event_ids = if stats.source_event_ids.is_empty() {
                vec![trigger_uuid]
            } else {
                std::mem::take(&mut stats.source_event_ids)
            };

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
                DerivedOutput::reconciled(payload, now, source_event_ids, entity_key.clone())
                    .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
                    .with_semantics_version("1.0.0")
                    .with_equivalence_key(format!("entity-enricher:{entity_id}:{now}"));

            outputs.push(output);
        }

        Ok(outputs)
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type EntityEnricherRuntime = ScopeReconcilerAdapter<EntityEnricher>;

// ── Helper functions ────────────────────────────────────────────────────────

/// Extract the hour-of-day (0-23) from a timestamp.
fn hour_of_day(ts: Timestamp) -> u8 {
    // Use the raw seconds-since-epoch to compute the hour.
    let total_seconds = ts.unix_timestamp();
    let hours = (total_seconds / 3600) % 24;
    hours as u8
}

/// Map `entity_type` to a coarse `EntityCategory`.
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

// ── Source descriptor (issue #690 / #734) ──────────────────────────────

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

register_source_contract! {
    SourceContract {
        id: "entity-enricher",
        namespace: "derived",
        event_types: &[
            ("entity-enricher", "entity.enriched"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(entity_id, observation_window)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:entity-enricher"),
        "entity-enricher",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("entity.enriched")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("entity-enricher")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}

#[cfg(test)]
#[path = "entity_enricher_test.rs"]
mod tests;
