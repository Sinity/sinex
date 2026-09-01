//! Entity resolver — [`Windowed`] implementation.
//!
//! Model classification: **Windowed** — stateful deduplication over extracted
//! entities. Each `entity.extracted` candidate is canonicalized by type and
//! assigned a deterministic `UUIDv5` identity. Already-resolved entities are
//! silently skipped.
//!
//! # Design note
//!
//! The processing model is 1:1 (one input → zero or one output), but the
//! stateful deduplication map needs checkpoint persistence. A `Windowed`
//! with instant windows (`window_complete` returns true whenever a pending
//! resolution exists) gives exactly the 1:1 semantics with full state
//! persistence without widening to a `ScopeReconciler`.

use crate::runtime::automaton::{AutomatonContext, DerivedOutput, WindowedAdapter};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, Windowed};
use serde::{Deserialize, Serialize};
use sinex_primitives::Uuid;
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::{EntityTypeName, SyntheticTemporalPolicy};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{EntityExtractedPayload, EntityResolvedPayload};
use sinex_primitives::temporal::Timestamp;
use std::collections::HashMap;
use tracing::warn;

/// sinex-audit-entity-unbounded-maps: cap on the dedup cache. Unlike
/// `interval_lift`'s `active_subject_states` (bounded to 1024 because that map
/// tracks *concurrently open* states, a naturally small cardinality),
/// `known_entities` is a cumulative "every entity ever resolved" cache whose
/// key space is every distinct `(entity_type, canonical_name)` seen over the
/// automaton's lifetime -- much larger cardinality is expected, especially
/// while replaying a large historical backlog. 20_000 entries at roughly
/// 100 bytes each (a short `String` key plus a `KnownEntity`) caps growth
/// around ~2 MB while still giving the dedup cache room to be useful.
/// Eviction is safe: a re-resolved entity gets the same deterministic
/// `UUIDv5` id and the same `entity-resolver:{id}:{name}` equivalence key,
/// so a duplicate `entity.resolved` re-emission is caught by the normal
/// admission-time equivalence-key dedup rather than persisted twice.
const MAX_KNOWN_ENTITIES: usize = 20_000;

/// A dedup-cache entry: the deterministic entity id plus a staleness marker
/// used by the bounded-map eviction guard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownEntity {
    pub entity_id: Uuid,
    /// Time this key was last resolved/touched. Falls back to the
    /// automaton's processing time when the trigger event carries no
    /// `ts_orig`, so eviction ordering never blocks on a required source
    /// timestamp.
    pub last_touched: Timestamp,
}

/// Persistent resolver state: the deduplication map of `canonical_key` → `entity_id`.
///
/// Checkpointed by the runtime so restarts do not re-compute the same `UUIDv5`
/// identities from scratch.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResolverState {
    /// Map from `"{entity_type}:{canonical_name}"` to a bounded dedup-cache
    /// entry. Bounded to `MAX_KNOWN_ENTITIES` with stalest-eviction (see
    /// `MAX_KNOWN_ENTITIES` doc) -- sinex-audit-entity-unbounded-maps.
    pub known_entities: HashMap<String, KnownEntity>,

    /// Number of new candidates processed (for observability).
    pub candidates_processed: u64,

    /// Pending resolution to emit on the next `emit()` call.
    /// If `None`, the window is not complete.
    pending: Option<EntityResolvedPayload>,

    /// Provenance parent for the pending resolution.
    ///
    /// Entity IDs are deterministic UUIDv5 occurrence identities and must stay
    /// in the payload/equivalence key. Derived provenance must point at the
    /// triggering event interpretation, which is a UUIDv7.
    #[serde(default)]
    pending_source_event_id: Option<Uuid>,
}

/// Derivation control-plane declaration for `entity-resolver` (sinex-0vx.1/0vx.3).
pub const ENTITY_RESOLVER_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "entity-resolver.entity.resolved",
        owner: "entity-resolver",
        product_class: DerivedProductClass::SemanticCandidate,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("entity-resolver"),
        output_event_type: Some("entity.resolved"),
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
        verification_command: "xtask test -p sinexd -E 'test(entity_resolver)'",
    }];

#[derive(Default)]
pub struct EntityResolver;

impl Windowed for EntityResolver {
    type State = ResolverState;
    type Input = EntityExtractedPayload;
    type Output = EntityResolvedPayload;

    fn name(&self) -> &'static str {
        "entity-resolver"
    }

    fn input_event_type(&self) -> &'static str {
        EntityExtractedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        EntityResolvedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        EntityResolvedPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::SynthesizedOnly
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        ENTITY_RESOLVER_OUTPUT_DECLARATIONS;

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<(), AutomatonLogicError> {
        // ── Type-aware canonicalization ──────────────────────────────────
        let canonical_name = canonicalize_name(&input.entity_type, &input.raw_name);
        let touch_time = context.ts_orig.unwrap_or_else(Timestamp::now);

        // ── Deduplication check ──────────────────────────────────────────
        let key = canonical_key(&input.entity_type, &canonical_name);
        if let Some(existing) = state.known_entities.get_mut(&key) {
            // Already resolved — refresh staleness ordering, skip re-emission.
            existing.last_touched = touch_time;
            return Ok(());
        }

        // sinex-audit-entity-unbounded-maps: bound the map — evict the
        // stalest known entity (with a debt warning) before inserting a
        // genuinely new one at capacity. See `MAX_KNOWN_ENTITIES` doc for why
        // eviction here is safe (equivalence-key dedup catches re-emission).
        if state.known_entities.len() >= MAX_KNOWN_ENTITIES {
            if let Some(stalest) = state
                .known_entities
                .iter()
                .min_by_key(|(_, v)| v.last_touched)
                .map(|(k, _)| k.clone())
            {
                warn!(
                    module = "entity-resolver",
                    evicted = %stalest,
                    cap = MAX_KNOWN_ENTITIES,
                    "entity-resolver evicted stalest known entity (durable debt, unbounded-growth guard)"
                );
                state.known_entities.remove(&stalest);
            }
        }

        // ── Deterministic identity ───────────────────────────────────────
        let entity_id = compute_entity_id(&input.entity_type, &canonical_name);
        state.known_entities.insert(
            key,
            KnownEntity {
                entity_id,
                last_touched: touch_time,
            },
        );
        state.candidates_processed += 1;

        // ── Stage for emission ───────────────────────────────────────────
        state.pending_source_event_id = Some(context.trigger_uuid());
        state.pending = Some(EntityResolvedPayload {
            entity_id,
            canonical_name,
            entity_type: input.entity_type,
            original_name: input.raw_name,
        });

        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        state.pending.is_some()
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let Some(payload) = state.pending.take() else {
            return Ok(None);
        };
        let source_event_id = state
            .pending_source_event_id
            .take()
            .unwrap_or_else(|| context.trigger_uuid());

        let entity_id = payload.entity_id;
        let canonical_name = payload.canonical_name.clone();

        let declaration = &ENTITY_RESOLVER_OUTPUT_DECLARATIONS[0];
        let output = DerivedOutput::windowed_now(payload, vec![source_event_id])
            .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
            .with_semantics_version("1.0.0")
            .with_derived_equivalence_key(declaration, format!("{entity_id}:{canonical_name}"))
            .with_declaration_id(declaration.declaration_id)
            .with_product_class(declaration.product_class)
            .with_claim_support(declaration.default_support.instantiate(1, 0, 1, 0));

        Ok(Some(output))
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type EntityResolverRuntime = WindowedAdapter<EntityResolver>;

// ── Canonicalization logic ──────────────────────────────────────────────────

/// Compute the canonical form of an entity name, based on its type.
pub(crate) fn canonicalize_name(entity_type: &EntityTypeName, raw_name: &str) -> String {
    match entity_type.as_str() {
        "tool" => raw_name.trim().to_lowercase(),
        "url" => normalize_url_host(raw_name),
        "file" => raw_name.trim().to_string(),
        _ => raw_name.trim().to_lowercase(),
    }
}

/// Build the stable lookup key: `"{entity_type}:{canonical_name}"`.
pub(crate) fn canonical_key(entity_type: &EntityTypeName, canonical_name: &str) -> String {
    format!("{}:{}", entity_type.as_str(), canonical_name)
}

/// Deterministic `UUIDv5` from `(entity_type, canonical_name)`.
pub(crate) fn compute_entity_id(entity_type: &EntityTypeName, canonical_name: &str) -> Uuid {
    let input = format!("{}:{}", entity_type.as_str(), canonical_name);
    Uuid::new_v5(&Uuid::NAMESPACE_OID, input.as_bytes())
}

/// Normalize a URL host: lowercase, strip `www.` prefix.
fn normalize_url_host(raw: &str) -> String {
    let trimmed = raw.trim().to_lowercase();
    let stripped = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(&trimmed);
    // Remove trailing slash and path
    let host = match stripped.find('/') {
        Some(pos) => &stripped[..pos],
        None => stripped,
    };
    host.strip_prefix("www.").unwrap_or(host).to_string()
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
        id: "entity-resolver",
        namespace: "derived",
        event_types: &[
            ("entity-resolver", "entity.resolved"),
        ],
        source_role: sinex_primitives::sources::SourceRole::Activity,
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(entity_type, canonical_name)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:entity-resolver"),
        "entity-resolver",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("entity.resolved")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("entity-resolver")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .recovery_policy(sinex_primitives::source_contracts::SourceRecoveryPolicy::DERIVED_INTERNAL)
    .build()
}

#[cfg(test)]
#[path = "entity_resolver_test.rs"]
mod tests;
