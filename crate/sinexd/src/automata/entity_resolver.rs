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
use crate::runtime::{InputProvenanceFilter, AutomatonLogicError, Windowed};
use serde::{Deserialize, Serialize};
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EntityTypeName, SyntheticTemporalPolicy};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{EntityExtractedPayload, EntityResolvedPayload};
use std::collections::HashMap;

/// Persistent resolver state: the deduplication map of `canonical_key` → `entity_id`.
///
/// Checkpointed by the SDK so restarts do not re-compute the same `UUIDv5`
/// identities from scratch.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResolverState {
    /// Map from `"{entity_type}:{canonical_name}"` to deterministic `UUIDv5` entity ID.
    pub known_entities: HashMap<String, Uuid>,

    /// Number of new candidates processed (for observability).
    pub candidates_processed: u64,

    /// Pending resolution to emit on the next `emit()` call.
    /// If `None`, the window is not complete.
    pending: Option<EntityResolvedPayload>,
}

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
    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        _context: &AutomatonContext,
    ) -> Result<(), AutomatonLogicError> {
        // ── Type-aware canonicalization ──────────────────────────────────
        let canonical_name = canonicalize_name(&input.entity_type, &input.raw_name);

        // ── Deduplication check ──────────────────────────────────────────
        let key = canonical_key(&input.entity_type, &canonical_name);
        if state.known_entities.contains_key(&key) {
            // Already resolved — skip.
            return Ok(());
        }

        // ── Deterministic identity ───────────────────────────────────────
        let entity_id = compute_entity_id(&input.entity_type, &canonical_name);
        state.known_entities.insert(key, entity_id);
        state.candidates_processed += 1;

        // ── Stage for emission ───────────────────────────────────────────
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
        _context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let Some(payload) = state.pending.take() else {
            return Ok(None);
        };

        let entity_id = payload.entity_id;
        let canonical_name = payload.canonical_name.clone();

        let output = DerivedOutput::windowed_now(payload, vec![entity_id])
            .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
            .with_semantics_version("1.0.0")
            .with_equivalence_key(format!("entity-resolver:{entity_id}:{canonical_name}"));

        Ok(Some(output))
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type EntityResolverNode = WindowedAdapter<EntityResolver>;

// ── Canonicalization logic ──────────────────────────────────────────────────

/// Compute the canonical form of an entity name, based on its type.
fn canonicalize_name(entity_type: &EntityTypeName, raw_name: &str) -> String {
    match entity_type.as_str() {
        "tool" => raw_name.trim().to_lowercase(),
        "url" => normalize_url_host(raw_name),
        "file" => raw_name.trim().to_string(),
        _ => raw_name.trim().to_lowercase(),
    }
}

/// Build the stable lookup key: `"{entity_type}:{canonical_name}"`.
fn canonical_key(entity_type: &EntityTypeName, canonical_name: &str) -> String {
    format!("{}:{}", entity_type.as_str(), canonical_name)
}

/// Deterministic `UUIDv5` from `(entity_type, canonical_name)`.
fn compute_entity_id(entity_type: &EntityTypeName, canonical_name: &str) -> Uuid {
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

use sinex_primitives::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceRuntimeBinding,
    SourceContract, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

register_source_contract! {
    SourceContract {
        id: "entity-resolver",
        namespace: "derived",
        event_types: &[
            ("entity-resolver", "entity.resolved"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(entity_type, canonical_name)",
        ),
        access_policy: "event_stream_read",
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
    .privacy_context("inherits_from_parents")
    .material_policy("derived_parents")
    .checkpoint_policy("append_stream")
    .resource_shape("event_stream_consumer")
    .source_id("entity-resolver")
    .runner_pack("sinexd")
    .checkpoint_family(SuCheckpointFamily::AppendStream)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("in_process:sinexd")
    .build_impact(sinex_primitives::proof::SourceBuildImpact::ZERO)
    .build()
}
