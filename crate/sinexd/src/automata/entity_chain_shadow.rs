//! Entity-chain shadow lane over a `DerivationScope::StreamCheckpoint` scope
//! (sinex-0vx.9).
//!
//! `pq5` (2026-07-08, operator-ratified) retired the 4-stage entity chain
//! (extractor -> resolver -> relation-extractor -> enricher, `sinex-ijz6`)
//! from the default-enabled automaton set, and ruled that its SOLE re-entry
//! path is a `sinex-0vx` shadow lane — never a direct re-enable of the live
//! automaton pipeline. This module is that re-entry path, and the second
//! `sinex-0vx` generality proof (`sinex-0vx.7`, sessionization, is the
//! first): it proves `DerivationScope::StreamCheckpoint` — the scope kind
//! continuous automata need because "all input ids" cannot be enumerated
//! over an unbounded confirmed-event stream — end-to-end over a real
//! continuous automaton chain.
//!
//! The extraction/resolution/relation logic below is PORTED from the live
//! automata (`crate::automata::entity_extractor::find_first_entity`,
//! `crate::automata::entity_resolver::{canonicalize_name, canonical_key,
//! compute_entity_id}`, `crate::automata::entity_enricher::refine_category`,
//! and the co-occurrence window constants from
//! `crate::automata::relation_extractor`) — not reimplemented. Those
//! functions are marked `pub(crate)` so this module can call them directly;
//! their bodies are untouched. This module only ADDS the batching and
//! frozen-scope machinery around them: read a bounded, frozen slice of
//! `core.events`, run the same pure per-event logic in-memory, and persist
//! the result to `derivation.lane_outputs` (never `core.events`) with an
//! honest, per-row [`ClaimSupport`] vector.
//!
//! ## Stream-position interpretation (scoping decision, tracked follow-up)
//!
//! `DerivationScope::StreamCheckpoint`'s `start_seq`/`end_seq` are named
//! after the durable JetStream consumer sequence they eventually
//! generalize to. Wiring an exact NATS wire-sequence read path (JetStream
//! direct-get by raw stream sequence) is real future infrastructure work,
//! not something this bead's scope covers — `core.events` does not
//! currently persist the NATS stream sequence a row was confirmed-published
//! under. This implementation interprets the ordinates as **stable ordinal
//! positions in the deterministic `core.events.id` (UUIDv7) ascending order,
//! restricted to `filter_subjects`**: `core.events` rows are append-only for
//! any already-covered range (replay archives before re-emitting, never
//! rewrites in place), and `id` is monotonically time-ordered per the
//! existing ts_coided/UUIDv7 doctrine, so a frozen `(start_seq, end_seq]`
//! ordinal range is exactly reproducible on re-run regardless of how many
//! further events accumulate past `end_seq`. This satisfies the scope's
//! actual contract — a frozen, non-wall-clock, byte-reproducible
//! high-watermark range — without inventing new schema. A follow-up
//! (`sinex-0vx.9` bead comment) tracks lifting this to a literal NATS
//! stream-sequence read when a named consumer needs byte-exact JetStream
//! sequence numbers rather than a DB-ordinal surrogate.
//!
//! ## Doctrine guard
//!
//! Every row this module writes lands in `derivation.lane_outputs` with
//! `product_class = semantic_candidate` and `adjudication = unreviewed` —
//! never `core.events`. The only bridge to the canonical event spine is the
//! curation `proposal -> judgment -> finalizer` path
//! ([`promote_entity_related_output`] below), exactly like every other
//! `sinex-0vx` lane.

use std::collections::HashMap;

use serde_json::json;
use sqlx::PgPool;

use sinex_db::repositories::{
    CreateDerivationEpoch, CreateDerivationLane, DbPoolExt, LaneOutputRow,
};
use sinex_primitives::Uuid;
use sinex_primitives::derivation::{
    ClaimSupport, ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationScope, DerivationWriteSurface, DerivedProductClass, InputEligibility, LaneDiffReport,
    SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::EntityTypeName;
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::events::payloads::{
    CurationJudgmentDecision, CurationJudgmentPayload, CurationProposalPayload,
    CurationProposalStatus, EntityCategory,
};
use sinex_primitives::events::{CurationJudgmentActorKind, Event, EventPayload};
use sinex_primitives::rpc::curation::CurationFinalizeRequest;
use sinex_primitives::semantic::{
    EntityRelationLaneOutputs, SemanticEntityOutput, SemanticRelationOutput,
};
use sinex_primitives::temporal::{Duration, Timestamp};

use crate::automata::entity_enricher::refine_category;
use crate::automata::entity_extractor::{extract_text_fields, find_first_entity};
use crate::automata::entity_resolver::{canonical_key, canonicalize_name, compute_entity_id};
use crate::automata::relation_extractor::{
    CO_OCCURRENCE_CONFIDENCE, MAX_WINDOW_ENTITIES, WINDOW_FORCE_EMIT_SECS, WINDOW_GAP_SECS,
};

/// `document.chunked` / `command.canonical` / `command.executed` — the same
/// text-bearing event types `EntityExtractor::input_event_types()` declares.
const ENTITY_CHAIN_FILTER_SUBJECTS: &[&str] =
    &["document.chunked", "command.canonical", "command.executed"];

const ENTITY_CHAIN_STREAM: &str = "core.events";

/// The `derivation.product_declarations` row this shadow lane's epochs/lanes
/// foreign-key against. `output_source`/`output_event_type` are `None`
/// (`CurationWriter`-style, not a `DerivedOutput` automaton write) — mirrors
/// `SEMANTIC_ENTITY_RELATION_DECLARATION` (`crate::api::handlers::semantic`)
/// and `CURATION_PROPOSAL_DECLARATION` (`crate::api::handlers::curation`):
/// one declaration_id covering the batched entity+relation candidates this
/// module writes, not the per-automaton declarations the live chain uses.
pub const ENTITY_CHAIN_SHADOW_DECLARATION_ID: &str =
    "entity-chain-shadow.entity_relation.semantic_candidate";

pub const ENTITY_CHAIN_SHADOW_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] = &[
    DerivationOutputDeclaration {
        declaration_id: ENTITY_CHAIN_SHADOW_DECLARATION_ID,
        owner: "entity-chain-shadow",
        product_class: DerivedProductClass::SemanticCandidate,
        write_surface: DerivationWriteSurface::CurationWriter,
        output_source: None,
        output_event_type: None,
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::ExplicitOnly,
        // Per-row ClaimSupport is computed HONESTLY from real evidence in
        // `run_entity_chain` below (never taken from this template) — this
        // is only the FK-bound placeholder the declaration itself carries.
        default_support: ClaimSupportTemplate::UNKNOWN,
        verification_command: "xtask test -p sinexd -E 'test(entity_chain_stream_checkpoint_shadow_lane)'",
    },
];

/// `authority.finalizer_registry` declaration authorizing promotion of an
/// entity-chain shadow-lane `entity.related` candidate (sinex-0vx.9),
/// mirroring `curation::CURATION_FINALIZER_DECLARATIONS`'s pattern exactly.
/// `derivation_declaration_id` must equal
/// `crate::api::handlers::curation::CURATION_FINALIZED_DECLARATION.declaration_id`
/// — every finalizer's output is the same `curation.finalized` receipt event,
/// checked directly (not duplicated as a string literal) by
/// [`promote_entity_related_output`] and by
/// `entity_chain_finalizer_targets_curation_finalized_declaration` in the
/// test module.
pub const ENTITY_CHAIN_RELATION_FINALIZER_DECLARATIONS:
    &[crate::authority::FinalizerDeclaration] = &[crate::authority::FinalizerDeclaration {
    finalizer_id: "entity-chain-shadow.entity.related",
    proposal_kind: "entity.related",
    output_source: "relation-extractor",
    output_event_type: "entity.related",
    output_product_class: DerivedProductClass::ReportArtifact,
    derivation_declaration_id: crate::api::handlers::curation::CURATION_FINALIZED_DECLARATION
        .declaration_id,
    // Safe default posture (0vx design decision (a)): promotion always
    // requires a human/policy-granted confirmation, same as every other
    // registered finalizer.
    requires_human_judgment: true,
    auto_accept_policy: None,
    registered_by: "entity-chain-shadow",
}];

/// One `core.events` row read back for shadow-lane processing.
struct StreamCheckpointEventRow {
    event_id: Uuid,
    payload: serde_json::Value,
    ts_orig: Timestamp,
    source_material_id: Option<Uuid>,
}

/// Freeze a `DerivationScope::StreamCheckpoint` scope for the entity chain:
/// `end_seq` is the count of matching rows at freeze instant — an explicit
/// operation, not a wall clock (see module doc). `start_seq = 0` covers the
/// full available history; pass a prior run's `end_seq` to scope a
/// subsequent incremental shadow run to only the newly-arrived slice.
pub async fn freeze_entity_chain_stream_checkpoint(
    pool: &PgPool,
    start_seq: u64,
) -> Result<DerivationScope> {
    let filter_subjects: Vec<String> = ENTITY_CHAIN_FILTER_SUBJECTS
        .iter()
        .map(|subject| (*subject).to_string())
        .collect();

    let matching = sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!" FROM core.events WHERE event_type = ANY($1)"#,
        &filter_subjects,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SinexError::database("count entity-chain shadow-lane candidate events")
            .with_std_error(&error)
    })?;
    let end_seq = u64::try_from(matching).unwrap_or(0);
    if end_seq < start_seq {
        return Err(SinexError::validation(
            "entity-chain stream checkpoint freeze: start_seq exceeds available matching events",
        )
        .with_context("start_seq", start_seq.to_string())
        .with_context("available", end_seq.to_string()));
    }

    let hash_input = format!(
        "{ENTITY_CHAIN_STREAM}|{}|{start_seq}|{end_seq}",
        filter_subjects.join(",")
    );
    let input_set_hash = blake3::hash(hash_input.as_bytes()).to_hex().to_string();

    Ok(DerivationScope::StreamCheckpoint {
        stream: ENTITY_CHAIN_STREAM.to_string(),
        filter_subjects,
        start_seq,
        end_seq: Some(end_seq),
        coverage_window: None,
        input_set_hash,
    })
}

/// Read the events covered by a FROZEN `StreamCheckpoint` scope. Errors if
/// the scope isn't a `StreamCheckpoint` or `end_seq` is `None` — an
/// open-ended scope is the live canonical lane, not a comparable shadow
/// snapshot, and has no defined byte-reproducible boundary to read.
async fn read_stream_checkpoint_events(
    pool: &PgPool,
    scope: &DerivationScope,
) -> Result<Vec<StreamCheckpointEventRow>> {
    let DerivationScope::StreamCheckpoint {
        filter_subjects,
        start_seq,
        end_seq,
        ..
    } = scope
    else {
        return Err(SinexError::validation(
            "read_stream_checkpoint_events requires a DerivationScope::StreamCheckpoint scope",
        ));
    };
    let end_seq = end_seq.ok_or_else(|| {
        SinexError::validation(
            "read_stream_checkpoint_events requires a frozen (Some) end_seq -- an open-ended \
             stream checkpoint is the live canonical lane, not a bounded shadow snapshot",
        )
    })?;
    let offset = i64::try_from(*start_seq).map_err(|_| {
        SinexError::validation("entity-chain stream checkpoint start_seq exceeds i64 range")
    })?;
    let limit = i64::try_from(end_seq.saturating_sub(*start_seq)).map_err(|_| {
        SinexError::validation("entity-chain stream checkpoint range exceeds i64 range")
    })?;

    let rows = sqlx::query!(
        r#"
        SELECT
            id as "id!: Uuid",
            payload as "payload!",
            ts_orig as "ts_orig!: Timestamp",
            source_material_id::uuid as "source_material_id: Uuid"
        FROM core.events
        WHERE event_type = ANY($1)
        ORDER BY id ASC
        OFFSET $2
        LIMIT $3
        "#,
        filter_subjects,
        offset,
        limit,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("read entity-chain stream checkpoint events").with_std_error(&error)
    })?;

    Ok(rows
        .into_iter()
        .map(|row| StreamCheckpointEventRow {
            event_id: row.id,
            payload: row.payload,
            ts_orig: row.ts_orig,
            source_material_id: row.source_material_id,
        })
        .collect())
}

/// One entry in the batched co-occurrence window (batched port of
/// `relation_extractor::WindowEntry`).
struct BatchWindowEntry {
    entity_id: Uuid,
    canonical_name: String,
    entity_type: String,
    arrived_at: Timestamp,
    source_event_id: Uuid,
}

/// Run the ported 4-stage entity chain over `events` in-memory, returning
/// both the typed [`EntityRelationLaneOutputs`] (for the generic
/// [`LaneDiffReport`]) and the [`LaneOutputRow`]s carrying an honest,
/// per-row [`ClaimSupport`] vector (for persistence). Deterministic: same
/// `events` slice in the same order always produces the same outputs and
/// row set, which is what makes a re-run over the identical frozen scope
/// byte-reproducible.
fn run_entity_chain(
    events: &[StreamCheckpointEventRow],
) -> (EntityRelationLaneOutputs, Vec<LaneOutputRow>) {
    let mut known_entities: HashMap<String, Uuid> = HashMap::new();
    let mut entity_outputs: Vec<SemanticEntityOutput> = Vec::new();
    let mut rows: Vec<LaneOutputRow> = Vec::new();

    let mut window: Vec<BatchWindowEntry> = Vec::new();
    let mut window_started_at: Option<Timestamp> = None;
    let mut last_seen: Option<Timestamp> = None;
    let mut relation_outputs: Vec<SemanticRelationOutput> = Vec::new();

    for event in events {
        // ── Stage 1: extractor (ported `find_first_entity`) ──────────────
        let text = extract_text_fields(&event.payload);
        if text.is_empty() {
            continue;
        }
        let Some(candidate) = find_first_entity(&text) else {
            continue;
        };

        // ── Stage 2: resolver (ported canonicalize_name/compute_entity_id) ─
        let canonical_name = canonicalize_name(&candidate.entity_type, &candidate.raw_name);
        let key = canonical_key(&candidate.entity_type, &canonical_name);
        let is_new = !known_entities.contains_key(&key);
        let entity_id = *known_entities
            .entry(key)
            .or_insert_with(|| compute_entity_id(&candidate.entity_type, &canonical_name));

        if is_new {
            // ── Stage 4: enricher category refinement, folded in at first
            // sight rather than a separate periodic sweep -- the shadow
            // lane is a bounded batch, not a live stream, so there is no
            // "periodic" to wait for.
            let category = refine_category(candidate.entity_type.as_str());
            let entity_key = entity_id.to_string();
            let output = SemanticEntityOutput {
                entity_key: entity_key.clone(),
                canonical_name: canonical_name.clone(),
                entity_type: candidate.entity_type.to_string(),
                category: Some(category_label(category).to_string()),
                confidence: Some(candidate.confidence),
                metadata: json!({
                    "source_event_id": event.event_id,
                    "original_name": candidate.raw_name,
                    "producer": "entity-chain-shadow",
                }),
            };
            let claim_support = honest_claim_support(event, 1);
            rows.push(LaneOutputRow {
                output_kind: "entity".to_string(),
                output_key: entity_key,
                payload: serde_json::to_value(&output).unwrap_or(serde_json::Value::Null),
                claim_support: claim_support_json(&claim_support),
                source_event_id: Some(event.event_id),
                source_material_id: event.source_material_id,
                source_anchor: None,
                metadata: json!({"producer": "entity-chain-shadow", "stage": "extractor+resolver"}),
            });
            entity_outputs.push(output);
        }

        // ── Stage 3: relation-extractor co-occurrence window (batched port
        // of `relation_extractor::reconcile`/`drain_and_emit_pairs`) ──────
        let now = event.ts_orig;
        let gap_triggered =
            last_seen.is_some_and(|last| now - last >= Duration::seconds(WINDOW_GAP_SECS));
        let age_triggered = window_started_at
            .is_some_and(|opened| now - opened >= Duration::seconds(WINDOW_FORCE_EMIT_SECS));
        let capacity_triggered = window.len() >= MAX_WINDOW_ENTITIES;
        let should_close =
            (gap_triggered || age_triggered || capacity_triggered) && window.len() >= 2;
        if should_close {
            drain_relation_pairs(&mut window, &mut relation_outputs, &mut rows);
            window_started_at = None;
        }
        if window.is_empty() {
            window_started_at = Some(now);
        }
        window.push(BatchWindowEntry {
            entity_id,
            canonical_name,
            entity_type: candidate.entity_type.to_string(),
            arrived_at: now,
            source_event_id: event.event_id,
        });
        last_seen = Some(now);
    }

    // The scope FREEZE is the batch-mode "next arrival" that closes the
    // final window (module doc: the freeze is an operation, not a wall
    // clock) -- draining here is the bounded-scope analog of
    // closure-by-next-arrival, not a clock-triggered close.
    if window.len() >= 2 {
        drain_relation_pairs(&mut window, &mut relation_outputs, &mut rows);
    }

    (
        EntityRelationLaneOutputs {
            entities: entity_outputs,
            relations: relation_outputs,
        },
        rows,
    )
}

fn drain_relation_pairs(
    window: &mut Vec<BatchWindowEntry>,
    relation_outputs: &mut Vec<SemanticRelationOutput>,
    rows: &mut Vec<LaneOutputRow>,
) {
    let entries = std::mem::take(window);
    for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            let a = &entries[i];
            let b = &entries[j];
            let relation_key = format!("{}:{}:co_occurs_with", a.entity_id, b.entity_id);
            let output = SemanticRelationOutput {
                relation_key: relation_key.clone(),
                source_entity_key: a.entity_id.to_string(),
                target_entity_key: b.entity_id.to_string(),
                predicate: "co_occurs_with".to_string(),
                weight: Some(CO_OCCURRENCE_CONFIDENCE),
                metadata: json!({
                    "source_names": [a.canonical_name, b.canonical_name],
                    "entity_types": [a.entity_type, b.entity_type],
                    "producer": "entity-chain-shadow",
                }),
            };
            let evidence_event_count = 2;
            let claim_support = ClaimSupport::unreviewed(
                SupportLevel::Heuristic,
                SourceCoverage::Covered,
                ClaimTemporalQuality::WindowBoundary,
                evidence_event_count,
                0,
                1,
                0,
            );
            rows.push(LaneOutputRow {
                output_kind: "relation".to_string(),
                output_key: relation_key,
                payload: serde_json::to_value(&output).unwrap_or(serde_json::Value::Null),
                claim_support: claim_support_json(&claim_support),
                source_event_id: Some(a.source_event_id),
                source_material_id: None,
                source_anchor: None,
                metadata: json!({
                    "producer": "entity-chain-shadow",
                    "stage": "relation-extractor",
                    "second_source_event_id": b.source_event_id,
                }),
            });
            relation_outputs.push(output);
        }
    }
}

/// Build an honest, per-row [`ClaimSupport`] vector from real evidence: the
/// `Heuristic` support level always applies (regex/pattern-match, never a
/// model or human), `source_coverage` reflects whether the row's source
/// event actually resolved to a registered source material (a real
/// evidentiary anchor, not an assumption), and `temporal_quality` reflects
/// that the entity's timestamp is inherited from the source event's own
/// `ts_orig` rather than observed independently. `adjudication` is always
/// `Unreviewed` -- promotion is a separate, explicit step
/// ([`promote_entity_related_output`]), never implicit here.
fn honest_claim_support(
    event: &StreamCheckpointEventRow,
    evidence_event_count: u32,
) -> ClaimSupport {
    let source_coverage = if event.source_material_id.is_some() {
        SourceCoverage::Covered
    } else {
        SourceCoverage::Partial
    };
    ClaimSupport::unreviewed(
        SupportLevel::Heuristic,
        source_coverage,
        ClaimTemporalQuality::IntrinsicContent,
        evidence_event_count,
        u32::from(event.source_material_id.is_some()),
        1,
        0,
    )
}

fn claim_support_json(support: &ClaimSupport) -> serde_json::Value {
    serde_json::to_value(support).unwrap_or(serde_json::Value::Null)
}

fn category_label(category: EntityCategory) -> &'static str {
    match category {
        EntityCategory::Tool => "tool",
        EntityCategory::Project => "project",
        EntityCategory::Website => "website",
        EntityCategory::Document => "document",
        EntityCategory::Person => "person",
    }
}

/// Full outcome of one [`run_entity_chain_shadow_lane`] call.
pub struct EntityChainShadowLaneResult {
    pub epoch_id: Uuid,
    pub baseline_lane_id: Uuid,
    pub candidate_lane_id: Uuid,
    pub outputs: EntityRelationLaneOutputs,
    pub diff: LaneDiffReport,
    pub scope: DerivationScope,
}

/// Run the entity chain as a SHADOW derivation lane over a frozen
/// `DerivationScope::StreamCheckpoint` scope, writing
/// `derivation.lane_outputs` (never `core.events`) with an honest
/// `ClaimSupport` vector per row, and recording a [`LaneDiffReport`] against
/// a freshly-created empty baseline lane.
///
/// Byte-reproducible: calling this twice with the SAME frozen `scope` (same
/// `input_set_hash`), the same `epoch_id`, and the same `existing_lanes`
/// produces byte-identical `outputs`/`diff.counts`/`diff.examples` —
/// `run_entity_chain` above is a pure function of the events the scope
/// resolves to, and the scope itself is frozen (an explicit `end_seq`, not
/// "whatever's in the table now").
///
/// `existing_lanes`: `None` mints a fresh `(baseline, candidate)` lane pair
/// (the normal first-run path). `Some((baseline_lane_id, candidate_lane_id))`
/// REUSES those lane rows instead of inserting new ones — a re-run under the
/// same epoch + scope is, by construction, the same lane identity
/// (`uk_derivation_lanes_declaration_kind_candidate_scope` would reject a
/// second insert under the identical natural key anyway), so re-running
/// upserts the candidate lane's outputs (`write_lane_outputs` is
/// `ON CONFLICT DO UPDATE`) rather than duplicating lane rows. The diff
/// report is still recomputed and returned for comparison, but only
/// persisted via `record_lane_diff` on a fresh-lane run — persisting the
/// byte-identical report a second time would itself collide on
/// `uk_derivation_lane_diffs_report`, which is exactly the reproducibility
/// property being proved, not a bug to route around silently.
pub async fn run_entity_chain_shadow_lane(
    pool: &PgPool,
    scope: DerivationScope,
    epoch_id: Option<Uuid>,
    existing_lanes: Option<(Uuid, Uuid)>,
    lane_suffix: &str,
) -> Result<EntityChainShadowLaneResult> {
    let DerivationScope::StreamCheckpoint { end_seq, .. } = &scope else {
        return Err(SinexError::validation(
            "entity-chain shadow lane requires a DerivationScope::StreamCheckpoint scope",
        ));
    };
    if end_seq.is_none() {
        return Err(SinexError::validation(
            "entity-chain shadow lane requires a frozen (Some) end_seq -- an open-ended stream \
             checkpoint is the live canonical lane, not a comparable shadow snapshot",
        ));
    }

    let scope_json = serde_json::to_value(&scope).map_err(|error| {
        SinexError::serialization("serialize entity-chain stream checkpoint scope")
            .with_std_error(&error)
    })?;
    let product_class = DerivedProductClass::SemanticCandidate.as_str();
    let repo = pool.derivation_lanes();

    // `epoch_id` means "re-run under this EXISTING epoch" (the bead's own
    // wording: "a frozen StreamCheckpoint scope re-run under the SAME
    // epoch is byte-reproducible") -- reuse the id directly rather than
    // minting a new epoch row. A fresh insert under an identical
    // (declaration_id, scope, semantics_version, config_hash) tuple would
    // collide with `uk_derivation_epochs_declaration_scope_semver_confighash`
    // anyway, since a frozen scope's config_hash IS its input_set_hash --
    // two runs over the identical frozen scope are, by construction, the
    // same epoch, not two epochs that happen to look alike.
    let epoch_id = match epoch_id {
        Some(id) => id,
        None => {
            repo.create_epoch(CreateDerivationEpoch {
                id: None,
                declaration_id: ENTITY_CHAIN_SHADOW_DECLARATION_ID.to_string(),
                name: format!("entity-chain-shadow-{lane_suffix}"),
                product_class: product_class.to_string(),
                scope_model: scope.scope_model().to_string(),
                scope: scope_json.clone(),
                semantics_version: "1.0.0".to_string(),
                code_ref: None,
                config_hash: scope.input_set_hash().to_string(),
                components: json!([
                    {"component": "entity-extractor", "version": "1.0.0"},
                    {"component": "entity-resolver", "version": "1.0.0"},
                    {"component": "relation-extractor", "version": "1.0.0"},
                    {"component": "entity-enricher", "version": "1.0.0"},
                ]),
                prompt_set_hash: None,
                model_config_hash: None,
                created_by: "entity-chain-shadow".to_string(),
                operation_id: None,
                supersedes_epoch_id: None,
            })
            .await?
            .id
        }
    };

    let is_rerun = existing_lanes.is_some();
    let (baseline_lane_id, candidate_lane_id) = match existing_lanes {
        Some(ids) => ids,
        None => {
            let baseline_lane = repo
                .create_lane(CreateDerivationLane {
                    id: None,
                    declaration_id: ENTITY_CHAIN_SHADOW_DECLARATION_ID.to_string(),
                    name: format!("entity-chain-shadow-baseline-{lane_suffix}"),
                    // `canonical`, not `shadow`: the uniqueness key
                    // (declaration_id, kind, candidate_epoch_id, scope) would
                    // otherwise collide with the candidate lane below, which
                    // shares the same epoch and scope by construction --
                    // this mirrors the established baseline/candidate
                    // convention (`sinex-db/tests/repositories_derivation.rs`:
                    // baseline lanes are `kind = "canonical"`, candidate
                    // lanes are `kind = "shadow"`).
                    kind: "canonical".to_string(),
                    product_class: product_class.to_string(),
                    base_epoch_id: None,
                    candidate_epoch_id: epoch_id,
                    scope: scope_json.clone(),
                    purpose: Some(
                        "empty baseline for entity-chain stream-checkpoint diff".to_string(),
                    ),
                    operation_id: None,
                    expires_at: None,
                })
                .await?;
            repo.set_lane_status(baseline_lane.id, "completed", Some(Timestamp::now()))
                .await?;

            let candidate_lane = repo
                .create_lane(CreateDerivationLane {
                    id: None,
                    declaration_id: ENTITY_CHAIN_SHADOW_DECLARATION_ID.to_string(),
                    name: format!("entity-chain-shadow-{lane_suffix}"),
                    kind: "shadow".to_string(),
                    product_class: product_class.to_string(),
                    base_epoch_id: Some(epoch_id),
                    candidate_epoch_id: epoch_id,
                    scope: scope_json,
                    purpose: Some(
                        "pq5-B entity-chain re-entry over a frozen StreamCheckpoint scope"
                            .to_string(),
                    ),
                    operation_id: None,
                    expires_at: None,
                })
                .await?;
            (baseline_lane.id, candidate_lane.id)
        }
    };

    let events = read_stream_checkpoint_events(pool, &scope).await?;
    let (outputs, rows) = run_entity_chain(&events);

    if !rows.is_empty() {
        repo.write_lane_outputs(candidate_lane_id, product_class, &rows)
            .await?;
    }
    repo.set_lane_status(candidate_lane_id, "completed", Some(Timestamp::now()))
        .await?;

    let diff = LaneDiffReport::compute::<EntityRelationLaneOutputs>(
        baseline_lane_id,
        candidate_lane_id,
        DerivedProductClass::SemanticCandidate,
        scope.input_set_hash().to_string(),
        &EntityRelationLaneOutputs::default(),
        &outputs,
        50,
    )?;
    if !is_rerun {
        repo.record_lane_diff(Uuid::now_v7(), &diff).await?;
    }

    Ok(EntityChainShadowLaneResult {
        epoch_id,
        baseline_lane_id,
        candidate_lane_id,
        outputs,
        diff,
        scope,
    })
}

/// Promote one `entity.related` candidate lane output through the curation
/// `proposal -> judgment -> finalizer` seam (sinex-0vx.5): builds and
/// inserts a `curation.proposal` event, an `Accept` `curation.judgment`
/// event from `actor_kind`, then calls the SAME production
/// `crate::api::handlers::curation::handle_curation_finalize` every other
/// finalization path uses (not a reimplementation) -- which itself calls
/// `crate::authority::authorize_finalization` and REJECTS the promotion if
/// `ENTITY_CHAIN_RELATION_FINALIZER_DECLARATIONS` has not been reconciled
/// into `authority.finalizer_registry`, or if `actor_kind`'s judgment isn't
/// sufficient authority under that finalizer's policy.
///
/// On success, flips the shadow lane's status to `promoted` and returns the
/// `curation.finalized` event -- the canonical output carrying
/// `adjudication_event_id` the bead's AC names.
pub async fn promote_entity_related_output(
    pool: &PgPool,
    lane_id: Uuid,
    relation_output: &SemanticRelationOutput,
    source_event_ids: Vec<Uuid>,
    actor_kind: CurationJudgmentActorKind,
    actor_id: impl Into<String>,
) -> Result<Event<serde_json::Value>> {
    let actor_id = actor_id.into();
    let candidate_payload = serde_json::to_value(relation_output).map_err(|error| {
        SinexError::serialization("serialize entity-chain relation candidate for promotion")
            .with_std_error(&error)
    })?;

    // `authority_proposal: None` -- the same shape ordinary
    // `handle_curation_record_judgment` proposals carry (only the
    // duplicate-resolution workbench path builds an `authority::Proposal`
    // wrapper with its own paired `authority_judgment`; requiring one here
    // would just be unused ceremony this promotion path doesn't need).
    let proposal = CurationProposalPayload {
        proposal_id: Uuid::now_v7(),
        proposal_key: format!("entity-chain-shadow:{}", relation_output.relation_key),
        proposal_kind: "entity.related".to_string(),
        target_ref: Some(relation_output.relation_key.clone()),
        candidate_source: "relation-extractor".to_string(),
        candidate_event_type: "entity.related".to_string(),
        candidate_payload,
        authority_proposal: None,
        evidence_event_ids: source_event_ids.clone(),
        evidence_material_ids: Vec::new(),
        producer: "entity-chain-shadow@1".to_string(),
        confidence: relation_output.weight.unwrap_or(0.5),
        rationale: format!(
            "promoted from entity-chain shadow lane {lane_id} co-occurrence candidate"
        ),
        status: CurationProposalStatus::Pending,
    };
    let source_event_ids_typed: Vec<_> = source_event_ids
        .iter()
        .copied()
        .map(sinex_primitives::events::EventId::from_uuid)
        .collect();
    let mut proposal_event = proposal
        .clone()
        .from_parents(source_event_ids_typed)?
        .at_time(Timestamp::now())
        .build()?;
    proposal_event.product_class = Some(DerivedProductClass::SemanticCandidate);
    proposal_event.claim_support = Some(ClaimSupport::unreviewed(
        SupportLevel::Heuristic,
        SourceCoverage::Covered,
        ClaimTemporalQuality::WindowBoundary,
        source_event_ids.len() as u32,
        0,
        1,
        0,
    ));
    proposal_event.derivation_declaration_id = Some(ENTITY_CHAIN_SHADOW_DECLARATION_ID.to_string());
    let proposal_event = pool.events().insert(proposal_event).await?;
    let proposal_event_id = proposal_event.id.ok_or_else(|| {
        SinexError::invalid_state("entity-chain shadow promotion: proposal event missing id")
    })?;

    let judgment = CurationJudgmentPayload {
        judgment_id: Uuid::now_v7(),
        proposal_id: proposal.proposal_id,
        actor_kind,
        actor_id,
        decision: CurationJudgmentDecision::Accept,
        authority_judgment: None,
        corrected_payload: None,
        comment: Some("entity-chain shadow-lane promotion".to_string()),
        judged_at: Timestamp::now(),
        authorization_context: None,
    };
    let mut judgment_event = judgment
        .clone()
        .from_parents([proposal_event_id])?
        .at_time(judgment.judged_at)
        .build()?;
    judgment_event.product_class = Some(DerivedProductClass::OperatorJudgment);
    judgment_event.claim_support = Some(ClaimSupport::unreviewed(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
        1,
        0,
        1,
        0,
    ));
    judgment_event.derivation_declaration_id = Some(
        crate::api::handlers::curation::CURATION_JUDGMENT_DECLARATION
            .declaration_id
            .to_string(),
    );
    let judgment_event = pool.events().insert(judgment_event).await?;
    let judgment_event_id = judgment_event.id.ok_or_else(|| {
        SinexError::invalid_state("entity-chain shadow promotion: judgment event missing id")
    })?;

    // Reuses the real production finalize handler -- the curation-bypass
    // rejection and actor-kind gate both run inside it.
    let finalize_response = crate::api::handlers::curation::handle_curation_finalize(
        pool,
        CurationFinalizeRequest {
            judgment_event_id: judgment_event_id.to_string(),
        },
    )
    .await?;

    pool.derivation_lanes()
        .set_lane_status(lane_id, "promoted", Some(Timestamp::now()))
        .await?;

    Ok(finalize_response.event)
}

#[cfg(test)]
#[path = "entity_chain_shadow_test.rs"]
mod tests;
