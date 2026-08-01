//! Semantic epoch and shadow-lane RPC handlers.
//!
//! Storage port (sinex-0vx.6): these handlers used to persist onto the
//! entity/relation-only `semantic.epochs`/`semantic.lanes`/
//! `semantic.lane_outputs`/`semantic.lane_diffs` tables via `SemanticRepository`.
//! They now persist onto the generic `derivation.epochs`/`derivation.lanes`/
//! `derivation.lane_outputs`/`derivation.lane_diffs` tables (sinex-0vx.4) via
//! `DerivationRepository`, with `output_kind` in `{"entity", "relation"}` and
//! `product_class = "semantic_candidate"` — a PORT, not a wrapper: the
//! `semantic.*` tables are retired in the same change (see
//! `crate::api::handlers::semantic::SEMANTIC_LANE_OUTPUT_DECLARATIONS` below
//! for the `derivation.product_declarations` row this writer registers, the
//! same non-automaton-writer pattern `curation.rs`/`instructions.rs` use).
//!
//! The wire RPC request/response shapes (`sinex_primitives::rpc::semantic`)
//! are unchanged — storage-agnostic by design — so callers (sinexctl, MCP,
//! the gateway) need no changes.
//!
//! One deliberate behavior narrowing from the port: the old
//! `semantic.lanes.status` vocabulary included a `compared` state specific to
//! entity/relation diffing. `derivation.lanes.status` is the generic
//! `{planned, running, completed, promoted, discarded, expired, failed}`
//! vocabulary (sinex-0vx.4) with no `compared` state — a lane-diff-specific
//! status doesn't generalize any better than `EntityRelationDiffReport` did.
//! `SemanticLaneStatus::Compared` now collapses to `derivation.lanes.status =
//! "completed"` at the storage boundary (see `lane_status_str` below); the
//! wire type still round-trips `compared` for callers that only read it back
//! out of a response they built.

use sinex_db::repositories::{CreateDerivationEpoch, CreateDerivationLane, DbPoolExt};
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, LaneDiffReport, SourceCoverage,
    SupportLevel,
};
use sinex_primitives::rpc::semantic::{
    SemanticEpochCreateRequest, SemanticEpochListRequest, SemanticEpochListResponse,
    SemanticEpochRecordResponse, SemanticLaneCreateRequest,
    SemanticLaneDiffRecordEntityRelationRequest, SemanticLaneDiffRecordResponse,
    SemanticLaneDiffsListRequest, SemanticLaneDiffsListResponse, SemanticLaneDiscardRequest,
    SemanticLaneDiscardResponse, SemanticLaneListRequest, SemanticLaneListResponse,
    SemanticLaneOutputsListRequest, SemanticLaneOutputsListResponse,
    SemanticLaneOutputsSeedCanonicalGraphRequest, SemanticLaneOutputsSeedCanonicalGraphResponse,
    SemanticLaneOutputsSeedEntityEventsRequest, SemanticLaneOutputsSeedEntityEventsResponse,
    SemanticLaneOutputsWriteRequest, SemanticLaneOutputsWriteResponse, SemanticLaneRecordResponse,
    SemanticLaneSetStatusRequest,
};
use sinex_primitives::{
    EntityRelationLaneOutputs, Result, SemanticLaneKind, SemanticLaneStatus, SemanticScope,
    SinexError, Timestamp, Uuid,
};
use sqlx::PgPool;

use crate::api::rpc_server::RpcAuthContext;

/// Derivation control-plane declaration for the semantic-lane RPC handlers
/// (sinex-0vx.6). Same non-automaton-writer pattern as
/// `curation::CURATION_OUTPUT_DECLARATIONS` /
/// `instructions::INSTRUCTIONS_OUTPUT_DECLARATIONS` — reconciled from
/// `Supervisor::run` via `reconcile_declarations`, not through the automaton
/// adapter. One declaration covers both `entity` and `relation`
/// `output_kind`s: they share `product_class = SemanticCandidate` and are
/// written by the same RPC surface, so a single declaration_id is the
/// correct granularity (mirrors `CURATION_JUDGMENT_DECLARATION` covering two
/// call sites that emit the same `(source, event_type)`).
pub const SEMANTIC_LANE_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[SEMANTIC_ENTITY_RELATION_DECLARATION];

/// Entity/relation candidate lane outputs written by the semantic-lane RPC
/// handlers below — awaiting explicit authority/deterministic-policy
/// finalization, never itself canonical. `SemanticCandidate` /
/// `curation_writer` (a shadow-lane RPC write, not an automaton
/// `DerivedOutput`, mirroring `CURATION_PROPOSAL_DECLARATION`'s rationale).
const SEMANTIC_ENTITY_RELATION_DECLARATION: DerivationOutputDeclaration =
    DerivationOutputDeclaration {
        declaration_id: "semantic-rpc.entity_relation.semantic_candidate",
        owner: "semantic-rpc",
        product_class: DerivedProductClass::SemanticCandidate,
        write_surface: DerivationWriteSurface::CurationWriter,
        output_source: None,
        output_event_type: None,
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::ExplicitOnly,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Heuristic,
            SourceCoverage::Partial,
            ClaimTemporalQuality::Unknown,
        ),
        verification_command:
            "xtask test -p sinexd -E 'test(semantic_handlers_generic_lane)'",
    };

pub async fn handle_semantic_epoch_create(
    pool: &PgPool,
    req: SemanticEpochCreateRequest,
    auth: &RpcAuthContext,
) -> Result<SemanticEpochRecordResponse> {
    validate_non_empty("semantic.epochs.create", "name", &req.name)?;
    validate_non_empty("semantic.epochs.create", "config_hash", &req.config_hash)?;
    validate_scope(
        "semantic.epochs.create",
        req.scope.input_ids.len(),
        &req.scope.input_set_hash,
    )?;

    let scope_json = serde_json::to_value(&req.scope).map_err(serialize_error)?;
    let components_json = serde_json::to_value(&req.components).map_err(serialize_error)?;

    let created = pool
        .derivation_lanes()
        .create_epoch(CreateDerivationEpoch {
            id: req.epoch_id,
            declaration_id: SEMANTIC_ENTITY_RELATION_DECLARATION
                .declaration_id
                .to_string(),
            name: req.name,
            product_class: DerivedProductClass::SemanticCandidate.as_str().to_string(),
            scope_model: scope_model_for(&req.scope.kind).to_string(),
            scope: scope_json,
            semantics_version: SEMANTIC_ENTITY_RELATION_DECLARATION
                .semantics_version
                .to_string(),
            code_ref: req.code_ref,
            config_hash: req.config_hash,
            components: components_json,
            prompt_set_hash: req.prompt_set_hash,
            model_config_hash: req.model_config_hash,
            created_by: req
                .created_by
                .unwrap_or_else(|| auth.actor_id().to_string()),
            operation_id: req.operation_id,
            supersedes_epoch_id: req.supersedes_epoch_id,
        })
        .await?;

    Ok(SemanticEpochRecordResponse {
        epoch: serde_json::to_value(created).map_err(serialize_error)?,
    })
}

pub async fn handle_semantic_epoch_list(
    pool: &PgPool,
    req: SemanticEpochListRequest,
) -> Result<SemanticEpochListResponse> {
    let epochs = pool.derivation_lanes().list_epochs(req.limit).await?;
    Ok(SemanticEpochListResponse {
        epochs: serialize_records(epochs)?,
    })
}

pub async fn handle_semantic_lane_create(
    pool: &PgPool,
    req: SemanticLaneCreateRequest,
) -> Result<SemanticLaneRecordResponse> {
    validate_non_empty("semantic.lanes.create", "name", &req.name)?;
    validate_non_empty("semantic.lanes.create", "purpose", &req.purpose)?;
    validate_scope(
        "semantic.lanes.create",
        req.scope.input_ids.len(),
        &req.scope.input_set_hash,
    )?;

    let scope_json = serde_json::to_value(&req.scope).map_err(serialize_error)?;
    let created = pool
        .derivation_lanes()
        .create_lane(CreateDerivationLane {
            id: req.lane_id,
            declaration_id: SEMANTIC_ENTITY_RELATION_DECLARATION
                .declaration_id
                .to_string(),
            name: req.name,
            kind: lane_kind_str(req.kind).to_string(),
            product_class: DerivedProductClass::SemanticCandidate.as_str().to_string(),
            base_epoch_id: req.base_epoch_id,
            candidate_epoch_id: req.candidate_epoch_id,
            scope: scope_json,
            purpose: Some(req.purpose),
            operation_id: req.operation_id,
            expires_at: req.expires_at,
        })
        .await?;

    Ok(SemanticLaneRecordResponse {
        lane: serde_json::to_value(created).map_err(serialize_error)?,
    })
}

pub async fn handle_semantic_lanes_list(
    pool: &PgPool,
    req: SemanticLaneListRequest,
) -> Result<SemanticLaneListResponse> {
    let status = req.status.map(lane_status_str);
    let lanes = pool
        .derivation_lanes()
        .list_lanes(status, req.limit)
        .await?;
    Ok(SemanticLaneListResponse {
        lanes: serialize_records(lanes)?,
    })
}

pub async fn handle_semantic_lane_set_status(
    pool: &PgPool,
    req: SemanticLaneSetStatusRequest,
) -> Result<SemanticLaneRecordResponse> {
    let lane = pool
        .derivation_lanes()
        .set_lane_status(req.lane_id, lane_status_str(req.status), req.completed_at)
        .await?;
    Ok(SemanticLaneRecordResponse {
        lane: serde_json::to_value(lane).map_err(serialize_error)?,
    })
}

pub async fn handle_semantic_lane_discard(
    pool: &PgPool,
    req: SemanticLaneDiscardRequest,
) -> Result<SemanticLaneDiscardResponse> {
    let (lane, discarded_outputs) = pool
        .derivation_lanes()
        .discard_lane_outputs(req.lane_id, Timestamp::now())
        .await?;
    Ok(SemanticLaneDiscardResponse {
        lane: serde_json::to_value(lane).map_err(serialize_error)?,
        discarded_outputs,
    })
}

pub async fn handle_semantic_lane_outputs_list(
    pool: &PgPool,
    req: SemanticLaneOutputsListRequest,
) -> Result<SemanticLaneOutputsListResponse> {
    let outputs = pool
        .derivation_lanes()
        .list_lane_outputs(req.lane_id, req.limit)
        .await?;
    Ok(SemanticLaneOutputsListResponse {
        lane_id: req.lane_id,
        outputs: serialize_records(outputs)?,
    })
}

pub async fn handle_semantic_lane_outputs_write(
    pool: &PgPool,
    req: SemanticLaneOutputsWriteRequest,
) -> Result<SemanticLaneOutputsWriteResponse> {
    let written = pool
        .derivation_lanes()
        .write_entity_relation_outputs(
            req.lane_id,
            DerivedProductClass::SemanticCandidate.as_str(),
            &req.outputs,
        )
        .await?;
    Ok(SemanticLaneOutputsWriteResponse {
        lane_id: req.lane_id,
        written,
    })
}

pub async fn handle_semantic_lane_outputs_seed_canonical_graph(
    pool: &PgPool,
    req: SemanticLaneOutputsSeedCanonicalGraphRequest,
) -> Result<SemanticLaneOutputsSeedCanonicalGraphResponse> {
    let written = pool
        .derivation_lanes()
        .seed_entity_relation_outputs_from_canonical_graph(
            req.lane_id,
            DerivedProductClass::SemanticCandidate.as_str(),
        )
        .await?;
    Ok(SemanticLaneOutputsSeedCanonicalGraphResponse {
        lane_id: req.lane_id,
        written,
    })
}

pub async fn handle_semantic_lane_outputs_seed_entity_events(
    pool: &PgPool,
    req: SemanticLaneOutputsSeedEntityEventsRequest,
) -> Result<SemanticLaneOutputsSeedEntityEventsResponse> {
    let written = pool
        .derivation_lanes()
        .seed_entity_relation_outputs_from_event_scope(
            req.lane_id,
            DerivedProductClass::SemanticCandidate.as_str(),
        )
        .await?;
    Ok(SemanticLaneOutputsSeedEntityEventsResponse {
        lane_id: req.lane_id,
        written,
    })
}

pub async fn handle_semantic_lane_diffs_list(
    pool: &PgPool,
    req: SemanticLaneDiffsListRequest,
) -> Result<SemanticLaneDiffsListResponse> {
    let diffs = pool
        .derivation_lanes()
        .list_lane_diffs(req.lane_id, req.limit)
        .await?;
    Ok(SemanticLaneDiffsListResponse {
        lane_id: req.lane_id,
        diffs: serialize_records(diffs)?,
    })
}

pub async fn handle_semantic_lane_diff_record_entity_relation(
    pool: &PgPool,
    req: SemanticLaneDiffRecordEntityRelationRequest,
) -> Result<SemanticLaneDiffRecordResponse> {
    let repo = pool.derivation_lanes();
    let baseline_lane = repo.get_lane(req.baseline_lane_id).await?;
    let candidate_lane = repo.get_lane(req.candidate_lane_id).await?;
    let baseline_scope = parse_scope(&baseline_lane.scope)?;
    let candidate_scope = parse_scope(&candidate_lane.scope)?;
    if baseline_scope.input_set_hash != candidate_scope.input_set_hash {
        return Err(SinexError::validation(
            "semantic.lane_diffs.record_entity_relation: lane input_set_hash values differ",
        )
        .with_context("baseline_lane_id", req.baseline_lane_id.to_string())
        .with_context("candidate_lane_id", req.candidate_lane_id.to_string()));
    }

    let baseline_outputs = repo.read_entity_relation_outputs(req.baseline_lane_id).await?;
    let candidate_outputs = repo
        .read_entity_relation_outputs(req.candidate_lane_id)
        .await?;
    let report = LaneDiffReport::compute::<EntityRelationLaneOutputs>(
        req.baseline_lane_id,
        req.candidate_lane_id,
        DerivedProductClass::SemanticCandidate,
        candidate_scope.input_set_hash,
        &baseline_outputs,
        &candidate_outputs,
        req.max_examples,
    )?;
    let diff = repo
        .record_lane_diff(req.diff_id.unwrap_or_else(Uuid::now_v7), &report)
        .await?;
    let candidate_lane = if req.mark_candidate_compared {
        Some(
            repo.set_lane_status(
                req.candidate_lane_id,
                // See module doc: `compared` collapses to `completed` --
                // derivation.lanes has no lane-diff-specific status.
                lane_status_str(SemanticLaneStatus::Completed),
                Some(Timestamp::now()),
            )
            .await?,
        )
    } else {
        None
    };

    Ok(SemanticLaneDiffRecordResponse {
        diff: serde_json::to_value(diff).map_err(serialize_error)?,
        candidate_lane: candidate_lane
            .map(serde_json::to_value)
            .transpose()
            .map_err(serialize_error)?,
    })
}

/// Map a free-form `SemanticScope.kind` onto the fixed
/// `derivation.epochs.scope_model` / `derivation.lanes` vocabulary
/// (`DerivationScope::scope_model()`). Every semantic-lane scope observed in
/// practice is an enumerated id set, so this always resolves to one of the
/// three batch variants; anything unrecognized defaults to `event_set`
/// (the vast majority of semantic-lane scopes today) rather than failing the
/// request outright.
fn scope_model_for(kind: &str) -> &'static str {
    match kind {
        "source_material" | "source_material_set" => "source_material_set",
        "document_chunk" | "document_chunk_set" => "document_chunk_set",
        _ => "event_set",
    }
}

/// `derivation.lanes.kind` CHECK-constraint spelling
/// (`'canonical' | 'shadow' | 'experiment'`) -- unchanged from
/// `semantic.lanes.kind`.
fn lane_kind_str(kind: SemanticLaneKind) -> &'static str {
    match kind {
        SemanticLaneKind::Canonical => "canonical",
        SemanticLaneKind::Shadow => "shadow",
        SemanticLaneKind::Experiment => "experiment",
    }
}

/// `derivation.lanes.status` CHECK-constraint spelling. See module doc:
/// `Compared` collapses to `"completed"` -- the generic vocabulary has no
/// lane-diff-specific status.
fn lane_status_str(status: SemanticLaneStatus) -> &'static str {
    match status {
        SemanticLaneStatus::Planned => "planned",
        SemanticLaneStatus::Running => "running",
        SemanticLaneStatus::Completed | SemanticLaneStatus::Compared => "completed",
        SemanticLaneStatus::Promoted => "promoted",
        SemanticLaneStatus::Discarded => "discarded",
        SemanticLaneStatus::Expired => "expired",
    }
}

fn validate_non_empty(method: &str, field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(SinexError::validation(format!(
            "{method}: {field} must not be empty"
        )));
    }
    Ok(())
}

fn validate_scope(method: &str, input_count: usize, input_set_hash: &str) -> Result<()> {
    if input_count == 0 {
        return Err(SinexError::validation(format!(
            "{method}: scope.input_ids must not be empty"
        )));
    }
    if input_set_hash.trim().is_empty() {
        return Err(SinexError::validation(format!(
            "{method}: scope.input_set_hash must not be empty"
        )));
    }
    Ok(())
}

fn parse_scope(scope: &serde_json::Value) -> Result<SemanticScope> {
    serde_json::from_value(scope.clone()).map_err(|error| {
        SinexError::serialization("deserialize semantic lane scope").with_std_error(&error)
    })
}

fn serialize_records<T: serde::Serialize>(records: Vec<T>) -> Result<Vec<serde_json::Value>> {
    records
        .into_iter()
        .map(|record| serde_json::to_value(record).map_err(serialize_error))
        .collect()
}

fn serialize_error(error: serde_json::Error) -> SinexError {
    SinexError::serialization("semantic RPC response serialization failed").with_std_error(&error)
}
