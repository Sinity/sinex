//! Curation proposal/judgment RPC handlers.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::state::Operation as DbOperation;
use sinex_primitives::authority::{Judgment, JudgmentVerdict, Proposal, ProposalKind};
use sinex_primitives::derivation::{
    AdjudicationStatus, ClaimSupport, ClaimSupportTemplate, ClaimTemporalQuality,
    DerivationOutputDeclaration, DerivationWriteSurface, DerivedProductClass, InputEligibility,
    SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::{EventSource, EventType, OperationStatus};
use sinex_primitives::events::payloads::{
    CurationFinalizedPayload, CurationJudgmentDecision, CurationJudgmentPayload,
    CurationProposalPayload, CurationProposalStatus,
};
use sinex_primitives::events::{Event, EventId, EventPayload, Provenance};
use sinex_primitives::query::{EventQuery, EventQueryResult, PayloadFilter};
use sinex_primitives::rpc::curation::{
    CurationDuplicateAction, CurationDuplicateCandidateCluster, CurationDuplicateCandidateEvent,
    CurationFinalizeRequest, CurationFinalizeResponse, CurationListDuplicateCandidatesRequest,
    CurationListDuplicateCandidatesResponse, CurationListProposalsRequest,
    CurationRecordDuplicateJudgmentRequest, CurationRecordDuplicateJudgmentResponse,
    CurationRecordJudgmentRequest, CurationRecordJudgmentResponse,
};
use sinex_primitives::rpc::ops::Operation;
use sinex_primitives::views::{SinexObjectKind, SinexObjectRef};
use sinex_primitives::{Id, JsonValue, Result, SinexError, Timestamp, Uuid};
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use std::str::FromStr;

use crate::api::rpc_server::RpcAuthContext;

/// Derivation control-plane declarations for the curation RPC handlers
/// (sinex-q46n). Unlike automaton outputs (`AutomatonSpec.outputs`, reconciled
/// by `crate::automata::product_declarations::reconcile_product_declarations`),
/// these handlers build and insert their own derived events directly rather
/// than through the automaton adapter — reconciled through the same
/// `reconcile_declarations` primitive, called separately from
/// `Supervisor::run` with this slice.
///
/// One declaration per distinct `(source, event_type)` pair the curation
/// handlers emit: `handle_curation_record_judgment` and
/// `handle_curation_record_duplicate_judgment` both emit `curation.judgment`
/// and share `CURATION_JUDGMENT_DECLARATION`; `handle_curation_finalize`
/// emits `curation.finalized`; `handle_curation_record_duplicate_judgment`
/// additionally emits a `curation.proposal` (the duplicate-resolution
/// cluster proposal it records ahead of its judgment).
pub const CURATION_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] = &[
    CURATION_PROPOSAL_DECLARATION,
    CURATION_JUDGMENT_DECLARATION,
    CURATION_FINALIZED_DECLARATION,
];

/// `curation.proposal` written by `handle_curation_record_duplicate_judgment`
/// for the duplicate-candidate cluster it proposes resolving. A candidate
/// awaiting explicit authority/deterministic-policy finalization —
/// `SemanticCandidate`, `curation_writer` (this is a curation-domain RPC
/// write, not an automaton `DerivedOutput`).
const CURATION_PROPOSAL_DECLARATION: DerivationOutputDeclaration = DerivationOutputDeclaration {
    declaration_id: "curation-rpc.curation.proposal",
    owner: "curation-rpc",
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
        ClaimTemporalQuality::RealtimeCapture,
    ),
    verification_command:
        "xtask test -p sinexd -E 'test(curation_duplicate_judgment_records_proposal_over_candidate_set)'",
};

/// `curation.judgment` written by `handle_curation_record_judgment` and
/// `handle_curation_record_duplicate_judgment` (same source/event_type,
/// shared declaration). An authority decision over a proposal —
/// `OperatorJudgment` per `DerivedProductClass::OperatorJudgment`'s doc
/// ("only the curation/authority finalizer writer may emit this class").
const CURATION_JUDGMENT_DECLARATION: DerivationOutputDeclaration = DerivationOutputDeclaration {
    declaration_id: "curation-rpc.curation.judgment",
    owner: "curation-rpc",
    product_class: DerivedProductClass::OperatorJudgment,
    write_surface: DerivationWriteSurface::CurationWriter,
    output_source: None,
    output_event_type: None,
    projection_kind: None,
    artifact_kind: None,
    proposal_kind: None,
    semantics_version: "1.0.0",
    input_eligibility: InputEligibility::ExplicitOnly,
    default_support: ClaimSupportTemplate::new(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
    ),
    verification_command:
        "xtask test -p sinexd -E 'test(curation_record_judgment_persists_synthesis_event)'",
};

/// `curation.finalized` written by `handle_curation_finalize`: a
/// deterministic receipt recording that an accepted/modified judgment was
/// applied — `ReportArtifact` ("a persisted generated report, receipt,
/// export, or artifact pointer"), `artifact_writer`.
pub(crate) const CURATION_FINALIZED_DECLARATION: DerivationOutputDeclaration = DerivationOutputDeclaration {
    declaration_id: "curation-rpc.curation.finalized",
    owner: "curation-rpc",
    product_class: DerivedProductClass::ReportArtifact,
    write_surface: DerivationWriteSurface::ArtifactWriter,
    output_source: None,
    output_event_type: None,
    projection_kind: None,
    artifact_kind: None,
    proposal_kind: None,
    semantics_version: "1.0.0",
    input_eligibility: InputEligibility::NeverInput,
    default_support: ClaimSupportTemplate::new(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
    ),
    verification_command:
        "xtask test -p sinexd -E 'test(curation_finalize_persists_lineage_to_original_proposal_and_judgment)'",
};

/// Static `authority.finalizer_registry` declarations for the curation-rpc
/// writer (sinex-0vx.5): the set of `proposal_kind`s this handler file is
/// permitted to finalize, and the actor-kind policy governing each one.
/// Reconciled at `sinexd` startup the same way as `CURATION_OUTPUT_DECLARATIONS`
/// (`crate::authority::reconcile_finalizer_registrations`, called from
/// `Supervisor::run`) — a proposal whose `(proposal_kind, candidate_source,
/// candidate_event_type)` doesn't match a row here is rejected by
/// `handle_curation_finalize` before it ever reaches an adjudicated write.
///
/// Both entries default to `requires_human_judgment: true` with no
/// `auto_accept_policy` — the safe default posture (decision (a) of the 0vx
/// design pass): every actor kind including `Agent` gets exactly the same
/// treatment here, because neither of the currently-registered proposal
/// kinds has been granted any auto-accept exception. Operators wishing to
/// grant an actor kind auto-accept authority for a specific proposal kind do
/// so by adding an explicit `auto_accept_policy` here — never implicitly.
pub const CURATION_FINALIZER_DECLARATIONS: &[crate::authority::FinalizerDeclaration] = &[
    crate::authority::FinalizerDeclaration {
        finalizer_id: "curation-rpc.knowledge.tag_applied",
        proposal_kind: "knowledge.tag_applied",
        output_source: "knowledge-graph",
        output_event_type: "knowledge.tag_applied",
        output_product_class: DerivedProductClass::ReportArtifact,
        derivation_declaration_id: CURATION_FINALIZED_DECLARATION.declaration_id,
        requires_human_judgment: true,
        auto_accept_policy: None,
        registered_by: "curation-rpc",
    },
    crate::authority::FinalizerDeclaration {
        finalizer_id: "curation-rpc.curation.duplicate_resolution",
        proposal_kind: "curation.duplicate_resolution",
        output_source: "curation",
        output_event_type: "curation.duplicate_resolution",
        output_product_class: DerivedProductClass::ReportArtifact,
        derivation_declaration_id: CURATION_FINALIZED_DECLARATION.declaration_id,
        requires_human_judgment: true,
        auto_accept_policy: None,
        registered_by: "curation-rpc",
    },
];

/// Stamp `product_class`/`claim_support`/`derivation_declaration_id` on a
/// handler-built event from its static declaration, so
/// `derivation.enforce_event_product_declaration()` (sinex-0vx.4) admits the
/// write once `CURATION_OUTPUT_DECLARATIONS` has been reconciled into
/// `derivation.product_declarations` (sinex-x79t's `reconcile_declarations`,
/// called with this file's declarations from `Supervisor::run`).
///
/// Produces an UNREVIEWED claim-support vector — the shape every proposal
/// and judgment record carries (they are the evidence trail, not themselves
/// adjudicated claims). Use [`apply_curation_finalized_declaration`] for the
/// one event that IS an adjudicated output: `curation.finalized`.
fn apply_curation_declaration<T>(
    event: &mut Event<T>,
    declaration: &DerivationOutputDeclaration,
    evidence_event_count: u32,
    evidence_material_count: u32,
) {
    event.product_class = Some(declaration.product_class);
    event.claim_support = Some(declaration.default_support.instantiate(
        evidence_event_count,
        evidence_material_count,
        1,
        0,
    ));
    event.derivation_declaration_id = Some(declaration.declaration_id.to_string());
}

/// Stamp the `curation.finalized` event with an ADJUDICATED claim-support
/// vector (sinex-0vx.5): this is the one curation-rpc write that represents
/// an actual promoted claim, so — unlike `apply_curation_declaration` above
/// — its `claim_support.adjudication` is `Accepted` and carries
/// `adjudication_event_id` pointing at the judgment event that authorized
/// it. `Event.adjudication_event_id` (the DB column mirror) is set to the
/// same id so `derivation.enforce_event_product_declaration()`'s adjudicated-
/// claim check (sinex-0vx.4) is satisfied by construction, not by accident.
fn apply_curation_finalized_declaration<T>(
    event: &mut Event<T>,
    judgment_event_id: EventId,
    evidence_event_count: u32,
    evidence_material_count: u32,
) -> Result<()> {
    let template = CURATION_FINALIZED_DECLARATION.default_support;
    let claim_support = ClaimSupport::adjudicated(
        template.support_level,
        template.source_coverage,
        template.temporal_quality,
        AdjudicationStatus::Accepted,
        judgment_event_id,
        evidence_event_count,
        evidence_material_count,
        1,
        0,
    )?;
    event.product_class = Some(CURATION_FINALIZED_DECLARATION.product_class);
    event.claim_support = Some(claim_support);
    event.derivation_declaration_id =
        Some(CURATION_FINALIZED_DECLARATION.declaration_id.to_string());
    event.adjudication_event_id = Some(judgment_event_id.to_uuid());
    Ok(())
}

pub async fn handle_curation_list_proposals(
    pool: &PgPool,
    req: CurationListProposalsRequest,
) -> Result<EventQueryResult> {
    let mut query = EventQuery {
        sources: vec![EventSource::from_static("curation")],
        event_types: vec![EventType::from_static("curation.proposal")],
        payload: Some(PayloadFilter::Contains {
            value: json!({ "status": req.status }),
        }),
        limit: req.limit,
        ..Default::default()
    };
    query.validate()?;

    pool.events().query(query).await
}

pub async fn handle_curation_record_judgment(
    pool: &PgPool,
    req: CurationRecordJudgmentRequest,
    auth: &RpcAuthContext,
) -> Result<CurationRecordJudgmentResponse> {
    let proposal_event_uuid = Uuid::from_str(&req.proposal_event_id).map_err(|error| {
        SinexError::validation("curation.judgments.record: invalid proposal_event_id")
            .with_context("proposal_event_id", &req.proposal_event_id)
            .with_std_error(&error)
    })?;
    let proposal_event_id = Id::<Event<JsonValue>>::from_uuid(proposal_event_uuid);
    let proposal_event = pool
        .events()
        .get_by_id(proposal_event_id)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("curation.judgments.record: proposal event not found")
                .with_context("proposal_event_id", &req.proposal_event_id)
        })?;
    if proposal_event.source.as_str() != "curation"
        || proposal_event.event_type.as_str() != "curation.proposal"
    {
        return Err(SinexError::validation(
            "curation.judgments.record: event is not a curation proposal",
        )
        .with_context("proposal_event_id", &req.proposal_event_id)
        .with_context("source", proposal_event.source.as_str())
        .with_context("event_type", proposal_event.event_type.as_str()));
    }
    let proposal: CurationProposalPayload = serde_json::from_value(proposal_event.payload.clone())
        .map_err(|error| {
            SinexError::serialization("curation.judgments.record: invalid proposal payload")
                .with_std_error(&error)
        })?;

    let actor_id = req
        .actor_id
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| auth.actor_id().to_string());
    let judgment = CurationJudgmentPayload {
        judgment_id: Uuid::now_v7(),
        proposal_id: proposal.proposal_id,
        actor_kind: req.actor_kind,
        actor_id,
        decision: req.decision,
        authority_judgment: None,
        corrected_payload: req.corrected_payload,
        comment: req.comment,
        judged_at: Timestamp::now(),
        authorization_context: req.authorization_context,
    };
    let parent = proposal_event.id.ok_or_else(|| {
        SinexError::invalid_state("curation.judgments.record: persisted proposal event missing id")
    })?;
    let mut event = judgment
        .clone()
        .from_parents([parent])?
        .at_time(judgment.judged_at)
        .build()?;
    apply_curation_declaration(&mut event, &CURATION_JUDGMENT_DECLARATION, 1, 0);
    let inserted = pool.events().insert(event).await?;

    Ok(CurationRecordJudgmentResponse {
        judgment,
        event: inserted,
    })
}

pub async fn handle_curation_list_duplicate_candidates(
    pool: &PgPool,
    req: CurationListDuplicateCandidatesRequest,
) -> Result<CurationListDuplicateCandidatesResponse> {
    let limit = req.limit.clamp(1, 1000);
    let events_per_cluster = req.events_per_cluster.clamp(1, 50);
    let source = req
        .source
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    let event_type = req
        .event_type
        .as_deref()
        .filter(|value| !value.trim().is_empty());

    let cluster_rows = sqlx::query(
        r"
        SELECT
            source,
            event_type,
            equivalence_key,
            COUNT(*)::bigint AS event_count,
            COUNT(DISTINCT source_material_id)::bigint AS material_count
        FROM core.events
        WHERE source_material_id IS NOT NULL
          AND equivalence_key IS NOT NULL AND equivalence_key <> ''
          AND ($1::text IS NULL OR source = $1)
          AND ($2::text IS NULL OR event_type = $2)
        GROUP BY source, event_type, equivalence_key
        HAVING COUNT(*) > 1 AND COUNT(DISTINCT source_material_id) > 1
        ORDER BY MAX(ts_orig) DESC
        LIMIT $3
        ",
    )
    .bind(source)
    .bind(event_type)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("failed to list duplicate candidate clusters").with_std_error(&error)
    })?;

    let mut clusters = Vec::with_capacity(cluster_rows.len());
    for row in cluster_rows {
        let source: String = row.try_get("source").map_err(cluster_row_error)?;
        let event_type: String = row.try_get("event_type").map_err(cluster_row_error)?;
        let equivalence_key: String = row.try_get("equivalence_key").map_err(cluster_row_error)?;
        let event_count: i64 = row.try_get("event_count").map_err(cluster_row_error)?;
        let material_count: i64 = row.try_get("material_count").map_err(cluster_row_error)?;

        let event_rows = sqlx::query(
            r"
            SELECT id, source_material_id, ts_orig
            FROM core.events
            WHERE source = $1
              AND event_type = $2
              AND source_material_id IS NOT NULL
              AND equivalence_key = $3
            ORDER BY ts_orig DESC, id DESC
            LIMIT $4
            ",
        )
        .bind(&source)
        .bind(&event_type)
        .bind(&equivalence_key)
        .bind(events_per_cluster)
        .fetch_all(pool)
        .await
        .map_err(|error| {
            SinexError::database("failed to list duplicate candidate events")
                .with_context("source", &source)
                .with_context("event_type", &event_type)
                .with_context("equivalence_key", &equivalence_key)
                .with_std_error(&error)
        })?;

        let mut events = Vec::with_capacity(event_rows.len());
        for event_row in event_rows {
            events.push(CurationDuplicateCandidateEvent {
                event_id: event_row.try_get("id").map_err(cluster_row_error)?,
                source_material_id: event_row
                    .try_get("source_material_id")
                    .map_err(cluster_row_error)?,
                ts_orig: event_row.try_get("ts_orig").map_err(cluster_row_error)?,
            });
        }

        clusters.push(CurationDuplicateCandidateCluster {
            cluster_id: duplicate_cluster_id(&source, &event_type, &equivalence_key),
            source,
            event_type,
            equivalence_key,
            event_count,
            material_count,
            events,
        });
    }

    Ok(CurationListDuplicateCandidatesResponse { clusters })
}

pub async fn handle_curation_record_duplicate_judgment(
    pool: &PgPool,
    req: CurationRecordDuplicateJudgmentRequest,
    auth: &RpcAuthContext,
) -> Result<CurationRecordDuplicateJudgmentResponse> {
    validate_duplicate_judgment_request(&req)?;

    let event_ids: Vec<EventId> = req
        .event_ids
        .iter()
        .copied()
        .map(EventId::from_uuid)
        .collect();
    let events = pool.events().get_by_ids(&event_ids).await?;
    if events.len() != event_ids.len() {
        return Err(SinexError::not_found(
            "curation.duplicate_judgments.record: not all candidate events were found",
        )
        .with_context("requested", event_ids.len().to_string())
        .with_context("found", events.len().to_string()));
    }

    let mut evidence_material_ids = Vec::with_capacity(events.len());
    for event in &events {
        if event.source.as_str() != req.source || event.event_type.as_str() != req.event_type {
            return Err(SinexError::validation(
                "curation.duplicate_judgments.record: candidate event is outside requested cluster",
            )
            .with_context("event_id", persisted_event_id(event)?.to_string())
            .with_context("expected_source", &req.source)
            .with_context("actual_source", event.source.as_str())
            .with_context("expected_event_type", &req.event_type)
            .with_context("actual_event_type", event.event_type.as_str()));
        }
        let key = duplicate_logical_key(event).ok_or_else(|| {
            SinexError::validation(
                "curation.duplicate_judgments.record: candidate event has no logical key",
            )
            .with_context(
                "event_id",
                persisted_event_id(event)
                    .map_or_else(|_| "<missing-id>".to_string(), |id| id.to_string()),
            )
        })?;
        if key != req.equivalence_key {
            return Err(SinexError::validation(
                "curation.duplicate_judgments.record: candidate event logical key mismatch",
            )
            .with_context("event_id", persisted_event_id(event)?.to_string())
            .with_context("expected_equivalence_key", &req.equivalence_key)
            .with_context("actual_equivalence_key", key));
        }
        let Provenance::Material { id, .. } = &event.provenance else {
            return Err(SinexError::validation(
                "curation.duplicate_judgments.record: candidate event is not material-provenance",
            )
            .with_context("event_id", persisted_event_id(event)?.to_string()));
        };
        evidence_material_ids.push(id.to_uuid());
    }
    evidence_material_ids.sort_unstable();
    evidence_material_ids.dedup();
    if evidence_material_ids.len() < 2 {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires candidate events from at least two source materials",
        )
        .with_context("material_count", evidence_material_ids.len().to_string()));
    }

    let action_value = serde_json::to_value(req.action).map_err(|error| {
        SinexError::serialization("failed to serialize duplicate curation action")
            .with_std_error(&error)
    })?;
    let action_label = action_value
        .as_str()
        .ok_or_else(|| {
            SinexError::serialization("duplicate curation action did not serialize as string")
        })?
        .to_string();
    let candidate_payload = json!({
        "source": req.source.clone(),
        "event_type": req.event_type.clone(),
        "equivalence_key": req.equivalence_key.clone(),
        "action": action_label,
        "preferred_event_id": req.preferred_event_id,
        "candidate_event_ids": req.event_ids.clone(),
        "candidate_material_ids": evidence_material_ids.clone(),
    });
    let cluster_id = duplicate_cluster_id(&req.source, &req.event_type, &req.equivalence_key);
    let authority_proposal = Proposal::new(
        ProposalKind::DuplicateCandidate,
        SinexObjectRef::new(SinexObjectKind::Event, cluster_id.clone())
            .with_label("duplicate candidate cluster"),
        1.0,
        candidate_payload.clone(),
        "rule:sinexd.duplicate-workbench",
    )
    .with_caveat(
        "authority.operator_required",
        "duplicate finalization requires an explicit operator judgment",
    );
    let proposal = CurationProposalPayload {
        proposal_id: Uuid::now_v7(),
        proposal_key: format!("duplicate-resolution:{cluster_id}"),
        proposal_kind: "curation.duplicate_resolution".to_string(),
        target_ref: Some(cluster_id),
        candidate_source: "curation".to_string(),
        candidate_event_type: "curation.duplicate_resolution".to_string(),
        candidate_payload,
        authority_proposal: Some(authority_proposal.clone()),
        evidence_event_ids: req.event_ids.clone(),
        evidence_material_ids: evidence_material_ids.clone(),
        producer: "sinexd.duplicate-workbench@1".to_string(),
        confidence: 1.0,
        rationale: "operator duplicate-resolution action over cross-material candidate events"
            .to_string(),
        status: CurationProposalStatus::Pending,
    };
    let mut proposal_event = proposal
        .clone()
        .from_parents(event_ids.clone())?
        .at_time(Timestamp::now())
        .build()?;
    apply_curation_declaration(
        &mut proposal_event,
        &CURATION_PROPOSAL_DECLARATION,
        event_ids.len() as u32,
        evidence_material_ids.len() as u32,
    );
    let proposal_event = pool.events().insert(proposal_event).await?;
    let proposal_event_id = proposal_event.id.ok_or_else(|| {
        SinexError::invalid_state(
            "curation.duplicate_judgments.record: persisted proposal event missing id",
        )
    })?;

    let actor_id = req
        .actor_id
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| auth.actor_id().to_string());
    let judgment = CurationJudgmentPayload {
        judgment_id: Uuid::now_v7(),
        proposal_id: proposal.proposal_id,
        actor_kind: req.actor_kind,
        actor_id,
        decision: duplicate_action_decision(req.action),
        authority_judgment: Some(
            Judgment::new(
                authority_proposal.id.clone(),
                duplicate_action_verdict(req.action),
                auth.actor_id(),
            )
            .with_note(req.comment.clone().unwrap_or_else(|| {
                "duplicate candidate reviewed through curation duplicate workbench".to_string()
            })),
        ),
        corrected_payload: None,
        comment: req.comment,
        judged_at: Timestamp::now(),
        authorization_context: Some(json!({
            "duplicate_action": action_label,
            "cluster_id": proposal.target_ref.clone(),
            "preferred_event_id": req.preferred_event_id,
        })),
    };
    let mut judgment_event = judgment
        .clone()
        .from_parents([proposal_event_id])?
        .at_time(judgment.judged_at)
        .build()?;
    apply_curation_declaration(&mut judgment_event, &CURATION_JUDGMENT_DECLARATION, 1, 0);
    let judgment_event = pool.events().insert(judgment_event).await?;

    Ok(CurationRecordDuplicateJudgmentResponse {
        proposal,
        proposal_event,
        judgment,
        judgment_event,
    })
}

pub async fn handle_curation_finalize(
    pool: &PgPool,
    req: CurationFinalizeRequest,
) -> Result<CurationFinalizeResponse> {
    let judgment_event_uuid = Uuid::from_str(&req.judgment_event_id).map_err(|error| {
        SinexError::validation("curation.finalize: invalid judgment_event_id")
            .with_context("judgment_event_id", &req.judgment_event_id)
            .with_std_error(&error)
    })?;
    let judgment_event_id = Id::<Event<JsonValue>>::from_uuid(judgment_event_uuid);
    let judgment_event = pool
        .events()
        .get_by_id(judgment_event_id)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("curation.finalize: judgment event not found")
                .with_context("judgment_event_id", &req.judgment_event_id)
        })?;
    if judgment_event.source.as_str() != "curation"
        || judgment_event.event_type.as_str() != "curation.judgment"
    {
        return Err(
            SinexError::validation("curation.finalize: event is not a curation judgment")
                .with_context("judgment_event_id", &req.judgment_event_id)
                .with_context("source", judgment_event.source.as_str())
                .with_context("event_type", judgment_event.event_type.as_str()),
        );
    }
    let judgment: CurationJudgmentPayload = serde_json::from_value(judgment_event.payload.clone())
        .map_err(|error| {
            SinexError::serialization("curation.finalize: invalid judgment payload")
                .with_std_error(&error)
        })?;
    let parent_ids = judgment_event.get_source_event_ids().ok_or_else(|| {
        SinexError::invalid_state("curation.finalize: judgment event has no proposal parent")
            .with_context("judgment_event_id", &req.judgment_event_id)
    })?;
    let proposal_event_id = parent_ids.first().copied().ok_or_else(|| {
        SinexError::invalid_state("curation.finalize: judgment event has no proposal parent")
            .with_context("judgment_event_id", &req.judgment_event_id)
    })?;
    let proposal_event = pool
        .events()
        .get_by_id(proposal_event_id)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("curation.finalize: proposal parent not found")
                .with_context("judgment_event_id", &req.judgment_event_id)
                .with_context("proposal_event_id", proposal_event_id.to_string())
        })?;
    if proposal_event.source.as_str() != "curation"
        || proposal_event.event_type.as_str() != "curation.proposal"
    {
        return Err(SinexError::validation(
            "curation.finalize: judgment parent is not a curation proposal",
        )
        .with_context("judgment_event_id", &req.judgment_event_id)
        .with_context("proposal_event_id", proposal_event_id.to_string())
        .with_context("source", proposal_event.source.as_str())
        .with_context("event_type", proposal_event.event_type.as_str()));
    }
    let proposal: CurationProposalPayload = serde_json::from_value(proposal_event.payload.clone())
        .map_err(|error| {
            SinexError::serialization("curation.finalize: invalid proposal payload")
                .with_std_error(&error)
        })?;

    // sinex-0vx.5: the curation-bypass rejection AND the actor-kind
    // acceptance gate. A finalizer must be registered for this exact
    // (proposal_kind, output_source, output_event_type) triple, and the
    // judgment's actor_kind must be sufficient authority under that
    // finalizer's policy (Agent is never sufficient by default). Runs
    // before any adjudicated write is built.
    crate::authority::authorize_finalization(
        pool,
        &proposal.proposal_kind,
        &proposal.candidate_source,
        &proposal.candidate_event_type,
        judgment.actor_kind,
    )
    .await?;

    let finalized_at = Timestamp::now();
    let finalized = CurationFinalizedPayload::from_judgment(
        Uuid::now_v7(),
        &proposal,
        &judgment,
        finalized_at,
    )?;
    let judgment_parent = judgment_event.id.ok_or_else(|| {
        SinexError::invalid_state("curation.finalize: persisted judgment event missing id")
    })?;
    let parents: [EventId; 2] = [proposal_event_id, judgment_parent];
    let mut event = finalized
        .clone()
        .from_parents(parents)?
        .at_time(finalized_at)
        .build()?;
    apply_curation_finalized_declaration(&mut event, judgment_parent, 2, 0)?;
    let inserted = pool.events().insert(event).await?;

    // Generic lane-promotion bridge (sinex-0vx.7): ANY proposal whose
    // candidate_payload carries a "lane_id" is understood as promoting a
    // derivation.lanes row on successful finalize -- this file has no
    // sessionization-specific knowledge, only that a finalized proposal
    // referencing a lane_id promotes that lane. Session-lane promotion
    // (this bead) is the first user; a later lane kind (e.g. sinex-0vx.9's
    // entity-chain lane) reuses the same bridge rather than inventing a
    // parallel per-kind finalize path.
    if let Some(lane_id) = extract_lane_id(&proposal.candidate_payload)? {
        pool.derivation_lanes()
            .set_lane_status(lane_id, "promoted", Some(finalized_at))
            .await?;
    }

    let operation_record = pool
        .state()
        .log_operation(DbOperation {
            id: None,
            operation_type: "curation.finalize".to_string(),
            operator: "curation.finalize".to_string(),
            scope: Some(json!({
                "proposal_event_id": proposal_event_id,
                "judgment_event_id": judgment_parent,
                "finalization_event_id": inserted.id,
                "proposal_id": proposal.proposal_id,
                "judgment_id": judgment.judgment_id,
            })),
            result_status: OperationStatus::Success,
            result_message: Some("curation judgment finalized".to_string()),
            preview_summary: Some(json!({
                "output_source": finalized.output_source,
                "output_event_type": finalized.output_event_type,
                "proposal_kind": proposal.proposal_kind,
            })),
            duration_ms: None,
        })
        .await?;

    Ok(CurationFinalizeResponse {
        finalized,
        event: inserted,
        operation: operation_record_to_rpc(operation_record),
    })
}

/// Extract `candidate_payload.lane_id` if present -- the generic
/// curation-to-lane-promotion bridge's input (see the call site in
/// `handle_curation_finalize`). Fails closed on a present-but-malformed
/// value rather than silently skipping promotion.
fn extract_lane_id(candidate_payload: &JsonValue) -> Result<Option<Uuid>> {
    let Some(value) = candidate_payload.get("lane_id") else {
        return Ok(None);
    };
    let raw = value.as_str().ok_or_else(|| {
        SinexError::validation("curation.finalize: candidate_payload.lane_id must be a string")
            .with_context("lane_id", value.to_string())
    })?;
    let lane_id = Uuid::parse_str(raw).map_err(|error| {
        SinexError::validation("curation.finalize: candidate_payload.lane_id is not a valid uuid")
            .with_context("lane_id", raw)
            .with_std_error(&error)
    })?;
    Ok(Some(lane_id))
}

fn cluster_row_error(error: sqlx::Error) -> SinexError {
    SinexError::database("failed to decode duplicate candidate row").with_std_error(&error)
}

fn duplicate_cluster_id(source: &str, event_type: &str, equivalence_key: &str) -> String {
    format!("{source}/{event_type}/{equivalence_key}")
}

fn duplicate_logical_key(event: &Event<JsonValue>) -> Option<String> {
    event
        .equivalence_key
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn persisted_event_id(event: &Event<JsonValue>) -> Result<EventId> {
    event.id.ok_or_else(|| {
        SinexError::invalid_state("curation duplicate candidate event missing persisted id")
    })
}

fn validate_duplicate_judgment_request(req: &CurationRecordDuplicateJudgmentRequest) -> Result<()> {
    if req.source.trim().is_empty() {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires source",
        ));
    }
    if req.event_type.trim().is_empty() {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires event_type",
        ));
    }
    if req.equivalence_key.trim().is_empty() {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires equivalence_key",
        ));
    }
    if req.event_ids.len() < 2 {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires at least two candidate events",
        ));
    }
    let mut unique = HashSet::with_capacity(req.event_ids.len());
    for id in &req.event_ids {
        if !unique.insert(*id) {
            return Err(SinexError::validation(
                "curation.duplicate_judgments.record candidate event ids must be unique",
            )
            .with_context("event_id", id.to_string()));
        }
    }
    if req.action == CurationDuplicateAction::Prefer {
        let preferred_event_id = req.preferred_event_id.ok_or_else(|| {
            SinexError::validation("prefer duplicate action requires preferred_event_id")
        })?;
        if !unique.contains(&preferred_event_id) {
            return Err(SinexError::validation(
                "preferred_event_id must be one of the candidate event ids",
            )
            .with_context("preferred_event_id", preferred_event_id.to_string()));
        }
    } else if req.preferred_event_id.is_some() {
        return Err(SinexError::validation(
            "preferred_event_id is only valid with prefer duplicate action",
        ));
    }
    Ok(())
}

fn duplicate_action_decision(action: CurationDuplicateAction) -> CurationJudgmentDecision {
    match action {
        CurationDuplicateAction::Merge | CurationDuplicateAction::Prefer => {
            CurationJudgmentDecision::Accept
        }
        CurationDuplicateAction::Ignore => CurationJudgmentDecision::Reject,
    }
}

fn duplicate_action_verdict(action: CurationDuplicateAction) -> JudgmentVerdict {
    match action {
        CurationDuplicateAction::Merge | CurationDuplicateAction::Prefer => JudgmentVerdict::Accept,
        CurationDuplicateAction::Ignore => JudgmentVerdict::Reject,
    }
}

fn operation_record_to_rpc(record: sinex_db::repositories::OperationRecord) -> Operation {
    Operation {
        id: record.id.to_string(),
        operation_type: record.operation_type,
        operator: record.operator,
        scope: record.scope,
        result_status: record.result_status,
        result_message: record.result_message,
        preview_summary: record.preview_summary,
        duration_ms: record.duration_ms,
    }
}
