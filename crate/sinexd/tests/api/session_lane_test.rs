//! End-to-end proof for sinex-0vx.7: sessionization runs as a declared
//! derivation lane, its shadow diff against a canonical baseline uses the
//! generic `LaneDiffReport` (typed `SessionBoundaryDiffCounts`/
//! `SessionBoundaryDiffExample` examples), and the curation
//! proposal/judgment/finalizer path (sinex-0vx.5) promotes the shadow lane,
//! flipping `derivation.lanes.status` to `promoted` through the GENERIC
//! `candidate_payload.lane_id` bridge in `handle_curation_finalize` (not a
//! session-specific promotion path).

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::{Event, Provenance};
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::derivation::{ClaimSupport, DerivedProductClass, LaneDiffReport};
use sinex_primitives::events::payloads::{
    ActivitySessionBoundaryPayload, ActivityWindowCloseReason, ActivityWindowSummaryPayload,
    CurationJudgmentActorKind, CurationJudgmentDecision, CurationProposalPayload,
    CurationProposalStatus,
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::rpc::curation::{CurationFinalizeRequest, CurationRecordJudgmentRequest};
use sinex_primitives::session_lane::SessionLaneOutputs;
use sinex_primitives::{Id, Timestamp, Uuid};
use sinexd::api::handlers::{handle_curation_finalize, handle_curation_record_judgment};
use sinexd::api::rpc_server::RpcAuthContext;
use sinexd::session_lane::{SESSION_LANE_FINALIZER_DECLARATIONS, SESSION_LANE_OUTPUT_DECLARATIONS};
use std::collections::BTreeMap;
use xtask::sandbox::prelude::*;

#[path = "common/mod.rs"]
mod common;

fn window_summary(
    window_id: &str,
    start: Timestamp,
    duration_secs: i64,
    event_count: u64,
    close_reason: ActivityWindowCloseReason,
) -> ActivityWindowSummaryPayload {
    let end = start + time::Duration::seconds(duration_secs.max(0));
    let mut counts = BTreeMap::new();
    counts.insert(ActivitySourceKind::Window, event_count);
    ActivityWindowSummaryPayload {
        window_id: window_id.to_string(),
        window_start: start,
        window_end: end,
        duration_secs: duration_secs.max(0) as u64,
        event_count,
        source_count: 1,
        sources: vec!["session-lane-shadow-diff".to_string()],
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: counts,
        primary_source: ActivitySourceKind::Window,
        close_reason,
    }
}

/// Seed both the session-lane output declaration and its finalizer
/// registration through the real production reconcilers -- the same thing
/// `Supervisor::run` does at `sinexd` startup (`crate::supervisor::
/// reconcile_product_declarations`), never run inside this test sandbox.
async fn seed_session_lane_declarations(pool: &sqlx::PgPool) -> TestResult<()> {
    sinexd::automata::product_declarations::reconcile_declarations(
        pool,
        "session-lane",
        SESSION_LANE_OUTPUT_DECLARATIONS,
    )
    .await?;
    sinexd::authority::reconcile_finalizer_registrations(pool, SESSION_LANE_FINALIZER_DECLARATIONS)
        .await?;
    Ok(())
}

/// Anti-vacuity: this test exercises the REAL production
/// `DerivationRepository::seed_session_lane_outputs_from_{canonical_events,
/// window_scope}` (sinex-0vx.7 in `sinex-db`), the REAL
/// `LaneDiffReport::compute::<SessionLaneOutputs>` (sinex-primitives), and
/// the REAL `handle_curation_record_judgment`/`handle_curation_finalize`
/// RPC handlers plus the generic lane-promotion bridge added to
/// `handle_curation_finalize` in this same change. Deleting the
/// `candidate_payload.lane_id` promotion block in `curation.rs`, or
/// reverting `compute_session_boundaries`'s `Gap`-close grouping to treat
/// every window as its own session, makes this test fail: the diff would
/// stop reporting `duration_changed`, or the final `get_lane` assertion
/// would see `status != "promoted"`.
#[sinex_test]
async fn session_lane_shadow_diff_promotion(ctx: TestContext) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    seed_session_lane_declarations(ctx.pool()).await?;

    let repo = ctx.pool().derivation_lanes();
    let product_class = DerivedProductClass::SemanticCandidate.as_str();

    let material_record = ctx
        .pool()
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("session-lane-shadow-diff"),
            json!({ "test": true }),
        )
        .await?;
    let material_id =
        Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    // Canonical baseline: a real `activity.session.boundary` event, 60s
    // duration, already persisted (as if `SessionDetector` emitted it).
    let start = Timestamp::now();
    let mut baseline_counts = BTreeMap::new();
    baseline_counts.insert(ActivitySourceKind::Window, 5u64);
    let canonical_payload = ActivitySessionBoundaryPayload {
        session_id: "activity-session:promo-w1".to_string(),
        start_time: start,
        end_time: start + time::Duration::seconds(60),
        duration_secs: 60,
        event_count: 5,
        window_count: 1,
        source_count: 1,
        sources: vec!["session-lane-shadow-diff".to_string()],
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: baseline_counts,
        primary_source: ActivitySourceKind::Window,
    };
    ctx.pool()
        .events()
        .insert(
            Event::builder(canonical_payload)
                .with_provenance(Provenance::from_material(material_id, 0, None, None))
                .build()
                .expect("valid canonical session boundary event"),
        )
        .await?;

    // Shadow candidate: a raw window covering the SAME occurrence but a
    // DIFFERENT duration (90s, not 60s) -- proves the diff detects real
    // churn, not just presence/absence.
    let window_event = ctx
        .pool()
        .events()
        .insert(
            Event::builder(window_summary(
                "promo-w1",
                start,
                90,
                5,
                ActivityWindowCloseReason::Gap,
            ))
            .with_provenance(Provenance::from_material(material_id, 1, None, None))
            .build()
            .expect("valid window summary event"),
        )
        .await?;
    let window_event_id = *window_event
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("window event should have id"))?
        .as_uuid();

    // Baseline lane: an epoch + canonical-kind lane seeded from the
    // canonical event above.
    let baseline_epoch = repo
        .create_epoch(sinex_db::repositories::CreateDerivationEpoch {
            id: None,
            declaration_id: "session-lane.session_boundary.semantic_candidate".to_string(),
            name: "session-lane-baseline".to_string(),
            product_class: product_class.to_string(),
            scope_model: "event_set".to_string(),
            scope: json!({
                "kind": "event_set",
                "input_ids": ["all"],
                "input_set_hash": "session-lane-baseline-hash",
            }),
            semantics_version: "1.0.0".to_string(),
            code_ref: Some("test@session-lane-baseline".to_string()),
            config_hash: "session-lane-baseline-config".to_string(),
            components: json!([]),
            prompt_set_hash: None,
            model_config_hash: None,
            created_by: "test".to_string(),
            operation_id: None,
            supersedes_epoch_id: None,
        })
        .await?;
    let baseline_lane = repo
        .create_lane(sinex_db::repositories::CreateDerivationLane {
            id: None,
            declaration_id: "session-lane.session_boundary.semantic_candidate".to_string(),
            name: "session-lane-baseline".to_string(),
            kind: "canonical".to_string(),
            product_class: product_class.to_string(),
            base_epoch_id: None,
            candidate_epoch_id: baseline_epoch.id,
            scope: json!({
                "kind": "event_set",
                "input_ids": ["all"],
                "input_set_hash": "session-lane-baseline-hash",
            }),
            purpose: Some("shadow-diff baseline".to_string()),
            operation_id: None,
            expires_at: None,
        })
        .await?;
    let baseline_written = repo
        .seed_session_lane_outputs_from_canonical_events(baseline_lane.id, product_class)
        .await?;
    assert_eq!(baseline_written, 1);

    // Shadow lane: an epoch + shadow-kind lane scoped to the raw window
    // event, seeded by RECOMPUTING session boundaries.
    let shadow_scope = json!({
        "kind": "event_set",
        "input_ids": [format!("event:{window_event_id}")],
        "input_set_hash": "session-lane-shadow-hash",
    });
    let shadow_epoch = repo
        .create_epoch(sinex_db::repositories::CreateDerivationEpoch {
            id: None,
            declaration_id: "session-lane.session_boundary.semantic_candidate".to_string(),
            name: "session-lane-shadow".to_string(),
            product_class: product_class.to_string(),
            scope_model: "event_set".to_string(),
            scope: shadow_scope.clone(),
            semantics_version: "1.0.0".to_string(),
            code_ref: Some("test@session-lane-shadow".to_string()),
            config_hash: "session-lane-shadow-config".to_string(),
            components: json!([]),
            prompt_set_hash: None,
            model_config_hash: None,
            created_by: "test".to_string(),
            operation_id: None,
            supersedes_epoch_id: Some(baseline_epoch.id),
        })
        .await?;
    let shadow_lane = repo
        .create_lane(sinex_db::repositories::CreateDerivationLane {
            id: None,
            declaration_id: "session-lane.session_boundary.semantic_candidate".to_string(),
            name: "session-lane-shadow".to_string(),
            kind: "shadow".to_string(),
            product_class: product_class.to_string(),
            base_epoch_id: Some(baseline_epoch.id),
            candidate_epoch_id: shadow_epoch.id,
            scope: shadow_scope,
            purpose: Some("shadow-diff candidate".to_string()),
            operation_id: None,
            expires_at: None,
        })
        .await?;
    let shadow_written = repo
        .seed_session_lane_outputs_from_window_scope(shadow_lane.id, product_class)
        .await?;
    assert_eq!(shadow_written, 1);

    // Shadow diff via the generic LaneDiffReport, typed over
    // SessionLaneOutputs (SessionBoundaryDiffCounts/Example).
    let baseline_outputs = repo.read_session_lane_outputs(baseline_lane.id).await?;
    let candidate_outputs = repo.read_session_lane_outputs(shadow_lane.id).await?;
    let report = LaneDiffReport::compute::<SessionLaneOutputs>(
        baseline_lane.id,
        shadow_lane.id,
        DerivedProductClass::SemanticCandidate,
        "session-lane-shadow-hash",
        &baseline_outputs,
        &candidate_outputs,
        10,
    )
    .expect("compute session lane diff report");
    assert_eq!(report.output_kind, "session_boundary");
    assert_eq!(report.counts["duration_changed"], 1);
    assert_eq!(report.summary.changed, 1);
    repo.record_lane_diff(Uuid::now_v7(), &report).await?;
    repo.set_lane_status(shadow_lane.id, "completed", Some(Timestamp::now()))
        .await?;

    // Curation proposal referencing the shadow lane by id -- the generic
    // "this proposal, once finalized, promotes a lane" contract.
    let proposal = CurationProposalPayload {
        proposal_id: Uuid::now_v7(),
        proposal_key: format!("session-lane-promotion:{}", shadow_lane.id),
        proposal_kind: "session.lane_promotion".to_string(),
        target_ref: Some(shadow_lane.id.to_string()),
        candidate_source: "session-lane".to_string(),
        candidate_event_type: "session.lane_promotion".to_string(),
        candidate_payload: json!({
            "lane_id": shadow_lane.id.to_string(),
            "diff_summary": {
                "duration_changed": report.counts["duration_changed"],
            },
        }),
        authority_proposal: None,
        evidence_event_ids: vec![window_event_id],
        evidence_material_ids: vec![material_record.id],
        producer: "sinexd-test.session-lane@1".to_string(),
        confidence: 1.0,
        rationale: "shadow session lane diffed against canonical baseline; ready for promotion"
            .to_string(),
        status: CurationProposalStatus::Pending,
    };
    let mut proposal_event = proposal
        .clone()
        .from_parents([Id::<sinex_db::Event<sinex_primitives::JsonValue>>::from_uuid(
            window_event_id,
        )])?
        .build()?;
    // Same declaration_id `common::seed_rpc_handler_product_declarations`
    // reconciles for `curation.proposal` writes (`curation-rpc.curation.
    // proposal`, sinex-0vx.4/x79t) -- reused directly rather than
    // duplicating a private const, same as `curation_handlers_test.rs`'s
    // own fixture-proposal helper reuses/mirrors the real declaration shape.
    proposal_event.product_class = Some(DerivedProductClass::SemanticCandidate);
    proposal_event.claim_support = Some(ClaimSupport::unknown());
    proposal_event.derivation_declaration_id =
        Some("curation-rpc.curation.proposal".to_string());
    let proposal_event = ctx.pool().events().insert(proposal_event).await?;
    let proposal_event_id = proposal_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("proposal event missing id"))?;

    let auth = RpcAuthContext::system();
    let judgment_response = handle_curation_record_judgment(
        ctx.pool(),
        CurationRecordJudgmentRequest {
            proposal_event_id: proposal_event_id.to_uuid().to_string(),
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            decision: CurationJudgmentDecision::Accept,
            corrected_payload: None,
            comment: Some("shadow lane diff reviewed; promoting".to_string()),
            authorization_context: None,
        },
        &auth,
    )
    .await?;
    let judgment_event_id = judgment_response
        .event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment event missing id"))?;

    let finalization = handle_curation_finalize(
        ctx.pool(),
        CurationFinalizeRequest {
            judgment_event_id: judgment_event_id.to_uuid().to_string(),
        },
    )
    .await?;
    assert_eq!(finalization.finalized.output_source, "session-lane");
    assert_eq!(finalization.finalized.output_event_type, "session.lane_promotion");

    // The AC's core assertion: the shadow lane is now promoted, through the
    // generic curation finalize bridge -- not a session-specific write.
    let promoted_lane = repo.get_lane(shadow_lane.id).await?;
    assert_eq!(promoted_lane.status, "promoted");

    Ok(())
}
