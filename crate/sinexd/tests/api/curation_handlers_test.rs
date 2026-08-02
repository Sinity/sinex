use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::JsonValue;
use sinex_primitives::authority::JudgmentVerdict;
use sinex_primitives::derivation::{ClaimSupport, DerivedProductClass};
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::events::payloads::{
    CurationJudgmentActorKind, CurationJudgmentDecision, CurationProposalPayload,
};
use sinex_primitives::events::{EventPayload, payloads::CurationJudgmentPayload};
use sinex_primitives::query::EventQueryResult;
use sinex_primitives::rpc::curation::{
    CurationDuplicateAction, CurationFinalizeRequest, CurationListDuplicateCandidatesRequest,
    CurationListProposalsRequest, CurationRecordDuplicateJudgmentRequest,
    CurationRecordJudgmentRequest,
};
use sinexd::api::handlers::{
    handle_curation_finalize, handle_curation_list_duplicate_candidates,
    handle_curation_list_proposals, handle_curation_record_duplicate_judgment,
    handle_curation_record_judgment,
};
use sinexd::api::auth::Role;
use sinexd::api::rpc_server::RpcAuthContext;
use xtask::sandbox::prelude::*;

#[path = "common/mod.rs"]
mod common;

#[sinex_test]
async fn curation_list_proposals_returns_pending_events(ctx: TestContext) -> TestResult<()> {
    insert_fixture_proposal(&ctx).await?;

    let result = handle_curation_list_proposals(
        ctx.pool(),
        CurationListProposalsRequest {
            status: "pending".to_string(),
            ..Default::default()
        },
    )
    .await?;

    match result {
        EventQueryResult::Events { events, .. } => {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event.source.as_str(), "curation");
            assert_eq!(events[0].event.event_type.as_str(), "curation.proposal");
        }
        other => panic!("expected event listing, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn curation_record_judgment_persists_synthesis_event(ctx: TestContext) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let proposal_event = insert_fixture_proposal(&ctx).await?;
    let proposal_event_id = proposal_event
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("inserted proposal missing id"))?
        .to_uuid()
        .to_string();
    let auth = RpcAuthContext::system();

    let value = handle_curation_record_judgment(
        ctx.pool(),
        CurationRecordJudgmentRequest {
            proposal_event_id,
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            decision: CurationJudgmentDecision::Accept,
            corrected_payload: None,
            comment: Some("fixture accepted".to_string()),
            authorization_context: None,
        },
        &auth,
    )
    .await?;

    let judgment: CurationJudgmentPayload = value.judgment;
    assert_eq!(judgment.actor_id, auth.actor_id());
    assert_eq!(judgment.decision, CurationJudgmentDecision::Accept);

    let event_id = value
        .event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;
    let persisted = ctx
        .pool()
        .events()
        .get_by_id(event_id)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment event not persisted"))?;
    assert_eq!(persisted.source.as_str(), "curation");
    assert_eq!(persisted.event_type.as_str(), "curation.judgment");
    assert_eq!(
        persisted
            .get_source_event_ids()
            .map(<[sinex_db::Id<sinex_db::Event>]>::len),
        Some(1)
    );
    Ok(())
}

/// sinex-audit-actorkind: a plain `:write` token cannot self-authorize
/// curation finalization by claiming `actor_kind: "operator"` in the
/// request body. This reproduces the exploit the finding described --
/// record a judgment as a self-claimed `Operator` over a `Role::Write`
/// auth context, then attempt to finalize it -- and proves it now fails
/// closed: the server clamps the persisted `actor_kind` down to `Agent`
/// for anything below `Role::Admin`, and an `Agent`-kind judgment is never
/// sufficient authority by itself under the default (no
/// `auto_accept_policy`) finalizer posture.
#[sinex_test]
async fn curation_record_judgment_clamps_self_claimed_operator_actor_kind(
    ctx: TestContext,
) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let proposal_event = insert_fixture_proposal(&ctx).await?;
    let proposal_event_id = proposal_event
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("inserted proposal missing id"))?
        .to_uuid()
        .to_string();

    // An ordinary `:write` token -- the same tier ordinary event ingestion
    // uses, deliberately NOT the operator's elevated Admin token.
    let write_auth = RpcAuthContext {
        token_prefix: "test1234".to_string(),
        actor_id: "token:test1234".to_string(),
        authenticated_at: sinex_primitives::Timestamp::now(),
        role: Role::Write,
    };

    // The exploit attempt: self-claim Operator authority in the request
    // body, which pre-fix was copied verbatim into the persisted judgment.
    let value = handle_curation_record_judgment(
        ctx.pool(),
        CurationRecordJudgmentRequest {
            proposal_event_id,
            actor_kind: CurationJudgmentActorKind::Operator,
            actor_id: None,
            decision: CurationJudgmentDecision::Accept,
            corrected_payload: None,
            comment: Some("self-claimed operator judgment".to_string()),
            authorization_context: None,
        },
        &write_auth,
    )
    .await?;

    // The server must clamp the persisted actor_kind down to Agent -- never
    // trust the client-supplied claim above what the caller's role permits.
    assert_eq!(value.judgment.actor_kind, CurationJudgmentActorKind::Agent);

    let event_id = value
        .event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;

    // Finalize must fail closed: an Agent-kind judgment is never sufficient
    // authority by itself under the default finalizer policy
    // (requires_human_judgment = true, no auto_accept_policy).
    let error = handle_curation_finalize(
        ctx.pool(),
        CurationFinalizeRequest {
            judgment_event_id: event_id.to_uuid().to_string(),
        },
    )
    .await
    .expect_err("self-claimed operator actor_kind must not authorize finalization");
    assert!(
        error
            .to_string()
            .contains("not sufficient authority to finalize"),
        "unexpected error: {error}"
    );

    let operations = ctx
        .pool()
        .state()
        .list_operations(Some("curation.finalize"), None, 10)
        .await?;
    assert!(operations.is_empty());
    Ok(())
}

#[sinex_test]
async fn curation_duplicate_candidates_list_cross_material_clusters(
    ctx: TestContext,
) -> TestResult<()> {
    let candidate_a = insert_duplicate_candidate(&ctx, "visit-1", "material-a").await?;
    let candidate_b = insert_duplicate_candidate(&ctx, "visit-1", "material-b").await?;
    insert_duplicate_candidate(&ctx, "visit-2", "material-a").await?;

    let response = handle_curation_list_duplicate_candidates(
        ctx.pool(),
        CurationListDuplicateCandidatesRequest {
            source: Some("webhistory".to_string()),
            event_type: Some("page.visited".to_string()),
            limit: 10,
            events_per_cluster: 10,
        },
    )
    .await?;

    assert_eq!(response.clusters.len(), 1);
    let cluster = &response.clusters[0];
    assert_eq!(cluster.source, "webhistory");
    assert_eq!(cluster.event_type, "page.visited");
    assert_eq!(cluster.equivalence_key, "visit-1");
    assert_eq!(cluster.event_count, 2);
    assert_eq!(cluster.material_count, 2);
    let listed_ids: Vec<_> = cluster
        .events
        .iter()
        .map(|event| *event.event_id.as_uuid())
        .collect();
    assert!(listed_ids.contains(&candidate_a));
    assert!(listed_ids.contains(&candidate_b));
    Ok(())
}

#[sinex_test]
async fn curation_duplicate_judgment_records_proposal_over_candidate_set(
    ctx: TestContext,
) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let candidate_a = insert_duplicate_candidate(&ctx, "visit-1", "material-a").await?;
    let candidate_b = insert_duplicate_candidate(&ctx, "visit-1", "material-b").await?;
    let auth = RpcAuthContext::system();

    let response = handle_curation_record_duplicate_judgment(
        ctx.pool(),
        CurationRecordDuplicateJudgmentRequest {
            source: "webhistory".to_string(),
            event_type: "page.visited".to_string(),
            equivalence_key: "visit-1".to_string(),
            event_ids: vec![candidate_a, candidate_b],
            action: CurationDuplicateAction::Prefer,
            preferred_event_id: Some(candidate_a),
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            comment: Some("prefer first fixture".to_string()),
        },
        &auth,
    )
    .await?;

    assert_eq!(
        response.proposal.proposal_kind,
        "curation.duplicate_resolution"
    );
    assert_eq!(response.proposal.evidence_event_ids.len(), 2);
    assert_eq!(response.proposal.evidence_material_ids.len(), 2);
    assert_eq!(response.judgment.actor_id, auth.actor_id());
    assert_eq!(response.judgment.decision, CurationJudgmentDecision::Accept);
    let authority_proposal = response
        .proposal
        .authority_proposal
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("duplicate proposal missing shared authority"))?;
    let authority_judgment = response
        .judgment
        .authority_judgment
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("duplicate judgment missing shared authority"))?;
    assert_eq!(authority_judgment.proposal_id, authority_proposal.id);
    assert_eq!(authority_judgment.verdict, JudgmentVerdict::Accept);
    assert_eq!(
        response
            .judgment
            .authorization_context
            .as_ref()
            .and_then(|value| value.get("duplicate_action"))
            .and_then(JsonValue::as_str),
        Some("prefer")
    );

    let proposal_event_id = response
        .proposal_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("proposal response event missing id"))?;
    let proposal_event = ctx
        .pool()
        .events()
        .get_by_id(proposal_event_id)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("proposal event not persisted"))?;
    let parents = proposal_event
        .get_source_event_ids()
        .ok_or_else(|| color_eyre::eyre::eyre!("proposal missing candidate parents"))?;
    assert_eq!(parents.len(), 2);
    assert!(parents.iter().any(|id| id.to_uuid() == candidate_a));
    assert!(parents.iter().any(|id| id.to_uuid() == candidate_b));

    let judgment_event_id = response
        .judgment_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;
    let judgment_event = ctx
        .pool()
        .events()
        .get_by_id(judgment_event_id)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment event not persisted"))?;
    assert_eq!(
        judgment_event.get_source_event_ids(),
        Some([proposal_event_id].as_slice())
    );
    Ok(())
}

#[sinex_test]
async fn curation_duplicate_accept_finalizes_through_operation_record(
    ctx: TestContext,
) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let candidate_a = insert_duplicate_candidate(&ctx, "visit-1", "material-a").await?;
    let candidate_b = insert_duplicate_candidate(&ctx, "visit-1", "material-b").await?;
    let auth = RpcAuthContext::system();

    let judgment = handle_curation_record_duplicate_judgment(
        ctx.pool(),
        CurationRecordDuplicateJudgmentRequest {
            source: "webhistory".to_string(),
            event_type: "page.visited".to_string(),
            equivalence_key: "visit-1".to_string(),
            event_ids: vec![candidate_a, candidate_b],
            action: CurationDuplicateAction::Merge,
            preferred_event_id: None,
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            comment: Some("merge duplicate fixtures".to_string()),
        },
        &auth,
    )
    .await?;
    let judgment_event_id = judgment
        .judgment_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;

    let finalization = handle_curation_finalize(
        ctx.pool(),
        CurationFinalizeRequest {
            judgment_event_id: judgment_event_id.to_uuid().to_string(),
        },
    )
    .await?;
    let expected_judgment_event_id = judgment_event_id.to_string();

    assert_eq!(finalization.operation.operation_type, "curation.finalize");
    assert_eq!(
        finalization.operation.result_status,
        OperationStatus::Success
    );
    assert_eq!(
        finalization
            .operation
            .scope
            .as_ref()
            .and_then(|value| value.get("judgment_event_id"))
            .and_then(JsonValue::as_str),
        Some(expected_judgment_event_id.as_str())
    );
    assert_eq!(
        finalization.finalized.output_payload["action"].as_str(),
        Some("merge")
    );
    Ok(())
}

#[sinex_test]
async fn curation_duplicate_reject_does_not_create_finalization_operation(
    ctx: TestContext,
) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let candidate_a = insert_duplicate_candidate(&ctx, "visit-1", "material-a").await?;
    let candidate_b = insert_duplicate_candidate(&ctx, "visit-1", "material-b").await?;
    let auth = RpcAuthContext::system();

    let judgment = handle_curation_record_duplicate_judgment(
        ctx.pool(),
        CurationRecordDuplicateJudgmentRequest {
            source: "webhistory".to_string(),
            event_type: "page.visited".to_string(),
            equivalence_key: "visit-1".to_string(),
            event_ids: vec![candidate_a, candidate_b],
            action: CurationDuplicateAction::Ignore,
            preferred_event_id: None,
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            comment: Some("not duplicates".to_string()),
        },
        &auth,
    )
    .await?;
    let judgment_event_id = judgment
        .judgment_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;

    let error = handle_curation_finalize(
        ctx.pool(),
        CurationFinalizeRequest {
            judgment_event_id: judgment_event_id.to_uuid().to_string(),
        },
    )
    .await
    .expect_err("reject duplicate judgment must not finalize");
    assert!(
        error
            .to_string()
            .contains("only an Accept judgment may promote a proposal")
    );

    let operations = ctx
        .pool()
        .state()
        .list_operations(Some("curation.finalize"), None, 10)
        .await?;
    assert!(operations.is_empty());
    Ok(())
}

#[sinex_test]
async fn curation_finalize_persists_lineage_to_original_proposal_and_judgment(
    ctx: TestContext,
) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let proposal_event = insert_fixture_proposal(&ctx).await?;
    let original_proposal_event_id = proposal_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("inserted proposal missing id"))?;
    let auth = RpcAuthContext::system();
    let judgment_response = handle_curation_record_judgment(
        ctx.pool(),
        CurationRecordJudgmentRequest {
            proposal_event_id: original_proposal_event_id.to_uuid().to_string(),
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            decision: CurationJudgmentDecision::Accept,
            corrected_payload: None,
            comment: Some("fixture accepted".to_string()),
            authorization_context: None,
        },
        &auth,
    )
    .await?;
    let judgment_event_id = judgment_response
        .event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;

    let replayed_proposal = insert_replayed_fixture_proposal(&ctx).await?;
    let replayed_proposal_event_id = replayed_proposal
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("replayed proposal missing id"))?;
    assert_ne!(original_proposal_event_id, replayed_proposal_event_id);

    let finalization = handle_curation_finalize(
        ctx.pool(),
        CurationFinalizeRequest {
            judgment_event_id: judgment_event_id.to_uuid().to_string(),
        },
    )
    .await?;

    assert_eq!(
        finalization.finalized.proposal_id,
        judgment_response.judgment.proposal_id
    );
    assert_eq!(
        finalization.finalized.judgment_id,
        judgment_response.judgment.judgment_id
    );

    let finalization_event_id = finalization
        .event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("finalization response event missing id"))?;
    let persisted = ctx
        .pool()
        .events()
        .get_by_id(finalization_event_id)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("finalization event not persisted"))?;
    let parents = persisted
        .get_source_event_ids()
        .ok_or_else(|| color_eyre::eyre::eyre!("finalization event missing derived parents"))?;
    assert_eq!(parents, &[original_proposal_event_id, judgment_event_id]);
    assert!(!parents.contains(&replayed_proposal_event_id));
    assert_eq!(finalization.operation.operation_type, "curation.finalize");
    assert_eq!(
        finalization.operation.result_status,
        OperationStatus::Success
    );
    Ok(())
}

/// sinex-audit-dupe-workbench-stale-cluster: a finalized Merge judgment over
/// a 2-way cluster must archive the losing event so the exact same
/// `GROUP BY` query in `handle_curation_list_duplicate_candidates` no
/// longer returns it. Anti-vacuity: before the archive bridge in
/// `handle_curation_finalize`, this re-list returned the identical cluster
/// forever -- reverting that bridge reproduces the failure here.
#[sinex_test]
async fn curation_duplicate_merge_finalize_removes_two_way_cluster_from_list(
    ctx: TestContext,
) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let candidate_a = insert_duplicate_candidate(&ctx, "merge-2way", "material-a").await?;
    let candidate_b = insert_duplicate_candidate(&ctx, "merge-2way", "material-b").await?;
    let auth = RpcAuthContext::system();

    let judgment = handle_curation_record_duplicate_judgment(
        ctx.pool(),
        CurationRecordDuplicateJudgmentRequest {
            source: "webhistory".to_string(),
            event_type: "page.visited".to_string(),
            equivalence_key: "merge-2way".to_string(),
            event_ids: vec![candidate_a, candidate_b],
            action: CurationDuplicateAction::Merge,
            preferred_event_id: None,
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            comment: Some("merge duplicate fixtures".to_string()),
        },
        &auth,
    )
    .await?;
    let judgment_event_id = judgment
        .judgment_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;

    handle_curation_finalize(
        ctx.pool(),
        CurationFinalizeRequest {
            judgment_event_id: judgment_event_id.to_uuid().to_string(),
        },
    )
    .await?;

    let response = handle_curation_list_duplicate_candidates(
        ctx.pool(),
        CurationListDuplicateCandidatesRequest {
            source: Some("webhistory".to_string()),
            event_type: Some("page.visited".to_string()),
            limit: 10,
            events_per_cluster: 10,
        },
    )
    .await?;
    assert!(
        response
            .clusters
            .iter()
            .all(|cluster| cluster.equivalence_key != "merge-2way"),
        "resolved 2-way cluster still listed: {:?}",
        response.clusters
    );

    // Merge keeps the smallest (earliest-minted UUIDv7) id as canonical; the
    // other candidate must have been archived out of core.events.
    let winner = candidate_a.min(candidate_b);
    let loser = candidate_a.max(candidate_b);
    assert!(
        ctx.pool()
            .events()
            .get_by_id(sinex_primitives::events::EventId::from_uuid(winner))
            .await?
            .is_some(),
        "canonical merge winner should remain live"
    );
    assert!(
        ctx.pool()
            .events()
            .get_by_id(sinex_primitives::events::EventId::from_uuid(loser))
            .await?
            .is_none(),
        "losing duplicate should be archived out of core.events"
    );
    Ok(())
}

/// sinex-audit-dupe-workbench-stale-cluster: multi-way (3+) clusters need
/// proving separately from the pairwise 2-way case -- the finding notes the
/// cluster grouping itself handles 3-way correctly, but the archive bridge
/// must too. A fully-judged Prefer over all three candidates must remove
/// the whole cluster.
#[sinex_test]
async fn curation_duplicate_prefer_finalize_removes_three_way_cluster_from_list(
    ctx: TestContext,
) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let candidate_a = insert_duplicate_candidate(&ctx, "prefer-3way", "material-a").await?;
    let candidate_b = insert_duplicate_candidate(&ctx, "prefer-3way", "material-b").await?;
    let candidate_c = insert_duplicate_candidate(&ctx, "prefer-3way", "material-c").await?;
    let auth = RpcAuthContext::system();

    let judgment = handle_curation_record_duplicate_judgment(
        ctx.pool(),
        CurationRecordDuplicateJudgmentRequest {
            source: "webhistory".to_string(),
            event_type: "page.visited".to_string(),
            equivalence_key: "prefer-3way".to_string(),
            event_ids: vec![candidate_a, candidate_b, candidate_c],
            action: CurationDuplicateAction::Prefer,
            preferred_event_id: Some(candidate_b),
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            comment: Some("prefer middle fixture".to_string()),
        },
        &auth,
    )
    .await?;
    let judgment_event_id = judgment
        .judgment_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;

    handle_curation_finalize(
        ctx.pool(),
        CurationFinalizeRequest {
            judgment_event_id: judgment_event_id.to_uuid().to_string(),
        },
    )
    .await?;

    let response = handle_curation_list_duplicate_candidates(
        ctx.pool(),
        CurationListDuplicateCandidatesRequest {
            source: Some("webhistory".to_string()),
            event_type: Some("page.visited".to_string()),
            limit: 10,
            events_per_cluster: 10,
        },
    )
    .await?;
    assert!(
        response
            .clusters
            .iter()
            .all(|cluster| cluster.equivalence_key != "prefer-3way"),
        "resolved 3-way cluster still listed: {:?}",
        response.clusters
    );

    assert!(
        ctx.pool()
            .events()
            .get_by_id(sinex_primitives::events::EventId::from_uuid(candidate_b))
            .await?
            .is_some(),
        "preferred event should remain live"
    );
    for loser in [candidate_a, candidate_c] {
        assert!(
            ctx.pool()
                .events()
                .get_by_id(sinex_primitives::events::EventId::from_uuid(loser))
                .await?
                .is_none(),
            "non-preferred duplicate {loser} should be archived out of core.events"
        );
    }
    Ok(())
}

/// sinex-audit-dupe-workbench-stale-cluster: the finding also calls out
/// partial-subset judgments -- `handle_curation_record_duplicate_judgment`
/// only requires 2+ event_ids, not full cluster membership, so a 3-way
/// cluster can be judged over just 2 of its 3 members. That must correctly
/// REDUCE the cluster (archive the judged loser, leave the untouched
/// straggler and the winner live) rather than either silently vanishing the
/// unjudged straggler or leaving the whole cluster untouched.
#[sinex_test]
async fn curation_duplicate_partial_subset_judgment_reduces_cluster_without_full_removal(
    ctx: TestContext,
) -> TestResult<()> {
    common::seed_rpc_handler_product_declarations(ctx.pool()).await?;
    let candidate_a = insert_duplicate_candidate(&ctx, "partial-3way", "material-a").await?;
    let candidate_b = insert_duplicate_candidate(&ctx, "partial-3way", "material-b").await?;
    let straggler = insert_duplicate_candidate(&ctx, "partial-3way", "material-c").await?;
    let auth = RpcAuthContext::system();

    // Judge only candidate_a/candidate_b -- the straggler is never part of
    // this judgment's event_ids.
    let judgment = handle_curation_record_duplicate_judgment(
        ctx.pool(),
        CurationRecordDuplicateJudgmentRequest {
            source: "webhistory".to_string(),
            event_type: "page.visited".to_string(),
            equivalence_key: "partial-3way".to_string(),
            event_ids: vec![candidate_a, candidate_b],
            action: CurationDuplicateAction::Prefer,
            preferred_event_id: Some(candidate_a),
            actor_kind: CurationJudgmentActorKind::TestFixture,
            actor_id: None,
            comment: Some("prefer a over b, straggler untouched".to_string()),
        },
        &auth,
    )
    .await?;
    let judgment_event_id = judgment
        .judgment_event
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("judgment response event missing id"))?;

    handle_curation_finalize(
        ctx.pool(),
        CurationFinalizeRequest {
            judgment_event_id: judgment_event_id.to_uuid().to_string(),
        },
    )
    .await?;

    // Reduced, not gone: candidate_a (winner) and the untouched straggler
    // still form a live 2-member, 2-material cluster over the same
    // equivalence_key, so it correctly still surfaces for further review.
    let response = handle_curation_list_duplicate_candidates(
        ctx.pool(),
        CurationListDuplicateCandidatesRequest {
            source: Some("webhistory".to_string()),
            event_type: Some("page.visited".to_string()),
            limit: 10,
            events_per_cluster: 10,
        },
    )
    .await?;
    let cluster = response
        .clusters
        .iter()
        .find(|cluster| cluster.equivalence_key == "partial-3way")
        .ok_or_else(|| color_eyre::eyre::eyre!("reduced cluster should still be listed"))?;
    assert_eq!(cluster.event_count, 2);
    assert_eq!(cluster.material_count, 2);
    let listed_ids: Vec<_> = cluster
        .events
        .iter()
        .map(|event| *event.event_id.as_uuid())
        .collect();
    assert!(listed_ids.contains(&candidate_a));
    assert!(listed_ids.contains(&straggler));
    assert!(!listed_ids.contains(&candidate_b));

    assert!(
        ctx.pool()
            .events()
            .get_by_id(sinex_primitives::events::EventId::from_uuid(candidate_b))
            .await?
            .is_none(),
        "judged-away candidate_b should be archived"
    );
    assert!(
        ctx.pool()
            .events()
            .get_by_id(sinex_primitives::events::EventId::from_uuid(straggler))
            .await?
            .is_some(),
        "unjudged straggler should remain untouched"
    );
    Ok(())
}

async fn insert_fixture_proposal(
    ctx: &TestContext,
) -> TestResult<sinex_primitives::events::Event<JsonValue>> {
    let material_id = ctx
        .create_source_material(Some("curation-handler-test"))
        .await?;
    let parent = DynamicPayload::new(
        "curation.handler.test",
        "curation.handler.fixture",
        json!({ "fixture": true }),
    )
    .from_material(material_id)
    .build()?;
    let parent = ctx.pool().events().insert(parent).await?;
    let parent_id = parent
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("published parent missing id"))?;
    let proposal = CurationProposalPayload::test_fixture_tag();
    let mut event = proposal.from_parents([parent_id])?.build()?;
    seed_curation_proposal_declaration(ctx.pool()).await?;
    apply_curation_proposal_product_metadata(&mut event);
    Ok(ctx.pool().events().insert(event).await?)
}

async fn insert_duplicate_candidate(
    ctx: &TestContext,
    equivalence_key: &str,
    material_label: &str,
) -> TestResult<sinex_primitives::Uuid> {
    let material_id = ctx
        .create_source_material(Some(&format!("duplicate-candidate-{material_label}")))
        .await?;
    let mut event = DynamicPayload::new(
        "webhistory",
        "page.visited",
        json!({
            "url": format!("https://example.test/{equivalence_key}"),
        }),
    )
    .from_material(material_id)
    .build()?;
    event.equivalence_key = Some(equivalence_key.to_string());
    let inserted = ctx.pool().events().insert(event).await?;
    let id = inserted
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("duplicate candidate missing id"))?;
    Ok(id.to_uuid())
}

async fn insert_replayed_fixture_proposal(
    ctx: &TestContext,
) -> TestResult<sinex_primitives::events::Event<JsonValue>> {
    let material_id = ctx
        .create_source_material(Some("curation-handler-replayed-test"))
        .await?;
    let parent = DynamicPayload::new(
        "curation.handler.test",
        "curation.handler.replayed_fixture",
        json!({ "fixture": true, "replayed": true }),
    )
    .from_material(material_id)
    .build()?;
    let parent = ctx.pool().events().insert(parent).await?;
    let parent_id = parent
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("published replay parent missing id"))?;
    let mut proposal = CurationProposalPayload::test_fixture_tag();
    proposal.proposal_id = sinex_primitives::Uuid::from_u128(12);
    let mut event = proposal.from_parents([parent_id])?.build()?;
    seed_curation_proposal_declaration(ctx.pool()).await?;
    apply_curation_proposal_product_metadata(&mut event);
    Ok(ctx.pool().events().insert(event).await?)
}

/// `curation.proposal` events are derived (built via `from_parents`), so
/// they need a declared `product_class` to satisfy
/// `events_derived_requires_product_class` (sinex-0vx.4). NOTE: this only
/// covers the proposal fixtures this file builds directly -- the judgment /
/// duplicate-judgment / finalize RPC handlers (`handle_curation_*` in
/// `crate/sinexd/src/api/handlers/curation.rs`) build their OWN derived
/// events without ever setting `product_class`, which is a production-code
/// gap outside this test-fixture sweep's scope (sinex-egyf); tests that go
/// through those handlers still fail the same DB constraint downstream of
/// this fix. See PR description / bead notes for the tracked follow-up.
const CURATION_PROPOSAL_DECLARATION_ID: &str = "sinex.test.curation_handlers_proposal";

async fn seed_curation_proposal_declaration(pool: &sqlx::PgPool) -> TestResult<()> {
    common::seed_product_declaration(
        pool,
        CURATION_PROPOSAL_DECLARATION_ID,
        DerivedProductClass::CanonicalDerivedEvent,
        "curation",
        "curation.proposal",
    )
    .await
}

fn apply_curation_proposal_product_metadata<T>(event: &mut sinex_primitives::events::Event<T>) {
    event.product_class = Some(DerivedProductClass::CanonicalDerivedEvent);
    event.claim_support = Some(ClaimSupport::unknown());
    event.derivation_declaration_id = Some(CURATION_PROPOSAL_DECLARATION_ID.to_string());
}
