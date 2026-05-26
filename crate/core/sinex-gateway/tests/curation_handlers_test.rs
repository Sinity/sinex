use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{
    handle_curation_finalize, handle_curation_list_duplicate_candidates,
    handle_curation_list_proposals, handle_curation_record_duplicate_judgment,
    handle_curation_record_judgment,
};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::JsonValue;
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
use xtask::sandbox::prelude::*;

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
    assert_eq!(cluster.natural_key_hash, "visit-1");
    assert_eq!(cluster.event_count, 2);
    assert_eq!(cluster.material_count, 2);
    let listed_ids: Vec<_> = cluster.events.iter().map(|event| event.event_id).collect();
    assert!(listed_ids.contains(&candidate_a));
    assert!(listed_ids.contains(&candidate_b));
    Ok(())
}

#[sinex_test]
async fn curation_duplicate_judgment_records_proposal_over_candidate_set(
    ctx: TestContext,
) -> TestResult<()> {
    let candidate_a = insert_duplicate_candidate(&ctx, "visit-1", "material-a").await?;
    let candidate_b = insert_duplicate_candidate(&ctx, "visit-1", "material-b").await?;
    let auth = RpcAuthContext::system();

    let response = handle_curation_record_duplicate_judgment(
        ctx.pool(),
        CurationRecordDuplicateJudgmentRequest {
            source: "webhistory".to_string(),
            event_type: "page.visited".to_string(),
            natural_key_hash: "visit-1".to_string(),
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
async fn curation_finalize_persists_lineage_to_original_proposal_and_judgment(
    ctx: TestContext,
) -> TestResult<()> {
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
        .ok_or_else(|| color_eyre::eyre::eyre!("finalization event missing synthesis parents"))?;
    assert_eq!(parents, &[original_proposal_event_id, judgment_event_id]);
    assert!(!parents.contains(&replayed_proposal_event_id));
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
    let event = proposal.from_parents([parent_id])?.build()?;
    Ok(ctx.pool().events().insert(event).await?)
}

async fn insert_duplicate_candidate(
    ctx: &TestContext,
    natural_key_hash: &str,
    material_label: &str,
) -> TestResult<sinex_primitives::Uuid> {
    let material_id = ctx
        .create_source_material(Some(&format!("duplicate-candidate-{material_label}")))
        .await?;
    let event = DynamicPayload::new(
        "webhistory",
        "page.visited",
        json!({
            "natural_key_hash": natural_key_hash,
            "url": format!("https://example.test/{natural_key_hash}"),
        }),
    )
    .from_material(material_id)
    .build()?;
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
    let event = proposal.from_parents([parent_id])?.build()?;
    Ok(ctx.pool().events().insert(event).await?)
}
