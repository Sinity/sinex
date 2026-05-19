use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{handle_curation_list_proposals, handle_curation_record_judgment};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::JsonValue;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::events::payloads::{
    CurationJudgmentActorKind, CurationJudgmentDecision, CurationProposalPayload,
};
use sinex_primitives::events::{EventPayload, payloads::CurationJudgmentPayload};
use sinex_primitives::query::EventQueryResult;
use sinex_primitives::rpc::curation::{
    CurationListProposalsRequest, CurationRecordJudgmentRequest,
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
            .map(|parents| parents.len()),
        Some(1)
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
    let event = proposal.from_parents([parent_id])?.build()?;
    Ok(ctx.pool().events().insert(event).await?)
}
