use serde_json::json;
use sinex_primitives::Timestamp;
use sinex_primitives::Uuid;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    CurationFinalizedPayload, CurationJudgmentDecision, CurationJudgmentPayload,
    CurationProposalPayload,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn curation_payloads_publish_stable_event_names() -> TestResult<()> {
    assert_eq!(CurationProposalPayload::SOURCE.as_str(), "curation");
    assert_eq!(
        CurationProposalPayload::EVENT_TYPE.as_str(),
        "curation.proposal"
    );
    assert_eq!(
        CurationJudgmentPayload::EVENT_TYPE.as_str(),
        "curation.judgment"
    );
    assert_eq!(
        CurationFinalizedPayload::EVENT_TYPE.as_str(),
        "curation.finalized"
    );
    Ok(())
}

#[sinex_test]
async fn accept_judgment_finalizes_candidate_payload() -> TestResult<()> {
    let proposal = CurationProposalPayload::test_fixture_tag();
    let judgment = CurationJudgmentPayload::test_accept(proposal.proposal_id);

    let finalized = CurationFinalizedPayload::from_judgment(
        Uuid::from_u128(9),
        &proposal,
        &judgment,
        Timestamp::UNIX_EPOCH,
    )?;

    assert_eq!(finalized.proposal_id, proposal.proposal_id);
    assert_eq!(finalized.judgment_id, judgment.judgment_id);
    assert_eq!(finalized.output_source, "knowledge-graph");
    assert_eq!(finalized.output_event_type, "knowledge.tag_applied");
    assert_eq!(finalized.output_payload, proposal.candidate_payload);
    Ok(())
}

#[sinex_test]
async fn modify_judgment_finalizes_corrected_payload() -> TestResult<()> {
    let proposal = CurationProposalPayload::test_fixture_tag();
    let corrected = json!({
        "entity_id": "00000000-0000-0000-0000-000000000002",
        "tag_name": "accepted",
        "tag_source": "curation.fixture"
    });
    let mut judgment = CurationJudgmentPayload::test_accept(proposal.proposal_id);
    judgment.decision = CurationJudgmentDecision::Modify;
    judgment.corrected_payload = Some(corrected.clone());

    let finalized = CurationFinalizedPayload::from_judgment(
        Uuid::from_u128(10),
        &proposal,
        &judgment,
        Timestamp::UNIX_EPOCH,
    )?;

    assert_eq!(finalized.output_payload, corrected);
    Ok(())
}

#[sinex_test]
async fn reject_judgment_cannot_finalize() -> TestResult<()> {
    let proposal = CurationProposalPayload::test_fixture_tag();
    let mut judgment = CurationJudgmentPayload::test_accept(proposal.proposal_id);
    judgment.decision = CurationJudgmentDecision::Reject;

    let error = CurationFinalizedPayload::from_judgment(
        Uuid::from_u128(11),
        &proposal,
        &judgment,
        Timestamp::UNIX_EPOCH,
    )
    .expect_err("reject judgments must not finalize");

    assert!(error.to_string().contains("only accept or modify"));
    Ok(())
}

#[sinex_test]
async fn replayed_identical_proposal_keeps_judgment_addressable() -> TestResult<()> {
    let original = CurationProposalPayload::test_fixture_tag();
    let judgment = CurationJudgmentPayload::test_accept(original.proposal_id);
    let mut replayed = original.clone();
    replayed.proposal_id = Uuid::from_u128(12);

    assert_eq!(replayed.proposal_key, original.proposal_key);
    assert_ne!(judgment.proposal_id, replayed.proposal_id);
    assert_eq!(judgment.proposal_id, original.proposal_id);
    Ok(())
}
