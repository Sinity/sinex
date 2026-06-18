use serde_json::json;
use sinex_primitives::authority::{Judgment, JudgmentVerdict, Proposal, ProposalKind};
use sinex_primitives::Timestamp;
use sinex_primitives::Uuid;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    CurationFinalizedPayload, CurationJudgmentDecision, CurationJudgmentPayload,
    CurationProposalPayload,
};
use sinex_primitives::views::{SinexObjectKind, SinexObjectRef};
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
async fn authority_backed_accept_uses_shared_proposal_gate() -> TestResult<()> {
    let (proposal, judgment) = authority_backed_curation_pair(JudgmentVerdict::Accept);

    let finalized = CurationFinalizedPayload::from_judgment(
        Uuid::from_u128(20),
        &proposal,
        &judgment,
        Timestamp::UNIX_EPOCH,
    )?;

    assert_eq!(finalized.output_payload, proposal.candidate_payload);
    assert_eq!(
        finalized.output_payload["cluster_id"].as_str(),
        Some("webhistory/page.visited/visit-1")
    );
    Ok(())
}

#[sinex_test]
async fn authority_backed_reject_and_defer_cannot_finalize() -> TestResult<()> {
    for verdict in [JudgmentVerdict::Reject, JudgmentVerdict::Defer] {
        let (proposal, judgment) = authority_backed_curation_pair(verdict);
        let error = CurationFinalizedPayload::from_judgment(
            Uuid::from_u128(21),
            &proposal,
            &judgment,
            Timestamp::UNIX_EPOCH,
        )
        .expect_err("non-accepted authority judgments must not finalize");

        assert!(
            error
                .to_string()
                .contains("only an Accept judgment may promote a proposal"),
            "unexpected error for {verdict:?}: {error:?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn authority_backed_wrong_proposal_judgment_cannot_finalize() -> TestResult<()> {
    let (proposal, mut judgment) = authority_backed_curation_pair(JudgmentVerdict::Accept);
    judgment.authority_judgment = Some(Judgment::new(
        "wrong-authority-proposal",
        JudgmentVerdict::Accept,
        "operator:sinity",
    ));

    let error = CurationFinalizedPayload::from_judgment(
        Uuid::from_u128(22),
        &proposal,
        &judgment,
        Timestamp::UNIX_EPOCH,
    )
    .expect_err("judgment for another shared proposal must not finalize");

    assert!(
        error
            .to_string()
            .contains("judgment does not reference this proposal")
    );
    Ok(())
}

#[sinex_test]
async fn confidence_without_authority_judgment_cannot_finalize() -> TestResult<()> {
    let (proposal, mut judgment) = authority_backed_curation_pair(JudgmentVerdict::Accept);
    judgment.authority_judgment = None;

    let error = CurationFinalizedPayload::from_judgment(
        Uuid::from_u128(23),
        &proposal,
        &judgment,
        Timestamp::UNIX_EPOCH,
    )
    .expect_err("confidence and a curation decision alone must not finalize");

    assert!(error.to_string().contains("shared authority judgment"));
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

fn authority_backed_curation_pair(
    verdict: JudgmentVerdict,
) -> (CurationProposalPayload, CurationJudgmentPayload) {
    let candidate_payload = json!({
        "cluster_id": "webhistory/page.visited/visit-1",
        "candidate_event_ids": [
            "00000000-0000-0000-0000-000000000101",
            "00000000-0000-0000-0000-000000000102"
        ],
        "preferred_event_id": "00000000-0000-0000-0000-000000000101"
    });
    let subject =
        SinexObjectRef::new(SinexObjectKind::Event, "duplicate:webhistory/page.visited/visit-1")
            .with_label("duplicate candidate cluster");
    let authority_proposal = Proposal::new(
        ProposalKind::DuplicateCandidate,
        subject,
        0.91,
        candidate_payload.clone(),
        "rule:test-duplicate",
    )
    .with_caveat(
        "authority.human_required",
        "duplicate finalization requires an explicit accept judgment",
    );
    let authority_judgment = Judgment::new(
        authority_proposal.id.clone(),
        verdict,
        "operator:sinity",
    );

    (
        CurationProposalPayload {
            proposal_id: Uuid::from_u128(30),
            proposal_key: "duplicate-resolution:webhistory/page.visited/visit-1".to_string(),
            proposal_kind: "curation.duplicate_resolution".to_string(),
            target_ref: None,
            candidate_source: "curation".to_string(),
            candidate_event_type: "curation.duplicate_resolution".to_string(),
            candidate_payload,
            authority_proposal: Some(authority_proposal),
            evidence_event_ids: vec![Uuid::from_u128(101), Uuid::from_u128(102)],
            evidence_material_ids: vec![Uuid::from_u128(201), Uuid::from_u128(202)],
            producer: "rule:test-duplicate".to_string(),
            confidence: 0.91,
            rationale: "test duplicate cluster".to_string(),
            status: sinex_primitives::events::payloads::CurationProposalStatus::Pending,
        },
        CurationJudgmentPayload {
            judgment_id: Uuid::from_u128(31),
            proposal_id: Uuid::from_u128(30),
            actor_kind: sinex_primitives::events::payloads::CurationJudgmentActorKind::TestFixture,
            actor_id: "operator:sinity".to_string(),
            decision: CurationJudgmentDecision::Accept,
            authority_judgment: Some(authority_judgment),
            corrected_payload: None,
            comment: Some("shared authority fixture".to_string()),
            judged_at: Timestamp::UNIX_EPOCH,
            authorization_context: None,
        },
    )
}
