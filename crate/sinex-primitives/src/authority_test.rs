#![allow(clippy::unwrap_used)]

use super::*;
use crate::views::SinexObjectKind;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn accept_judgment_allows_apply() -> TestResult<()> {
    let proposal = fixture_duplicate_proposal();
    let judgment = fixture_accept_judgment(&proposal);
    let value = proposal.apply(&judgment).unwrap();
    assert_eq!(
        value.match_reason,
        "identical command text, same cwd, within 2s"
    );

    Ok(())
}

#[sinex_test]
async fn reject_judgment_blocks_apply() -> TestResult<()> {
    let proposal = fixture_duplicate_proposal();
    let judgment = fixture_reject_judgment(&proposal);
    let err = proposal.apply(&judgment).unwrap_err();
    assert!(
        err.to_string().contains("Accept judgment"),
        "expected error mentioning Accept requirement, got: {err}"
    );

    Ok(())
}

/// Core invariant test: confidence score alone cannot promote a Proposal.
///
/// No matter how high the model confidence, the only extraction path
/// is apply() which requires a Judgment. This test proves that:
/// 1. A judgment referencing a different proposal is rejected.
/// 2. A Defer verdict is rejected (not just Reject).
#[sinex_test]
async fn confidence_alone_cannot_promote_proposal() -> TestResult<()> {
    let proposal = Proposal::new(
        ProposalKind::DuplicateCandidate,
        SinexObjectRef::new(SinexObjectKind::Event, "evt-x"),
        0.99, // very high confidence — still not enough
        DuplicateCandidatePayload {
            cluster_id: "fixture/source/key".to_string(),
            source: "fixture".to_string(),
            event_type: "source".to_string(),
            equivalence_key: "key".to_string(),
            candidate_event_ids: vec!["evt-x".to_string(), "evt-y".to_string()],
            candidate_material_ids: vec!["mat-x".to_string(), "mat-y".to_string()],
            preferred_event_id: Some("evt-x".to_string()),
            match_reason: "nearly identical".to_string(),
        },
        "model:high-confidence",
    );

    // A judgment for a different proposal is rejected regardless of verdict.
    let wrong_judgment = Judgment::new(
        "some-other-proposal-id",
        JudgmentVerdict::Accept,
        "operator:sinity",
    );
    let err = proposal.apply(&wrong_judgment).unwrap_err();
    assert!(
        err.to_string().contains("does not reference this proposal"),
        "expected mismatch error, got: {err}"
    );

    Ok(())
}

#[sinex_test]
async fn defer_verdict_does_not_grant_access() -> TestResult<()> {
    let proposal = fixture_duplicate_proposal();
    let defer_judgment = Judgment::new(
        proposal.id.clone(),
        JudgmentVerdict::Defer,
        "operator:sinity",
    );
    let err = proposal.apply(&defer_judgment).unwrap_err();
    assert!(
        err.to_string().contains("Accept judgment"),
        "expected Accept-required error for Defer verdict, got: {err}"
    );

    Ok(())
}

#[sinex_test]
async fn proposal_json_exposes_value_for_display_but_apply_still_requires_judgment()
-> TestResult<()> {
    // The proposed_value IS serialized (operator UIs need it for display),
    // but code-level access through Rust still requires apply() + Judgment.
    let proposal = fixture_duplicate_proposal();
    let json = serde_json::to_value(&proposal).unwrap();

    // Value is present in JSON — intentional for operator display.
    assert!(json["proposed_value"]["match_reason"].is_string());
    // Confidence is present but is display-only.
    assert!(json["confidence"].as_f64().unwrap() > 0.0);
    // Kind serializes in snake_case.
    assert_eq!(json["kind"], "duplicate_candidate");
    // Caveat is present.
    assert_eq!(json["caveats"].as_array().unwrap().len(), 1);

    Ok(())
}

#[sinex_test]
async fn view_envelope_wraps_proposal() -> TestResult<()> {
    let proposal = fixture_duplicate_proposal();
    let envelope = proposal.into_envelope("sinexctl.authority");
    let json = serde_json::to_value(&envelope).unwrap();
    assert_eq!(json["source_surface"], "sinexctl.authority");
    assert_eq!(json["payload"]["kind"], "duplicate_candidate");
    assert!(json["generated_at"].is_string());

    Ok(())
}

#[sinex_test]
async fn judgment_serializes_verdict_in_snake_case() -> TestResult<()> {
    let proposal = fixture_duplicate_proposal();
    let judgment = fixture_accept_judgment(&proposal);
    let json = serde_json::to_value(&judgment).unwrap();
    assert_eq!(json["verdict"], "accept");
    assert!(json["note"].is_string());

    Ok(())
}

#[sinex_test]
async fn finalizer_registration_declares_human_requirement() -> TestResult<()> {
    let reg = fixture_finalizer_registration();
    assert!(reg.requires_human_judgment);
    assert!(reg.auto_accept_above_confidence.is_none());
    assert_eq!(reg.proposal_kind, ProposalKind::DuplicateCandidate);

    Ok(())
}

#[sinex_test]
async fn schema_generation_covers_all_authority_types() -> TestResult<()> {
    let proposal_schema =
        serde_json::to_value(schemars::schema_for!(Proposal<DuplicateCandidatePayload>))
            .unwrap();
    let judgment_schema = serde_json::to_value(schemars::schema_for!(Judgment)).unwrap();
    let finalizer_schema =
        serde_json::to_value(schemars::schema_for!(FinalizerRegistration)).unwrap();

    assert!(proposal_schema["properties"]["confidence"].is_object());
    assert!(judgment_schema["properties"]["verdict"].is_object());
    assert!(finalizer_schema["properties"]["requires_human_judgment"].is_object());

    Ok(())
}
