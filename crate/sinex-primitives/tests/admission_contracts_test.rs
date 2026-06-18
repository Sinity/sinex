use sinex_primitives::{
    AdmissionOutcome, AdmissionOutcomeReason, AdmissionPolicyScope, EventOccurrenceContract,
    EventSource, EventType, OutputKind, ProposalKind, STANDARD_EVENT_ADMISSION_POLICY_ID,
    event_contracts::{
        SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID, find_event_contract,
        find_event_contract_for_pair,
    },
    find_admission_policy,
    source_contracts::OccurrenceIdentity,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn event_contract_registry_uses_contract_id_as_semantic_coordinate() -> TestResult<()> {
    let contract = find_event_contract(SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID)
        .expect("shell history command contract should be registered");

    assert_eq!(contract.id, SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID);
    assert_eq!(contract.event_source, "shell.history");
    assert_eq!(contract.event_type, "command.imported");
    assert_eq!(contract.output_kind, OutputKind::CanonicalEvent);
    assert_eq!(contract.occurrence, EventOccurrenceContract::SourceDeclared);
    assert_eq!(
        contract.source_occurrences,
        &[OccurrenceIdentity::Anchor, OccurrenceIdentity::Natural]
    );
    assert!(contract.is_canonical_event());
    assert_eq!(
        contract.admission_policy_ref,
        Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
    );

    let by_pair = find_event_contract_for_pair(
        &EventSource::from_static("shell.history"),
        &EventType::from_static("command.imported"),
    )
    .expect("source/type pair should resolve to the contract");
    assert_eq!(by_pair.id, contract.id);

    let source_only = find_event_contract_for_pair(
        &EventSource::from_static("shell.history"),
        &EventType::from_static("different.event"),
    );
    assert!(
        source_only.is_none(),
        "source namespace alone must not be event-contract authority"
    );
    Ok(())
}

#[sinex_test]
async fn admission_policy_registry_references_event_contracts() -> TestResult<()> {
    let policy = find_admission_policy(STANDARD_EVENT_ADMISSION_POLICY_ID)
        .expect("standard admission policy should be registered");

    assert_eq!(policy.id, STANDARD_EVENT_ADMISSION_POLICY_ID);
    assert_eq!(policy.scope, AdmissionPolicyScope::GlobalDefault);
    assert_eq!(
        policy.accepted_event_contracts,
        &[SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID]
    );
    assert_eq!(
        policy.disclosure_policy_ref,
        Some("operator.default-disclosure")
    );
    Ok(())
}

#[sinex_test]
async fn admission_outcome_vocabulary_covers_success_failure_and_proposal() -> TestResult<()> {
    let admitted = AdmissionOutcome::Admitted {
        policy_id: STANDARD_EVENT_ADMISSION_POLICY_ID.to_string(),
        event_contract_id: Some(SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID.to_string()),
        event_ids: vec![],
    };
    assert!(admitted.is_admitted());
    assert_eq!(admitted.policy_id(), STANDARD_EVENT_ADMISSION_POLICY_ID);

    let rejected = AdmissionOutcome::Rejected {
        policy_id: STANDARD_EVENT_ADMISSION_POLICY_ID.to_string(),
        reason: AdmissionOutcomeReason::new("schema_validation_failed", "payload schema rejected"),
        refs: vec![],
    };
    assert!(!rejected.is_admitted());

    let quarantined = AdmissionOutcome::Quarantined {
        policy_id: STANDARD_EVENT_ADMISSION_POLICY_ID.to_string(),
        reason: AdmissionOutcomeReason::new("malformed_material", "material could not be parsed")
            .with_policy_owner("operator.default-disclosure"),
        refs: vec![],
    };
    assert_eq!(quarantined.policy_id(), STANDARD_EVENT_ADMISSION_POLICY_ID);

    let proposed = AdmissionOutcome::Proposed {
        policy_id: STANDARD_EVENT_ADMISSION_POLICY_ID.to_string(),
        proposal_id: "proposal:duplicate:1".to_string(),
        proposal_kind: ProposalKind::DuplicateCandidate,
        refs: vec![],
    };
    assert!(proposed.creates_proposal());

    for outcome in [admitted, rejected, quarantined, proposed] {
        let json = serde_json::to_string(&outcome)?;
        let roundtrip: AdmissionOutcome = serde_json::from_str(&json)?;
        assert_eq!(roundtrip.policy_id(), STANDARD_EVENT_ADMISSION_POLICY_ID);
    }
    Ok(())
}
