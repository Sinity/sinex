use sinex_primitives::{
    DerivationOperationHook, FreshnessPolicy, InvalidationTrigger, OutputKind,
    TASK_CURRENT_OBJECTS_DERIVATION_ID, affected_derivations, derivations_for_output,
    find_derivation_spec,
    task_domain::{TASK_REDUCER_INPUT_EVENT_TYPES, TASK_REDUCER_SPEC},
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn task_projection_declares_derivation_contract() -> TestResult<()> {
    let spec = find_derivation_spec(TASK_CURRENT_OBJECTS_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing task derivation spec"))?;

    assert_eq!(spec.output_id, "domain.current_objects");
    assert_eq!(spec.output_kind, OutputKind::ProjectionRow);
    assert_eq!(spec.output_kind, TASK_REDUCER_SPEC.output_kind);
    assert_eq!(spec.freshness_policy, FreshnessPolicy::RebuildOnInputChange);
    assert!(
        spec.operation_hooks
            .contains(&DerivationOperationHook::Rebuild)
    );
    assert!(
        spec.operation_hooks
            .contains(&DerivationOperationHook::Explain)
    );
    Ok(())
}

#[sinex_test]
async fn derivation_contract_keeps_input_scope_with_projection_spec() -> TestResult<()> {
    let spec = find_derivation_spec(TASK_CURRENT_OBJECTS_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing task derivation spec"))?;

    match spec.input_scope {
        sinex_primitives::DerivationInputScope::EventTypes {
            domain_id,
            event_types,
        } => {
            assert_eq!(domain_id, TASK_REDUCER_SPEC.domain_id);
            assert_eq!(event_types, TASK_REDUCER_INPUT_EVENT_TYPES);
        }
        other => panic!("task derivation should use event input scope, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn invalidation_planning_reports_affected_derivations() -> TestResult<()> {
    let replay_ids: Vec<_> = affected_derivations(InvalidationTrigger::Replay)
        .map(|spec| spec.id)
        .collect();
    assert!(replay_ids.contains(&TASK_CURRENT_OBJECTS_DERIVATION_ID));

    let redaction_ids: Vec<_> = affected_derivations(InvalidationTrigger::Redaction)
        .map(|spec| spec.id)
        .collect();
    assert!(redaction_ids.contains(&TASK_CURRENT_OBJECTS_DERIVATION_ID));

    let output_ids: Vec<_> = derivations_for_output("domain.current_objects")
        .map(|spec| spec.id)
        .collect();
    assert_eq!(output_ids, vec![TASK_CURRENT_OBJECTS_DERIVATION_ID]);
    Ok(())
}
