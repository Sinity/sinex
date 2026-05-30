use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    ActuationStatus, DesktopWorkspaceSwitchInstructionPayload, InstructionExpectationStatus,
    InstructionExpectationStatusPayload, evaluate_hyprland_workspace_expectation,
    plan_hyprland_workspace_switch,
};
use sinex_primitives::{Timestamp, Uuid};
use xtask::sandbox::prelude::*;

fn workspace_instruction(
    dry_run: bool,
) -> Result<DesktopWorkspaceSwitchInstructionPayload, sinex_primitives::SinexError> {
    DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
        Uuid::from_u128(1),
        3,
        "operator",
        Some(Timestamp::UNIX_EPOCH),
        dry_run,
    )
}

#[sinex_test]
async fn instruction_payloads_publish_stable_event_names() -> TestResult<()> {
    assert_eq!(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        "runtime.instruction"
    );
    assert_eq!(
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        "desktop.workspace.switch_requested"
    );
    assert_eq!(
        InstructionExpectationStatusPayload::EVENT_TYPE.as_str(),
        "expectation.status"
    );
    Ok(())
}

#[sinex_test]
async fn hyprland_workspace_instruction_uses_canonical_idempotency_key() -> TestResult<()> {
    let instruction = workspace_instruction(false)?;

    assert_eq!(instruction.desired_event_source, "wm.hyprland");
    assert_eq!(instruction.desired_event_type, "workspace.switched");
    assert_eq!(instruction.idempotency_key, "desktop.hyprland.workspace:3");
    Ok(())
}

#[sinex_test]
async fn hyprland_planner_noops_when_workspace_already_satisfied() -> TestResult<()> {
    let instruction = workspace_instruction(false)?;
    let attempt =
        plan_hyprland_workspace_switch(&instruction, Some(3), true, Timestamp::UNIX_EPOCH);

    assert_eq!(attempt.status, ActuationStatus::NoopAlreadySatisfied);
    assert!(attempt.command_summary.command.is_none());
    assert!(attempt.error.is_none());
    Ok(())
}

#[sinex_test]
async fn hyprland_planner_caveats_when_observation_is_unavailable() -> TestResult<()> {
    let instruction = workspace_instruction(false)?;
    let attempt = plan_hyprland_workspace_switch(&instruction, None, false, Timestamp::UNIX_EPOCH);

    assert_eq!(attempt.status, ActuationStatus::Unavailable);
    assert!(attempt.command_summary.command.is_none());
    assert_eq!(
        attempt.error.as_deref(),
        Some("desktop.window-manager observation is not ready")
    );
    Ok(())
}

#[sinex_test]
async fn hyprland_planner_emits_typed_command_socket_message() -> TestResult<()> {
    let instruction = workspace_instruction(false)?;
    let attempt =
        plan_hyprland_workspace_switch(&instruction, Some(2), true, Timestamp::UNIX_EPOCH);

    let command = attempt
        .command_summary
        .command
        .expect("non-satisfied workspace switch should produce command");
    assert_eq!(attempt.status, ActuationStatus::Attempted);
    assert_eq!(command.command_socket_message(), "dispatch workspace 3");
    Ok(())
}

#[sinex_test]
async fn hyprland_planner_keeps_dry_run_non_executing() -> TestResult<()> {
    let instruction = workspace_instruction(true)?;
    let attempt =
        plan_hyprland_workspace_switch(&instruction, Some(2), true, Timestamp::UNIX_EPOCH);

    assert_eq!(attempt.status, ActuationStatus::DryRun);
    assert!(attempt.command_summary.command.is_some());
    Ok(())
}

#[sinex_test]
async fn hyprland_expectation_matches_only_observed_workspace() -> TestResult<()> {
    let instruction = workspace_instruction(false)?;
    let fulfilled = evaluate_hyprland_workspace_expectation(
        &instruction,
        3,
        Uuid::from_u128(9),
        Timestamp::UNIX_EPOCH,
    );
    let contradicted = evaluate_hyprland_workspace_expectation(
        &instruction,
        4,
        Uuid::from_u128(10),
        Timestamp::UNIX_EPOCH,
    );

    assert_eq!(fulfilled.status, InstructionExpectationStatus::Fulfilled);
    assert_eq!(
        contradicted.status,
        InstructionExpectationStatus::Contradicted
    );
    assert_eq!(fulfilled.matched_event_ids, vec![Uuid::from_u128(9)]);
    Ok(())
}
