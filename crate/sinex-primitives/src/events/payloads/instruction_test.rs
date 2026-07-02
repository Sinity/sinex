use super::*;
use crate::Timestamp;
use xtask::sandbox::prelude::sinex_test;

fn make_instruction(wid: i32) -> DesktopWorkspaceSwitchInstructionPayload {
    DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
        Uuid::now_v7(),
        wid,
        "test-operator",
        None,
        false,
    )
    .expect("valid instruction")
}

fn now() -> Timestamp {
    Timestamp::now()
}

#[sinex_test]
async fn plan_noop_when_already_at_desired() -> xtask::sandbox::TestResult<()> {
    let a = plan_hyprland_workspace_switch(&make_instruction(3), Some(3), true, now());
    assert_eq!(a.status, ActuationStatus::NoopAlreadySatisfied);
    assert!(a.command_summary.command.is_none());
    Ok(())
}

#[sinex_test]
async fn plan_unavailable_when_obs_not_ready() -> xtask::sandbox::TestResult<()> {
    let a = plan_hyprland_workspace_switch(&make_instruction(3), Some(1), false, now());
    assert_eq!(a.status, ActuationStatus::Unavailable);
    Ok(())
}

#[sinex_test]
async fn plan_attempted_when_different_workspace() -> xtask::sandbox::TestResult<()> {
    let a = plan_hyprland_workspace_switch(&make_instruction(5), Some(2), true, now());
    assert_eq!(a.status, ActuationStatus::Attempted);
    assert_eq!(a.command_summary.command.unwrap().workspace_id, 5);
    Ok(())
}

#[sinex_test]
async fn plan_dry_run_when_flag_set() -> xtask::sandbox::TestResult<()> {
    let mut i = make_instruction(4);
    i.dry_run = true;
    let a = plan_hyprland_workspace_switch(&i, Some(1), true, now());
    assert_eq!(a.status, ActuationStatus::DryRun);
    Ok(())
}

#[sinex_test]
async fn plan_rejects_invalid_workspace() -> xtask::sandbox::TestResult<()> {
    assert!(
        DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
            Uuid::now_v7(),
            0,
            "test",
            None,
            false
        )
        .is_err()
    );
    Ok(())
}

#[sinex_test]
async fn expectation_fulfilled_when_observed_matches() -> xtask::sandbox::TestResult<()> {
    let i = make_instruction(3);
    let eid = Uuid::now_v7();
    let s = evaluate_hyprland_workspace_expectation(&i, 3, eid, now());
    assert_eq!(s.status, InstructionExpectationStatus::Fulfilled);
    assert_eq!(s.matched_event_ids, vec![eid]);
    Ok(())
}

#[sinex_test]
async fn expectation_contradicted_when_observed_differs() -> xtask::sandbox::TestResult<()> {
    let s =
        evaluate_hyprland_workspace_expectation(&make_instruction(3), 7, Uuid::now_v7(), now());
    assert_eq!(s.status, InstructionExpectationStatus::Contradicted);
    Ok(())
}

#[sinex_test]
async fn expectation_preserves_instruction_id() -> xtask::sandbox::TestResult<()> {
    let i = make_instruction(2);
    let s = evaluate_hyprland_workspace_expectation(&i, 2, Uuid::now_v7(), now());
    assert_eq!(s.instruction_id, i.instruction_id);
    Ok(())
}

#[sinex_test]
async fn idempotency_key_deterministic() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        make_instruction(3).idempotency_key,
        make_instruction(3).idempotency_key
    );
    Ok(())
}

#[sinex_test]
async fn idempotency_key_differs_by_workspace() -> xtask::sandbox::TestResult<()> {
    assert_ne!(
        make_instruction(1).idempotency_key,
        make_instruction(2).idempotency_key
    );
    Ok(())
}

#[sinex_test]
async fn command_renders_dispatch() -> xtask::sandbox::TestResult<()> {
    let cmd = HyprlandWorkspaceCommand {
        dispatch: HyprlandDispatch::Workspace,
        workspace_id: 5,
    };
    assert_eq!(cmd.command_socket_message(), "dispatch workspace 5");
    Ok(())
}

#[sinex_test]
async fn command_rejects_shell_injection() -> xtask::sandbox::TestResult<()> {
    let cmd = HyprlandWorkspaceCommand {
        dispatch: HyprlandDispatch::Workspace,
        workspace_id: 1,
    };
    let r = cmd.command_socket_message();
    assert!(!r.contains(';') && !r.contains('|') && !r.contains('$'));
    Ok(())
}

#[sinex_test]
async fn operator_direct_has_correct_authority_and_target() -> xtask::sandbox::TestResult<()> {
    let i = make_instruction(1);
    assert_eq!(i.authority, InstructionAuthorityClass::OperatorDirect);
    assert_eq!(i.target, InstructionTarget::DesktopHyprlandWorkspace);
    Ok(())
}
