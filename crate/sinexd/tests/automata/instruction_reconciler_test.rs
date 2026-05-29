use sinexd::node_sdk::ScopeReconciler;
use sinexd::node_sdk::derived_node::AutomatonContext;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::payloads::{
    DesktopWorkspaceSwitchInstructionPayload, HyprlandWorkspaceSwitchedPayload,
    InstructionExpectationStatus,
};
use sinex_primitives::events::{Event, EventPayload};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{Id, JsonValue, Uuid};
use sinex_process::automata::instruction_reconciler::{
    InstructionExpectationReconciler, InstructionExpectationState,
};
use xtask::sandbox::prelude::*;

fn context(source: &str, event_type: &str, ts_orig: Timestamp) -> AutomatonContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id: event_id,
        source: source.into(),
        event_type: event_type.into(),
        ts_orig: Some(ts_orig),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn instruction(
    desired_workspace_id: i32,
    deadline: Option<Timestamp>,
    dry_run: bool,
) -> DesktopWorkspaceSwitchInstructionPayload {
    DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
        Uuid::now_v7(),
        desired_workspace_id,
        "operator",
        deadline,
        dry_run,
    )
    .expect("valid instruction")
}

fn observation(to_workspace_id: i32) -> HyprlandWorkspaceSwitchedPayload {
    HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: 1,
        to_workspace_id,
        monitor_id: 0,
        active_window_id: None,
    }
}

#[sinex_test]
async fn fulfilled_workspace_observation_emits_expectation_status() -> TestResult<()> {
    let mut reconciler = InstructionExpectationReconciler;
    let mut state = InstructionExpectationState::default();
    let started_at = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let observed_at = started_at + Duration::seconds(2);

    let instruction_ctx = context(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        started_at,
    );
    let outputs = reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(instruction(4, None, false))?,
            &instruction_ctx,
        )
        .await?;
    assert!(outputs.is_empty());

    let observation_ctx = context(
        HyprlandWorkspaceSwitchedPayload::SOURCE.as_str(),
        HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_str(),
        observed_at,
    );
    let outputs = reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(observation(4))?,
            &observation_ctx,
        )
        .await?;

    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(
        output.payload.status,
        InstructionExpectationStatus::Fulfilled
    );
    assert_eq!(
        output.payload.matched_event_ids,
        vec![observation_ctx.trigger_uuid()]
    );
    assert_eq!(output.ts_orig, observed_at);
    assert_eq!(
        output.source_event_ids,
        vec![
            instruction_ctx.trigger_uuid(),
            observation_ctx.trigger_uuid()
        ]
    );
    let expected_key = format!(
        "hyprland-workspace-expectation:{}",
        output.payload.instruction_id
    );
    assert_eq!(
        output.equivalence_key.as_deref(),
        Some(expected_key.as_str())
    );
    Ok(())
}

#[sinex_test]
async fn non_matching_first_workspace_observation_contradicts_instruction() -> TestResult<()> {
    let mut reconciler = InstructionExpectationReconciler;
    let mut state = InstructionExpectationState::default();
    let started_at = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let observed_at = started_at + Duration::seconds(2);

    let instruction_ctx = context(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        started_at,
    );
    reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(instruction(4, None, false))?,
            &instruction_ctx,
        )
        .await?;

    let observation_ctx = context(
        HyprlandWorkspaceSwitchedPayload::SOURCE.as_str(),
        HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_str(),
        observed_at,
    );
    let outputs = reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(observation(3))?,
            &observation_ctx,
        )
        .await?;

    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0].payload.status,
        InstructionExpectationStatus::Contradicted
    );
    Ok(())
}

#[sinex_test]
async fn late_workspace_observation_times_out_instruction() -> TestResult<()> {
    let mut reconciler = InstructionExpectationReconciler;
    let mut state = InstructionExpectationState::default();
    let started_at = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let deadline = started_at + Duration::seconds(1);
    let observed_at = started_at + Duration::seconds(5);

    let instruction_ctx = context(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        started_at,
    );
    reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(instruction(4, Some(deadline), false))?,
            &instruction_ctx,
        )
        .await?;

    let observation_ctx = context(
        HyprlandWorkspaceSwitchedPayload::SOURCE.as_str(),
        HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_str(),
        observed_at,
    );
    let outputs = reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(observation(4))?,
            &observation_ctx,
        )
        .await?;

    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0].payload.status,
        InstructionExpectationStatus::TimedOut
    );
    assert!(
        outputs[0]
            .payload
            .caveat
            .as_deref()
            .is_some_and(|caveat| caveat.contains("after instruction deadline"))
    );
    Ok(())
}

#[sinex_test]
async fn dry_run_instruction_does_not_wait_for_observation() -> TestResult<()> {
    let mut reconciler = InstructionExpectationReconciler;
    let mut state = InstructionExpectationState::default();
    let started_at = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let observed_at = started_at + Duration::seconds(2);

    let instruction_ctx = context(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        started_at,
    );
    reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(instruction(4, None, true))?,
            &instruction_ctx,
        )
        .await?;

    let observation_ctx = context(
        HyprlandWorkspaceSwitchedPayload::SOURCE.as_str(),
        HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_str(),
        observed_at,
    );
    let outputs = reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(observation(4))?,
            &observation_ctx,
        )
        .await?;

    assert!(outputs.is_empty());
    Ok(())
}
