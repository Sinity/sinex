use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::payloads::{
    DesktopWorkspaceSwitchInstructionPayload, HyprlandWorkspaceSwitchedPayload,
    InstructionExpectationStatus,
};
use sinex_primitives::events::{Event, EventPayload};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{Id, JsonValue, Uuid};
use sinexd::automata::instruction_reconciler::{
    InstructionExpectationReconciler, InstructionExpectationState,
};
use sinexd::runtime::ScopeReconciler;
use sinexd::runtime::automaton::AutomatonContext;
use xtask::sandbox::prelude::*;

fn context(source: &str, event_type: &str, ts_orig: Timestamp) -> AutomatonContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id: event_id,
        source: source.into(),
        event_type: event_type.into(),
        ts_orig: Some(ts_orig),
        ts_coided: event_id.timestamp().expect("test ID must be UUIDv7"),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

fn instruction(
    desired_workspace_id: i32,
    deadline: Option<Timestamp>,
    dry_run: bool,
) -> Result<DesktopWorkspaceSwitchInstructionPayload, sinex_primitives::SinexError> {
    DesktopWorkspaceSwitchInstructionPayload::hyprland_operator_direct(
        Uuid::now_v7(),
        desired_workspace_id,
        "operator",
        deadline,
        dry_run,
    )
}

fn observation(to_workspace_id: i32) -> HyprlandWorkspaceSwitchedPayload {
    HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: Some(1),
        to_workspace_id,
        workspace_name: None,
        monitor_id: Some(0),
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
            serde_json::to_value(instruction(4, None, false)?)?,
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
            serde_json::to_value(instruction(4, None, false)?)?,
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
            serde_json::to_value(instruction(4, Some(deadline), false)?)?,
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

/// sinex-audit-instructionreconciler: two concurrent pending instructions
/// targeting DIFFERENT workspaces. Only the observation matching the FIRST
/// instruction's target ever arrives. The bug drained and evaluated ALL
/// pending instructions against whichever single observation arrived first,
/// so the second (unrelated) instruction was falsely marked `Contradicted`.
///
/// Anti-vacuity: reverting the per-candidate correlation fix in
/// `reconcile_workspace_observation` (back to unconditionally mapping every
/// pending instruction against the arriving observation) makes this test
/// fail — the second instruction would come back `Contradicted` in the same
/// output batch instead of staying pending.
#[sinex_test]
async fn concurrent_pending_instructions_are_not_cross_contaminated() -> TestResult<()> {
    let mut reconciler = InstructionExpectationReconciler;
    let mut state = InstructionExpectationState::default();
    let started_at = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let observed_at = started_at + Duration::seconds(2);

    let first_instruction_ctx = context(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        started_at,
    );
    reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(instruction(4, None, false)?)?,
            &first_instruction_ctx,
        )
        .await?;

    let second_instruction_ctx = context(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        started_at,
    );
    let second_outputs = reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(instruction(7, None, false)?)?,
            &second_instruction_ctx,
        )
        .await?;
    assert!(second_outputs.is_empty());
    assert_eq!(state.pending_hyprland_workspace_len(), 2);

    // Only the observation matching the FIRST instruction's target (4) ever
    // arrives. The second instruction (target 7) has no matching observation
    // yet -- it must stay pending, not get resolved by this unrelated one.
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
        InstructionExpectationStatus::Fulfilled
    );

    // The second instruction is still pending -- not falsely Contradicted.
    assert_eq!(state.pending_hyprland_workspace_len(), 1);

    // Now the second instruction's own matching observation arrives.
    let second_observation_ctx = context(
        HyprlandWorkspaceSwitchedPayload::SOURCE.as_str(),
        HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_str(),
        observed_at + Duration::seconds(1),
    );
    let second_observation_outputs = reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(observation(7))?,
            &second_observation_ctx,
        )
        .await?;

    assert_eq!(second_observation_outputs.len(), 1);
    assert_eq!(
        second_observation_outputs[0].payload.status,
        InstructionExpectationStatus::Fulfilled
    );
    assert_eq!(state.pending_hyprland_workspace_len(), 0);
    Ok(())
}

/// sinex-audit-instructionreconciler: a pending instruction whose deadline
/// elapses with NO observation ever arriving (a stalled observation source)
/// must resolve `TimedOut` via the idle-flush path, not accumulate forever.
#[sinex_test]
async fn idle_flush_times_out_pending_instruction_with_no_observation() -> TestResult<()> {
    let mut reconciler = InstructionExpectationReconciler;
    let mut state = InstructionExpectationState::default();
    let started_at = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let deadline = started_at + Duration::seconds(1);
    let flush_at = started_at + Duration::seconds(10);

    let instruction_ctx = context(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        started_at,
    );
    reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(instruction(4, Some(deadline), false)?)?,
            &instruction_ctx,
        )
        .await?;
    assert_eq!(state.pending_hyprland_workspace_len(), 1);

    // No observation arrives before the deadline: flush_due must go true, and
    // flush() must resolve the instruction without needing a triggering
    // observation event.
    assert!(!reconciler.flush_due(&state, started_at));
    assert!(reconciler.flush_due(&state, flush_at));

    let outputs = reconciler.flush(&mut state, flush_at).await?;
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0].payload.status,
        InstructionExpectationStatus::TimedOut
    );
    assert!(outputs[0].payload.matched_event_ids.is_empty());
    assert_eq!(state.pending_hyprland_workspace_len(), 0);
    assert!(!reconciler.flush_due(&state, flush_at));
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-hj66 open: idle-flush claim_support hardcodes the same evidence_event_count as the \
            observation-match path even though the idle-flush output has one parent event and zero \
            matched observations, fabricating evidentiary weight the output doesn't have"]
async fn idle_flush_claim_support_reports_the_real_evidence_count_not_the_matched_path_count()
-> TestResult<()> {
    let mut reconciler = InstructionExpectationReconciler;
    let mut state = InstructionExpectationState::default();
    let started_at = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let deadline = started_at + Duration::seconds(1);
    let flush_at = started_at + Duration::seconds(10);

    let instruction_ctx = context(
        DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str(),
        DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str(),
        started_at,
    );
    reconciler
        .reconcile(
            &mut state,
            "desktop.hyprland.workspace",
            serde_json::to_value(instruction(4, Some(deadline), false)?)?,
            &instruction_ctx,
        )
        .await?;

    let outputs = reconciler.flush(&mut state, flush_at).await?;
    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];

    // The idle-flush path has exactly ONE parent event (the instruction
    // itself) and ZERO matched observation events -- claim_support must
    // reflect that, not the two-event count the observation-match path
    // (reconcile_workspace_observation) legitimately reports.
    assert_eq!(
        output.source_event_ids.len(),
        1,
        "sanity: idle-flush should carry exactly one parent (the instruction event)"
    );
    assert!(output.payload.matched_event_ids.is_empty());

    let claim_support = output
        .claim_support
        .as_ref()
        .expect("idle-flush output should carry claim_support");
    assert_eq!(
        claim_support.evidence_event_count(),
        1,
        "claim_support.evidence_event_count() reported {} but the idle-flush output only has \
         {} real parent event(s) and 0 matched observations -- this fabricates evidentiary \
         support the output doesn't actually have",
        claim_support.evidence_event_count(),
        output.source_event_ids.len(),
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
            serde_json::to_value(instruction(4, None, true)?)?,
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
