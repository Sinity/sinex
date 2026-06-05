//! Instruction expectation reconciler.
//!
//! This automaton closes local desired-state loops by comparing admitted
//! instruction events with ordinary observation events. The first slice handles
//! Hyprland workspace-switch instructions and `wm.hyprland/workspace.switched`
//! observations.

use crate::runtime::automaton::{AutomatonContext, DerivedOutput, ScopeReconcilerAdapter};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, ScopeReconciler};
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    DesktopWorkspaceSwitchInstructionPayload, HyprlandWorkspaceSwitchedPayload,
    InstructionExpectationStatus, InstructionExpectationStatusPayload,
    evaluate_hyprland_workspace_expectation,
};
use sinex_primitives::proof::{
    CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    RetentionPolicy as ContractRetentionPolicy, RuntimeShape as ContractRuntimeShape,
    SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{
    JsonValue, Timestamp, Uuid, register_source_contract, register_source_runtime_binding,
};

const HYPRLAND_WORKSPACE_SCOPE: &str = "desktop.hyprland.workspace";
const SEMANTICS_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstructionExpectationState {
    pending_hyprland_workspace: Vec<PendingWorkspaceInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingWorkspaceInstruction {
    instruction_event_id: Uuid,
    instruction: DesktopWorkspaceSwitchInstructionPayload,
}

#[derive(Debug, Clone, Default)]
pub struct InstructionExpectationReconciler;

impl ScopeReconciler for InstructionExpectationReconciler {
    type State = InstructionExpectationState;
    type Input = JsonValue;
    type Output = InstructionExpectationStatusPayload;

    fn name(&self) -> &'static str {
        "instruction-expectation-reconciler"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn output_event_type(&self) -> &'static str {
        InstructionExpectationStatusPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        InstructionExpectationStatusPayload::SOURCE.as_static_str()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }

    fn scope_keys(&self, _input: &Self::Input, context: &AutomatonContext) -> Vec<String> {
        if is_hyprland_workspace_instruction(context) || is_hyprland_workspace_observation(context)
        {
            vec![HYPRLAND_WORKSPACE_SCOPE.to_string()]
        } else {
            Vec::new()
        }
    }

    async fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        if scope_key != HYPRLAND_WORKSPACE_SCOPE {
            return Err(AutomatonLogicError::InputParsing(format!(
                "instruction expectation scope key '{scope_key}' is not supported"
            )));
        }

        if is_hyprland_workspace_instruction(context) {
            return record_pending_instruction(state, input, context);
        }

        if is_hyprland_workspace_observation(context) {
            return reconcile_workspace_observation(state, input, context);
        }

        Ok(Vec::new())
    }
}

fn is_hyprland_workspace_instruction(context: &AutomatonContext) -> bool {
    context.source.as_str() == DesktopWorkspaceSwitchInstructionPayload::SOURCE.as_str()
        && context.event_type.as_str()
            == DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_str()
}

fn is_hyprland_workspace_observation(context: &AutomatonContext) -> bool {
    context.source.as_str() == HyprlandWorkspaceSwitchedPayload::SOURCE.as_str()
        && context.event_type.as_str() == HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_str()
}

fn record_pending_instruction(
    state: &mut InstructionExpectationState,
    input: JsonValue,
    context: &AutomatonContext,
) -> Result<Vec<DerivedOutput<InstructionExpectationStatusPayload>>, AutomatonLogicError> {
    let instruction: DesktopWorkspaceSwitchInstructionPayload = serde_json::from_value(input)
        .map_err(|error| {
            AutomatonLogicError::InputParsing(format!(
                "failed to parse Hyprland workspace instruction: {error}"
            ))
        })?;

    if !instruction.dry_run {
        state
            .pending_hyprland_workspace
            .push(PendingWorkspaceInstruction {
                instruction_event_id: context.trigger_uuid(),
                instruction,
            });
    }

    Ok(Vec::new())
}

fn reconcile_workspace_observation(
    state: &mut InstructionExpectationState,
    input: JsonValue,
    context: &AutomatonContext,
) -> Result<Vec<DerivedOutput<InstructionExpectationStatusPayload>>, AutomatonLogicError> {
    if state.pending_hyprland_workspace.is_empty() {
        return Ok(Vec::new());
    }

    let observed_at = context.require_ts_orig()?;
    let observation: HyprlandWorkspaceSwitchedPayload =
        serde_json::from_value(input).map_err(|error| {
            AutomatonLogicError::InputParsing(format!(
                "failed to parse Hyprland workspace observation: {error}"
            ))
        })?;

    let observation_event_id = context.trigger_uuid();
    let pending = std::mem::take(&mut state.pending_hyprland_workspace);
    let outputs = pending
        .into_iter()
        .map(|pending| {
            let payload = evaluate_pending_workspace_instruction(
                &pending.instruction,
                observation.to_workspace_id,
                observation_event_id,
                observed_at,
            );
            DerivedOutput::reconciled(
                payload,
                observed_at,
                vec![pending.instruction_event_id, observation_event_id],
                HYPRLAND_WORKSPACE_SCOPE.to_string(),
            )
            .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
            .with_semantics_version(SEMANTICS_VERSION)
            .with_equivalence_key(format!(
                "hyprland-workspace-expectation:{}",
                pending.instruction.instruction_id
            ))
        })
        .collect();

    Ok(outputs)
}

fn evaluate_pending_workspace_instruction(
    instruction: &DesktopWorkspaceSwitchInstructionPayload,
    observed_workspace_id: i32,
    observation_event_id: Uuid,
    observed_at: Timestamp,
) -> InstructionExpectationStatusPayload {
    if instruction
        .deadline
        .is_some_and(|deadline| observed_at > deadline)
    {
        return InstructionExpectationStatusPayload {
            instruction_id: instruction.instruction_id,
            desired_event_source: instruction.desired_event_source.clone(),
            desired_event_type: instruction.desired_event_type.clone(),
            status: InstructionExpectationStatus::TimedOut,
            matched_event_ids: vec![observation_event_id],
            caveat: Some("workspace observation arrived after instruction deadline".to_string()),
            evaluated_at: observed_at,
        };
    }

    evaluate_hyprland_workspace_expectation(
        instruction,
        observed_workspace_id,
        observation_event_id,
        observed_at,
    )
}

pub type InstructionExpectationReconcilerRuntime =
    ScopeReconcilerAdapter<InstructionExpectationReconciler>;

register_source_contract! {
    SourceContract {
        id: "instruction-expectation-reconciler",
        namespace: "derived",
        event_types: &[
            ("runtime.instruction", "expectation.status"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(instruction_id, desired_event_source, desired_event_type)",
        ),
        access_policy: "event_stream_read",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:instruction-expectation-reconciler"),
        "instruction-expectation-reconciler",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("expectation.status")
    .privacy_context("metadata")
    .material_policy("derived_parents")
    .checkpoint_policy("append_stream")
    .resource_shape("event_stream_consumer")
    .source_id("instruction-expectation-reconciler")
    .runner_pack("sinexd")
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("in_process:sinexd")
    .build_impact(sinex_primitives::proof::SourceBuildImpact::ZERO)
    .build()
}
