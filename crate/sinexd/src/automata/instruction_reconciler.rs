//! Instruction expectation reconciler.
//!
//! This automaton closes local desired-state loops by comparing admitted
//! instruction events with ordinary observation events. The first slice handles
//! Hyprland workspace-switch instructions and `wm.hyprland/workspace.switched`
//! observations.

use crate::runtime::automaton::{AutomatonContext, DerivedOutput, ScopeReconcilerAdapter};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, ScopeReconciler};
use serde::{Deserialize, Serialize};
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    DesktopWorkspaceSwitchInstructionPayload, HyprlandWorkspaceSwitchedPayload,
    InstructionExpectationStatus, InstructionExpectationStatusPayload,
    evaluate_hyprland_workspace_expectation,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
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

impl InstructionExpectationState {
    /// Number of pending Hyprland workspace instructions awaiting a matching
    /// observation or idle-flush timeout. Test/observability accessor.
    #[must_use]
    pub fn pending_hyprland_workspace_len(&self) -> usize {
        self.pending_hyprland_workspace.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingWorkspaceInstruction {
    instruction_event_id: Uuid,
    instruction: DesktopWorkspaceSwitchInstructionPayload,
}

/// Derivation control-plane declaration for `instruction-reconciler`
/// (sinex-0vx.1/0vx.3).
///
/// `analysis_claim`: a comparison/evaluation of admitted instruction vs.
/// observation events, not a fact about the world in its own right.
pub const INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "instruction-reconciler.expectation.status",
        owner: "instruction-reconciler",
        product_class: DerivedProductClass::AnalysisClaim,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("runtime.instruction"),
        output_event_type: Some("expectation.status"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: SEMANTICS_VERSION,
        input_eligibility: InputEligibility::ExplicitOnly,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Direct,
            SourceCoverage::Partial,
            ClaimTemporalQuality::DeclaredEffective,
        ),
        verification_command: "xtask test -p sinexd -E 'test(instruction_reconciler)'",
    }];

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

    fn input_event_types(&self) -> Vec<&'static str> {
        vec![
            DesktopWorkspaceSwitchInstructionPayload::EVENT_TYPE.as_static_str(),
            HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_static_str(),
        ]
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

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS;

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

    /// A stalled `wm.hyprland/workspace.switched` observation source must not
    /// let pending instructions accumulate forever with no terminal status.
    /// Flush is due whenever any pending instruction's deadline has already
    /// elapsed as of `now` — mirroring the deadline check `reconcile()` itself
    /// applies to observations, but without needing an observation to trigger
    /// it.
    fn flush_due(&self, state: &Self::State, now: Timestamp) -> bool {
        state
            .pending_hyprland_workspace
            .iter()
            .any(|pending| pending.instruction.deadline.is_some_and(|deadline| now > deadline))
    }

    async fn flush(
        &mut self,
        state: &mut Self::State,
        now: Timestamp,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        Ok(flush_timed_out_workspace_instructions(state, now))
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

/// Reconcile pending instructions against ONE arriving workspace observation.
///
/// A `wm.hyprland/workspace.switched` observation does not carry any
/// dispatch-correlation id back to the instruction that produced it — the
/// only correlating data available is `desired_workspace_id` vs.
/// `to_workspace_id`. With multiple concurrent pending instructions targeting
/// *different* workspaces, an observation whose workspace doesn't match a
/// given pending instruction is not evidence about that instruction at all;
/// resolving it anyway (the original bug) falsely marks unrelated pending
/// instructions `Contradicted`.
///
/// A pending instruction is resolved against this observation only when:
/// - its deadline has already elapsed (deadline-elapsed is decided from
///   `observed_at` alone, independent of which workspace was observed), or
/// - its `desired_workspace_id` matches the observed workspace (direct
///   evidence of fulfillment), or
/// - it is the SOLE pending instruction for this scope, in which case the
///   observation is the only causal candidate and a workspace mismatch is
///   unambiguous evidence of contradiction.
///
/// Anything else is left untouched in `state` — it stays `Pending` until its
/// own matching observation arrives or `flush_due`/`flush` times it out.
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
    let sole_candidate = pending.len() == 1;

    let mut outputs = Vec::new();
    let mut still_pending = Vec::new();

    for candidate in pending {
        let deadline_elapsed = candidate
            .instruction
            .deadline
            .is_some_and(|deadline| observed_at > deadline);
        let target_matches = candidate.instruction.desired_workspace_id == observation.to_workspace_id;

        if !deadline_elapsed && !target_matches && !sole_candidate {
            // This observation doesn't correspond to `candidate`, and there is
            // at least one other pending instruction it could plausibly belong
            // to instead. Leave it pending for its own observation or an
            // eventual idle-flush timeout.
            still_pending.push(candidate);
            continue;
        }

        let payload = evaluate_pending_workspace_instruction(
            &candidate.instruction,
            observation.to_workspace_id,
            observation_event_id,
            observed_at,
        );
        outputs.push(
            DerivedOutput::reconciled(
                payload,
                observed_at,
                vec![candidate.instruction_event_id, observation_event_id],
                HYPRLAND_WORKSPACE_SCOPE.to_string(),
            )
            .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
            .with_semantics_version(SEMANTICS_VERSION)
            .with_equivalence_key(format!(
                "hyprland-workspace-expectation:{}",
                candidate.instruction.instruction_id
            ))
            .with_declaration_id(INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS[0].declaration_id)
            .with_product_class(INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS[0].product_class)
            .with_claim_support(
                INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS[0]
                    .default_support
                    .instantiate(2, 0, 1, 0),
            ),
        );
    }

    state.pending_hyprland_workspace = still_pending;
    Ok(outputs)
}

/// Idle-flush path: resolve pending instructions whose deadline has elapsed
/// even though no (matching or unrelated) observation ever arrived to drive
/// resolution via `reconcile_workspace_observation`. Instructions without a
/// `deadline` are left pending indefinitely by design — there is nothing to
/// flush them against.
fn flush_timed_out_workspace_instructions(
    state: &mut InstructionExpectationState,
    now: Timestamp,
) -> Vec<DerivedOutput<InstructionExpectationStatusPayload>> {
    let pending = std::mem::take(&mut state.pending_hyprland_workspace);
    let mut outputs = Vec::new();
    let mut still_pending = Vec::new();

    for candidate in pending {
        let Some(deadline) = candidate.instruction.deadline else {
            still_pending.push(candidate);
            continue;
        };

        if now <= deadline {
            still_pending.push(candidate);
            continue;
        }

        let payload = InstructionExpectationStatusPayload {
            instruction_id: candidate.instruction.instruction_id,
            desired_event_source: candidate.instruction.desired_event_source.clone(),
            desired_event_type: candidate.instruction.desired_event_type.clone(),
            status: InstructionExpectationStatus::TimedOut,
            matched_event_ids: Vec::new(),
            caveat: Some(
                "no matching workspace observation arrived before instruction deadline \
                 (idle flush)"
                    .to_string(),
            ),
            evaluated_at: now,
        };

        outputs.push(
            DerivedOutput::reconciled(
                payload,
                now,
                vec![candidate.instruction_event_id],
                HYPRLAND_WORKSPACE_SCOPE.to_string(),
            )
            .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
            .with_semantics_version(SEMANTICS_VERSION)
            .with_equivalence_key(format!(
                "hyprland-workspace-expectation:{}",
                candidate.instruction.instruction_id
            ))
            .with_declaration_id(INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS[0].declaration_id)
            .with_product_class(INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS[0].product_class)
            .with_claim_support(
                INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS[0]
                    .default_support
                    .instantiate(2, 0, 1, 0),
            ),
        );
    }

    state.pending_hyprland_workspace = still_pending;
    outputs
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
        source_role: sinex_primitives::sources::SourceRole::Activity,
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(instruction_id, desired_event_source, desired_event_type)",
        ),
        access_scope: AccessScope::Internal,
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
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("instruction-expectation-reconciler")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .recovery_policy(sinex_primitives::source_contracts::SourceRecoveryPolicy::DERIVED_INTERNAL)
    .build()
}
