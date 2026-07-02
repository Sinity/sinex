//! Instruction and actuator-loop payloads.
//!
//! Instructions are desired-state records. Actuation attempts record what an
//! actuator did or refused to do. Expectation statuses record whether ordinary
//! observation events later proved the requested state.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::{Result, SinexError, Timestamp, Uuid};

/// Authority class attached to an admitted instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstructionAuthorityClass {
    OperatorDirect,
    UserDeclared,
    DeterministicPolicy,
    ApprovedProposal,
    ModelSuggested,
}

/// Narrow target classes that may be admitted as instructions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstructionTarget {
    DesktopHyprlandWorkspace,
}

/// Lifecycle status for a local actuation attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActuationStatus {
    Accepted,
    Rejected,
    DryRun,
    NoopAlreadySatisfied,
    Attempted,
    Failed,
    Unavailable,
}

/// Reconciler status for the desired observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstructionExpectationStatus {
    Pending,
    AlreadySatisfied,
    Fulfilled,
    TimedOut,
    Contradicted,
    Impossible,
    Cancelled,
}

/// Declared local actuator capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HyprlandActuatorCapability {
    WorkspaceSwitch,
}

/// Hyprland command-socket dispatch kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HyprlandDispatch {
    Workspace,
}

/// Typed Hyprland command-socket message for switching workspaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HyprlandWorkspaceCommand {
    pub dispatch: HyprlandDispatch,
    pub workspace_id: i32,
}

impl HyprlandWorkspaceCommand {
    /// Render the Hyprland command-socket payload.
    ///
    /// This is deliberately capability-specific. It is not a user-provided
    /// command line and cannot express arbitrary process execution.
    #[must_use]
    pub fn command_socket_message(&self) -> String {
        match self.dispatch {
            HyprlandDispatch::Workspace => format!("dispatch workspace {}", self.workspace_id),
        }
    }
}

/// Sanitized command summary safe to persist in an actuation-attempt event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HyprlandCommandSummary {
    pub capability: HyprlandActuatorCapability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<HyprlandWorkspaceCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_workspace_id: Option<i32>,
    pub observation_ready: bool,
}

/// Desired local desktop workspace state admitted by the operator plane.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "runtime.instruction",
    event_type = "desktop.workspace.switch_requested",
    version = "1.0.0"
)]
pub struct DesktopWorkspaceSwitchInstructionPayload {
    pub instruction_id: Uuid,
    pub target: InstructionTarget,
    pub desired_event_source: String,
    pub desired_event_type: String,
    pub desired_workspace_id: i32,
    pub actor_id: String,
    pub authority: InstructionAuthorityClass,
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    pub dry_run: bool,
    pub safety_policy_ref: String,
}

impl DesktopWorkspaceSwitchInstructionPayload {
    /// Build the canonical direct-operator Hyprland workspace instruction.
    pub fn hyprland_operator_direct(
        instruction_id: Uuid,
        desired_workspace_id: i32,
        actor_id: impl Into<String>,
        deadline: Option<Timestamp>,
        dry_run: bool,
    ) -> Result<Self> {
        if desired_workspace_id < 1 {
            return Err(SinexError::validation(
                "Hyprland workspace instructions require positive workspace ids",
            )
            .with_context("desired_workspace_id", desired_workspace_id.to_string()));
        }

        Ok(Self {
            instruction_id,
            target: InstructionTarget::DesktopHyprlandWorkspace,
            desired_event_source: "wm.hyprland".to_string(),
            desired_event_type: "workspace.switched".to_string(),
            desired_workspace_id,
            actor_id: actor_id.into(),
            authority: InstructionAuthorityClass::OperatorDirect,
            idempotency_key: format!("desktop.hyprland.workspace:{desired_workspace_id}"),
            deadline,
            dry_run,
            safety_policy_ref: "desktop.hyprland.workspace-switch.operator-direct".to_string(),
        })
    }
}

/// Sanitized record of an actuator decision or command-socket attempt.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "runtime.instruction",
    event_type = "actuation.attempted",
    version = "1.0.0"
)]
pub struct ActuationAttemptPayload {
    pub instruction_id: Uuid,
    pub actuator_id: String,
    pub capability: String,
    pub status: ActuationStatus,
    pub command_summary: HyprlandCommandSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub attempted_at: Timestamp,
}

/// Reconciler output that ties desired state to ordinary observation events.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "runtime.instruction",
    event_type = "expectation.status",
    version = "1.0.0"
)]
pub struct InstructionExpectationStatusPayload {
    pub instruction_id: Uuid,
    pub desired_event_source: String,
    pub desired_event_type: String,
    pub status: InstructionExpectationStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matched_event_ids: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caveat: Option<String>,
    pub evaluated_at: Timestamp,
}

/// Build a deterministic Hyprland workspace actuation attempt from current
/// readiness and state observations.
#[must_use]
pub fn plan_hyprland_workspace_switch(
    instruction: &DesktopWorkspaceSwitchInstructionPayload,
    current_workspace_id: Option<i32>,
    observation_ready: bool,
    attempted_at: Timestamp,
) -> ActuationAttemptPayload {
    let command_summary = HyprlandCommandSummary {
        capability: HyprlandActuatorCapability::WorkspaceSwitch,
        command: None,
        current_workspace_id,
        observation_ready,
    };

    if !observation_ready {
        return ActuationAttemptPayload {
            instruction_id: instruction.instruction_id,
            actuator_id: "hyprland-command-socket".to_string(),
            capability: "desktop.hyprland.workspace-switch".to_string(),
            status: ActuationStatus::Unavailable,
            command_summary,
            error: Some("desktop.window-manager observation is not ready".to_string()),
            attempted_at,
        };
    }

    if current_workspace_id == Some(instruction.desired_workspace_id) {
        return ActuationAttemptPayload {
            instruction_id: instruction.instruction_id,
            actuator_id: "hyprland-command-socket".to_string(),
            capability: "desktop.hyprland.workspace-switch".to_string(),
            status: ActuationStatus::NoopAlreadySatisfied,
            command_summary,
            error: None,
            attempted_at,
        };
    }

    let mut command_summary = command_summary;
    command_summary.command = Some(HyprlandWorkspaceCommand {
        dispatch: HyprlandDispatch::Workspace,
        workspace_id: instruction.desired_workspace_id,
    });

    ActuationAttemptPayload {
        instruction_id: instruction.instruction_id,
        actuator_id: "hyprland-command-socket".to_string(),
        capability: "desktop.hyprland.workspace-switch".to_string(),
        status: if instruction.dry_run {
            ActuationStatus::DryRun
        } else {
            ActuationStatus::Attempted
        },
        command_summary,
        error: None,
        attempted_at,
    }
}

/// Evaluate whether a `wm.hyprland/workspace.switched` observation proves the
/// instruction's desired workspace.
#[must_use]
pub fn evaluate_hyprland_workspace_expectation(
    instruction: &DesktopWorkspaceSwitchInstructionPayload,
    observed_to_workspace_id: i32,
    matched_event_id: Uuid,
    evaluated_at: Timestamp,
) -> InstructionExpectationStatusPayload {
    let status = if observed_to_workspace_id == instruction.desired_workspace_id {
        InstructionExpectationStatus::Fulfilled
    } else {
        InstructionExpectationStatus::Contradicted
    };

    InstructionExpectationStatusPayload {
        instruction_id: instruction.instruction_id,
        desired_event_source: instruction.desired_event_source.clone(),
        desired_event_type: instruction.desired_event_type.clone(),
        status,
        matched_event_ids: vec![matched_event_id],
        caveat: None,
        evaluated_at,
    }
}

#[cfg(test)]
#[path = "instruction_test.rs"]
mod tests;
