//! Instruction/actuator-loop RPC contracts.

use serde::{Deserialize, Serialize};

use crate::events::{
    Event, SourceMaterial,
    payloads::{ActuationAttemptPayload, DesktopWorkspaceSwitchInstructionPayload},
};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::{Id, JsonValue, Timestamp, Uuid};

pub const INSTRUCTIONS_HYPRLAND_WORKSPACE_SWITCH_METHOD: RpcMethod<
    HyprlandWorkspaceSwitchRequest,
    HyprlandWorkspaceSwitchResponse,
> = RpcMethod::new(
    methods::INSTRUCTIONS_HYPRLAND_WORKSPACE_SWITCH,
    RpcRole::Write,
    RpcDomain::Instructions,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyprlandWorkspaceSwitchRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_id: Option<Uuid>,
    pub desired_workspace_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_socket_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyprlandWorkspaceSwitchResponse {
    pub instruction: DesktopWorkspaceSwitchInstructionPayload,
    pub instruction_event: Event<JsonValue>,
    pub attempt: ActuationAttemptPayload,
    pub attempt_event: Event<JsonValue>,
    pub material_id: Id<SourceMaterial>,
    pub observation_ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_workspace_id: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_socket_response: Option<String>,
}
