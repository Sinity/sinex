use serde::{Deserialize, Serialize};

use crate::privacy::{PrivateModeReasonClass, RuntimePrivateModeState};

use super::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};

pub const PRIVACY_PRIVATE_MODE_STATUS_METHOD: RpcMethod<
    PrivateModeStatusRequest,
    PrivateModeStateResponse,
> = RpcMethod::new(
    methods::PRIVACY_PRIVATE_MODE_STATUS,
    RpcRole::ReadOnly,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const PRIVACY_PRIVATE_MODE_ENABLE_METHOD: RpcMethod<
    PrivateModeEnableRequest,
    PrivateModeStateResponse,
> = RpcMethod::new(
    methods::PRIVACY_PRIVATE_MODE_ENABLE,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_PRIVATE_MODE_DISABLE_METHOD: RpcMethod<
    PrivateModeDisableRequest,
    PrivateModeStateResponse,
> = RpcMethod::new(
    methods::PRIVACY_PRIVATE_MODE_DISABLE,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrivateModeStatusRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateModeEnableRequest {
    #[serde(default = "default_actor")]
    pub actor: String,

    #[serde(default)]
    pub reason_class: PrivateModeReasonClass,

    #[serde(default)]
    pub source_classes: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<crate::temporal::Timestamp>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrivateModeDisableRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateModeStateResponse {
    pub state: RuntimePrivateModeState,
}

fn default_actor() -> String {
    "operator".to_string()
}

impl Default for PrivateModeEnableRequest {
    fn default() -> Self {
        Self {
            actor: default_actor(),
            reason_class: PrivateModeReasonClass::default(),
            source_classes: Vec::new(),
            expires_at: None,
        }
    }
}
