//! RuntimeModule operations types

use crate::Timestamp;
use crate::Uuid;
use crate::domain::{ModuleKind, ModuleName, ModuleState, OperationStatus};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use serde::{Deserialize, Serialize};

pub const RUNTIME_LIST_METHOD: RpcMethod<RuntimeListRequest, RuntimeListResponse> = RpcMethod::new(
    methods::RUNTIME_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Runtime,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const RUNTIME_LIST_ACTIVE_METHOD: RpcMethod<
    RuntimeListActiveRequest,
    RuntimeListActiveResponse,
> = RpcMethod::new(
    methods::RUNTIME_LIST_ACTIVE,
    RpcRole::ReadOnly,
    RpcDomain::Runtime,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const RUNTIME_HEALTH_METHOD: RpcMethod<RuntimeHealthRequest, RuntimeHealthResponse> =
    RpcMethod::new(
        methods::RUNTIME_HEALTH,
        RpcRole::ReadOnly,
        RpcDomain::Runtime,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const RUNTIME_DRAIN_METHOD: RpcMethod<RuntimeDrainRequest, RuntimeDrainResponse> =
    RpcMethod::new(
        methods::RUNTIME_DRAIN,
        RpcRole::Write,
        RpcDomain::Runtime,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const RUNTIME_RESUME_METHOD: RpcMethod<RuntimeResumeRequest, RuntimeResumeResponse> =
    RpcMethod::new(
        methods::RUNTIME_RESUME,
        RpcRole::Write,
        RpcDomain::Runtime,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const RUNTIME_SET_HORIZON_METHOD: RpcMethod<
    RuntimeSetHorizonRequest,
    RuntimeSetHorizonResponse,
> = RpcMethod::new(
    methods::RUNTIME_SET_HORIZON,
    RpcRole::Write,
    RpcDomain::Runtime,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

/// RuntimeModule status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStatus {
    pub module_name: ModuleName,
    pub state: ModuleState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing_horizon: Option<Timestamp>,
}

// ─────────────────────────────────────────────────────────────
// runtime.list
// ─────────────────────────────────────────────────────────────

/// Request: runtime.list (no params required)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeListRequest {}

/// Response: runtime.list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeListResponse {
    pub modules: Vec<RuntimeStatus>,
}

fn default_stale_after_secs() -> u64 {
    300
}

// ─────────────────────────────────────────────────────────────
// runtime.list_active
// ─────────────────────────────────────────────────────────────

/// Request: `runtime.list_active`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeListActiveRequest {
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
}

impl Default for RuntimeListActiveRequest {
    fn default() -> Self {
        Self {
            stale_after_secs: default_stale_after_secs(),
        }
    }
}

/// Response: `runtime.list_active`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeListActiveResponse {
    pub modules: Vec<RuntimeInfo>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeHeartbeatSource {
    Run,
    Manifest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInfo {
    pub module_name: ModuleName,
    pub module_kind: ModuleKind,
    pub version: String,
    pub description: Option<String>,
    pub service_name: Option<String>,
    pub instance_id: Option<String>,
    pub module_run_id: Option<Uuid>,
    pub host: Option<String>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
    pub started_at: Option<Timestamp>,
    pub heartbeat_source: RuntimeHeartbeatSource,
}

// ─────────────────────────────────────────────────────────────
// runtime.health
// ─────────────────────────────────────────────────────────────

/// Request: runtime.health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeHealthRequest {
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
}

impl Default for RuntimeHealthRequest {
    fn default() -> Self {
        Self {
            stale_after_secs: default_stale_after_secs(),
        }
    }
}

/// Response: runtime.health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeHealthResponse {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_modules: i64,
    pub active_run_count: i64,
    pub oldest_heartbeat: Option<Timestamp>,
}

// ─────────────────────────────────────────────────────────────
// runtime.drain
// ─────────────────────────────────────────────────────────────

/// Request: runtime.drain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDrainRequest {
    pub module_name: ModuleName,
    /// Optional reason for draining
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Response: runtime.drain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDrainResponse {
    pub status: OperationStatus,
    pub module_name: ModuleName,
}

// ─────────────────────────────────────────────────────────────
// runtime.resume
// ─────────────────────────────────────────────────────────────

/// Request: runtime.resume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeResumeRequest {
    pub module_name: ModuleName,
}

/// Response: runtime.resume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeResumeResponse {
    pub status: OperationStatus,
    pub module_name: ModuleName,
}

// ─────────────────────────────────────────────────────────────
// runtime.set_horizon
// ─────────────────────────────────────────────────────────────

/// Request: `runtime.set_horizon`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSetHorizonRequest {
    pub module_name: ModuleName,
    /// Horizon timestamp
    pub horizon: Timestamp,
}

/// Response: `runtime.set_horizon`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSetHorizonResponse {
    pub status: OperationStatus,
    pub module_name: ModuleName,
    pub horizon: Timestamp,
}
