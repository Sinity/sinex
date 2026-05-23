//! Operations log types

use crate::domain::OperationStatus;
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Operation record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub operation_type: String,
    pub operator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Value>,
    pub result_status: OperationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_summary: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i32>,
}

// ─────────────────────────────────────────────────────────────
// ops.start
// ─────────────────────────────────────────────────────────────

pub const OPS_START_METHOD: RpcMethod<OpsStartRequest, OpsStartResponse> = RpcMethod::new(
    methods::OPS_START,
    RpcRole::Write,
    RpcDomain::Ops,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

/// Request: ops.start
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsStartRequest {
    pub operation_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Value>,
}

/// Response: ops.start
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsStartResponse {
    pub operation: Operation,
}

// ─────────────────────────────────────────────────────────────
// ops.list
// ─────────────────────────────────────────────────────────────

pub const OPS_LIST_METHOD: RpcMethod<OpsListRequest, OpsListResponse> = RpcMethod::new(
    methods::OPS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Ops,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

/// Request: ops.list
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpsListRequest {
    /// Filter by operation type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_type: Option<String>,
    /// Filter by status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<OperationStatus>,
    /// Limit number of results (default: 100)
    #[serde(default = "default_ops_limit")]
    pub limit: i64,
}

fn default_ops_limit() -> i64 {
    100
}

/// Response: ops.list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsListResponse {
    pub operations: Vec<Operation>,
}

// ─────────────────────────────────────────────────────────────
// ops.get
// ─────────────────────────────────────────────────────────────

pub const OPS_GET_METHOD: RpcMethod<OpsGetRequest, OpsGetResponse> = RpcMethod::new(
    methods::OPS_GET,
    RpcRole::ReadOnly,
    RpcDomain::Ops,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

/// Request: ops.get
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsGetRequest {
    pub operation_id: String,
}

/// Response: ops.get
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsGetResponse {
    pub operation: Operation,
}

// ─────────────────────────────────────────────────────────────
// ops.cancel
// ─────────────────────────────────────────────────────────────

pub const OPS_CANCEL_METHOD: RpcMethod<OpsCancelRequest, OpsCancelResponse> = RpcMethod::new(
    methods::OPS_CANCEL,
    RpcRole::Admin,
    RpcDomain::Ops,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

/// Request: ops.cancel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsCancelRequest {
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Response: ops.cancel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsCancelResponse {
    pub operation: Operation,
    pub cancelled: bool,
}
