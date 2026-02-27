//! Operations log types

use crate::domain::OperationStatus;
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

/// Request: ops.start
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsStartRequest {
    pub operation_type: String,
    pub operator: String,
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
