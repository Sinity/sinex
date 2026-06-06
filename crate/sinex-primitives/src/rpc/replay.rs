//! Replay control types
//!
//! These types mirror `sinex_db::replay::state_machine` for RPC serialization.
//! The gateway uses sinex-db types internally; these are wire-compatible equivalents.

use crate::domain::{ModuleName, ReplayOutcome};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub const REPLAY_OPERATION_STATUS_METHOD: RpcMethod<ReplayStatusRequest, ReplayStatusResponse> =
    RpcMethod::new(
        methods::REPLAY_OPERATION_STATUS,
        RpcRole::ReadOnly,
        RpcDomain::Replay,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const REPLAY_LIST_OPERATIONS_METHOD: RpcMethod<ReplayListRequest, ReplayListResponse> =
    RpcMethod::new(
        methods::REPLAY_LIST_OPERATIONS,
        RpcRole::ReadOnly,
        RpcDomain::Replay,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const REPLAY_CREATE_OPERATION_METHOD: RpcMethod<ReplayCreateRequest, ReplayCreateResponse> =
    RpcMethod::new(
        methods::REPLAY_CREATE_OPERATION,
        RpcRole::Write,
        RpcDomain::Replay,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const REPLAY_PREVIEW_OPERATION_METHOD: RpcMethod<ReplayPreviewRequest, ReplayPreviewResponse> =
    RpcMethod::new(
        methods::REPLAY_PREVIEW_OPERATION,
        RpcRole::Write,
        RpcDomain::Replay,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const REPLAY_APPROVE_OPERATION_METHOD: RpcMethod<ReplayApproveRequest, ReplayApproveResponse> =
    RpcMethod::new(
        methods::REPLAY_APPROVE_OPERATION,
        RpcRole::Admin,
        RpcDomain::Replay,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const REPLAY_SUBMIT_OPERATION_METHOD: RpcMethod<ReplaySubmitRequest, ReplaySubmitResponse> =
    RpcMethod::new(
        methods::REPLAY_SUBMIT_OPERATION,
        RpcRole::Admin,
        RpcDomain::Replay,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const REPLAY_EXECUTE_OPERATION_METHOD: RpcMethod<ReplayExecuteRequest, ReplayExecuteResponse> =
    RpcMethod::new(
        methods::REPLAY_EXECUTE_OPERATION,
        RpcRole::Admin,
        RpcDomain::Replay,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const REPLAY_CANCEL_OPERATION_METHOD: RpcMethod<ReplayCancelRequest, ReplayCancelResponse> =
    RpcMethod::new(
        methods::REPLAY_CANCEL_OPERATION,
        RpcRole::Admin,
        RpcDomain::Replay,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

/// Replay operation states with well-defined transitions.
///
/// This is the wire-serialization mirror of the authoritative
/// [`sinex_db::replay::state_machine::ReplayState`]. The two are
/// structurally identical; the duplicate exists because the
/// authoritative version carries `sqlx::Type` annotations that the
/// primitives crate cannot import (dependency direction: primitives
/// ranks below sinex-db).
///
/// Preferred canonical path for non-RPC consumers:
/// `sinex_db::replay::state_machine::ReplayState`.
///
/// Serde uses default `PascalCase` to match the DB type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplayState {
    /// Initial state, gathering scope and planning
    Planning,
    /// Preview computed, awaiting approval
    Previewed,
    /// Approved for execution
    Approved,
    /// Active replay in progress
    Executing,
    /// Operator requested cancellation; executor is still unwinding.
    Cancelling,
    /// Finalizing changes
    Committing,
    /// Successfully finished
    Completed,
    /// Error occurred
    Failed,
    /// User cancelled
    Cancelled,
}

impl ReplayState {
    /// Check if state is terminal
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled
        )
    }
}

/// Scope defining what to replay
///
/// Mirrors `sinex_db::replay::state_machine::ReplayScope`
///
/// ## Compatibility
///
/// The four staged-source fields (`source_id`, `source_material_id`,
/// `parser_id`, `parser_version`) are optional and backward-compatible:
/// A scope that
/// specifies any staged-source field implies the replay planner should queue
/// parser jobs for the matching materials rather than scanning a live source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayScope {
    /// Source runtime to replay
    #[serde(alias = "module_name")]
    pub source_name: String,
    /// Optional time window as (start, end) ISO8601 timestamps
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_window: Option<(String, String)>,
    /// Optional material filter (`UUIDv7` strings)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub material_filter: Option<Vec<String>>,
    /// Additional filters as JSON
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub filters: HashMap<String, Value>,
    /// ── Staged-source architecture extension (#1060) ────────────
    /// Replay only material registered by this source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    /// Replay only this specific source material (`UUIDv7`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_material_id: Option<String>,
    /// Queue parser jobs from this parser profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parser_id: Option<String>,
    /// Constrain parser version (resolves latest matching if omitted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parser_version: Option<String>,
}

impl ReplayScope {
    /// Returns `true` when the scope references the staged-source architecture
    /// (source contracts, materials, or parsers). Replay planning uses this to
    /// decide between live-source scanning and parser-queue replay.
    #[must_use]
    pub fn is_staged_source_scope(&self) -> bool {
        self.source_id.is_some()
            || self.source_material_id.is_some()
            || self.parser_id.is_some()
            || self.parser_version.is_some()
    }
}

/// Checkpoint for resumable execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCheckpoint {
    /// Number of events processed
    pub processed_events: u64,
    /// Total events to process
    pub total_events: u64,
    /// Last processed event ID
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<String>,
    /// Current batch number
    pub batch_number: u32,
    /// `PostgreSQL` savepoint ID if in transaction
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub savepoint_id: Option<String>,
    /// Timestamp of last update
    pub updated_at: String,
}

/// Complete replay operation record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayOperation {
    /// Unique operation ID
    pub operation_id: String,
    /// Current state
    pub state: ReplayState,
    /// Replay scope
    pub scope: ReplayScope,
    /// Preview results (if computed)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_summary: Option<Value>,
    /// Execution checkpoint
    pub checkpoint: ReplayCheckpoint,
    /// Who created this operation
    pub actor: String,
    /// When operation was created
    pub created_at: String,
    /// Who approved (if approved)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    /// When approved
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<String>,
    /// Runtime module or authenticated actor executing the replay
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_module: Option<ModuleName>,
    /// When execution started
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// When execution finished
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    /// Outcome of a terminal replay operation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<ReplayOutcome>,
    /// Error details if failed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_details: Option<String>,
}

// ─────────────────────────────────────────────────────────────
// replay.create_operation
// ─────────────────────────────────────────────────────────────

/// Request: `replay.create_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCreateRequest {
    /// Scope defining what to replay
    pub scope: ReplayScope,
}

/// Response: `replay.create_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCreateResponse {
    pub operation: ReplayOperation,
}

// ─────────────────────────────────────────────────────────────
// replay.preview_operation
// ─────────────────────────────────────────────────────────────

/// Request: `replay.preview_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayPreviewRequest {
    pub operation_id: String,
}

/// Response: `replay.preview_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayPreviewResponse {
    pub operation: ReplayOperation,
    pub preview: Value,
}

/// Explicit operator overrides for replay preview gates.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplayGateOverrides {
    /// Permit replay when previewed anchor churn exceeds the default threshold.
    #[serde(default)]
    pub allow_anchor_churn: bool,
    /// Permit replay when timestamp-quality flips exceed the default threshold.
    #[serde(default)]
    pub allow_time_quality_flips: bool,
    /// Permit replay when cascade depth exceeds the default warning gate.
    #[serde(default)]
    pub allow_deep_cascade: bool,
    /// Permit replay across a payload-schema boundary.
    #[serde(default)]
    pub force_schema_mismatch: bool,
}

// ─────────────────────────────────────────────────────────────
// replay.approve_operation
// ─────────────────────────────────────────────────────────────

/// Request: `replay.approve_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayApproveRequest {
    pub operation_id: String,
}

/// Response: `replay.approve_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayApproveResponse {
    pub operation: ReplayOperation,
}

// ─────────────────────────────────────────────────────────────
// replay.submit_operation
// ─────────────────────────────────────────────────────────────

/// Request: `replay.submit_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySubmitRequest {
    pub operation_id: String,
    #[serde(default)]
    pub gate_overrides: ReplayGateOverrides,
}

/// Response: `replay.submit_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySubmitResponse {
    pub operation: ReplayOperation,
}

// ─────────────────────────────────────────────────────────────
// replay.execute_operation
// ─────────────────────────────────────────────────────────────

/// Request: `replay.execute_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayExecuteRequest {
    pub operation_id: String,
    /// When true, compute cascade impact without archiving or dispatching scan.
    /// The operation transitions to Cancelled with a dry-run report in the
    /// checkpoint, proving the advisory lock can be acquired and the cascade
    /// expansion is valid. No side effects are persisted.
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub gate_overrides: ReplayGateOverrides,
}

/// Response: `replay.execute_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayExecuteResponse {
    pub operation: ReplayOperation,
}

// ─────────────────────────────────────────────────────────────
// replay.cancel_operation
// ─────────────────────────────────────────────────────────────

/// Request: `replay.cancel_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCancelRequest {
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Response: `replay.cancel_operation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCancelResponse {
    pub cancelled: bool,
    pub operation: ReplayOperation,
}

// ─────────────────────────────────────────────────────────────
// replay.operation_status
// ─────────────────────────────────────────────────────────────

/// Request: `replay.operation_status`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayStatusRequest {
    pub operation_id: String,
}

/// Response: `replay.operation_status`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayStatusResponse {
    pub operation: ReplayOperation,
}

// ─────────────────────────────────────────────────────────────
// replay.list_operations
// ─────────────────────────────────────────────────────────────

/// Request: `replay.list_operations`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplayListRequest {
    /// Filter by state
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ReplayState>,
    /// Filter by source module ID
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    /// Maximum results (default: unbounded)
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Response: `replay.list_operations`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayListResponse {
    pub operations: Vec<ReplayOperation>,
}
