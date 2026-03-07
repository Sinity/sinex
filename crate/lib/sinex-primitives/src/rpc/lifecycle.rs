//! Data lifecycle RPC types
//!
//! Types for the three-tier data lifecycle: Live ↔ Archive → Tombstone

use crate::domain::{DataTier, EventSource};
use serde::{Deserialize, Serialize};

/// Lifecycle tier status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierStatus {
    /// The data tier this record describes
    pub tier: DataTier,
    /// Number of events in this tier
    pub event_count: i64,
    /// Oldest event timestamp (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_ts: Option<String>,
    /// Newest event timestamp (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest_ts: Option<String>,
    /// Number of distinct sources in this tier
    pub distinct_sources: i64,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.status
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.status
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleStatusRequest {
    /// If true, include per-source breakdown
    #[serde(default)]
    pub by_source: bool,
}

/// Response: lifecycle.status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleStatusResponse {
    /// Status for each tier
    pub tiers: Vec<TierStatus>,
    /// Total events across all tiers
    pub total_events: i64,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.archive
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.archive (Live → Archive)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleArchiveRequest {
    /// Archive events older than this duration (e.g., "30d")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    /// Filter by source
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<EventSource>,
    /// Archive specific event IDs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_ids: Option<Vec<String>>,
    /// Maximum events to archive
    #[serde(default = "default_batch_limit")]
    pub limit: i64,
    /// Reason for archiving
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Dry run (analyze but don't execute)
    #[serde(default)]
    pub dry_run: bool,
}

/// Response: lifecycle.archive
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleArchiveResponse {
    /// Number of events archived
    pub archived_count: u64,
    /// Cascade depth (how many levels of dependencies)
    pub cascade_depth: usize,
    /// Total events affected (including cascade)
    pub cascade_total: usize,
    /// Operation ID for audit
    pub operation_id: String,
    /// Whether this was a dry run
    pub dry_run: bool,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.restore
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.restore (Archive → Live)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRestoreRequest {
    /// Restore specific archived event IDs
    pub event_ids: Vec<String>,
    /// Dry run (analyze but don't execute)
    #[serde(default)]
    pub dry_run: bool,
}

/// Response: lifecycle.restore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRestoreResponse {
    /// Number of events restored
    pub restored_count: u64,
    /// Cascade depth
    pub cascade_depth: usize,
    /// Total events affected (including cascade)
    pub cascade_total: usize,
    /// Operation ID for audit
    pub operation_id: String,
    /// Whether this was a dry run
    pub dry_run: bool,
}

fn default_batch_limit() -> i64 {
    1000
}

// ─────────────────────────────────────────────────────────────
// Two-Step Tombstone Operations (SEC-003)
// ─────────────────────────────────────────────────────────────

/// State machine for tombstone operations
///
/// Tombstone is a destructive, one-way operation. To prevent accidental
/// data loss, it uses a two-step confirmation flow:
///
/// ```text
/// Pending ──create──→ Previewed ──approve──→ Approved ──execute──→ Completed
///              │            │                     │
///              └──cancel────┴────────────────────→ Cancelled
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TombstoneOperationState {
    /// Operation created, pending preview
    Pending,
    /// Preview computed, awaiting approval (TTL active)
    Previewed,
    /// Approved for execution
    Approved,
    /// Tombstone in progress
    Executing,
    /// Successfully completed
    Completed,
    /// User cancelled
    Cancelled,
    /// Error occurred
    Failed,
    /// Expired (TTL exceeded without approval)
    Expired,
}

/// Canonical tombstone workflow phase persisted in operation scope.
///
/// This is the authoritative tombstone progress model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TombstoneOperationPhase {
    Pending,
    Previewed,
    Approved,
    Executing,
    Completed,
    Cancelled,
    Failed,
    Expired,
}

impl From<TombstoneOperationState> for TombstoneOperationPhase {
    fn from(state: TombstoneOperationState) -> Self {
        match state {
            TombstoneOperationState::Pending => Self::Pending,
            TombstoneOperationState::Previewed => Self::Previewed,
            TombstoneOperationState::Approved => Self::Approved,
            TombstoneOperationState::Executing => Self::Executing,
            TombstoneOperationState::Completed => Self::Completed,
            TombstoneOperationState::Cancelled => Self::Cancelled,
            TombstoneOperationState::Failed => Self::Failed,
            TombstoneOperationState::Expired => Self::Expired,
        }
    }
}

impl From<TombstoneOperationPhase> for TombstoneOperationState {
    fn from(phase: TombstoneOperationPhase) -> Self {
        match phase {
            TombstoneOperationPhase::Pending => Self::Pending,
            TombstoneOperationPhase::Previewed => Self::Previewed,
            TombstoneOperationPhase::Approved => Self::Approved,
            TombstoneOperationPhase::Executing => Self::Executing,
            TombstoneOperationPhase::Completed => Self::Completed,
            TombstoneOperationPhase::Cancelled => Self::Cancelled,
            TombstoneOperationPhase::Failed => Self::Failed,
            TombstoneOperationPhase::Expired => Self::Expired,
        }
    }
}

impl TombstoneOperationState {
    /// Check if state is terminal
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Expired
        )
    }

    /// Check if operation can be cancelled
    #[must_use]
    pub fn is_cancellable(&self) -> bool {
        matches!(self, Self::Pending | Self::Previewed | Self::Approved)
    }

    /// Check if operation can be approved
    #[must_use]
    pub fn can_approve(&self) -> bool {
        matches!(self, Self::Previewed)
    }
}

/// Cascade analysis for tombstone preview
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneCascadeAnalysis {
    /// Root events matching the filter criteria
    pub root_event_count: usize,
    /// Total events in cascade (roots + descendants)
    pub cascade_total: usize,
    /// Maximum depth of cascade chain
    pub cascade_depth: usize,
    /// Event counts by source
    pub by_source: std::collections::HashMap<String, usize>,
    /// Sample event IDs for inspection (first 10)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_ids: Vec<String>,
}

/// A tombstone operation record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneOperation {
    /// Unique operation ID
    pub operation_id: String,
    /// Canonical workflow phase (authoritative)
    pub phase: TombstoneOperationPhase,
    /// Current state
    pub state: TombstoneOperationState,
    /// Filter: events older than this duration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    /// Filter: specific source
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<EventSource>,
    /// Filter: specific event IDs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_ids: Option<Vec<String>>,
    /// Reason for tombstoning
    pub reason: String,
    /// Cascade analysis (populated after preview)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cascade_analysis: Option<TombstoneCascadeAnalysis>,
    /// Who created this operation (token prefix)
    pub created_by: String,
    /// When operation was created (RFC3339)
    pub created_at: String,
    /// When operation expires (RFC3339) - typically 1 hour after creation
    pub expires_at: String,
    /// Who approved (if approved)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    /// When approved (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<String>,
    /// When execution started (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// When execution finished (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    /// Number of events actually tombstoned
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstoned_count: Option<u64>,
    /// Error details if failed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_details: Option<String>,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.tombstone.create
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.tombstone.create
///
/// Creates a new tombstone operation and computes the cascade preview.
/// The operation must be approved within 1 hour or it expires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneCreateRequest {
    /// Tombstone archived events older than this duration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    /// Filter by source
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<EventSource>,
    /// Tombstone specific archived event IDs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_ids: Option<Vec<String>>,
    /// Maximum events to tombstone
    #[serde(default = "default_batch_limit")]
    pub limit: i64,
    /// Reason for tombstoning (required for audit)
    pub reason: String,
}

/// Response: lifecycle.tombstone.create
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneCreateResponse {
    /// The created operation
    pub operation: TombstoneOperation,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.tombstone.preview
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.tombstone.preview
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstonePreviewRequest {
    /// Operation ID to preview
    pub operation_id: String,
}

/// Response: lifecycle.tombstone.preview
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstonePreviewResponse {
    /// The operation with cascade analysis
    pub operation: TombstoneOperation,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.tombstone.approve
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.tombstone.approve
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneApproveRequest {
    /// Operation ID to approve
    pub operation_id: String,
    /// Explicit acknowledgment required
    #[serde(default)]
    pub yes_i_understand_data_is_gone: bool,
}

/// Response: lifecycle.tombstone.approve
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneApproveResponse {
    /// The approved and executed operation
    pub operation: TombstoneOperation,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.tombstone.cancel
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.tombstone.cancel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneCancelRequest {
    /// Operation ID to cancel
    pub operation_id: String,
    /// Optional cancellation reason
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Response: lifecycle.tombstone.cancel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneCancelResponse {
    /// Status message
    pub status: String,
    /// Operation ID that was cancelled
    pub operation_id: String,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.tombstone.list
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.tombstone.list
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TombstoneListRequest {
    /// Filter by state
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<TombstoneOperationState>,
    /// Maximum results
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Response: lifecycle.tombstone.list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneListResponse {
    /// List of tombstone operations
    pub operations: Vec<TombstoneOperation>,
}

// ─────────────────────────────────────────────────────────────
// lifecycle.tombstone.status
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.tombstone.status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneStatusRequest {
    /// Operation ID to query
    pub operation_id: String,
}

/// Response: lifecycle.tombstone.status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneStatusResponse {
    /// The operation status
    pub operation: TombstoneOperation,
}
