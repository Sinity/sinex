//! Data lifecycle RPC types
//!
//! Types for the three-tier data lifecycle: Live ↔ Archive → Tombstone

use serde::{Deserialize, Serialize};

/// Lifecycle tier status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierStatus {
    /// Tier name: "live", "archive", or "tombstone"
    pub tier: String,
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
    pub source: Option<String>,
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

// ─────────────────────────────────────────────────────────────
// lifecycle.tombstone
// ─────────────────────────────────────────────────────────────

/// Request: lifecycle.tombstone (Archive → Tombstone)
///
/// WARNING: This is a ONE-WAY operation. Data is permanently deleted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleTombstoneRequest {
    /// Tombstone archived events older than this duration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    /// Filter by source
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Tombstone specific archived event IDs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_ids: Option<Vec<String>>,
    /// Maximum events to tombstone
    #[serde(default = "default_batch_limit")]
    pub limit: i64,
    /// Reason for tombstoning (required for audit)
    pub reason: String,
    /// Dry run (analyze but don't execute)
    #[serde(default)]
    pub dry_run: bool,
}

/// Response: lifecycle.tombstone
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleTombstoneResponse {
    /// Number of events tombstoned
    pub tombstoned_count: u64,
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
