//! Source material RPC types for `sources.*` methods.

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// sources.stage
// ─────────────────────────────────────────────────────────────

/// Request: `sources.stage`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesStageRequest {
    /// Absolute or relative path to the file to stage
    pub file_path: String,
}

/// Response: `sources.stage`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesStageResponse {
    /// UUID of the newly registered source material
    pub material_id: String,
    /// Canonical source identifier (the file path)
    pub source_identifier: String,
    /// File size in bytes, if available
    pub total_bytes: Option<i64>,
}

// ─────────────────────────────────────────────────────────────
// sources.list
// ─────────────────────────────────────────────────────────────

/// Request: `sources.list`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourcesListRequest {
    /// Optional status filter (e.g. "completed", "sensing", "failed")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// Summary row for the source material list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMaterialSummary {
    /// UUID of the material
    pub id: String,
    /// Material kind ("annex", "git", "local_cas")
    pub material_kind: String,
    /// Source identifier (typically the file path)
    pub source_identifier: String,
    /// Lifecycle status
    pub status: String,
    /// When the material was staged
    pub staged_at: Option<String>,
    /// Who staged the material
    pub staged_by: Option<String>,
    /// Total size in bytes
    pub size_bytes: Option<i64>,
    /// MIME type from the associated blob, if any
    pub mime_type: Option<String>,
}

/// Response: `sources.list`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesListResponse {
    pub materials: Vec<SourceMaterialSummary>,
}

// ─────────────────────────────────────────────────────────────
// sources.show
// ─────────────────────────────────────────────────────────────

/// Request: `sources.show`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesShowRequest {
    /// UUID of the source material to inspect
    pub material_id: String,
}

/// Full detail for a single source material
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMaterialDetail {
    pub id: String,
    pub material_kind: String,
    pub source_identifier: String,
    pub status: String,
    pub timing_info_type: String,
    pub metadata: serde_json::Value,
    pub staged_at: Option<String>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub staged_by: Option<String>,
    pub staged_on_host: Option<String>,
    pub optional_blob_id: Option<String>,
    pub total_bytes: Option<i64>,
    /// Number of events referencing this source material
    pub event_count: Option<i64>,
}

/// Response: `sources.show`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesShowResponse {
    pub material: SourceMaterialDetail,
}

// ─────────────────────────────────────────────────────────────
// sources.coverage
// ─────────────────────────────────────────────────────────────

/// Request: `sources.coverage` (no required params)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourcesCoverageRequest {}

/// One coverage bucket grouped by source identifier and kind
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCoverageEntry {
    pub source_identifier: String,
    pub material_kind: String,
    pub earliest_ts: Option<String>,
    pub latest_ts: Option<String>,
    pub event_count: Option<i64>,
    pub material_count: Option<i64>,
}

/// Response: `sources.coverage`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesCoverageResponse {
    pub sources: Vec<SourceCoverageEntry>,
}
