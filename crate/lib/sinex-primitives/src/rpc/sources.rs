//! Source material RPC types for `sources.*` methods.

use crate::domain::{SourceMaterialFormat, SourceMaterialTimingInfoType};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const SOURCE_MATERIAL_CONTRACT_METADATA_KEY: &str = "source_material_contract";

/// Versioned source-material metadata contract stored under
/// `metadata.source_material_contract`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceMaterialMetadataContract {
    pub version: u16,
    pub format: SourceMaterialFormat,
    pub timing: SourceMaterialTimingInfoType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<SourceOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<SourceAnnotations>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statistics: Option<SourceMaterialStatistics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<SourcePolicyEvidence>,
}

impl SourceMaterialMetadataContract {
    pub const VERSION: u16 = 1;

    #[must_use]
    pub fn new(format: SourceMaterialFormat, timing: SourceMaterialTimingInfoType) -> Self {
        Self {
            version: Self::VERSION,
            format,
            timing,
            origin: None,
            annotations: None,
            statistics: None,
            policy: None,
        }
    }

    #[must_use]
    pub fn from_metadata(metadata: &JsonValue) -> Option<Self> {
        metadata
            .get(SOURCE_MATERIAL_CONTRACT_METADATA_KEY)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }

    #[must_use]
    pub fn metadata_patch(&self) -> JsonValue {
        let mut object = serde_json::Map::new();
        if let Ok(value) = serde_json::to_value(self) {
            object.insert(SOURCE_MATERIAL_CONTRACT_METADATA_KEY.to_string(), value);
        }
        JsonValue::Object(object)
    }
}

/// Where the material came from before staging.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceOrigin {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_mtime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_on_host: Option<String>,
}

/// Operator annotations on a staged material.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceAnnotations {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_end_time: Option<String>,
}

/// Cheap material statistics known at staging/finalization time.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceMaterialStatistics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_blake3: Option<String>,
}

/// Policy/admission evidence attached to the raw material itself.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourcePolicyEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine_reason: Option<String>,
}

/// Query-time summary of temporal ledger evidence for a material.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalEvidenceSummary {
    pub ledger_entries: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_types: Vec<String>,
}

// ─────────────────────────────────────────────────────────────
// sources.stage
// ─────────────────────────────────────────────────────────────

/// Request: `sources.stage`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesStageRequest {
    /// Absolute or relative path to the file to stage
    pub file_path: String,
    /// Optional explicit material format; otherwise inferred from the path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<SourceMaterialFormat>,
    /// Optional coarse timing category for the staged material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing_info_type: Option<SourceMaterialTimingInfoType>,
    /// Human-readable staging reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Operator tags attached to the material contract.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Optional binding name — if provided, the binding's privacy policy is applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_name: Option<String>,
    /// Whether to store raw bytes in the content store (default: true).
    #[serde(default = "default_with_bytes")]
    pub with_bytes: bool,
}

fn default_with_bytes() -> bool {
    true
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
    /// Content-store blob ID, if bytes were stored
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_id: Option<String>,
    /// BLAKE3 checksum of the stored bytes, if bytes were stored
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_blake3: Option<String>,
    pub contract: SourceMaterialMetadataContract,
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
    /// Maximum number of rows to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
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
    pub timing_info_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<SourceMaterialFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_version: Option<u16>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<SourceMaterialMetadataContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_evidence: Option<TemporalEvidenceSummary>,
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

// ─────────────────────────────────────────────────────────────
// sources.presets.list
// ─────────────────────────────────────────────────────────────

/// A built-in resolver preset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcePresetDescriptor {
    /// Preset identifier (e.g. "atuin.default", "zsh.default").
    pub name: String,
    /// Human-readable label.
    pub description: String,
    /// Source family (e.g. "terminal", "browser", "desktop").
    pub source_family: String,
    /// Expected input shape kind.
    pub input_shape_kind: String,
    /// Format hint for the material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub material_format_hint: Option<String>,
    /// Default resolver preset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolver_preset: Option<String>,
}

/// Response: `sources.presets.list`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesPresetsListResponse {
    pub presets: Vec<SourcePresetDescriptor>,
}

// ─────────────────────────────────────────────────────────────
// sources.bindings.list
// ─────────────────────────────────────────────────────────────

/// Request: `sources.bindings.list`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourcesBindingsListRequest {
    /// Optional source family filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_family: Option<String>,
    /// Include disabled bindings.
    #[serde(default)]
    pub include_disabled: bool,
}

/// Summary row for a source binding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBindingSummary {
    pub id: String,
    pub name: String,
    pub source_family: String,
    pub binding_mode: String,
    pub input_shape_kind: String,
    pub enabled: bool,
    pub status: String,
    pub last_error: Option<String>,
    pub created_at: Option<String>,
}

/// Response: `sources.bindings.list`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesBindingsListResponse {
    pub bindings: Vec<SourceBindingSummary>,
}

// ─────────────────────────────────────────────────────────────
// sources.bindings.create
// ─────────────────────────────────────────────────────────────

/// Request: `sources.bindings.create`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesBindingsCreateRequest {
    /// Unique binding name.
    pub name: String,
    /// Source family (e.g. "terminal", "browser").
    pub source_family: String,
    /// Binding mode: stage_only, stage_then_parse, live_capture, external_producer.
    pub binding_mode: String,
    /// Expected input shape kind.
    pub input_shape_kind: String,
    /// Resolver preset name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolver_preset: Option<String>,
    /// Locator JSON (path, URL, etc.).
    #[serde(default)]
    pub locator: JsonValue,
    /// Format hint for the material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub material_format_hint: Option<String>,
    /// Privacy policy ID.
    #[serde(default = "default_privacy_policy_id")]
    pub privacy_policy_id: String,
    /// Raw material policy JSON.
    #[serde(default)]
    pub raw_material_policy: JsonValue,
    /// Whether the binding is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_privacy_policy_id() -> String {
    "allowed_plaintext".to_string()
}

fn default_true() -> bool {
    true
}

/// Response: `sources.bindings.create`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesBindingsCreateResponse {
    pub id: String,
    pub name: String,
}

// ─────────────────────────────────────────────────────────────
// sources.bindings.resolve
// ─────────────────────────────────────────────────────────────

/// Request: `sources.bindings.resolve`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesBindingsResolveRequest {
    /// Name of the binding to resolve.
    pub binding_name: String,
}

/// Response: `sources.bindings.resolve`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesBindingsResolveResponse {
    pub binding_name: String,
    pub resolved: bool,
    pub candidate_count: i32,
    pub selected_locator: Option<JsonValue>,
    pub error_summary: Option<String>,
}
