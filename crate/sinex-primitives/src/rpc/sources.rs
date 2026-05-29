//! Source material RPC types for `sources.*` methods.
//!
//! # External Producer Wire Contract
//!
//! Non-Rust producers (Python, shell scripts, external tools) can publish
//! [`EventIntent`] envelopes to NATS `JetStream` without depending on the
//! Rust SDK. The contract below is the minimum a producer must satisfy for
//! ingestd to accept the payload.
//!
//! ## Envelope format
//!
//! Publish a JSON [`EventIntent`] to the `JetStream` subject
//! `{env}.sinex.events.raw.{source}.{event_type}` (typically
//! `sinex.events.raw.{source}.{event_type}` in development).
//!
//! ### Required JSON shape
//!
//! ```json
//! {
//!   "envelope_version": "1",
//!   "source_unit_id": "integration.polylogue",
//!   "parser_id": "polylogue-bridge",
//!   "parser_version": "0.1.0",
//!   "events": [
//!     {
//!       "id": "018f9e4b-...",
//!       "source": "integration.polylogue",
//!       "event_type": "integration.polylogue.conversation_indexed",
//!       "payload": { "...": "..." },
//!       "ts_orig": "2026-05-07T20:00:00Z",
//!       "host": "sinnix-prime",
//!       "Material": {
//!         "id": "018f9e4b-...",
//!         "anchor_byte": 0,
//!         "offset_kind": "Byte"
//!       }
//!     }
//!   ],
//!   "admitted_at": "2026-05-07T20:00:01Z",
//!   "admitted_by": "sinnix-prime"
//! }
//! ```
//!
//! ### Field reference
//!
//! | Field | Type | Required | Notes |
//! |-------|------|----------|-------|
//! | `envelope_version` | `"1"` | yes | Must be `"1"` (only accepted version) |
//! | `source_unit_id` | string | yes | Producer identifier, e.g. `"integration.polylogue"` |
//! | `parser_id` | string | yes | Parser that interpreted the material, e.g. `"polylogue-bridge"` |
//! | `parser_version` | string | yes | Semver, e.g. `"0.1.0"` |
//! | `events` | array | yes | At least one event (see Event shape below) |
//! | `admitted_at` | ISO 8601 | yes | When this intent was created |
//! | `admitted_by` | string | yes | Hostname that performed admission checks |
//!
//! ### Event shape (each element in `events`)
//!
//! | Field | Type | Required | Notes |
//! |-------|------|----------|-------|
//! | `id` | `UUIDv7` | yes | Event identifier; use deterministic `UUIDv5` if replayable |
//! | `source` | string | yes | Event source, typically matching `source_unit_id` |
//! | `event_type` | string | yes | Dotted event type, e.g. `"integration.polylogue.conversation_indexed"` |
//! | `payload` | JSON object | yes | Free-form event payload |
//! | `ts_orig` | ISO 8601 | no | Real-world occurrence timestamp |
//! | `host` | string | yes | Originating hostname |
//! | `Material` | object | XOR * | Material provenance (external producers use a virtual material) |
//! | `Derived` | object | XOR * | Derived provenance (derived from parent events) |
//!
//! For external producers emitting metadata-only events, use `Material`
//! provenance with a deterministic `UUIDv5` material ID derived from a
//! producer-specific namespace and a stable key (e.g. the archive database
//! path). This gives the event an occurrence identity without requiring a
//! pre-registered source material.
//!
//! ### NATS headers
//!
//! | Header | Value | Required | Notes |
//! |--------|-------|----------|-------|
//! | `Nats-Msg-Id` | `UUIDv7` of the first event | yes | Idempotency key for `JetStream` dedup |
//! | `Sinex-Traffic-Class` | `"raw_event"` | yes | Traffic class for rate limiting |
//!
//! ### Subjects
//!
//! | Pattern | Use |
//! |---------|-----|
//! | `{env}.sinex.events.raw.integration.polylogue.conversation_indexed` | Conversation indexed |
//! | `{env}.sinex.events.raw.integration.polylogue.conversation_updated` | Conversation updated |
//! | `{env}.sinex.events.raw.analysis.lynchpin.artifact_staged` | Lynchpin artifact staged |
//!
//! ## Known external producer identifiers
//!
//! These are the `source_unit_id` / `source` values for producers that are not
//! Rust ingestors inside the sinex workspace:
//!
//! | Identifier | Project | Description |
//! |------------|---------|-------------|
//! | `integration.polylogue` | [polylogue](https://github.com/sinity/polylogue) | AI chat archive indexer |
//! | `analysis.lynchpin` | [lynchpin](https://github.com/sinity/sinity-lynchpin) | Analysis artifact staging |

use crate::domain::{SourceMaterialFormat, SourceMaterialTimingInfoType};
use crate::parser::{ParserId, SourceBindingId, SourceUnitId};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::sources::continuity::{
    SourcesContinuityGetRequest, SourcesContinuityGetResponse, SourcesContinuityListRequest,
    SourcesContinuityListResponse, SourcesExplainGapRequest, SourcesExplainGapResponse,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const SOURCE_MATERIAL_CONTRACT_METADATA_KEY: &str = "source_material_contract";

pub const SOURCES_LIST_METHOD: RpcMethod<SourcesListRequest, SourcesListResponse> = RpcMethod::new(
    methods::SOURCES_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_SHOW_METHOD: RpcMethod<SourcesShowRequest, SourcesShowResponse> = RpcMethod::new(
    methods::SOURCES_SHOW,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_COVERAGE_METHOD: RpcMethod<SourcesCoverageRequest, SourcesCoverageResponse> =
    RpcMethod::new(
        methods::SOURCES_COVERAGE,
        RpcRole::ReadOnly,
        RpcDomain::Sources,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const SOURCES_CONTINUITY_METHOD: RpcMethod<
    SourcesContinuityRequest,
    SourcesContinuityResponse,
> = RpcMethod::new(
    methods::SOURCES_CONTINUITY,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_CONTINUITY_LIST_METHOD: RpcMethod<
    SourcesContinuityListRequest,
    SourcesContinuityListResponse,
> = RpcMethod::new(
    methods::SOURCES_CONTINUITY_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_CONTINUITY_GET_METHOD: RpcMethod<
    SourcesContinuityGetRequest,
    SourcesContinuityGetResponse,
> = RpcMethod::new(
    methods::SOURCES_CONTINUITY_GET,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_CONTINUITY_EXPLAIN_GAP_METHOD: RpcMethod<
    SourcesExplainGapRequest,
    SourcesExplainGapResponse,
> = RpcMethod::new(
    methods::SOURCES_CONTINUITY_EXPLAIN_GAP,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_READINESS_LIST_METHOD: RpcMethod<
    SourcesReadinessListRequest,
    SourcesReadinessListResponse,
> = RpcMethod::new(
    methods::SOURCES_READINESS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_READINESS_GET_METHOD: RpcMethod<
    SourcesReadinessGetRequest,
    SourcesReadinessGetResponse,
> = RpcMethod::new(
    methods::SOURCES_READINESS_GET,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_DRIFT_LIST_METHOD: RpcMethod<SourcesDriftListRequest, SourcesDriftListResponse> =
    RpcMethod::new(
        methods::SOURCES_DRIFT_LIST,
        RpcRole::ReadOnly,
        RpcDomain::Sources,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const SOURCES_STAGE_METHOD: RpcMethod<SourcesStageRequest, SourcesStageResponse> =
    RpcMethod::new(
        methods::SOURCES_STAGE,
        RpcRole::Write,
        RpcDomain::Sources,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const SOURCES_ANNOTATE_METHOD: RpcMethod<SourcesAnnotateRequest, SourcesAnnotateResponse> =
    RpcMethod::new(
        methods::SOURCES_ANNOTATE,
        RpcRole::Write,
        RpcDomain::Sources,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const SOURCES_ARCHIVE_METHOD: RpcMethod<SourcesArchiveRequest, SourcesArchiveResponse> =
    RpcMethod::new(
        methods::SOURCES_ARCHIVE,
        RpcRole::Admin,
        RpcDomain::Sources,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const SOURCES_PRESETS_LIST_METHOD: RpcMethod<
    SourcesPresetsListRequest,
    SourcesPresetsListResponse,
> = RpcMethod::new(
    methods::SOURCES_PRESETS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_BINDINGS_LIST_METHOD: RpcMethod<
    SourcesBindingsListRequest,
    SourcesBindingsListResponse,
> = RpcMethod::new(
    methods::SOURCES_BINDINGS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SOURCES_BINDINGS_CREATE_METHOD: RpcMethod<
    SourcesBindingsCreateRequest,
    SourcesBindingsCreateResponse,
> = RpcMethod::new(
    methods::SOURCES_BINDINGS_CREATE,
    RpcRole::Write,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const SOURCES_BINDINGS_RESOLVE_METHOD: RpcMethod<
    SourcesBindingsResolveRequest,
    SourcesBindingsResolveResponse,
> = RpcMethod::new(
    methods::SOURCES_BINDINGS_RESOLVE,
    RpcRole::Write,
    RpcDomain::Sources,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

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
    /// Material kind ("annex", "git", "`local_cas`")
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

/// Request: `sources.presets.list`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourcesPresetsListRequest {}

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
    /// Binding mode: `stage_only`, `stage_then_parse`, `live_capture`, `external_producer`.
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

// ─────────────────────────────────────────────────────────────
// sources.annotate — operator annotations on staged material
// ─────────────────────────────────────────────────────────────

/// Request: `sources.annotate`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesAnnotateRequest {
    /// UUID of the source material to annotate.
    pub material_id: String,
    /// Free-form operator notes (appended to existing notes if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    /// Tags to merge into the material contract (additive; duplicates ignored).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Override declared start time for the material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_start_time: Option<String>,
    /// Override declared end time for the material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_end_time: Option<String>,
}

/// Response: `sources.annotate`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesAnnotateResponse {
    pub material_id: String,
    pub annotations: SourceAnnotations,
}

// ─────────────────────────────────────────────────────────────
// sources.archive — archive a staged source material
// ─────────────────────────────────────────────────────────────

/// Request: `sources.archive`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesArchiveRequest {
    /// UUID of the source material to archive.
    pub material_id: String,
    /// When true, compute cascade preview without archiving.
    #[serde(default)]
    pub dry_run: bool,
    /// Reason for archival (audit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Response: `sources.archive`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesArchiveResponse {
    pub material_id: String,
    /// Archival operation ID (only set when `dry_run` is false).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    /// Number of events in the cascade.
    pub cascade_count: i64,
    /// Whether this was a dry-run preview.
    pub dry_run: bool,
    /// Preview summary (populated in dry-run mode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<JsonValue>,
}

// ─────────────────────────────────────────────────────────────
// sources.continuity — temporal-gap and replayability diagnostics
// ─────────────────────────────────────────────────────────────

/// Request: `sources.continuity`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesContinuityRequest {
    /// Source identifier (file path, URI, or source name).
    pub source_identifier: String,
    /// Optional material kind filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub material_kind: Option<String>,
}

/// A gap in the temporal coverage for a source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageGap {
    /// Start of the gap (ISO8601).
    pub gap_start: Option<String>,
    /// End of the gap (ISO8601).
    pub gap_end: Option<String>,
    /// Duration of the gap in seconds (if both bounds are known).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap_duration_seconds: Option<i64>,
    /// Gap classification: "temporal" (events missing in a time range) or
    /// "`material_missing`" (no source material registered for a period).
    pub gap_type: String,
}

/// Whether a coverage contract is satisfied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityContractStatus {
    /// True if the source has an explicit coverage contract bound.
    pub has_coverage_contract: bool,
    /// Expected interval between observations in seconds, if contracted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_interval_seconds: Option<i64>,
    /// Percentage of the expected interval range that is actually covered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_coverage_percent: Option<f64>,
    /// Human-readable descriptions of contract breaches.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub breaches: Vec<String>,
}

/// Whether source material is replayable from currently staged materials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayabilityStatus {
    /// True when all required materials are staged and current.
    pub replayable: bool,
    /// Human-readable explanation when not replayable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Count of staged materials matching this source.
    pub material_count: i64,
    /// Total events referencing those materials.
    pub events_count: i64,
}

/// Response: `sources.continuity`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesContinuityResponse {
    pub source_identifier: String,
    /// Detected temporal gaps in coverage.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub coverage_gaps: Vec<CoverageGap>,
    /// Coverage contract status.
    pub contract_status: ContinuityContractStatus,
    /// Replayability assessment.
    pub replayability: ReplayabilityStatus,
}

// ─────────────────────────────────────────────────────────────
// External producer and sibling-tool source presets
// ─────────────────────────────────────────────────────────────

/// Source presets for producers outside the sinex Rust workspace.
///
/// These are candidates for `sources.presets.list` but are defined here so the
/// preset catalog stays discoverable from `sinex-primitives`. The gateway's
/// `builtin_presets()` may extend its list from this function.
///
/// See the [external producer wire contract](self#external-producer-wire-contract)
/// for the JSON envelope these producers use.
#[must_use]
pub fn external_producer_presets() -> Vec<SourcePresetDescriptor> {
    // External producer presets are registered through operator configuration,
    // not hardcoded here. See the source-bindings NixOS module for the
    // configuration surface.
    vec![]
}

/// Presets for source material paths that bridge external producer systems.
///
/// These differ from [`external_producer_presets`] in that they represent
/// material *discovery* paths (file paths, directories) rather than event
/// *production* identifiers.
#[must_use]
pub fn bridge_material_presets() -> Vec<SourcePresetDescriptor> {
    vec![
        // ── Polylogue chat archive ──────────────────────────────
        SourcePresetDescriptor {
            name: "polylogue.exports.default".into(),
            description: "Polylogue chat archive root".into(),
            source_family: "chat".into(),
            input_shape_kind: "directory".into(),
            material_format_hint: None,
            resolver_preset: None,
        },
    ]
}

// ─────────────────────────────────────────────────────────────
// sources.readiness — source readiness and caveat surface (#1099)
// ─────────────────────────────────────────────────────────────

/// Status of a source's readiness.
///
/// Precedence (highest first): `Disabled` > `Blocked` > `Missing` > `Error` >
/// `Stale` > `Partial` > `Available` > `Unknown`. Classification is performed
/// in the readiness derivation; `Unknown` indicates the underlying signals
/// were inconclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceReadinessStatus {
    /// Source is configured, recent successful acquisition+parse, fresh enough.
    Available,
    /// Source has staged material but no parsed events, or only partial coverage.
    Partial,
    /// Last successful acquisition is older than the freshness threshold.
    Stale,
    /// Source has acquisition or parser failures recorded recently.
    Error,
    /// Locator could not be resolved or material is missing/unreadable.
    Missing,
    /// Source is intentionally blocked (privacy policy, admission decision, etc.).
    Blocked,
    /// Source is configured but explicitly disabled.
    Disabled,
    /// Underlying signals are insufficient to classify.
    Unknown,
}

/// Cost class for using a source.
///
/// Operators and agents use this hint to choose between sources or to warn
/// before using an expensive one (e.g., gating heavy parses behind explicit
/// confirmation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceReadinessCost {
    /// Already-staged material; cheap local read.
    LocalFast,
    /// Local but heavy: large parse, decompression, or scan.
    LocalHeavy,
    /// Requires network access to acquire or refresh.
    Network,
    /// Requires operator review or manual approval before use.
    OperatorReview,
    /// Source is currently unavailable; cost is undefined.
    Unavailable,
}

/// Severity of a caveat attached to a readiness report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CaveatSeverity {
    /// Diagnostic information; does not change usability.
    Info,
    /// Caller should be aware but the source is still usable.
    Warning,
    /// Source is degraded and results may be incomplete.
    Degraded,
    /// Source is not usable in the current state.
    Blocking,
}

/// Stable caveat codes consumed by CLIs, context packs, and agents.
///
/// Codes are short, dotted strings; new codes can be introduced freely but
/// existing codes must not change meaning. Consumers should treat unknown
/// codes as `Info`-severity caveats they pass through unchanged.
pub mod caveat_codes {
    /// Binding's locator/path could not be found on disk.
    pub const BINDING_MISSING_PATH: &str = "binding.missing_path";
    /// Binding's locator was found but is not readable.
    pub const BINDING_PERMISSION_DENIED: &str = "binding.permission_denied";
    /// Binding is not declared in current configuration.
    pub const BINDING_UNDECLARED: &str = "binding.undeclared";
    /// Privacy policy or admission decision blocks this source.
    pub const POLICY_RAW_MATERIAL_BLOCKED: &str = "policy.raw_material_blocked";
    /// Material is staged but has no parsed events referencing it.
    pub const MATERIAL_STAGED_UNPARSED: &str = "material.staged_unparsed";
    /// No recent successful staging; coverage may have gaps.
    pub const MATERIAL_NO_RECENT_SNAPSHOT: &str = "material.no_recent_snapshot";
    /// Source has no parser binding declared.
    pub const PARSER_NO_BINDING: &str = "parser.no_binding";
    /// Parser job failed recently.
    pub const PARSER_FAILED_RECENTLY: &str = "parser.failed_recently";
    /// Parser version differs from the version that produced existing events.
    pub const PARSER_VERSION_DRIFT: &str = "parser.version_drift";
    /// Parser input shape changed but only additive fields were observed.
    pub const SOURCE_SHAPE_CHANGED: &str = "source.shape_changed";
    /// Parser input shape is missing fields seen in the previous accepted shape.
    pub const PARSER_REQUIRED_FIELD_MISSING: &str = "parser.required_field_missing";
    /// Parser input shape changed the scalar type of one or more existing fields.
    pub const PARSER_FIELD_TYPE_CHANGED: &str = "parser.field_type_changed";
    /// Coverage is partial within the requested time window.
    pub const COVERAGE_PARTIAL_TIME_WINDOW: &str = "coverage.partial_time_window";
    /// Using this source will trigger local-heavy work.
    pub const COST_LOCAL_HEAVY: &str = "cost.local_heavy";
    /// Using this source requires network access.
    pub const COST_NETWORK_REQUIRED: &str = "cost.network_required";
    /// Parser jobs are not yet tracked in the database (#1057 follow-up).
    pub const PARSER_JOBS_UNTRACKED: &str = "parser.jobs_untracked";
    /// Source bindings are declared in Nix configuration; no DB catalog (#1098).
    pub const BINDINGS_NOT_IN_DB: &str = "binding.not_in_db";
    /// Runtime private-mode state could not be read, so readiness fails closed.
    pub const POLICY_PRIVATE_MODE_STATE_UNAVAILABLE: &str = "policy.private_mode_state_unavailable";
}

/// A single caveat attached to a readiness report.
///
/// Caveats explain why a source is not in `Available` status, or surface
/// information operators/agents should know even when the source is usable.
/// Codes from [`caveat_codes`] are stable; messages are human-readable and
/// may evolve.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceCaveat {
    /// Stable caveat code (see [`caveat_codes`]).
    pub code: String,
    /// Severity of this caveat.
    pub severity: CaveatSeverity,
    /// Human-readable description.
    pub message: String,
    /// Optional reference to evidence: a material UUID, run ID, or path.
    /// Sensitive paths must be redacted before being placed here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_ref: Option<String>,
}

/// A readiness report for one source.
///
/// Sources are identified by `(source_family, source_unit_id)` when both are
/// known; the readiness derivation may also report at the `source_identifier`
/// level when only material registry data is available.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceReadiness {
    /// Binding ID, when bindings are tracked (currently always None — see
    /// [`caveat_codes::BINDINGS_NOT_IN_DB`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_id: Option<SourceBindingId>,
    /// Source family (e.g. "terminal", "browser", "desktop", "chat").
    pub source_family: String,
    /// Source unit identifier when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_unit_id: Option<SourceUnitId>,
    /// Parser ID when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parser_id: Option<ParserId>,
    /// Logical source identifier from the material registry. May be a
    /// privacy-redacted path display.
    pub source_identifier: String,
    /// Computed readiness status.
    pub status: SourceReadinessStatus,
    /// Cost class for using this source.
    pub cost: SourceReadinessCost,
    /// Time since the most recent successful staging, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness_seconds: Option<i64>,
    /// Number of source materials registered for this source.
    pub material_count: u64,
    /// Number of parsed events referencing materials from this source, when
    /// known. May be `None` when the join is too expensive or unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parsed_event_count: Option<u64>,
    /// Timestamp of the last successful staging (`completed` material), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<String>,
    /// Caveats explaining the status and the limits of this report.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<SourceCaveat>,
    /// Free-form evidence (material counts by status, latest blob ID, etc.).
    /// Sensitive paths must be redacted.
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub evidence: JsonValue,
}

/// Request: `sources.readiness.list`
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SourcesReadinessListRequest {
    /// Optional source family filter (e.g. "terminal", "browser").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_family: Option<String>,
    /// Maximum freshness threshold in seconds for the `Stale` classification.
    /// Defaults to 7 days when not provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_after_seconds: Option<i64>,
}

/// Response: `sources.readiness.list`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesReadinessListResponse {
    pub sources: Vec<SourceReadiness>,
}

/// Request: `sources.readiness.get`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesReadinessGetRequest {
    /// Logical source identifier from the material registry.
    pub source_identifier: String,
    /// Optional source family filter (disambiguates same identifier in
    /// multiple families).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_family: Option<String>,
    /// Stale-after threshold in seconds (default 7 days).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_after_seconds: Option<i64>,
}

/// Response: `sources.readiness.get`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesReadinessGetResponse {
    /// `None` when no material has been registered for the requested source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<SourceReadiness>,
}

// ─────────────────────────────────────────────────────────────
// sources.drift.list — checkpointed source-shape drift (#1103)
// ─────────────────────────────────────────────────────────────

/// Request: `sources.drift.list`
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SourcesDriftListRequest {
    /// Optional source-unit filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_unit_id: Option<SourceUnitId>,
    /// Maximum drift observations to return. Defaults to 50.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// One scalar type-change observed in a source-shape drift event.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceShapeTypeChange {
    pub key: String,
    pub previous_type: String,
    pub current_type: String,
}

/// Checkpointed source-shape drift observed by an adapter-backed source unit.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceShapeDriftObservation {
    /// Checkpoint KV key that supplied this observation.
    pub checkpoint_key: String,
    /// Source unit that reported the drift.
    pub source_unit_id: SourceUnitId,
    /// Checkpoint consumer group, when recoverable from the KV key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    /// Checkpoint consumer name, when recoverable from the KV key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_name: Option<String>,
    pub previous_hash: String,
    pub current_hash: String,
    pub format: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub type_changes: Vec<SourceShapeTypeChange>,
    /// Parser-declared input keys that must be present for this drift surface.
    ///
    /// When populated by the producer, [`readiness_caveats`](Self::readiness_caveats)
    /// can distinguish removed optional/previously-observed keys from removed
    /// parser-required input keys.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_input_keys: Vec<String>,
    pub observed_at: String,
}

impl SourceShapeDriftObservation {
    /// Convert this checkpointed drift observation into readiness caveats.
    #[must_use]
    pub fn readiness_caveats(&self) -> Vec<SourceCaveat> {
        self.readiness_caveats_with_required_fields(&self.required_input_keys)
    }

    /// Convert this drift observation into readiness caveats while honoring
    /// parser-declared required input keys.
    #[must_use]
    pub fn readiness_caveats_with_required_fields(
        &self,
        required_input_keys: &[String],
    ) -> Vec<SourceCaveat> {
        source_shape_drift_readiness_caveats_with_required_fields(
            &self.source_unit_id,
            &self.current_hash,
            self.added_keys.len(),
            &self.removed_keys,
            self.type_changes.len(),
            required_input_keys,
        )
    }
}

/// Build the canonical readiness caveats for source-shape drift.
///
/// Added fields are advisory because existing parser mappings can usually
/// ignore them. Removed fields and type changes are degraded because they are
/// the shapes most likely to produce missing/defaulted parsed values.
#[must_use]
pub fn source_shape_drift_readiness_caveats(
    source_unit_id: &SourceUnitId,
    current_hash: &str,
    added_key_count: usize,
    removed_key_count: usize,
    type_change_count: usize,
) -> Vec<SourceCaveat> {
    let mut caveats = Vec::new();
    let evidence_ref = Some(format!("drift:{current_hash}"));

    if type_change_count > 0 {
        caveats.push(SourceCaveat {
            code: caveat_codes::PARSER_FIELD_TYPE_CHANGED.to_string(),
            severity: CaveatSeverity::Degraded,
            message: format!(
                "{} input field type(s) changed for source unit {}.",
                type_change_count,
                source_unit_id.as_str()
            ),
            evidence_ref: evidence_ref.clone(),
        });
    }

    if removed_key_count > 0 {
        caveats.push(SourceCaveat {
            code: caveat_codes::PARSER_REQUIRED_FIELD_MISSING.to_string(),
            severity: CaveatSeverity::Degraded,
            message: format!(
                "{} previously observed input field(s) are missing for source unit {}.",
                removed_key_count,
                source_unit_id.as_str()
            ),
            evidence_ref: evidence_ref.clone(),
        });
    }

    if added_key_count > 0 && caveats.is_empty() {
        caveats.push(SourceCaveat {
            code: caveat_codes::SOURCE_SHAPE_CHANGED.to_string(),
            severity: CaveatSeverity::Info,
            message: format!(
                "{} new input field(s) observed for source unit {}.",
                added_key_count,
                source_unit_id.as_str()
            ),
            evidence_ref,
        });
    }

    caveats
}

/// Build readiness caveats for source-shape drift using parser-declared
/// required input keys when they are available.
///
/// This is the fail-closed policy surface for parser manifests: removed fields
/// that were only previously observed degrade readiness, while removed fields
/// that the parser declares as required block readiness.
#[must_use]
pub fn source_shape_drift_readiness_caveats_with_required_fields(
    source_unit_id: &SourceUnitId,
    current_hash: &str,
    added_key_count: usize,
    removed_keys: &[String],
    type_change_count: usize,
    required_input_keys: &[String],
) -> Vec<SourceCaveat> {
    let mut caveats = Vec::new();
    let evidence_ref = Some(format!("drift:{current_hash}"));
    let required_missing = removed_required_input_keys(removed_keys, required_input_keys);

    if type_change_count > 0 {
        caveats.push(SourceCaveat {
            code: caveat_codes::PARSER_FIELD_TYPE_CHANGED.to_string(),
            severity: CaveatSeverity::Degraded,
            message: format!(
                "{} input field type(s) changed for source unit {}.",
                type_change_count,
                source_unit_id.as_str()
            ),
            evidence_ref: evidence_ref.clone(),
        });
    }

    if !removed_keys.is_empty() {
        let (severity, message) = if required_missing.is_empty() {
            (
                CaveatSeverity::Degraded,
                format!(
                    "{} previously observed input field(s) are missing for source unit {}.",
                    removed_keys.len(),
                    source_unit_id.as_str()
                ),
            )
        } else {
            (
                CaveatSeverity::Blocking,
                format!(
                    "{} required input field(s) are missing for source unit {}: {}.",
                    required_missing.len(),
                    source_unit_id.as_str(),
                    summarize_field_names(&required_missing)
                ),
            )
        };
        caveats.push(SourceCaveat {
            code: caveat_codes::PARSER_REQUIRED_FIELD_MISSING.to_string(),
            severity,
            message,
            evidence_ref: evidence_ref.clone(),
        });
    }

    if added_key_count > 0 && caveats.is_empty() {
        caveats.push(SourceCaveat {
            code: caveat_codes::SOURCE_SHAPE_CHANGED.to_string(),
            severity: CaveatSeverity::Info,
            message: format!(
                "{} new input field(s) observed for source unit {}.",
                added_key_count,
                source_unit_id.as_str()
            ),
            evidence_ref,
        });
    }

    caveats
}

fn removed_required_input_keys(
    removed_keys: &[String],
    required_input_keys: &[String],
) -> Vec<String> {
    required_input_keys
        .iter()
        .filter(|required| removed_keys.iter().any(|removed| removed == *required))
        .cloned()
        .collect()
}

fn summarize_field_names(fields: &[String]) -> String {
    const MAX_FIELD_NAMES: usize = 5;

    let shown = fields
        .iter()
        .take(MAX_FIELD_NAMES)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    let hidden = fields.len().saturating_sub(MAX_FIELD_NAMES);
    if hidden == 0 {
        shown
    } else {
        format!("{shown}, +{hidden} more")
    }
}

/// Response: `sources.drift.list`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesDriftListResponse {
    pub drifts: Vec<SourceShapeDriftObservation>,
}
