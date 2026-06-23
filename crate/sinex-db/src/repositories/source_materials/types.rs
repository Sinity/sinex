use super::helpers::contract_for_source;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use sinex_primitives::domain::{
    MaterialStatus, MaterialStorageKind, SourceMaterialFormat, SourceMaterialTimingInfoType,
    TemporalClock, TemporalPrecision, TemporalSourceType,
};
use sinex_primitives::rpc::sources::SourceMaterialMetadataContract;
use sinex_primitives::{Id, Timestamp, events::OffsetKind};
use uuid::Uuid;

/// Canonical storage/backend kinds recognised by the registry.
///
/// These values select how raw material is stored or addressed. They are not
/// capture-package material classes; richer classes such as transcript
/// documents, OCR segments, API pages, or live stream segments belong in
/// material metadata and package-mode contracts.
pub mod material_kinds {
    use sinex_primitives::domain::MaterialStorageKind as K;

    pub const ANNEX: K = K::Annex;
    pub const GIT: K = K::Git;
    pub const LOCAL_CAS: K = K::LocalCas;
}
/// Canonical timing info types — use `SourceMaterialTimingInfoType` variants directly.
///
/// These string constants are kept only for raw SQL queries that cannot use the
/// typed enum. Prefer `SourceMaterialTimingInfoType::Realtime.as_str()` etc.
pub mod timing_info_types {
    use sinex_primitives::domain::SourceMaterialTimingInfoType as T;
    pub const REALTIME: &str = T::Realtime.as_str();
    pub const INTRINSIC: &str = T::Intrinsic.as_str();
    pub const INFERRED: &str = T::Inferred.as_str();
    pub const DECLARED: &str = T::Declared.as_str();
    pub const ATEMPORAL: &str = T::Atemporal.as_str();
    pub const STAGED_AT: &str = T::StagedAt.as_str();
}
/// Canonical material type constants stored in metadata.
pub mod material_types {
    pub const FILE: &str = "file";
    pub const STREAM: &str = "stream";
    pub const BLOB: &str = "blob";
    pub const BLOB_BINARY: &str = "blob.binary";
    pub const BLOB_TEXT: &str = "blob.text";
    pub const CHUNK: &str = "chunk";
}
/// Canonical relation types for source-material evidence links.
pub mod relation_types {
    /// The source material on the left is backed by auxiliary evidence on the right.
    ///
    /// Example: a JSONL row-stream material backed by a `SQLite` snapshot material.
    pub const BACKED_BY: &str = "backed_by";
}
/// Source material registration payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMaterial {
    pub(super) material_kind: MaterialStorageKind,
    pub(super) source_identifier: String,
    pub(super) timing_info_type: String,
    pub(super) status: MaterialStatus,
    pub(super) metadata: JsonValue,
    pub(super) optional_blob_id: Option<Id<crate::Blob>>,
    pub start_time: Option<Timestamp>,
    pub end_time: Option<Timestamp>,
    pub(super) staged_by: Option<String>,
    pub(super) staged_on_host: Option<String>,
}
impl SourceMaterial {
    pub(super) fn new(
        material_kind: MaterialStorageKind,
        source_identifier: impl Into<String>,
    ) -> Self {
        Self {
            material_kind,
            source_identifier: source_identifier.into(),
            timing_info_type: timing_info_types::INTRINSIC.to_string(),
            status: MaterialStatus::Completed,
            metadata: json!({}),
            optional_blob_id: None,
            start_time: None,
            end_time: None,
            staged_by: None,
            staged_on_host: None,
        }
    }
    #[allow(clippy::expect_used)] // invariant: metadata set to object on line above
    pub(super) fn metadata_object_mut(&mut self) -> &mut JsonMap<String, JsonValue> {
        if !self.metadata.is_object() {
            self.metadata = json!({});
        }
        self.metadata
            .as_object_mut()
            .expect("metadata forced to object")
    }
    pub(super) fn merge_metadata(&mut self, extra: JsonValue) {
        match extra {
            JsonValue::Object(map) => {
                let target = self.metadata_object_mut();
                for (key, value) in map {
                    target.insert(key, value);
                }
            }
            JsonValue::Null => {}
            other => {
                let target = self.metadata_object_mut();
                target.insert("_meta".to_string(), other);
            }
        }
    }
    /// Create a file-backed source material entry.
    pub fn file(path: impl Into<String>) -> Self {
        let path_str = path.into();
        let mut material = Self::new(material_kinds::ANNEX, path_str.clone());
        material.metadata_object_mut().insert(
            "source_uri".to_string(),
            JsonValue::String(path_str.clone()),
        );
        material.metadata_object_mut().insert(
            "material_type".to_string(),
            JsonValue::String(material_types::FILE.to_string()),
        );
        let contract = contract_for_source(
            SourceMaterialFormat::infer_from_path(&path_str),
            SourceMaterialTimingInfoType::Intrinsic,
            Some(&path_str),
            None,
        );
        material.merge_metadata(contract.metadata_patch());
        material
    }
    /// Create a stream-backed source material entry.
    pub fn stream(uri: impl Into<String>) -> Self {
        let uri_str = uri.into();
        let mut material = Self::new(material_kinds::ANNEX, uri_str.clone());
        material
            .metadata_object_mut()
            .insert("source_uri".to_string(), JsonValue::String(uri_str.clone()));
        material.metadata_object_mut().insert(
            "material_type".to_string(),
            JsonValue::String(material_types::STREAM.to_string()),
        );
        let contract = contract_for_source(
            SourceMaterialFormat::Jsonl,
            SourceMaterialTimingInfoType::Realtime,
            Some(&uri_str),
            None,
        );
        material.merge_metadata(contract.metadata_patch());
        material.with_timing_info_type(timing_info_types::REALTIME)
    }
    /// Create an in-memory blob source material entry.
    #[must_use]
    pub fn blob() -> Self {
        let mut material = Self::new(material_kinds::ANNEX, "memory://inline");
        material.metadata_object_mut().insert(
            "material_type".to_string(),
            JsonValue::String(material_types::BLOB.to_string()),
        );
        let contract = contract_for_source(
            SourceMaterialFormat::Binary,
            SourceMaterialTimingInfoType::Intrinsic,
            Some("memory://inline"),
            None,
        );
        material.merge_metadata(contract.metadata_patch());
        material
    }
    /// Create a binary blob source material entry.
    pub fn blob_binary(filename: impl Into<String>) -> Self {
        let filename = filename.into();
        let mut material = Self::new(material_kinds::ANNEX, filename.clone());
        let metadata = material.metadata_object_mut();
        metadata.insert("filename".to_string(), JsonValue::String(filename.clone()));
        metadata.insert(
            "material_type".to_string(),
            JsonValue::String(material_types::BLOB_BINARY.to_string()),
        );
        let contract = contract_for_source(
            SourceMaterialFormat::Binary,
            SourceMaterialTimingInfoType::Intrinsic,
            Some(&filename),
            None,
        );
        material.merge_metadata(contract.metadata_patch());
        material
    }
    /// Create a text blob source material entry.
    pub fn blob_text(filename: impl Into<String>) -> Self {
        let filename = filename.into();
        let mut material = Self::new(material_kinds::ANNEX, filename.clone());
        {
            let metadata = material.metadata_object_mut();
            metadata.insert("filename".to_string(), JsonValue::String(filename.clone()));
            metadata.insert(
                "material_type".to_string(),
                JsonValue::String(material_types::BLOB_TEXT.to_string()),
            );
            metadata.insert(
                "encoding".to_string(),
                JsonValue::String("utf-8".to_string()),
            );
        }
        let contract = contract_for_source(
            SourceMaterialFormat::Text,
            SourceMaterialTimingInfoType::Intrinsic,
            Some(&filename),
            None,
        );
        material.merge_metadata(contract.metadata_patch());
        material
    }
    /// Create a chunk source material (for large file processing)
    pub fn chunk(parent_id: impl Into<String>, index: usize) -> Self {
        let identifier = format!("chunk://{}#{}", parent_id.into(), index);
        let mut material = Self::new(material_kinds::ANNEX, identifier.clone());
        let metadata = material.metadata_object_mut();
        metadata.insert(
            "chunk_uri".to_string(),
            JsonValue::String(identifier.clone()),
        );
        metadata.insert(
            "material_type".to_string(),
            JsonValue::String(material_types::CHUNK.to_string()),
        );
        let contract = contract_for_source(
            SourceMaterialFormat::Binary,
            SourceMaterialTimingInfoType::Intrinsic,
            Some(&identifier),
            None,
        );
        material.merge_metadata(contract.metadata_patch());
        material
    }
    /// Fluent method to set blob ID
    #[must_use]
    pub fn with_blob_id(mut self, blob_id: Id<crate::Blob>) -> Self {
        self.optional_blob_id = Some(blob_id);
        self
    }
    /// Fluent method to set an optional blob ID.
    #[must_use]
    pub fn with_optional_blob_id(mut self, blob_id: Option<Id<crate::Blob>>) -> Self {
        self.optional_blob_id = blob_id;
        self
    }
    /// Fluent method to set encoding (stored in metadata)
    pub fn with_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.metadata_object_mut()
            .insert("encoding".to_string(), JsonValue::String(encoding.into()));
        self
    }
    /// Fluent method to set metadata (merged with existing entries)
    #[must_use]
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.merge_metadata(metadata);
        self
    }
    /// Fluent method to set the versioned source-material metadata contract.
    #[must_use]
    pub fn with_metadata_contract(mut self, contract: &SourceMaterialMetadataContract) -> Self {
        self.timing_info_type = contract.timing.to_string();
        self.merge_metadata(contract.metadata_patch());
        self
    }
    /// Fluent method to set content preview (stored in metadata)
    pub fn with_content_preview(mut self, preview: impl Into<String>) -> Self {
        self.metadata_object_mut().insert(
            "content_preview".to_string(),
            JsonValue::String(preview.into()),
        );
        self
    }
    /// Fluent method to set retention policy (stored in metadata)
    pub fn with_retention_policy(mut self, policy: impl Into<String>) -> Self {
        self.metadata_object_mut().insert(
            "retention_policy".to_string(),
            JsonValue::String(policy.into()),
        );
        self
    }
    /// Fluent method to override the status
    #[must_use]
    pub fn with_status(mut self, status: MaterialStatus) -> Self {
        self.status = status;
        self
    }
    /// Fluent method to override the timing info type
    pub fn with_timing_info_type(mut self, timing: impl Into<String>) -> Self {
        self.timing_info_type = timing.into();
        self
    }
    #[must_use]
    pub fn with_start_time(mut self, start_time: Timestamp) -> Self {
        self.start_time = Some(start_time);
        self
    }
    #[must_use]
    pub fn with_end_time(mut self, end_time: Timestamp) -> Self {
        self.end_time = Some(end_time);
        self
    }
    pub fn with_staged_by(mut self, staged_by: impl Into<String>) -> Self {
        self.staged_by = Some(staged_by.into());
        self
    }
    pub fn with_staged_on_host(mut self, host: impl Into<String>) -> Self {
        self.staged_on_host = Some(host.into());
        self
    }
}
/// Directional evidence link between two source materials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMaterialLink {
    pub from_material_id: Uuid,
    pub to_material_id: Uuid,
    pub relation_type: String,
    pub metadata: JsonValue,
}

impl SourceMaterialLink {
    /// Create a source-material link with empty metadata.
    pub fn new(
        from_material_id: impl Into<Uuid>,
        to_material_id: impl Into<Uuid>,
        relation_type: impl Into<String>,
    ) -> Self {
        Self {
            from_material_id: from_material_id.into(),
            to_material_id: to_material_id.into(),
            relation_type: relation_type.into(),
            metadata: json!({}),
        }
    }

    /// Create a canonical `backed_by` evidence link.
    pub fn backed_by(from_material_id: impl Into<Uuid>, to_material_id: impl Into<Uuid>) -> Self {
        Self::new(from_material_id, to_material_id, relation_types::BACKED_BY)
    }

    /// Deep-merge additional metadata into this link payload.
    ///
    /// Existing keys are overwritten by `metadata` on conflict; nested objects
    /// are merged recursively (rather than replaced wholesale).
    #[must_use]
    pub fn with_metadata(mut self, metadata: JsonValue) -> Self {
        match (&mut self.metadata, metadata) {
            (JsonValue::Object(existing), JsonValue::Object(incoming)) => {
                merge_json_objects(existing, incoming);
            }
            (existing, incoming) => {
                *existing = incoming;
            }
        }
        self
    }
}

/// Recursively merge `incoming` into `target`, with incoming values winning on conflict.
fn merge_json_objects(
    target: &mut JsonMap<String, JsonValue>,
    incoming: JsonMap<String, JsonValue>,
) {
    for (key, incoming_val) in incoming {
        match (target.get_mut(&key), incoming_val) {
            (Some(JsonValue::Object(existing_obj)), JsonValue::Object(incoming_obj)) => {
                merge_json_objects(existing_obj, incoming_obj);
            }
            (existing_slot, incoming_val) => {
                if let Some(slot) = existing_slot {
                    *slot = incoming_val;
                } else {
                    target.insert(key, incoming_val);
                }
            }
        }
    }
}

/// Entry for the `raw.temporal_ledger` table.
///
/// Tracks timing metadata for source materials, including capture windows
/// and clock synchronization information.
#[derive(Debug, Clone)]
pub struct TemporalLedgerEntry {
    /// ID of the source material this entry refers to
    pub source_material_id: uuid::Uuid,
    /// Start offset within the source material
    pub offset_start: i64,
    /// End offset within the source material
    pub offset_end: i64,
    /// Offset kind for the recorded range.
    pub offset_kind: OffsetKind,
    /// Capture timestamp
    pub ts_capture: Timestamp,
    /// Precision of the capture timing
    pub precision: TemporalPrecision,
    /// Clock type used
    pub clock: TemporalClock,
    /// How the capture timestamp was determined
    pub source_type: TemporalSourceType,
}
impl TemporalLedgerEntry {
    /// Create a new ledger entry for a realtime capture
    #[must_use]
    pub fn realtime_capture(
        source_material_id: uuid::Uuid,
        offset_end: i64,
        ts_capture: Timestamp,
    ) -> Self {
        Self {
            source_material_id,
            offset_start: 0,
            offset_end,
            offset_kind: OffsetKind::Byte,
            ts_capture,
            precision: TemporalPrecision::Bounded,
            clock: TemporalClock::Wall,
            source_type: TemporalSourceType::RealtimeCapture,
        }
    }

    /// Create a `staged_at` ledger entry — the fallback timestamp for material
    /// events that lack an intrinsic or inferred timestamp from the content.
    ///
    /// Written at material-begin time so that `LedgerReader::derive_ts_orig()`
    /// always finds a persisted timestamp instead of falling back to an
    /// ephemeral `Timestamp::now()`. This makes material `ts_orig` reproducible
    /// across replays.
    #[must_use]
    pub fn staged_at(
        source_material_id: uuid::Uuid,
        offset_end: i64,
        ts_capture: Timestamp,
    ) -> Self {
        Self {
            source_material_id,
            offset_start: 0,
            offset_end,
            offset_kind: OffsetKind::Byte,
            ts_capture,
            precision: TemporalPrecision::Bounded,
            clock: TemporalClock::Wall,
            source_type: TemporalSourceType::StagedAt,
        }
    }

    #[must_use]
    pub fn intrinsic_content(
        source_material_id: uuid::Uuid,
        offset_end: i64,
        ts_capture: Timestamp,
    ) -> Self {
        Self {
            source_material_id,
            offset_start: 0,
            offset_end,
            offset_kind: OffsetKind::Byte,
            ts_capture,
            precision: TemporalPrecision::Exact,
            clock: TemporalClock::Wall,
            source_type: TemporalSourceType::IntrinsicContent,
        }
    }

    #[must_use]
    pub fn inferred_mtime(
        source_material_id: uuid::Uuid,
        offset_end: i64,
        ts_capture: Timestamp,
    ) -> Self {
        Self {
            source_material_id,
            offset_start: 0,
            offset_end,
            offset_kind: OffsetKind::Byte,
            ts_capture,
            precision: TemporalPrecision::Bounded,
            clock: TemporalClock::Wall,
            source_type: TemporalSourceType::InferredMtime,
        }
    }

    #[must_use]
    pub fn inferred_user(
        source_material_id: uuid::Uuid,
        offset_end: i64,
        ts_capture: Timestamp,
    ) -> Self {
        Self {
            source_material_id,
            offset_start: 0,
            offset_end,
            offset_kind: OffsetKind::Byte,
            ts_capture,
            precision: TemporalPrecision::Bounded,
            clock: TemporalClock::Wall,
            source_type: TemporalSourceType::InferredUser,
        }
    }
}
