//! Lossless, versioned metadata envelope for source materials.
//!
//! A source-material row is the provenance root for events, but its historical
//! `metadata` JSON is intentionally not the authority for replay.  This module
//! defines the typed envelope that will be stored beside the exact source bytes
//! in CAS.  Every field has an explicit availability state so an older importer
//! cannot silently turn "not captured" into a guessed value.

use blake3::Hash;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use uuid::Uuid;

/// The canonical manifest discriminator.  It is part of the serialized object,
/// not inferred from the database row or the filename.
pub const MATERIAL_MANIFEST_V1: &str = "MaterialManifestV1";
pub const LEGACY_MANIFEST_V0: &str = "LegacyManifestV0";

/// The manifest discriminator is a closed set with an explicit forward-
/// compatibility bucket.  Unknown values must survive a read so operators can
/// distinguish an unsupported manifest from a missing one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterialManifestType {
    V1,
    LegacyV0,
    Unknown(String),
}

impl MaterialManifestType {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::V1 => MATERIAL_MANIFEST_V1,
            Self::LegacyV0 => LEGACY_MANIFEST_V0,
            Self::Unknown(value) => value,
        }
    }
}

impl Serialize for MaterialManifestType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for MaterialManifestType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            MATERIAL_MANIFEST_V1 => Self::V1,
            LEGACY_MANIFEST_V0 => Self::LegacyV0,
            _ => Self::Unknown(value),
        })
    }
}

/// Fidelity is a claim about what the acquisition route actually observed.
/// It is never inferred from a source identifier or an event timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestFidelity {
    Exact,
    Partial,
    Legacy,
    Unknown,
}

/// Whether a metadata value was actually observed by the acquisition route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataAvailability {
    Observed,
    Unknown,
    NotApplicable,
    Withheld,
}

/// Field-level disclosure classification carried with the witness envelope.
/// This is metadata for routing and review, not a replacement for the runtime
/// access-control boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestPrivacyClass {
    Public,
    Personal,
    Sensitive,
    Secret,
    Unknown,
}

/// A value together with its capture status.  `Unknown` is deliberately not
/// represented as a null value: consumers must distinguish absent evidence
/// from a source that explicitly contained a null.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Captured<T> {
    pub availability: MetadataAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<T>,
}

impl<T> Captured<T> {
    #[must_use]
    pub const fn observed(value: T) -> Self {
        Self {
            availability: MetadataAvailability::Observed,
            value: Some(value),
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            availability: MetadataAvailability::Unknown,
            value: None,
        }
    }

    #[must_use]
    pub const fn not_applicable() -> Self {
        Self {
            availability: MetadataAvailability::NotApplicable,
            value: None,
        }
    }
}

/// A digest of an exact byte representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentDigest {
    pub algorithm: String,
    /// Lower-case hexadecimal digest.  The manifest does not assume that all
    /// future authorities use BLAKE3, but BLAKE3 is the current CAS algorithm.
    pub value_hex: String,
}

/// Exact logical and encoded representations of a material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterialBytes {
    pub encoded: ContentDigest,
    pub encoded_size: u64,
    pub logical: Captured<ContentDigest>,
    pub logical_size: Captured<u64>,
    /// Byte ranges in the encoded object used by a parser.  Ranges are
    /// inclusive-exclusive and must never be used to synthesize a new digest.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parser_ranges: Vec<ByteRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

/// Filesystem facts captured without normalizing away identity-bearing data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemEnvelope {
    pub path_bytes_base64: Captured<String>,
    pub file_type: Captured<String>,
    pub size: Captured<u64>,
    pub mode: Captured<u32>,
    pub uid: Captured<u32>,
    pub gid: Captured<u32>,
    pub inode: Captured<u64>,
    pub device: Captured<u64>,
    pub link_target_bytes_base64: Captured<String>,
    pub xattrs: Captured<BTreeMap<String, String>>,
    pub sparse_extents: Captured<Vec<ByteRange>>,
}

/// Container/archive facts and member relationships.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerEnvelope {
    pub format: Captured<String>,
    pub header_bytes: Captured<ContentDigest>,
    pub member_path_bytes_base64: Captured<Vec<String>>,
    pub member_metadata: Captured<BTreeMap<String, JsonValue>>,
}

/// Evidence about interpretation of the bytes, never a replacement for them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterpretationEnvelope {
    pub mime_type: Captured<String>,
    pub mime_detection_method: Captured<String>,
    pub charset: Captured<String>,
    pub bom: Captured<String>,
    pub newline_style: Captured<String>,
    pub embedded_metadata: Captured<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportEnvelope {
    pub authority: Captured<String>,
    pub locator: Captured<String>,
    pub revision: Captured<String>,
    pub etag: Captured<String>,
    pub fetched_at: Captured<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuityEnvelope {
    pub logical_source: Captured<String>,
    pub parent_material_id: Captured<Uuid>,
    pub part_index: Captured<u64>,
    pub part_count: Captured<u64>,
    pub continuation_key: Captured<String>,
    pub member_key: Captured<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceEnvelope {
    pub acquired_by: Captured<String>,
    pub parser: Captured<String>,
    pub parser_version: Captured<String>,
    pub extractor_versions: BTreeMap<String, String>,
}

/// Canonical metadata envelope for one registered source material.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaterialManifestV1 {
    pub manifest_type: MaterialManifestType,
    pub fidelity: ManifestFidelity,
    pub source_material_id: Uuid,
    pub source_identifier: String,
    pub material_kind: String,
    pub bytes: MaterialBytes,
    pub filesystem: FilesystemEnvelope,
    pub container: ContainerEnvelope,
    pub interpretation: InterpretationEnvelope,
    pub transport: TransportEnvelope,
    pub continuity: ContinuityEnvelope,
    pub provenance: ProvenanceEnvelope,
    pub temporal_evidence: BTreeMap<String, JsonValue>,
    /// Keys are manifest field paths.  The `*` entry is the explicit fallback
    /// for fields not classified by a legacy route.
    #[serde(default)]
    pub privacy_classification: BTreeMap<String, ManifestPrivacyClass>,
    /// Extension data is sorted to keep the canonical representation stable;
    /// unknown fields remain recoverable rather than being dropped.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, JsonValue>,
    /// Forward-compatible top-level fields.  They are retained verbatim and
    /// included in canonical bytes instead of being silently discarded by
    /// serde when a newer producer adds a field.
    #[serde(flatten)]
    pub unknown_fields: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegacyManifestV0 {
    pub manifest_type: MaterialManifestType,
    #[serde(default)]
    pub source_material_id: Option<Uuid>,
    #[serde(default)]
    pub source_identifier: Option<String>,
    #[serde(default)]
    pub material_kind: Option<String>,
    #[serde(flatten)]
    pub fields: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnknownMaterialManifest {
    pub manifest_type: MaterialManifestType,
    #[serde(flatten)]
    pub fields: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DecodedMaterialManifest {
    V1(MaterialManifestV1),
    Legacy(LegacyManifestV0),
    Unknown(UnknownMaterialManifest),
}

fn captured_string_field(
    object: Option<&serde_json::Map<String, JsonValue>>,
    key: &str,
) -> Captured<String> {
    object
        .and_then(|object| object.get(key))
        .and_then(JsonValue::as_str)
        .map_or_else(Captured::unknown, |value| {
            Captured::observed(value.to_string())
        })
}

impl MaterialManifestV1 {
    /// Build the manifest emitted by the generic material assembler.  Route-
    /// specific enrichers can replace explicit `Unknown` fields later, but the
    /// generic path always emits a complete envelope and retains its original
    /// metadata under `extensions`.
    #[must_use]
    pub fn from_capture(
        source_material_id: Uuid,
        source_identifier: impl Into<String>,
        material_kind: impl Into<String>,
        content_hash: impl Into<String>,
        encoded_size: u64,
        metadata: JsonValue,
        started_at: impl Into<String>,
        ended_at: impl Into<String>,
    ) -> Self {
        let source_identifier = source_identifier.into();
        let material_kind = material_kind.into();
        let content_hash = content_hash.into();
        let metadata = match metadata {
            JsonValue::Object(_) => metadata,
            other => serde_json::json!({"value": other}),
        };
        let metadata_object = metadata.as_object().cloned();
        let embedded_metadata = metadata_object
            .as_ref()
            .and_then(|object| object.get("embedded_metadata"))
            .and_then(JsonValue::as_object)
            .map(|object| {
                Captured::observed(
                    object
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                )
            })
            .unwrap_or_else(Captured::unknown);
        let privacy_class = metadata_object
            .as_ref()
            .and_then(|object| object.get("privacy_class"))
            .and_then(JsonValue::as_str)
            .map_or(ManifestPrivacyClass::Unknown, |value| match value {
                "public" => ManifestPrivacyClass::Public,
                "personal" => ManifestPrivacyClass::Personal,
                "sensitive" => ManifestPrivacyClass::Sensitive,
                "secret" => ManifestPrivacyClass::Secret,
                _ => ManifestPrivacyClass::Unknown,
            });
        let mut temporal_evidence = BTreeMap::new();
        temporal_evidence.insert(
            "captured_started_at".to_string(),
            JsonValue::String(started_at.into()),
        );
        temporal_evidence.insert(
            "captured_ended_at".to_string(),
            JsonValue::String(ended_at.into()),
        );

        let mut extensions = BTreeMap::new();
        extensions.insert("capture_metadata".to_string(), metadata);

        Self {
            manifest_type: MaterialManifestType::V1,
            fidelity: ManifestFidelity::Partial,
            source_material_id,
            source_identifier,
            material_kind,
            bytes: MaterialBytes {
                encoded: ContentDigest {
                    algorithm: "blake3".to_string(),
                    value_hex: content_hash,
                },
                encoded_size,
                logical: Captured::unknown(),
                logical_size: Captured::unknown(),
                parser_ranges: Vec::new(),
            },
            filesystem: FilesystemEnvelope {
                path_bytes_base64: Captured::unknown(),
                file_type: Captured::unknown(),
                size: Captured::unknown(),
                mode: Captured::unknown(),
                uid: Captured::unknown(),
                gid: Captured::unknown(),
                inode: Captured::unknown(),
                device: Captured::unknown(),
                link_target_bytes_base64: Captured::unknown(),
                xattrs: Captured::unknown(),
                sparse_extents: Captured::unknown(),
            },
            container: ContainerEnvelope {
                format: Captured::unknown(),
                header_bytes: Captured::unknown(),
                member_path_bytes_base64: Captured::unknown(),
                member_metadata: Captured::unknown(),
            },
            interpretation: InterpretationEnvelope {
                mime_type: metadata_object
                    .as_ref()
                    .and_then(|object| {
                        object
                            .get("mime_type")
                            .or_else(|| object.get("content_type"))
                    })
                    .and_then(JsonValue::as_str)
                    .map_or_else(Captured::unknown, |value| {
                        Captured::observed(value.to_string())
                    }),
                mime_detection_method: captured_string_field(
                    metadata_object.as_ref(),
                    "mime_detection_method",
                ),
                charset: metadata_object
                    .as_ref()
                    .and_then(|object| object.get("charset").or_else(|| object.get("encoding")))
                    .and_then(JsonValue::as_str)
                    .map_or_else(Captured::unknown, |value| {
                        Captured::observed(value.to_string())
                    }),
                bom: captured_string_field(metadata_object.as_ref(), "bom"),
                newline_style: captured_string_field(metadata_object.as_ref(), "newline_style"),
                embedded_metadata,
            },
            transport: TransportEnvelope {
                authority: Captured::not_applicable(),
                locator: Captured::unknown(),
                revision: Captured::unknown(),
                etag: Captured::unknown(),
                fetched_at: Captured::unknown(),
            },
            continuity: ContinuityEnvelope {
                logical_source: captured_string_field(
                    metadata_object.as_ref(),
                    "logical_source_identifier",
                ),
                parent_material_id: Captured::unknown(),
                part_index: Captured::unknown(),
                part_count: Captured::unknown(),
                continuation_key: Captured::unknown(),
                member_key: Captured::unknown(),
            },
            provenance: ProvenanceEnvelope {
                acquired_by: captured_string_field(metadata_object.as_ref(), "acquired_by"),
                parser: captured_string_field(metadata_object.as_ref(), "parser"),
                parser_version: captured_string_field(metadata_object.as_ref(), "parser_version"),
                extractor_versions: BTreeMap::new(),
            },
            temporal_evidence,
            privacy_classification: BTreeMap::from([("*".to_string(), privacy_class)]),
            extensions,
            unknown_fields: BTreeMap::new(),
        }
    }

    /// Decode a manifest without conflating legacy, unknown, and malformed
    /// inputs.  Legacy and unknown payloads remain available to migration and
    /// inventory tooling, while replay can fail closed on them.
    pub fn decode(bytes: &[u8]) -> Result<DecodedMaterialManifest, String> {
        let value: JsonValue = serde_json::from_slice(bytes)
            .map_err(|error| format!("invalid material manifest JSON: {error}"))?;
        let Some(manifest_type) = value.get("manifest_type").and_then(JsonValue::as_str) else {
            return Ok(DecodedMaterialManifest::Unknown(UnknownMaterialManifest {
                manifest_type: MaterialManifestType::Unknown("<missing>".to_string()),
                fields: value
                    .as_object()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            }));
        };
        match manifest_type {
            MATERIAL_MANIFEST_V1 => serde_json::from_value(value)
                .map(DecodedMaterialManifest::V1)
                .map_err(|error| format!("invalid MaterialManifestV1: {error}")),
            LEGACY_MANIFEST_V0 => serde_json::from_value(value)
                .map(DecodedMaterialManifest::Legacy)
                .map_err(|error| format!("invalid LegacyManifestV0: {error}")),
            _ => {
                let fields = value
                    .as_object()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                Ok(DecodedMaterialManifest::Unknown(UnknownMaterialManifest {
                    manifest_type: MaterialManifestType::Unknown(manifest_type.to_string()),
                    fields,
                }))
            }
        }
    }

    /// Validate and convert an inclusive-exclusive byte range for replay.
    pub fn exact_range(
        &self,
        range: ByteRange,
    ) -> Result<std::ops::Range<usize>, &'static str> {
        if range.start >= range.end || range.end > self.bytes.encoded_size {
            return Err("material replay range is outside encoded material");
        }
        let start = usize::try_from(range.start)
            .map_err(|_| "material replay range does not fit host index")?;
        let end = usize::try_from(range.end)
            .map_err(|_| "material replay range does not fit host index")?;
        Ok(start..end)
    }

    /// Return the deterministic JSON representation used as the CAS object.
    /// Struct field order is fixed by declaration and nested JSON objects are
    /// recursively sorted as well.  This matters for extractor extensions,
    /// which may arrive from parsers using insertion-ordered JSON maps.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let value = serde_json::to_value(self)?;
        serde_json::to_vec(&canonicalize_json(value))
    }

    /// Compute the CAS identity of the canonical manifest object itself.
    pub fn canonical_digest(&self) -> Result<ContentDigest, serde_json::Error> {
        let hash: Hash = blake3::hash(&self.canonical_bytes()?);
        Ok(ContentDigest {
            algorithm: "blake3".to_string(),
            value_hex: hash.to_hex().to_string(),
        })
    }

    /// Reject discriminator drift before a manifest is persisted or replayed.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.manifest_type != MaterialManifestType::V1 {
            return Err("unsupported material manifest discriminator");
        }
        if self.bytes.encoded.algorithm.is_empty() || self.bytes.encoded.value_hex.is_empty() {
            return Err("encoded material digest is missing");
        }
        if self
            .bytes
            .parser_ranges
            .iter()
            .any(|range| range.start >= range.end || range.end > self.bytes.encoded_size)
        {
            return Err("parser byte range is outside encoded material");
        }
        if self.privacy_classification.is_empty() {
            return Err("manifest privacy classification is missing");
        }
        Ok(())
    }
}

fn canonicalize_json(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = serde_json::Map::new();
            for (key, value) in entries {
                sorted.insert(key, canonicalize_json(value));
            }
            JsonValue::Object(sorted)
        }
        JsonValue::Array(values) => {
            JsonValue::Array(values.into_iter().map(canonicalize_json).collect())
        }
        scalar => scalar,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_values_are_explicit_and_round_trip() {
        let value = Captured::<String>::unknown();
        let json = serde_json::to_string(&value).expect("serialize");
        assert!(json.contains("unknown"));
        assert!(!json.contains("value"));
        let decoded: Captured<String> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, value);
    }

    #[test]
    fn canonical_manifest_bytes_and_digest_are_stable() {
        let manifest = minimal_manifest();
        let first = manifest.canonical_bytes().expect("canonical bytes");
        let second = manifest.canonical_bytes().expect("canonical bytes");
        assert_eq!(first, second);
        let digest = manifest.canonical_digest().expect("digest");
        assert_eq!(digest.algorithm, "blake3");
        assert_eq!(digest.value_hex.len(), 64);
        manifest.validate().expect("valid manifest");
    }

    #[test]
    fn nested_extension_objects_are_canonicalized() {
        let value = serde_json::json!({"b": 1, "a": {"d": 1, "c": 2}});
        let canonical = serde_json::to_vec(&canonicalize_json(value)).expect("serialize");
        assert_eq!(canonical, br#"{"a":{"c":2,"d":1},"b":1}"#);
    }

    #[test]
    fn generic_capture_preserves_available_route_metadata() {
        let manifest = MaterialManifestV1::from_capture(
            Uuid::nil(),
            "archive.bin",
            "local_cas",
            &"a".repeat(64),
            3,
            serde_json::json!({
                "mime_type": "text/plain",
                "encoding": "utf-8",
                "logical_source_identifier": "terminal.text-history",
                "privacy_class": "personal",
            }),
            "2026-08-12T00:00:00Z",
            "2026-08-12T00:00:01Z",
        );
        assert_eq!(
            manifest.interpretation.mime_type,
            Captured::observed("text/plain".to_string())
        );
        assert_eq!(
            manifest.interpretation.charset,
            Captured::observed("utf-8".to_string())
        );
        assert_eq!(
            manifest.continuity.logical_source,
            Captured::observed("terminal.text-history".to_string())
        );
        assert_eq!(
            manifest.privacy_classification.get("*"),
            Some(&ManifestPrivacyClass::Personal)
        );
        manifest.validate().expect("generic capture must be valid");
    }

    #[test]
    fn discriminator_is_not_inferred() {
        let mut manifest = minimal_manifest();
        manifest.manifest_type = MaterialManifestType::LegacyV0;
        assert_eq!(
            manifest.validate(),
            Err("unsupported material manifest discriminator")
        );
    }

    #[test]
    fn legacy_and_unknown_manifests_are_explicitly_classified() {
        let legacy = MaterialManifestV1::decode(
            br#"{"manifest_type":"LegacyManifestV0","source_identifier":"old.log"}"#,
        )
        .expect("legacy manifest should decode");
        assert!(matches!(legacy, DecodedMaterialManifest::Legacy(_)));

        let unknown = MaterialManifestV1::decode(
            br#"{"manifest_type":"MaterialManifestV9","future_field":{"kept":true}}"#,
        )
        .expect("unknown manifest should decode");
        let DecodedMaterialManifest::Unknown(unknown) = unknown else {
            panic!("unknown manifest must remain unknown");
        };
        assert_eq!(unknown.manifest_type.as_str(), "MaterialManifestV9");
        assert_eq!(unknown.fields["future_field"]["kept"], true);
    }

    #[test]
    fn unknown_v1_fields_survive_canonical_round_trip() {
        let mut value = serde_json::to_value(minimal_manifest()).expect("serialize fixture");
        value["future_field"] = serde_json::json!({"b": 1, "a": 2});
        let manifest: MaterialManifestV1 = serde_json::from_value(value).expect("decode v1");
        let canonical = manifest.canonical_bytes().expect("canonical bytes");
        assert!(String::from_utf8(canonical).expect("utf8").contains("future_field"));
    }

    #[test]
    fn exact_replay_ranges_are_bounded_by_manifest_size() {
        let manifest = minimal_manifest();
        assert_eq!(
            manifest
                .exact_range(ByteRange { start: 0, end: 1 })
                .expect("bounded range"),
            0..1
        );
        assert!(manifest
            .exact_range(ByteRange { start: 0, end: 2 })
            .is_err());
    }

    #[test]
    fn byte_ranges_must_be_bounded_and_non_empty() {
        let mut manifest = minimal_manifest();
        manifest.bytes.parser_ranges = vec![ByteRange { start: 1, end: 2 }];
        assert_eq!(
            manifest.validate(),
            Err("parser byte range is outside encoded material")
        );
    }

    fn minimal_manifest() -> MaterialManifestV1 {
        MaterialManifestV1 {
            manifest_type: MaterialManifestType::V1,
            fidelity: ManifestFidelity::Exact,
            source_material_id: Uuid::nil(),
            source_identifier: "fixture/source".to_string(),
            material_kind: "local_cas".to_string(),
            bytes: MaterialBytes {
                encoded: ContentDigest {
                    algorithm: "blake3".to_string(),
                    value_hex: "a".repeat(64),
                },
                encoded_size: 1,
                logical: Captured::unknown(),
                logical_size: Captured::unknown(),
                parser_ranges: vec![ByteRange { start: 0, end: 1 }],
            },
            filesystem: FilesystemEnvelope {
                path_bytes_base64: Captured::observed("c291cmNl".to_string()),
                file_type: Captured::observed("regular".to_string()),
                size: Captured::observed(1),
                mode: Captured::unknown(),
                uid: Captured::unknown(),
                gid: Captured::unknown(),
                inode: Captured::unknown(),
                device: Captured::unknown(),
                link_target_bytes_base64: Captured::not_applicable(),
                xattrs: Captured::unknown(),
                sparse_extents: Captured::not_applicable(),
            },
            container: ContainerEnvelope {
                format: Captured::not_applicable(),
                header_bytes: Captured::not_applicable(),
                member_path_bytes_base64: Captured::not_applicable(),
                member_metadata: Captured::not_applicable(),
            },
            interpretation: InterpretationEnvelope {
                mime_type: Captured::unknown(),
                mime_detection_method: Captured::unknown(),
                charset: Captured::unknown(),
                bom: Captured::unknown(),
                newline_style: Captured::unknown(),
                embedded_metadata: Captured::unknown(),
            },
            transport: TransportEnvelope {
                authority: Captured::not_applicable(),
                locator: Captured::unknown(),
                revision: Captured::unknown(),
                etag: Captured::unknown(),
                fetched_at: Captured::unknown(),
            },
            continuity: ContinuityEnvelope {
                logical_source: Captured::observed("fixture".to_string()),
                parent_material_id: Captured::not_applicable(),
                part_index: Captured::not_applicable(),
                part_count: Captured::not_applicable(),
                continuation_key: Captured::unknown(),
                member_key: Captured::unknown(),
            },
            provenance: ProvenanceEnvelope {
                acquired_by: Captured::observed("fixture".to_string()),
                parser: Captured::unknown(),
                parser_version: Captured::unknown(),
                extractor_versions: BTreeMap::new(),
            },
            temporal_evidence: BTreeMap::new(),
            privacy_classification: BTreeMap::from([(
                "*".to_string(),
                ManifestPrivacyClass::Unknown,
            )]),
            extensions: BTreeMap::new(),
            unknown_fields: BTreeMap::new(),
        }
    }
}
