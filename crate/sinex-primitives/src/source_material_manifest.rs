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

/// Whether a metadata value was actually observed by the acquisition route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataAvailability {
    Observed,
    Unknown,
    NotApplicable,
    Withheld,
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
    pub manifest_type: String,
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
    /// Extension data is sorted to keep the canonical representation stable;
    /// unknown fields remain recoverable rather than being dropped.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, JsonValue>,
}

impl MaterialManifestV1 {
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
        if self.manifest_type != MATERIAL_MANIFEST_V1 {
            return Err("unsupported material manifest discriminator");
        }
        if self.bytes.encoded.algorithm.is_empty() || self.bytes.encoded.value_hex.is_empty() {
            return Err("encoded material digest is missing");
        }
        if self.bytes.parser_ranges.iter().any(|range| {
            range.start >= range.end || range.end > self.bytes.encoded_size
        }) {
            return Err("parser byte range is outside encoded material");
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
    fn discriminator_is_not_inferred() {
        let mut manifest = minimal_manifest();
        manifest.manifest_type = "LegacyManifestV0".to_string();
        assert_eq!(
            manifest.validate(),
            Err("unsupported material manifest discriminator")
        );
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
            manifest_type: MATERIAL_MANIFEST_V1.to_string(),
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
            extensions: BTreeMap::new(),
        }
    }
}
