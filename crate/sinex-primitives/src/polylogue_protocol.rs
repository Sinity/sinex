//! Polylogue material-protocol v1.
//!
//! The protocol keeps exact immutable NDJSON in registered source materials.
//! Sinex events carry typed facts and confirmed anchors, never transcript or
//! tool text.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

use crate::error::{Result, SinexError};

pub const PROTOCOL_VERSION: &str = "polylogue.material-protocol/v1";
pub const SEMANTICS_VERSION: u16 = 2;
pub const CANONICALIZER_VERSION: u16 = 1;
pub const HEAD_SEGMENT_INDEX: i32 = -1;
pub const ORIGIN_VOCABULARY_VERSION: u16 = 3;
pub const ORIGIN_VOCABULARY_DIGEST: &str =
    "f05126b022becf8fcebe9622919465b5e1f86163c25ecdda9d7e1259caba3512";

/// Complete normalized record vocabulary emitted by protocol v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolylogueRecordKind {
    Session,
    Lineage,
    Usage,
    Message,
    Block,
    Attachment,
    SessionEvent,
}

impl PolylogueRecordKind {
    pub const HEAD: [Self; 3] = [Self::Session, Self::Lineage, Self::Usage];
    pub const TRANSCRIPT: [Self; 4] = [
        Self::Message,
        Self::Block,
        Self::Attachment,
        Self::SessionEvent,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Lineage => "lineage",
            Self::Usage => "usage",
            Self::Message => "message",
            Self::Block => "block",
            Self::Attachment => "attachment",
            Self::SessionEvent => "session_event",
        }
    }

    #[must_use]
    pub const fn event_type(self) -> &'static str {
        match self {
            Self::Session => "integration.polylogue.session.observed",
            Self::Lineage => "integration.polylogue.lineage.observed",
            Self::Usage => "integration.polylogue.usage.observed",
            Self::Message => "integration.polylogue.message.observed",
            Self::Block => "integration.polylogue.block.observed",
            Self::Attachment => "integration.polylogue.attachment.observed",
            Self::SessionEvent => "integration.polylogue.session_event.observed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "session" => Ok(Self::Session),
            "lineage" => Ok(Self::Lineage),
            "usage" => Ok(Self::Usage),
            "message" => Ok(Self::Message),
            "block" => Ok(Self::Block),
            "attachment" => Ok(Self::Attachment),
            "session_event" => Ok(Self::SessionEvent),
            other => Err(protocol_error(format!("unknown record kind {other:?}"))),
        }
    }
}

/// Vocabulary metadata used by source catalogs and consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct PolylogueVocabularyEntry {
    pub kind: PolylogueRecordKind,
    pub event_type: &'static str,
    pub time_quality: &'static str,
    pub ordering: &'static str,
    pub identity: &'static str,
    pub semantics: &'static str,
    pub volume: &'static str,
    pub privacy: &'static str,
    pub consumer: &'static str,
}

pub const POLYLOGUE_VOCABULARY: [PolylogueVocabularyEntry; 7] = [
    entry(
        PolylogueRecordKind::Session,
        "source_domain_or_missing",
        "session_id",
        "stable_session_id_plus_revision",
        "session_summary",
        "low",
        "sensitive",
        "session_rebuild_and_recall",
    ),
    entry(
        PolylogueRecordKind::Lineage,
        "observed_or_missing",
        "typed_relation",
        "source_session_plus_destination_plus_link",
        "domain_topology",
        "low",
        "sensitive",
        "work_packets_and_session_rebuild",
    ),
    entry(
        PolylogueRecordKind::Usage,
        "source_domain_or_missing",
        "model_name",
        "session_plus_model",
        "provider_usage",
        "low",
        "sensitive",
        "accounting_and_model_effects",
    ),
    entry(
        PolylogueRecordKind::Message,
        "source_domain_or_missing",
        "session_position_plus_variant",
        "stable_message_id_plus_revision",
        "transcript_message",
        "high",
        "secret",
        "recall_context_and_parity",
    ),
    entry(
        PolylogueRecordKind::Block,
        "source_domain_or_missing",
        "message_position",
        "stable_block_id_plus_revision",
        "transcript_block_or_tool_fact",
        "high",
        "secret",
        "recall_context_and_tool_correlation",
    ),
    entry(
        PolylogueRecordKind::Attachment,
        "source_domain_or_missing",
        "message_position",
        "message_plus_attachment_position",
        "attachment_reference",
        "medium",
        "secret",
        "attachment_retrieval_and_deletion",
    ),
    entry(
        PolylogueRecordKind::SessionEvent,
        "source_domain_or_missing",
        "session_position",
        "session_plus_event_position",
        "session_lifecycle",
        "medium",
        "sensitive",
        "session_rebuild_and_context",
    ),
];

const fn entry(
    kind: PolylogueRecordKind,
    time_quality: &'static str,
    ordering: &'static str,
    identity: &'static str,
    semantics: &'static str,
    volume: &'static str,
    privacy: &'static str,
    consumer: &'static str,
) -> PolylogueVocabularyEntry {
    PolylogueVocabularyEntry {
        kind,
        event_type: kind.event_type(),
        time_quality,
        ordering,
        identity,
        semantics,
        volume,
        privacy,
        consumer,
    }
}

#[must_use]
pub fn vocabulary_entry(kind: PolylogueRecordKind) -> &'static PolylogueVocabularyEntry {
    POLYLOGUE_VOCABULARY
        .iter()
        .find(|entry| entry.kind == kind)
        .expect("all Polylogue kinds are represented")
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolylogueSegmentDescriptor {
    pub index: i32,
    pub filename: String,
    pub sha256: String,
    pub size_bytes: usize,
    pub record_count: usize,
    pub first_seq: u64,
    pub last_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolylogueContentDigest {
    pub polylogue_sha256: String,
    pub canonicalizer_version: u16,
    pub size_bytes: usize,
    pub media_type: String,
    pub sinex_cas_digest: Option<String>,
    pub provider_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PolylogueAnchor {
    pub segment_index: i32,
    pub line_index: usize,
    pub seq: u64,
    pub kind: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolylogueFidelityGap {
    pub scope: String,
    pub record_id: String,
    pub gap_kind: String,
    #[serde(default)]
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolylogueRevisionManifest {
    pub protocol_version: String,
    pub semantics_version: u16,
    pub origin_vocabulary_version: u16,
    pub origin_vocabulary_digest: String,
    pub session_id: String,
    pub origin: String,
    pub native_id: String,
    pub revision_id: String,
    pub superseded_revision_id: Option<String>,
    pub content_digest: PolylogueContentDigest,
    pub head_segment: PolylogueSegmentDescriptor,
    pub segments: Vec<PolylogueSegmentDescriptor>,
    pub expected_record_counts: BTreeMap<String, usize>,
    pub anchors: BTreeMap<String, PolylogueAnchor>,
    pub sequence_rule: String,
    pub completeness: String,
    #[serde(default)]
    pub fidelity_gaps: Vec<PolylogueFidelityGap>,
    pub revision_created_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PolylogueResolvedRecord {
    pub kind: PolylogueRecordKind,
    pub record_id: String,
    pub seq: u64,
    pub canonical_bytes: Vec<u8>,
    pub value: Value,
}

impl PolylogueRevisionManifest {
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|error| protocol_error(format!("invalid manifest: {error}")))
    }

    /// Verify segment bytes, sequence spaces, anchors, counts, and semantic
    /// closure before any record is admitted as evidence.
    pub fn verify(&self, segments: &BTreeMap<i32, Vec<u8>>) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION || self.semantics_version != SEMANTICS_VERSION
        {
            return Err(protocol_error("unsupported protocol or semantics version"));
        }
        if self.origin_vocabulary_version != ORIGIN_VOCABULARY_VERSION
            || self.origin_vocabulary_digest != ORIGIN_VOCABULARY_DIGEST
        {
            return Err(protocol_error("unknown or stale Origin vocabulary"));
        }
        if self.content_digest.canonicalizer_version != CANONICALIZER_VERSION {
            return Err(protocol_error("unsupported canonicalizer version"));
        }
        if self.head_segment.index != HEAD_SEGMENT_INDEX
            || self
                .segments
                .iter()
                .any(|segment| segment.index == HEAD_SEGMENT_INDEX)
        {
            return Err(protocol_error("invalid head/transcript segment layout"));
        }
        if self.revision_id != self.content_digest.polylogue_sha256 {
            return Err(protocol_error("revision id does not match content digest"));
        }
        if self
            .segments
            .windows(2)
            .any(|pair| pair[0].index >= pair[1].index)
        {
            return Err(protocol_error(
                "transcript segments are not uniquely ordered",
            ));
        }

        let mut counts = BTreeMap::new();
        let mut anchors = BTreeMap::new();
        if self.head_segment.first_seq != 0 {
            return Err(protocol_error("head sequence must start at zero"));
        }
        let head = self.verify_segment(
            &self.head_segment,
            segments,
            true,
            &mut counts,
            &mut anchors,
        )?;
        let mut transcript = Vec::new();
        let mut expected_transcript_seq = 0;
        for descriptor in &self.segments {
            if descriptor.first_seq != expected_transcript_seq {
                return Err(protocol_error(
                    "transcript sequence spaces are not contiguous",
                ));
            }
            transcript.extend(self.verify_segment(
                descriptor,
                segments,
                false,
                &mut counts,
                &mut anchors,
            )?);
            expected_transcript_seq = if descriptor.record_count == 0 {
                descriptor.first_seq
            } else {
                descriptor.last_seq + 1
            };
        }
        let mut joined = segments
            .get(&HEAD_SEGMENT_INDEX)
            .ok_or_else(|| protocol_error("missing head segment"))?
            .clone();
        for descriptor in &self.segments {
            joined.extend_from_slice(
                segments
                    .get(&descriptor.index)
                    .ok_or_else(|| protocol_error("missing transcript segment"))?,
            );
        }
        if sha256_hex(&joined) != self.content_digest.polylogue_sha256
            || joined.len() != self.content_digest.size_bytes
        {
            return Err(protocol_error("revision content digest or size mismatch"));
        }
        if counts != self.expected_record_counts || anchors != self.anchors {
            return Err(protocol_error("manifest counts or anchors mismatch"));
        }

        let sessions = head
            .iter()
            .filter(|record| record.kind == PolylogueRecordKind::Session)
            .collect::<Vec<_>>();
        if sessions.len() != 1 || sessions[0].record_id != self.session_id {
            return Err(protocol_error(
                "head must contain one session matching session_id",
            ));
        }
        if sessions[0]
            .value
            .get("message_count")
            .and_then(Value::as_u64)
            != Some(*counts.get("message").unwrap_or(&0) as u64)
        {
            return Err(protocol_error("session message_count mismatch"));
        }
        let mut block_counts = BTreeMap::<String, usize>::new();
        for record in &transcript {
            if record.kind == PolylogueRecordKind::Block {
                if let Some(message_id) = record.value.get("message_id").and_then(Value::as_str) {
                    *block_counts.entry(message_id.to_owned()).or_default() += 1;
                }
            }
        }
        for record in transcript
            .iter()
            .filter(|record| record.kind == PolylogueRecordKind::Message)
        {
            let declared = record.value.get("block_count").and_then(Value::as_u64);
            let actual = block_counts.get(&record.record_id).copied().unwrap_or(0) as u64;
            if declared != Some(actual) {
                return Err(protocol_error(format!(
                    "message {} block_count mismatch",
                    record.record_id
                )));
            }
        }
        Ok(())
    }

    fn verify_segment(
        &self,
        descriptor: &PolylogueSegmentDescriptor,
        segments: &BTreeMap<i32, Vec<u8>>,
        head: bool,
        counts: &mut BTreeMap<String, usize>,
        anchors: &mut BTreeMap<String, PolylogueAnchor>,
    ) -> Result<Vec<PolylogueResolvedRecord>> {
        let raw = segments
            .get(&descriptor.index)
            .ok_or_else(|| protocol_error(format!("missing segment {}", descriptor.index)))?;
        if sha256_hex(raw) != descriptor.sha256 || raw.len() != descriptor.size_bytes {
            return Err(protocol_error(format!(
                "segment {} digest or size mismatch",
                descriptor.index
            )));
        }
        let lines = split_ndjson(raw)?;
        if lines.len() != descriptor.record_count {
            return Err(protocol_error(format!(
                "segment {} record count mismatch",
                descriptor.index
            )));
        }
        let allowed = if head {
            &PolylogueRecordKind::HEAD[..]
        } else {
            &PolylogueRecordKind::TRANSCRIPT[..]
        };
        let mut expected_seq = descriptor.first_seq;
        let mut records = Vec::with_capacity(lines.len());
        for (line_index, line) in lines.into_iter().enumerate() {
            let value: Value = serde_json::from_slice(line)
                .map_err(|error| protocol_error(format!("invalid NDJSON record: {error}")))?;
            let object = value
                .as_object()
                .ok_or_else(|| protocol_error("record is not a JSON object"))?;
            let kind = PolylogueRecordKind::parse(
                object.get("kind").and_then(Value::as_str).unwrap_or(""),
            )?;
            if !allowed.contains(&kind) {
                return Err(protocol_error(format!(
                    "{} is in the wrong segment",
                    kind.as_str()
                )));
            }
            let seq = object
                .get("seq")
                .and_then(Value::as_u64)
                .ok_or_else(|| protocol_error("record has no integer seq"))?;
            if seq != expected_seq {
                return Err(protocol_error(format!(
                    "sequence gap: expected {expected_seq}, got {seq}"
                )));
            }
            expected_seq += 1;
            let record_id = object
                .get("record_id")
                .and_then(Value::as_str)
                .ok_or_else(|| protocol_error("record has no record_id"))?
                .to_owned();
            if anchors.contains_key(&record_id) {
                return Err(protocol_error(format!("duplicate record_id {record_id}")));
            }
            let canonical_bytes = canonical_json_bytes(&value)?;
            anchors.insert(
                record_id.clone(),
                PolylogueAnchor {
                    segment_index: descriptor.index,
                    line_index,
                    seq,
                    kind: kind.as_str().to_owned(),
                    sha256: sha256_hex(&canonical_bytes),
                },
            );
            *counts.entry(kind.as_str().to_owned()).or_default() += 1;
            records.push(PolylogueResolvedRecord {
                kind,
                record_id,
                seq,
                canonical_bytes,
                value,
            });
        }
        if descriptor.record_count > 0 && descriptor.last_seq + 1 != expected_seq {
            return Err(protocol_error(format!(
                "segment {} sequence bounds mismatch",
                descriptor.index
            )));
        }
        Ok(records)
    }

    /// Resolve one anchored record while rechecking its segment integrity.
    pub fn resolve_anchor(
        &self,
        segments: &BTreeMap<i32, Vec<u8>>,
        record_id: &str,
    ) -> Result<PolylogueResolvedRecord> {
        let anchor = self
            .anchors
            .get(record_id)
            .ok_or_else(|| protocol_error("anchor not found"))?;
        let descriptor = if anchor.segment_index == HEAD_SEGMENT_INDEX {
            &self.head_segment
        } else {
            self.segments
                .iter()
                .find(|segment| segment.index == anchor.segment_index)
                .ok_or_else(|| protocol_error("anchor names unknown segment"))?
        };
        let records = self.verify_segment(
            descriptor,
            segments,
            anchor.segment_index == HEAD_SEGMENT_INDEX,
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        )?;
        let record = records
            .into_iter()
            .nth(anchor.line_index)
            .ok_or_else(|| protocol_error("anchor line out of bounds"))?;
        if record.record_id != record_id
            || record.seq != anchor.seq
            || record.kind.as_str() != anchor.kind
            || sha256_hex(&record.canonical_bytes) != anchor.sha256
        {
            return Err(protocol_error("anchor mismatch"));
        }
        Ok(record)
    }
}

fn split_ndjson(raw: &[u8]) -> Result<Vec<&[u8]>> {
    if !raw.ends_with(b"\n") {
        return Err(protocol_error("segment must end with LF"));
    }
    let body = &raw[..raw.len() - 1];
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let lines = body.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    if lines.iter().any(|line| line.is_empty()) {
        return Err(protocol_error("segment contains an empty line"));
    }
    Ok(lines)
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort();
            let mut sorted = Map::with_capacity(object.len());
            for key in keys {
                sorted.insert(key.nfc().collect(), canonical_json(&object[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::String(value) => Value::String(value.nfc().collect()),
        other => other.clone(),
    }
}

fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(&canonical_json(value))
        .map_err(|error| protocol_error(format!("cannot canonicalize JSON: {error}")))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn protocol_error(message: impl std::fmt::Display) -> SinexError {
    SinexError::validation(format!("Polylogue material protocol: {message}"))
}

#[cfg(test)]
#[path = "polylogue_protocol_test.rs"]
mod tests;
