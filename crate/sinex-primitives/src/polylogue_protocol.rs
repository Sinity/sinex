//! Polylogue material-protocol v1.
//!
//! The protocol keeps exact immutable NDJSON in registered source materials.
//! Sinex events carry typed facts and confirmed anchors, never transcript or
//! tool text.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Cursor};
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::error::{Result, SinexError};

pub const PROTOCOL_VERSION: &str = "polylogue.material-protocol/v1";
pub const SEMANTICS_VERSION: u16 = 8;
pub const CANONICALIZER_VERSION: u16 = 2;
pub const HEAD_SEGMENT_INDEX: i32 = -1;
pub const ORIGIN_VOCABULARY_VERSION: u16 = 4;
pub const ORIGIN_VOCABULARY_DIGEST: &str =
    "f2413435b8a49c0bd5425dd04616d3ced4f2e44419b66858b1de04ed281a6036";

const KNOWN_ORIGIN_VOCABULARIES: &[(u16, &str)] = &[
    (
        3,
        "f05126b022becf8fcebe9622919465b5e1f86163c25ecdda9d7e1259caba3512",
    ),
    (ORIGIN_VOCABULARY_VERSION, ORIGIN_VOCABULARY_DIGEST),
];

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
    match kind {
        PolylogueRecordKind::Session => &POLYLOGUE_VOCABULARY[0],
        PolylogueRecordKind::Lineage => &POLYLOGUE_VOCABULARY[1],
        PolylogueRecordKind::Usage => &POLYLOGUE_VOCABULARY[2],
        PolylogueRecordKind::Message => &POLYLOGUE_VOCABULARY[3],
        PolylogueRecordKind::Block => &POLYLOGUE_VOCABULARY[4],
        PolylogueRecordKind::Attachment => &POLYLOGUE_VOCABULARY[5],
        PolylogueRecordKind::SessionEvent => &POLYLOGUE_VOCABULARY[6],
    }
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
    pub original_bytes: Vec<u8>,
    pub original_sha256: String,
    pub value: Value,
}

impl PolylogueRevisionManifest {
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let value = parse_strict_json(bytes)?;
        serde_json::from_value(value)
            .map_err(|error| protocol_error(format!("invalid manifest: {error}")))
    }

    /// Refuse protocol, semantic, Origin, or canonicalizer variants that this
    /// reader cannot interpret before inspecting any registered material.
    pub fn validate_compatibility(&self) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION || self.semantics_version != SEMANTICS_VERSION
        {
            return Err(protocol_error("unsupported protocol or semantics version"));
        }
        if !KNOWN_ORIGIN_VOCABULARIES.contains(&(
            self.origin_vocabulary_version,
            self.origin_vocabulary_digest.as_str(),
        )) {
            return Err(protocol_error("unknown or stale Origin vocabulary"));
        }
        if self.content_digest.media_type != "application/x-ndjson; charset=utf-8" {
            return Err(protocol_error("unsupported segment media type"));
        }
        if self.content_digest.canonicalizer_version != CANONICALIZER_VERSION {
            return Err(protocol_error("unsupported canonicalizer version"));
        }
        Ok(())
    }

    /// Verify segment bytes, sequence spaces, anchors, counts, and semantic
    /// closure before any record is admitted as evidence.
    pub fn verify(&self, segments: &BTreeMap<i32, Vec<u8>>) -> Result<()> {
        self.verify_readers(segments, |bytes| Ok(Cursor::new(bytes.as_slice())))
            .map(|_| ())
    }

    /// Verify immutable segment files once and return offsets relative to each
    /// registered segment. Memory holds record metadata and the largest record,
    /// rather than all segment bytes or transcript records.
    pub fn verify_files(&self, paths: &BTreeMap<i32, PathBuf>) -> Result<BTreeMap<String, i64>> {
        self.verify_readers(paths, |path| {
            std::fs::File::open(path)
                .map(BufReader::new)
                .map_err(|error| {
                    SinexError::io(format!("cannot open segment {}: {error}", path.display()))
                })
        })
    }

    fn verify_readers<'a, T: 'a, R: BufRead>(
        &self,
        segments: &'a BTreeMap<i32, T>,
        mut open: impl FnMut(&'a T) -> Result<R>,
    ) -> Result<BTreeMap<String, i64>> {
        self.validate_compatibility()?;
        if self.session_id.trim().is_empty()
            || self.origin.trim().is_empty()
            || self.native_id.trim().is_empty()
            || self.revision_id.trim().is_empty()
        {
            return Err(protocol_error("manifest identity fields must be non-empty"));
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
        if segments.len() != self.segments.len() + 1
            || !segments.contains_key(&HEAD_SEGMENT_INDEX)
            || self
                .segments
                .iter()
                .any(|descriptor| !segments.contains_key(&descriptor.index))
        {
            return Err(protocol_error(
                "material contains undeclared or missing segments",
            ));
        }
        if self.head_segment.first_seq != 0 {
            return Err(protocol_error("head sequence must start at zero"));
        }
        let mut counts = BTreeMap::new();
        let mut anchors = BTreeMap::new();
        let mut offsets = BTreeMap::new();
        let mut message_blocks = BTreeMap::new();
        let mut message_coordinates = std::collections::BTreeSet::new();
        let mut block_counts = BTreeMap::<String, u64>::new();
        let mut session_count = 0;
        let mut session_messages = None;
        let mut joined = Sha256::new();
        let mut joined_size = 0_usize;
        let mut expected_transcript_seq = 0;
        for descriptor in std::iter::once(&self.head_segment).chain(&self.segments) {
            let head = descriptor.index == HEAD_SEGMENT_INDEX;
            if !head && descriptor.first_seq != expected_transcript_seq {
                return Err(protocol_error(
                    "transcript sequence spaces are not contiguous",
                ));
            }
            let source = segments
                .get(&descriptor.index)
                .ok_or_else(|| protocol_error(format!("missing segment {}", descriptor.index)))?;
            let size = stream_segment(
                descriptor,
                open(source)?,
                head,
                &mut joined,
                |record, line_index, offset| {
                    if anchors.contains_key(&record.record_id) {
                        return Err(protocol_error(format!(
                            "duplicate record_id {}",
                            record.record_id
                        )));
                    }
                    anchors.insert(
                        record.record_id.clone(),
                        PolylogueAnchor {
                            segment_index: descriptor.index,
                            line_index,
                            seq: record.seq,
                            kind: record.kind.as_str().to_owned(),
                            sha256: sha256_hex(&record.canonical_bytes),
                        },
                    );
                    offsets.insert(record.record_id.clone(), offset);
                    *counts.entry(record.kind.as_str().to_owned()).or_default() += 1;
                    let session_field = if record.kind == PolylogueRecordKind::Lineage {
                        "src_session_id"
                    } else {
                        "session_id"
                    };
                    if record.value.get(session_field).and_then(Value::as_str)
                        != Some(self.session_id.as_str())
                    {
                        return Err(protocol_error(
                            "material record belongs to a different session",
                        ));
                    }
                    let owner = match record.kind {
                        PolylogueRecordKind::Block | PolylogueRecordKind::Attachment => {
                            record.value.get("message_id").and_then(Value::as_str)
                        }
                        PolylogueRecordKind::SessionEvent => record
                            .value
                            .get("source_message_id")
                            .and_then(Value::as_str),
                        _ => None,
                    };
                    // Transcript seq is strictly increasing across segments. A
                    // known message here therefore exists and precedes its child.
                    if let Some(owner) = owner {
                        if !message_blocks.contains_key(owner) {
                            return Err(protocol_error(format!(
                                "{} names an absent or later message owner",
                                record.kind.as_str()
                            )));
                        }
                    }
                    match record.kind {
                        PolylogueRecordKind::Session => {
                            session_count += 1;
                            if record.record_id != self.session_id
                                || record.value.get("session_id").and_then(Value::as_str)
                                    != Some(self.session_id.as_str())
                                || record.value.get("origin").and_then(Value::as_str)
                                    != Some(self.origin.as_str())
                                || record.value.get("native_id").and_then(Value::as_str)
                                    != Some(self.native_id.as_str())
                            {
                                return Err(protocol_error(
                                    "session identity does not match manifest",
                                ));
                            }
                            session_messages =
                                record.value.get("message_count").and_then(Value::as_u64);
                        }
                        PolylogueRecordKind::Message => {
                            if record.value.get("message_id").and_then(Value::as_str)
                                != Some(record.record_id.as_str())
                            {
                                return Err(protocol_error("message identity is inconsistent"));
                            }
                            let position = record
                                .value
                                .get("position")
                                .and_then(Value::as_u64)
                                .ok_or_else(|| protocol_error("invalid message position"))?;
                            let variant = record
                                .value
                                .get("variant_index")
                                .and_then(Value::as_u64)
                                .ok_or_else(|| protocol_error("invalid message variant_index"))?;
                            if !message_coordinates.insert((position, variant)) {
                                return Err(protocol_error("duplicate message ordinal"));
                            }
                            message_blocks.insert(
                                record.record_id,
                                record.value.get("block_count").and_then(Value::as_u64),
                            );
                        }
                        PolylogueRecordKind::Block => {
                            if record.value.get("block_id").and_then(Value::as_str)
                                != Some(record.record_id.as_str())
                            {
                                return Err(protocol_error("block identity is inconsistent"));
                            }
                            if let Some(message_id) =
                                record.value.get("message_id").and_then(Value::as_str)
                            {
                                *block_counts.entry(message_id.to_owned()).or_default() += 1;
                            }
                        }
                        _ => {}
                    }
                    Ok(())
                },
            )?;
            joined_size = joined_size
                .checked_add(size)
                .ok_or_else(|| protocol_error("revision size overflows usize"))?;
            if !head {
                expected_transcript_seq = if descriptor.record_count == 0 {
                    descriptor.first_seq
                } else {
                    descriptor
                        .last_seq
                        .checked_add(1)
                        .ok_or_else(|| protocol_error("transcript sequence overflows u64"))?
                };
            }
        }
        if format!("{:x}", joined.finalize()) != self.content_digest.polylogue_sha256
            || joined_size != self.content_digest.size_bytes
        {
            return Err(protocol_error("revision content digest or size mismatch"));
        }
        if counts != self.expected_record_counts || anchors != self.anchors {
            return Err(protocol_error("manifest counts or anchors mismatch"));
        }
        if session_count != 1 {
            return Err(protocol_error(
                "head must contain one session matching session_id",
            ));
        }
        if session_messages != Some(*counts.get("message").unwrap_or(&0) as u64) {
            return Err(protocol_error("session message_count mismatch"));
        }
        for (message_id, declared) in message_blocks {
            if declared != Some(block_counts.get(&message_id).copied().unwrap_or(0)) {
                return Err(protocol_error(format!(
                    "message {message_id} block_count mismatch"
                )));
            }
        }
        Ok(offsets)
    }

    /// Resolve one anchored record while rechecking its segment integrity.
    pub fn resolve_anchor(
        &self,
        segments: &BTreeMap<i32, Vec<u8>>,
        record_id: &str,
    ) -> Result<PolylogueResolvedRecord> {
        self.validate_compatibility()?;
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
        let raw = segments
            .get(&descriptor.index)
            .ok_or_else(|| protocol_error(format!("missing segment {}", descriptor.index)))?;
        let mut selected = None;
        let mut ids = std::collections::BTreeSet::new();
        stream_segment(
            descriptor,
            Cursor::new(raw.as_slice()),
            anchor.segment_index == HEAD_SEGMENT_INDEX,
            &mut Sha256::new(),
            |record, line_index, _| {
                if !ids.insert(record.record_id.clone()) {
                    return Err(protocol_error(format!(
                        "duplicate record_id {}",
                        record.record_id
                    )));
                }
                if line_index == anchor.line_index {
                    selected = Some(record);
                }
                Ok(())
            },
        )?;
        let record = selected.ok_or_else(|| protocol_error("anchor line out of bounds"))?;
        if record.record_id != record_id
            || record.seq != anchor.seq
            || record.kind.as_str() != anchor.kind
            || sha256_hex(&record.canonical_bytes) != anchor.sha256
        {
            return Err(protocol_error("anchor mismatch"));
        }
        Ok(record)
    }

    /// Resolve the byte offset within the registered segment after validating
    /// the anchor and the complete segment digest.
    pub fn anchor_byte(&self, segments: &BTreeMap<i32, Vec<u8>>, record_id: &str) -> Result<i64> {
        self.resolve_anchor(segments, record_id)?;
        let anchor = self
            .anchors
            .get(record_id)
            .ok_or_else(|| protocol_error("anchor not found"))?;
        let raw = segments
            .get(&anchor.segment_index)
            .ok_or_else(|| protocol_error("anchor names missing segment"))?;
        let offset = raw
            .split(|byte| *byte == b'\n')
            .take(anchor.line_index)
            .try_fold(0_usize, |offset, line| offset.checked_add(line.len() + 1))
            .ok_or_else(|| protocol_error("anchor byte offset overflows usize"))?;
        i64::try_from(offset).map_err(|_| protocol_error("anchor byte offset overflows i64"))
    }
}

/// Shared framing, integrity and per-record validation for memory and file
/// adapters. The visitor retains only metadata (or one selected record).
fn stream_segment(
    descriptor: &PolylogueSegmentDescriptor,
    mut reader: impl BufRead,
    head: bool,
    joined: &mut Sha256,
    mut visit: impl FnMut(PolylogueResolvedRecord, usize, i64) -> Result<()>,
) -> Result<usize> {
    let allowed = if head {
        &PolylogueRecordKind::HEAD[..]
    } else {
        &PolylogueRecordKind::TRANSCRIPT[..]
    };
    let mut digest = Sha256::new();
    let mut size = 0_usize;
    let mut record_count = 0_usize;
    let mut expected_seq = descriptor.first_seq;
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line).map_err(|error| {
            SinexError::io(format!("cannot read segment {}: {error}", descriptor.index))
        })?;
        if read == 0 {
            break;
        }
        let offset =
            i64::try_from(size).map_err(|_| protocol_error("anchor byte offset overflows i64"))?;
        size = size
            .checked_add(read)
            .ok_or_else(|| protocol_error("segment size overflows usize"))?;
        digest.update(&line);
        joined.update(&line);
        if line.last() != Some(&b'\n') {
            return Err(protocol_error("segment must end with LF"));
        }
        line.pop();
        if line.is_empty() {
            // Preserve the original empty-segment representation: exactly LF.
            if size == 1
                && reader
                    .fill_buf()
                    .map_err(|error| {
                        SinexError::io(format!("cannot read segment {}: {error}", descriptor.index))
                    })?
                    .is_empty()
            {
                break;
            }
            return Err(protocol_error("segment contains an empty line"));
        }
        let value = parse_strict_json(&line)?;
        let object = value
            .as_object()
            .ok_or_else(|| protocol_error("record is not a JSON object"))?;
        let kind =
            PolylogueRecordKind::parse(object.get("kind").and_then(Value::as_str).unwrap_or(""))?;
        if !allowed.contains(&kind) {
            return Err(protocol_error(format!(
                "{} is in the wrong segment",
                kind.as_str()
            )));
        }
        validate_record_shape(kind, object)?;
        let seq = object
            .get("seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| protocol_error("record has no integer seq"))?;
        if seq != expected_seq {
            return Err(protocol_error(format!(
                "sequence gap: expected {expected_seq}, got {seq}"
            )));
        }
        expected_seq = expected_seq
            .checked_add(1)
            .ok_or_else(|| protocol_error("record sequence overflows u64"))?;
        let record_id = object
            .get("record_id")
            .and_then(Value::as_str)
            .ok_or_else(|| protocol_error("record has no record_id"))?
            .to_owned();
        if record_id.is_empty() {
            return Err(protocol_error("record_id must be nonempty text"));
        }
        let canonical_bytes = canonical_json_bytes(&value)?;
        let original_sha256 = sha256_hex(&line);
        visit(
            PolylogueResolvedRecord {
                kind,
                record_id,
                seq,
                canonical_bytes,
                original_bytes: line.clone(),
                original_sha256,
                value,
            },
            record_count,
            offset,
        )?;
        record_count = record_count
            .checked_add(1)
            .ok_or_else(|| protocol_error("record count overflows usize"))?;
    }
    if size == 0 {
        return Err(protocol_error("segment must end with LF"));
    }
    if format!("{:x}", digest.finalize()) != descriptor.sha256 || size != descriptor.size_bytes {
        return Err(protocol_error(format!(
            "segment {} digest or size mismatch",
            descriptor.index
        )));
    }
    if record_count != descriptor.record_count {
        return Err(protocol_error(format!(
            "segment {} record count mismatch",
            descriptor.index
        )));
    }
    if record_count > 0 && descriptor.last_seq.checked_add(1) != Some(expected_seq) {
        return Err(protocol_error(format!(
            "segment {} sequence bounds mismatch",
            descriptor.index
        )));
    }
    Ok(size)
}

/// Shapes of declared material fields. Opaque objects keep their own JSON
/// contents; only the message usage object has a fixed nested field vocabulary.
#[derive(Clone, Copy)]
enum RecordFieldShape {
    Text,
    NullableText,
    Unsigned,
    NullableInteger,
    Number,
    NullableNumber,
    Object,
    NullableObject,
    NullableBoolean,
    TextArray,
    MessageUsage,
}

impl RecordFieldShape {
    fn accepts(self, value: &Value) -> bool {
        match self {
            Self::Text => value.is_string(),
            Self::NullableText => value.is_null() || value.is_string(),
            Self::Unsigned => value.as_u64().is_some(),
            Self::NullableInteger => {
                value.is_null() || value.as_i64().is_some() || value.as_u64().is_some()
            }
            Self::Number => value.is_number(),
            Self::NullableNumber => value.is_null() || value.is_number(),
            Self::Object => value.is_object(),
            Self::NullableObject => value.is_null() || value.is_object(),
            Self::NullableBoolean => value.is_null() || value.is_boolean(),
            Self::TextArray => value
                .as_array()
                .is_some_and(|values| values.iter().all(Value::is_string)),
            Self::MessageUsage => value.as_object().is_some_and(|object| {
                object.iter().all(|(field, value)| {
                    matches!(
                        field.as_str(),
                        "input_tokens"
                            | "output_tokens"
                            | "cache_read_tokens"
                            | "cache_write_tokens"
                            | "duration_ms"
                    ) && (value.is_null() || value.as_u64().is_some())
                })
            }),
        }
    }
}

/// Current semantics-8 builder keys and types. Absent optional fields remain
/// allowed; a present field must have its declared type, including nullability.
fn validate_record_shape(kind: PolylogueRecordKind, object: &Map<String, Value>) -> Result<()> {
    use PolylogueRecordKind as Kind;
    use RecordFieldShape as Shape;
    let required: &[&str] = match kind {
        Kind::Session => &["session_id", "origin", "native_id", "message_count"],
        Kind::Lineage => &["src_session_id"],
        Kind::Usage | Kind::SessionEvent => &["session_id"],
        Kind::Message => &[
            "session_id",
            "message_id",
            "position",
            "variant_index",
            "block_count",
        ],
        Kind::Block => &["session_id", "message_id", "block_id"],
        Kind::Attachment => &["session_id", "message_id"],
    };
    for field in ["kind", "record_id", "seq"].iter().chain(required) {
        if !object.contains_key(*field) {
            return Err(protocol_error(format!(
                "{} record has no {field}",
                kind.as_str()
            )));
        }
    }
    for (field, value) in object {
        let shape = match (kind, field.as_str()) {
            (_, "kind" | "record_id") => Shape::Text,
            (_, "seq") => Shape::Unsigned,
            (Kind::Session, "session_id" | "origin" | "native_id" | "session_kind") => Shape::Text,
            (Kind::Session, "message_count") => Shape::Unsigned,
            (
                Kind::Session,
                "title" | "git_branch" | "git_repository_url" | "provider_project_ref",
            ) => Shape::NullableText,
            (Kind::Session, "created_at_ms" | "updated_at_ms") => Shape::NullableInteger,
            (Kind::Session, "working_directories" | "tags") => Shape::TextArray,
            (Kind::Session, "metadata") => Shape::Object,
            (Kind::Lineage, "src_session_id" | "dst_origin" | "dst_native_id" | "link_type") => {
                Shape::Text
            }
            (
                Kind::Lineage,
                "branch_point_message_native_id"
                | "branch_point_source_native_id_json"
                | "inheritance"
                | "status",
            ) => Shape::NullableText,
            (Kind::Lineage, "confidence") => Shape::Number,
            (Kind::Lineage, "observed_at_ms") => Shape::NullableInteger,
            (Kind::Usage, "session_id" | "model_name") => Shape::Text,
            (
                Kind::Usage,
                "input_tokens" | "output_tokens" | "cache_read_tokens" | "cache_write_tokens",
            ) => Shape::Unsigned,
            (Kind::Usage, "cost_usd" | "cost_credits") => Shape::NullableNumber,
            (Kind::Usage, "cost_provenance") => Shape::NullableText,
            (
                Kind::Message,
                "session_id" | "message_id" | "role" | "message_type" | "material_origin",
            ) => Shape::Text,
            (
                Kind::Message,
                "native_id" | "source_native_id_json" | "text" | "model_name" | "parent_message_id",
            ) => Shape::NullableText,
            (Kind::Message, "position" | "variant_index" | "block_count") => Shape::Unsigned,
            (Kind::Message, "occurred_at_ms") => Shape::NullableInteger,
            (Kind::Message, "usage") => Shape::MessageUsage,
            (
                Kind::Block,
                "session_id" | "message_id" | "block_id" | "content_identity" | "block_type",
            ) => Shape::Text,
            (Kind::Block, "position" | "content_occurrence") => Shape::Unsigned,
            (
                Kind::Block,
                "text"
                | "tool_name"
                | "tool_id"
                | "tool_outcome"
                | "tool_result_outcome_unknown_reason"
                | "semantic_type"
                | "media_type"
                | "language",
            ) => Shape::NullableText,
            (Kind::Block, "tool_input") => Shape::NullableObject,
            (Kind::Block, "tool_result_is_error") => Shape::NullableBoolean,
            (Kind::Block, "tool_result_exit_code") => Shape::NullableInteger,
            (
                Kind::Attachment,
                "session_id" | "message_id" | "attachment_id" | "native_identity"
                | "acquisition_status",
            ) => Shape::Text,
            (Kind::Attachment, "position" | "byte_count") => Shape::Unsigned,
            (
                Kind::Attachment,
                "display_name" | "media_type" | "blob_sha256" | "upload_origin" | "direction"
                | "producer_ref" | "source_url" | "caption",
            ) => Shape::NullableText,
            (Kind::SessionEvent, "session_id" | "event_type" | "summary") => Shape::Text,
            (Kind::SessionEvent, "position") => Shape::Unsigned,
            (Kind::SessionEvent, "payload") => Shape::Object,
            (Kind::SessionEvent, "source_message_id") => Shape::NullableText,
            (Kind::SessionEvent, "occurred_at_ms") => Shape::NullableInteger,
            _ => {
                return Err(protocol_error(format!(
                    "unknown {} record field {field:?}",
                    kind.as_str()
                )));
            }
        };
        if !shape.accepts(value) {
            return Err(protocol_error(format!(
                "invalid {} record field {field:?}",
                kind.as_str()
            )));
        }
    }
    Ok(())
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an unambiguous JSON value")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Bool(value)))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(value.into())))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(value.into())))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                if !value.is_finite() {
                    return Err(E::custom("nonfinite JSON number"));
                }
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .map(StrictValue)
                    .ok_or_else(|| E::custom("unsupported JSON number"))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(value.to_owned())))
            }

            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(value)))
            }

            fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }

            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(StrictValue(value)) = seq.next_element()? {
                    values.push(value);
                }
                Ok(StrictValue(Value::Array(values)))
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate JSON key {key:?}")));
                    }
                    let StrictValue(value) = map.next_value()?;
                    values.insert(key, value);
                }
                Ok(StrictValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(StrictVisitor)
    }
}

fn parse_strict_json(bytes: &[u8]) -> Result<Value> {
    validate_number_tokens(bytes)?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let StrictValue(value) = StrictValue::deserialize(&mut deserializer)
        .map_err(|error| protocol_error(format!("invalid or ambiguous JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| protocol_error(format!("invalid JSON trailer: {error}")))?;
    Ok(value)
}

fn validate_number_tokens(bytes: &[u8]) -> Result<()> {
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            index += 1;
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len()
                && matches!(bytes[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
            {
                index += 1;
            }
            let token = std::str::from_utf8(&bytes[start..index])
                .map_err(|_| protocol_error("invalid number encoding"))?;
            if !token.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
                let supported = if token.starts_with('-') {
                    token.parse::<i64>().is_ok()
                } else {
                    token.parse::<u64>().is_ok()
                };
                if !supported {
                    return Err(protocol_error(
                        "integer is outside protocol v1's i64/u64 range",
                    ));
                }
            }
            continue;
        }
        index += 1;
    }
    Ok(())
}

fn canonical_json(value: &Value) -> Result<Value> {
    match value {
        Value::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort();
            let mut sorted = Map::with_capacity(object.len());
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&object[key])?);
            }
            Ok(Value::Object(sorted))
        }
        Value::Array(values) => values
            .iter()
            .map(canonical_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        Value::String(value) => Ok(Value::String(value.clone())),
        other => Ok(other.clone()),
    }
}

/// Match the producer's core-JSON float spelling, including positive exponent
/// signs and fixed notation through 1e-5. This is canonicalizer version 2;
/// opaque strings and keys are never normalized by this framing layer.
struct MaterialJsonFormatter;

impl serde_json::ser::Formatter for MaterialJsonFormatter {
    fn write_f64<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f64,
    ) -> std::io::Result<()> {
        let mut text = serde_json::to_string(&value).map_err(std::io::Error::other)?;
        if let Some((mantissa, exponent)) = text.split_once('e') {
            let exponent: i32 = exponent.parse().map_err(std::io::Error::other)?;
            if (-5..0).contains(&exponent) {
                let negative = mantissa.starts_with('-');
                let digits = mantissa.trim_start_matches('-').replace('.', "");
                text = format!(
                    "{}0.{}{}",
                    if negative { "-" } else { "" },
                    "0".repeat((-exponent - 1) as usize),
                    digits
                );
            } else {
                text = format!("{mantissa}e{exponent:+}");
            }
        }
        writer.write_all(text.as_bytes())
    }
}

fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    canonical_json(value)?
        .serialize(&mut serde_json::Serializer::with_formatter(
            &mut bytes,
            MaterialJsonFormatter,
        ))
        .map_err(|error| protocol_error(format!("cannot canonicalize JSON: {error}")))?;
    Ok(bytes)
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
