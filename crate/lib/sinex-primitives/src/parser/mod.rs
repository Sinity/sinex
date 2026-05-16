//! Parser and input-shape substrate types.
//!
//! These types define the boundary-crossing contracts for the staged-source
//! parser architecture (#1097). They are shared across `sinex-primitives`,
//! `sinex-node-sdk`, `sinex-db`, and `sinex-schema` so that parser authors,
//! source-worker runtime, and schema/repository layers share a single
//! vocabulary.
//!
//! # Relationship to other modules
//!
//! - `crate::events::occurrence` defines `AnchorKind` for the database layer.
//!   `MaterialAnchor` here is the parser-author type; the two are aligned but
//!   serve different consumers.
//! - `crate::domain` defines `EventSource`, `EventType`, and other
//!   event-level newtypes. Parser types build on those rather than
//!   redefining them.
//! - `crate::ids::Id<T>` is the canonical phantom-typed identifier.

use std::borrow::Cow;

use camino::Utf8PathBuf;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::SinexError;
use crate::domain::{EventSource, EventType};
use crate::events::SourceMaterial;
use crate::events::builder::EventId;
use crate::ids::Id;
use crate::primitives::Uuid;
use crate::temporal::Timestamp;

// =============================================================================
// Parser identity types
// =============================================================================

/// Identifies a parser implementation within the system.
///
/// A `ParserId` is a stable, human-readable identifier for a parser
/// (e.g. `"atuin-history"`, `"weechat-log"`). It is validated on
/// construction: lowercase ASCII letters, digits, hyphens, underscores,
/// and dots only.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct ParserId(Cow<'static, str>);

impl<'de> serde::Deserialize<'de> for ParserId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::validate_str(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(Cow::Owned(s)))
    }
}

impl ParserId {
    /// Creates a validated `ParserId` from a string.
    pub fn new(s: impl Into<String>) -> Result<Self, SinexError> {
        let s = s.into();
        Self::validate_str(&s)?;
        Ok(Self(Cow::Owned(s)))
    }

    /// Creates a const `ParserId` from a static string literal.
    ///
    /// Validated at compile time — invalid values produce a compile error.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        assert!(
            Self::const_validate(s),
            "ParserId must match [a-z][a-z0-9_.-]*"
        );
        Self(Cow::Borrowed(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn as_static_str(&self) -> &'static str {
        match &self.0 {
            Cow::Borrowed(s) => s,
            Cow::Owned(_) => panic!("ParserId::as_static_str called on owned value"),
        }
    }

    fn validate_str(s: &str) -> Result<(), SinexError> {
        if s.is_empty() {
            return Err(SinexError::validation("ParserId must not be empty"));
        }
        if !s.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' || c == '.'
        }) {
            return Err(SinexError::validation(
                "ParserId must contain only [a-z0-9_.-]",
            ));
        }
        Ok(())
    }

    const fn const_validate(s: &str) -> bool {
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if !(b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || b == b'-'
                || b == b'_'
                || b == b'.')
            {
                return false;
            }
            i += 1;
        }
        !s.is_empty()
    }
}

impl std::fmt::Display for ParserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identifies a logical source unit that parsers operate within.
///
/// A source unit is the stable identity that groups parser instances,
/// emitted event types, and configuration. It is NOT a process or
/// deployment identity — that is the source-domain/service split.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct SourceUnitId(Cow<'static, str>);

impl<'de> serde::Deserialize<'de> for SourceUnitId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::validate_str(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(Cow::Owned(s)))
    }
}

impl SourceUnitId {
    /// Creates a validated `SourceUnitId` from a string.
    pub fn new(s: impl Into<String>) -> Result<Self, SinexError> {
        let s = s.into();
        Self::validate_str(&s)?;
        Ok(Self(Cow::Owned(s)))
    }

    /// Creates a const `SourceUnitId` from a static string literal.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        assert!(
            Self::const_validate(s),
            "SourceUnitId must match [a-z][a-z0-9_.-]*"
        );
        Self(Cow::Borrowed(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn as_static_str(&self) -> &'static str {
        match &self.0 {
            Cow::Borrowed(s) => s,
            Cow::Owned(_) => panic!("SourceUnitId::as_static_str called on owned value"),
        }
    }

    fn validate_str(s: &str) -> Result<(), SinexError> {
        if s.is_empty() {
            return Err(SinexError::validation("SourceUnitId must not be empty"));
        }
        if !s.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' || c == '.'
        }) {
            return Err(SinexError::validation(
                "SourceUnitId must contain only [a-z0-9_.-]",
            ));
        }
        Ok(())
    }

    const fn const_validate(s: &str) -> bool {
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if !(b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || b == b'-'
                || b == b'_'
                || b == b'.')
            {
                return false;
            }
            i += 1;
        }
        !s.is_empty()
    }
}

impl std::fmt::Display for SourceUnitId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identifies a source binding in the catalog.
///
/// A `SourceBindingId` is a durable reference to a row in
/// `raw.source_bindings`. It links acquisition intent to parser
/// execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct SourceBindingId(pub Uuid);

impl SourceBindingId {
    #[must_use]
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for SourceBindingId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<SourceBindingId> for Uuid {
    fn from(id: SourceBindingId) -> Self {
        id.0
    }
}

// =============================================================================
// Input shape classification
// =============================================================================

/// The kind of input shape a parser or adapter operates on.
///
/// Each shape implies a different cursor type, record enumeration strategy,
/// and adapter lifecycle. The shape is declared at parser registration time
/// and matched to source material at job creation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputShapeKind {
    /// A single static file read once (e.g., a JSON/CSV export).
    StaticFile,

    /// An archive (tar, zip, etc.) containing multiple files.
    Archive,

    /// Recursive directory walk producing one record per file.
    DirectoryWalk,

    /// A hot folder where files appear over time.
    FileDrop,

    /// A file that grows by appending (log-style).
    AppendOnlyFile,

    /// SQLite database queried via rowid cursor.
    SqliteQuery,

    /// A git repository snapshot.
    RepositorySnapshot,

    /// An API cursor-based pagination source.
    ApiCursor,

    /// A long-lived child process emitting structured records (e.g.
    /// `journalctl -f -o json`). Cursor is process-defined (e.g. journal
    /// cursor string).
    Subprocess,

    /// A line-delimited Unix domain socket (e.g. Hyprland IPC). No cursor;
    /// anchor only.
    UnixSocket,

    /// A D-Bus signal subscription. Anchor only; no replay.
    DbusSubscription,

    /// A poll-and-detect-change adapter (e.g. clipboard hash polling).
    /// Anchor only; no cursor.
    Polling,
}

impl InputShapeKind {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StaticFile => "static_file",
            Self::Archive => "archive",
            Self::DirectoryWalk => "directory_walk",
            Self::FileDrop => "file_drop",
            Self::AppendOnlyFile => "append_only_file",
            Self::SqliteQuery => "sqlite_query",
            Self::RepositorySnapshot => "repository_snapshot",
            Self::ApiCursor => "api_cursor",
            Self::Subprocess => "subprocess",
            Self::UnixSocket => "unix_socket",
            Self::DbusSubscription => "dbus_subscription",
            Self::Polling => "polling",
        }
    }
}

impl std::fmt::Display for InputShapeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// =============================================================================
// Material anchors
// =============================================================================

/// Locates an occurrence within a source material.
///
/// Anchors identify *where* in the source material a record came from.
/// They are the parser-author equivalent of `AnchorKind` (the database
/// layer enum in `crate::events::occurrence`). Anchors must be stable
/// across re-reads of the same material so that replay can reconstruct
/// the same occurrences.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MaterialAnchor {
    /// Byte-offset range within a material blob.
    ByteRange { start: u64, len: u64 },

    /// A line within a text material.
    Line { byte_start: u64, line: u64 },

    /// A directory entry with optional content hash.
    DirectoryEntry {
        #[schemars(with = "String")]
        path: Utf8PathBuf,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
    },

    /// A row in a SQLite table.
    SqliteRow { table: String, rowid: i64 },

    /// A git object identified by OID.
    GitObject {
        oid: String,
        #[schemars(with = "Option<String>")]
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<Utf8PathBuf>,
    },

    /// A frame within a stream.
    StreamFrame {
        material_offset: u64,
        frame_index: u64,
    },
}

// =============================================================================
// Timing evidence
// =============================================================================

/// Confidence level for a timing derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TimingConfidence {
    /// Timestamp is intrinsic to the record (e.g., a log line timestamp).
    Intrinsic,

    /// Timestamp observed by the wrapper (e.g., mtime from file system).
    WrapperObserved,

    /// Timestamp inferred from context (e.g., filename contains date).
    Inferred,

    /// Timestamp declared by the user or import process.
    UserDeclared,

    /// No timing evidence available.
    None,
}

/// How a timestamp for a parsed record was derived.
///
/// This records the provenance of `ts_orig` on a parsed event so that
/// downstream consumers can assess timestamp quality.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum TimingEvidence {
    /// Timestamp comes from a named field within the record itself.
    Intrinsic {
        field: String,
        confidence: TimingConfidence,
    },

    /// Timestamp comes from a temporal ledger entry.
    Wrapper { ledger_id: Uuid },

    /// Timestamp inferred from file mtime.
    InferredMtime {
        #[schemars(with = "String")]
        path: Utf8PathBuf,
        mtime: Timestamp,
    },

    /// Timestamp explicitly declared by the user or import process.
    UserDeclared { value: Timestamp, reason: String },

    /// No timestamp available — the record is atemporal.
    Atemporal,

    /// Fallback: use the time material was staged.
    StagedAtFallback,
}

// =============================================================================
// Source record (adapter output, parser input)
// =============================================================================

/// A single record yielded by an input-shape adapter.
///
/// This is what a parser receives: anchored bytes from a source material.
/// The adapter owns enumeration, cursor advancement, and material access;
/// the parser owns semantic interpretation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRecord {
    /// The source material this record came from.
    pub material_id: Id<SourceMaterial>,

    /// Where in the material this record was found.
    pub anchor: MaterialAnchor,

    /// The raw bytes of the record.
    pub bytes: Vec<u8>,

    /// Optional logical path (e.g., for archive entries or directory walks).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_path: Option<Utf8PathBuf>,

    /// When the record was sourced (if known).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ts_hint: Option<TimingEvidence>,

    /// Additional metadata from the adapter.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub metadata: serde_json::Value,
}

// =============================================================================
// Parser output
// =============================================================================

/// Error type for `ParsedEventIntent` operations.
///
/// Currently a transparent alias over [`SinexError`]. A dedicated enum can
/// replace this alias if the call sites need finer discrimination.
pub type ParserError = SinexError;

/// A single event that a parser intends to publish.
///
/// This is the parser's output contract. The source-worker or transport
/// layer owns admission, privacy, NATS publication, and confirmation
/// tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedEventIntent {
    /// A freshly-generated UUIDv7 identity for this intent.
    ///
    /// The transport layer uses this as the event ID it persists and
    /// references in confirmations. Synthesis intents reference their
    /// parent's `id` in `synthesis_parents`.
    pub id: EventId,

    /// Which source unit the parser belongs to.
    pub source_unit_id: SourceUnitId,

    /// Which parser produced this intent.
    pub parser_id: ParserId,

    /// The semantics version of the parser at interpretation time.
    pub parser_version: String,

    /// The event type this intent represents.
    pub event_type: EventType,

    /// The event source namespace.
    pub event_source: EventSource,

    /// The payload to persist.
    pub payload: serde_json::Value,

    /// The real-world timestamp of the event.
    pub ts_orig: Timestamp,

    /// How `ts_orig` was derived.
    pub timing: TimingEvidence,

    /// Where in the source material this event came from.
    ///
    /// For synthesis intents produced via [`ParsedEventIntent::derive_synthesis`],
    /// this carries the parent's anchor verbatim (no independent material
    /// position). The transport layer uses `synthesis_parents` to detect synthesis
    /// provenance and ignores `anchor` for those intents.
    pub anchor: MaterialAnchor,

    /// An optional natural key for idempotent event creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub occurrence_key: Option<OccurrenceKey>,

    /// Privacy processing context for this event.
    pub privacy_context: crate::privacy::ProcessingContext,

    /// Per-field privacy decisions made during parsing.
    ///
    /// `None` for imperative parsers that don't populate it (the engine ran
    /// at call sites the same way it always has). `Some(vec)` for parsers
    /// authored via `#[derive(SourceRecord)]` or the YAML loader — the macro
    /// emits one entry per privacy-relevant field. Consumed by #1072 audit
    /// /export/redact CLI.
    ///
    /// Backward-compat: existing imperative parsers compile and behave
    /// identically; the field is `Option`, default `None`,
    /// `serde(skip_serializing_if = "Option::is_none")` so wire format is
    /// unchanged when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_privacy_log: Option<Vec<crate::privacy::FieldPrivacyDecision>>,

    /// Parent event IDs for synthesis provenance.
    ///
    /// `None` means this intent carries **material provenance** — it was
    /// derived directly from source bytes.  `Some(ids)` means this intent
    /// carries **synthesis provenance** — it was derived from one or more
    /// already-persisted events.  The transport layer checks this field
    /// before constructing the `Provenance` variant for DB insertion.
    ///
    /// Populated by [`ParsedEventIntent::derive_synthesis`]; do not set
    /// manually unless you are constructing a synthesis intent explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synthesis_parents: Option<Vec<EventId>>,
}

impl ParsedEventIntent {
    /// Derive a synthesis event from this material-provenance intent.
    ///
    /// Given `self` (a material-provenance parsed event), builds a new
    /// `ParsedEventIntent` whose provenance is
    /// `Synthesis { source_event_ids: [self.id] }`.
    ///
    /// The returned intent:
    /// - Carries the parent's `source_unit_id`, `parser_id`, `parser_version`,
    ///   `acquisition_time` (via `ts_orig`), and `anchor` (transport layer
    ///   ignores it for synthesis intents).
    /// - Has its own freshly-generated `id` (UUIDv7).
    /// - Has `synthesis_parents = Some(vec![self.id])` pointing to `self`.
    /// - Has `event_source` and `event_type` taken from `P::SOURCE` /
    ///   `P::EVENT_TYPE` (the *new* payload, **not** the parent's types).
    /// - Has `occurrence_key = None` and `field_privacy_log = None`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if:
    /// - `self` already has synthesis provenance (`synthesis_parents.is_some()`).
    ///   Chained synthesis requires explicit construction with the full parent
    ///   set — this helper is intentionally limited to single-hop derivation from
    ///   a material-provenance parent.
    /// - `self.id` would appear as both parent and child (self-referential
    ///   synthesis). This is structurally impossible with a freshly generated
    ///   child ID, but the check is made explicit for correctness.
    pub fn derive_synthesis<P>(&self, payload: P) -> Result<ParsedEventIntent, ParserError>
    where
        P: crate::events::EventPayload,
    {
        // Reject synthesis-from-synthesis: chained synthesis needs explicit
        // construction with the complete parent set.
        if self.synthesis_parents.is_some() {
            return Err(SinexError::validation(
                "derive_synthesis requires a material-provenance parent; \
                 chained synthesis must be constructed explicitly with the full parent set",
            )
            .with_context("parent_id", self.id.to_uuid().to_string()));
        }

        let child_id: EventId = Id::new();

        // Self-referential synthesis is impossible with a fresh ID, but guard
        // it explicitly so the invariant is visible and testable.
        if child_id == self.id {
            return Err(SinexError::validation(
                "derive_synthesis produced a self-referential synthesis (child id == parent id)",
            ));
        }

        let child_payload = serde_json::to_value(&payload).map_err(|e| {
            SinexError::serialization("failed to serialize synthesis payload")
                .with_context("event_type", P::EVENT_TYPE.as_str().to_string())
                .with_std_error(&e)
        })?;

        Ok(ParsedEventIntent {
            id: child_id,
            source_unit_id: self.source_unit_id.clone(),
            parser_id: self.parser_id.clone(),
            parser_version: self.parser_version.clone(),
            event_type: P::EVENT_TYPE,
            event_source: P::SOURCE,
            payload: child_payload,
            // Preserve the parent's real-world timestamp so the synthesis
            // event sits in the same temporal window as its material parent.
            ts_orig: self.ts_orig,
            timing: self.timing.clone(),
            // Carry the parent anchor verbatim; transport layer uses
            // synthesis_parents to detect synthesis and ignores anchor.
            anchor: self.anchor.clone(),
            occurrence_key: None,
            privacy_context: self.privacy_context,
            field_privacy_log: None,
            synthesis_parents: Some(vec![self.id]),
        })
    }

    /// Returns `true` if this intent carries material provenance.
    #[must_use]
    pub fn is_material(&self) -> bool {
        self.synthesis_parents.is_none()
    }

    /// Returns `true` if this intent carries synthesis provenance.
    #[must_use]
    pub fn is_synthesis(&self) -> bool {
        self.synthesis_parents.is_some()
    }
}

/// A natural key for idempotent event creation.
///
/// Occurrence keys allow replay to produce the same logical event
/// identity without relying on material anchor alone. They are
/// parser-defined and source-unit-scoped.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct OccurrenceKey {
    /// The source unit this key is scoped to.
    pub source_unit_id: SourceUnitId,

    /// The key fields that identify this occurrence.
    pub fields: Vec<(String, String)>,
}

// =============================================================================
// Parser manifest
// =============================================================================

/// Metadata declared by a parser at registration time.
///
/// The manifest declares what the parser accepts, what it emits, and
/// what proof obligations it claims to satisfy. It is the parser's
/// public identity record.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ParserManifest {
    /// Stable parser identifier.
    pub parser_id: ParserId,

    /// Semantics version of the parser.
    pub parser_version: String,

    /// What input shape(s) the parser accepts.
    pub accepted_input_shapes: Vec<InputShapeKind>,

    /// The source unit this parser belongs to.
    pub source_unit_id: SourceUnitId,

    /// Event types the parser can emit.
    pub declared_event_types: Vec<(EventSource, EventType)>,

    /// Privacy processing contexts this parser uses.
    #[schemars(skip)]
    pub privacy_contexts: Vec<crate::privacy::ProcessingContext>,

    /// Proof obligations the parser claims.
    #[serde(default)]
    pub proof_obligations: Vec<String>,

    /// Human-readable description.
    #[serde(default)]
    pub description: String,
}

// =============================================================================
// Parser context (passed to parse_record)
// =============================================================================

/// Context provided to a parser during record interpretation.
///
/// Carries provenance identifiers and helpers that parsers need without
/// owning transport, admission, or persistence.
#[derive(Debug, Clone)]
pub struct ParserContext {
    /// Which source unit this parse is for.
    pub source_unit_id: SourceUnitId,

    /// The source material being parsed.
    pub source_material_id: Id<crate::events::SourceMaterial>,

    /// The material anchor of the current record.
    pub record_anchor: MaterialAnchor,

    /// The operation that triggered this parse.
    pub operation_id: Uuid,

    /// The parse job identifier.
    pub job_id: Uuid,

    /// The host running the parser.
    pub host: String,

    /// When the record was acquired (for timestamp derivation).
    pub acquisition_time: Timestamp,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /// Build a minimal material-provenance `ParsedEventIntent` for tests.
    fn material_intent() -> ParsedEventIntent {
        use crate::events::EventPayload as _;
        use crate::events::payloads::DocumentIngestedPayload;

        let payload = DocumentIngestedPayload::test_default();
        ParsedEventIntent {
            id: Id::new(),
            source_unit_id: SourceUnitId::from_static("document.staging"),
            parser_id: ParserId::from_static("document-ingestor"),
            parser_version: "1.0.0".into(),
            event_type: payload.event_type(),
            event_source: payload.event_source(),
            payload: serde_json::to_value(&payload).unwrap(),
            ts_orig: Timestamp::now(),
            timing: TimingEvidence::Atemporal,
            anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            occurrence_key: None,
            privacy_context: crate::privacy::ProcessingContext::Metadata,
            field_privacy_log: None,
            synthesis_parents: None,
        }
    }

    /// Build a synthesis-provenance `ParsedEventIntent` for tests.
    fn synthesis_intent() -> ParsedEventIntent {
        let mut intent = material_intent();
        intent.synthesis_parents = Some(vec![Id::new()]);
        intent
    }

    // ---------------------------------------------------------------------------
    // derive_synthesis tests
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn derive_synthesis_from_material_event_succeeds() -> TestResult<()> {
        use crate::events::payloads::KnowledgeTagAppliedPayload;

        let parent = material_intent();
        let tag_payload = KnowledgeTagAppliedPayload::test_default();

        let child = parent.derive_synthesis(tag_payload)?;

        // synthesis_parents must be populated with the parent's id
        let parents = child
            .synthesis_parents
            .as_ref()
            .expect("synthesis_parents must be Some");
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0], parent.id);
        assert!(child.is_synthesis());
        assert!(!child.is_material());

        Ok(())
    }

    #[sinex_test]
    async fn derive_synthesis_preserves_parent_acquisition_time() -> TestResult<()> {
        use crate::events::payloads::KnowledgeTagAppliedPayload;

        let parent = material_intent();
        let parent_ts = parent.ts_orig;
        let tag_payload = KnowledgeTagAppliedPayload::test_default();

        let child = parent.derive_synthesis(tag_payload)?;

        assert_eq!(
            child.ts_orig, parent_ts,
            "child ts_orig must match parent ts_orig (same temporal window)"
        );

        Ok(())
    }

    #[sinex_test]
    async fn derive_synthesis_assigns_fresh_id() -> TestResult<()> {
        use crate::events::payloads::KnowledgeTagAppliedPayload;

        let parent = material_intent();
        let parent_id = parent.id;
        let tag_payload = KnowledgeTagAppliedPayload::test_default();

        let child = parent.derive_synthesis(tag_payload)?;

        assert_ne!(child.id, parent_id, "child id must differ from parent id");

        Ok(())
    }

    #[sinex_test]
    async fn derive_synthesis_rejects_synthesis_parent() -> TestResult<()> {
        use crate::events::payloads::KnowledgeTagAppliedPayload;

        let parent = synthesis_intent();
        let tag_payload = KnowledgeTagAppliedPayload::test_default();

        let result = parent.derive_synthesis(tag_payload);

        assert!(
            result.is_err(),
            "derive_synthesis must reject a synthesis-provenance parent"
        );
        let err = result.unwrap_err();
        let msg = err.message();
        assert!(
            msg.contains("material-provenance"),
            "error message should mention material-provenance; got: {msg}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn derive_synthesis_uses_new_event_type() -> TestResult<()> {
        use crate::events::EventPayload as _;
        use crate::events::payloads::KnowledgeTagAppliedPayload;

        let parent = material_intent();
        // Parent is document.ingested; child must be knowledge.tag_applied
        let parent_event_type = parent.event_type.clone();
        let tag_payload = KnowledgeTagAppliedPayload::test_default();

        let child = parent.derive_synthesis(tag_payload)?;

        assert_ne!(
            child.event_type, parent_event_type,
            "child event_type must come from the new payload, not the parent"
        );
        assert_eq!(
            child.event_type,
            KnowledgeTagAppliedPayload::EVENT_TYPE,
            "child event_type must match KnowledgeTagAppliedPayload::EVENT_TYPE"
        );
        assert_eq!(
            child.event_source,
            KnowledgeTagAppliedPayload::SOURCE,
            "child event_source must match KnowledgeTagAppliedPayload::SOURCE"
        );

        Ok(())
    }
}
