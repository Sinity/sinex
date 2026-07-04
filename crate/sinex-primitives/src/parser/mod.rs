//! Parser and input-shape substrate types.
//!
//! These types define the boundary-crossing contracts for the staged-source
//! parser architecture (#1097). They are shared across `sinex-primitives`,
//! `sinexd`, `sinex-db`, and `sinex-schema` so that parser authors,
//! source runtime, and schema/repository layers share a single
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
pub mod declarative;
pub mod fingerprint;
pub mod occurrence_filter;

use std::borrow::Cow;
use std::{error::Error, fmt};

use async_trait::async_trait;
use bon::Builder;
use camino::Utf8PathBuf;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
            Cow::Owned(_) => {
                unreachable!(
                    "ParserId::as_static_str is only valid on const-constructed (borrowed) ids"
                )
            }
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

/// Identifies a logical source that parsers operate within.
///
/// A source is the stable identity that groups parser instances,
/// emitted event types, and configuration. It is NOT a process or
/// deployment identity — that is the source-domain/service split.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct SourceId(Cow<'static, str>);

impl<'de> serde::Deserialize<'de> for SourceId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::validate_str(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(Cow::Owned(s)))
    }
}

impl SourceId {
    /// Creates a validated `SourceId` from a string.
    pub fn new(s: impl Into<String>) -> Result<Self, SinexError> {
        let s = s.into();
        Self::validate_str(&s)?;
        Ok(Self(Cow::Owned(s)))
    }

    /// Creates a const `SourceId` from a static string literal.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Self {
        assert!(
            Self::const_validate(s),
            "SourceId must match [a-z][a-z0-9_.-]*"
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
            Cow::Owned(_) => {
                unreachable!(
                    "SourceId::as_static_str is only valid on const-constructed (borrowed) ids"
                )
            }
        }
    }

    fn validate_str(s: &str) -> Result<(), SinexError> {
        if s.is_empty() {
            return Err(SinexError::validation("SourceId must not be empty"));
        }
        if !s.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' || c == '.'
        }) {
            return Err(SinexError::validation(
                "SourceId must contain only [a-z0-9_.-]",
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

impl std::fmt::Display for SourceId {
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

    /// `SQLite` database queried via rowid cursor.
    SqliteQuery,

    /// A git repository snapshot.
    RepositorySnapshot,

    /// An API cursor-based pagination source.
    ApiCursor,

    /// A periodically re-exported full dump where each export supersets the
    /// previous one (e.g. a GDPR/Takeout archive). Records already seen in a
    /// prior export are skipped via an order-key high-water mark, so only new
    /// records are emitted.
    IncrementalDump,

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
            Self::IncrementalDump => "incremental_dump",
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

    /// A row in a `SQLite` table.
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
    /// Timestamp observed at a live capture boundary.
    ///
    /// Used by streaming adapters that receive one live record at a time
    /// from a socket, bus, or similar runtime wrapper. The timestamp is not
    /// intrinsic content, but it is the best real-world observation time for
    /// the occurrence.
    RealtimeCapture {
        value: Timestamp,
        capture_source: String,
    },

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

impl TimingEvidence {
    /// Resolve this parser-side timing evidence to a quality rung on the
    /// temporal ladder, when the parser owns the answer (#1570 Prong B).
    ///
    /// Returns `None` for evidence that the parser cannot resolve on its own:
    /// - [`Self::Wrapper`] — defers to the sub-material temporal ledger;
    /// - [`Self::StagedAtFallback`] / [`Self::Atemporal`] — defer to the
    ///   material-tier timing on `raw.source_material_registry`.
    ///
    /// In those cases the event leaves `ts_orig`/`ts_quality` unresolved and the
    /// persistence (admission) stage fills them in from the material tier.
    #[must_use]
    pub fn resolved_quality(&self) -> Option<crate::domain::TemporalSourceType> {
        use crate::domain::TemporalSourceType;
        match self {
            Self::RealtimeCapture { .. } => Some(TemporalSourceType::RealtimeCapture),
            Self::Intrinsic { .. } => Some(TemporalSourceType::IntrinsicContent),
            Self::InferredMtime { .. } => Some(TemporalSourceType::InferredMtime),
            Self::UserDeclared { .. } => Some(TemporalSourceType::InferredUser),
            Self::Wrapper { .. } | Self::StagedAtFallback | Self::Atemporal => None,
        }
    }

    /// Return the concrete timestamp carried by this timing evidence, when it
    /// contains one directly.
    #[must_use]
    pub fn timestamp_value(&self) -> Option<Timestamp> {
        match self {
            Self::RealtimeCapture { value, .. } | Self::UserDeclared { value, .. } => Some(*value),
            Self::InferredMtime { mtime, .. } => Some(*mtime),
            Self::Intrinsic { .. }
            | Self::Wrapper { .. }
            | Self::StagedAtFallback
            | Self::Atemporal => None,
        }
    }
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

/// Error type for parser and adapter operations.
#[derive(Debug)]
pub enum ParserError {
    Adapter(String),

    Parse(String),

    Cursor(String),

    Io(std::io::Error),

    /// `SinexError` raised by code the parser calls (validators, runtime
    /// helpers). Converted via `?` so parsers don't have to
    /// `.map_err(|e| ParserError::Parse(e.to_string()))` everywhere.
    Sinex(crate::SinexError),

    InvalidInput(String),

    Config(String),

    MaterialNotFound(String),

    Field(String),

    Decode(String),
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(message) => write!(f, "adapter error: {message}"),
            Self::Parse(message) => write!(f, "parse error: {message}"),
            Self::Cursor(message) => write!(f, "cursor error: {message}"),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Sinex(error) => write!(f, "{error}"),
            Self::InvalidInput(message) => write!(f, "invalid input: {message}"),
            Self::Config(message) => write!(f, "configuration error: {message}"),
            Self::MaterialNotFound(message) => write!(f, "material not found: {message}"),
            Self::Field(message) => write!(f, "field error: {message}"),
            Self::Decode(message) => write!(f, "decode error: {message}"),
        }
    }
}

impl Error for ParserError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Sinex(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ParserError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<crate::SinexError> for ParserError {
    fn from(error: crate::SinexError) -> Self {
        Self::Sinex(error)
    }
}

/// Result type for parser substrate operations.
pub type ParserResult<T> = Result<T, ParserError>;

// =============================================================================
// Binding config — runtime values that suppress predicates check against
// =============================================================================

/// Runtime configuration values that `#[suppress_if]` predicates and other
/// binding-aware fields read at parse time. Supplied by the source host
/// from the active source-binding.
#[derive(Debug, Clone, Default)]
pub struct BindingConfig {
    flags: BTreeMap<String, bool>,
}

impl BindingConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_flag(mut self, name: impl Into<String>, value: bool) -> Self {
        self.flags.insert(name.into(), value);
        self
    }

    #[must_use]
    pub fn is_truthy(&self, name: &str) -> bool {
        self.flags.get(name).copied().unwrap_or(false)
    }
}

// =============================================================================
// InputShapeAdapter trait
// =============================================================================

/// Adapts a specific input shape into a stream of [`SourceRecord`]s.
///
/// Implementations own material access, enumeration, and cursor advancement.
/// The source runtime calls `open()` to get a record stream and
/// `cursor_after()` to advance the checkpoint after each record.
///
/// # Invariants
///
/// - `cursor_after(record)` must be monotonic within one material/input shape.
/// - Anchors must identify the occurrence inside the material, not the parser's
///   output row number.
/// - The stream must not buffer unboundedly — callers drive consumption.
#[async_trait]
pub trait InputShapeAdapter: Send + Sync {
    /// Adapter-specific configuration.
    type Config: DeserializeOwned + Serialize + Send + Sync;

    /// Cursor type for resumption.
    type Cursor: DeserializeOwned + Serialize + Clone + Send + Sync;

    /// The input shape kind this adapter handles.
    const KIND: InputShapeKind;

    /// Open the material and produce a stream of source records.
    ///
    /// If `cursor` is `Some`, the adapter should resume from that position.
    /// The returned stream must be driven to completion by the caller.
    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>>;

    /// Optionally compute a bounded structural fingerprint for the input
    /// substrate before row/record parsing.
    ///
    /// Adapters with a cheap schema/header surface can override this so
    /// callers can compare upstream shape before parser logic silently
    /// degrades. The default keeps existing adapters out of the drift path.
    fn input_fingerprint(
        &self,
        _config: &Self::Config,
    ) -> ParserResult<Option<fingerprint::SourceRecordFingerprint>> {
        Ok(None)
    }

    /// Compute the cursor position after consuming `record`.
    ///
    /// This is called by the runtime after each record is successfully
    /// parsed, so that checkpoints can be persisted.
    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor>;
}

// =============================================================================
// MaterialParser trait
// =============================================================================

/// Parses [`SourceRecord`]s into [`ParsedEventIntent`]s.
///
/// Parser authors implement this trait. They receive anchored bytes from
/// the input-shape adapter and return event intents. The runtime owns
/// admission, privacy, NATS publication, and confirmation tracking.
///
/// # Implementation guidance
///
/// - `manifest()` should return a static manifest with the parser's identity.
/// - `parse_record()` is called once per source record, in order.
/// - `parse_record_with_binding()` is called by binding-aware hosts and
///   defaults to `parse_record()` for parsers that do not consult runtime
///   binding flags.
/// - Return `Ok(vec![])` to skip a record without an error.
/// - Return an error only when the record is genuinely unparseable.
#[async_trait]
pub trait MaterialParser: Send + Sync {
    /// Parser-specific configuration.
    type Config: DeserializeOwned + Serialize + Send + Sync;

    /// Return the parser's static manifest.
    fn manifest(&self) -> ParserManifest;

    /// Parser-declared source-record keys that must be present for the parser
    /// to interpret records correctly.
    ///
    /// Imperative parsers can override this when they have a stable structural
    /// input contract. Generated declarative parsers derive it from their
    /// [`DeclarativeParserSpec`].
    #[must_use]
    fn required_input_keys(&self) -> Vec<String> {
        Vec::new()
    }

    /// Parser-declared field-level privacy metadata.
    ///
    /// Generated declarative parsers derive this from their
    /// [`DeclarativeParserSpec`]. Imperative parsers default to no field rows
    /// until they explicitly declare a stable field contract.
    #[must_use]
    fn field_privacy_metadata(&self) -> Vec<ParserFieldPrivacyMetadata> {
        Vec::new()
    }

    /// Parse a single source record into zero or more event intents.
    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>>;

    /// Parse a single source record with runtime binding flags.
    ///
    /// Generated declarative parsers use this to honor `#[suppress_if]`
    /// predicates such as `private_mode_active`. Imperative parsers can ignore
    /// the binding config by relying on this default implementation.
    async fn parse_record_with_binding(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
        _binding: &BindingConfig,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        self.parse_record(record, ctx).await
    }

    /// Optional baseline adapter config supplied by the parser type itself.
    ///
    /// The user-supplied `--runtime-config` JSON is merged OVER this baseline
    /// (user keys win) before deserializing into the adapter's `Config`.
    /// Lets a parser declare adapter-mandatory fields it knows the right
    /// value for (e.g. atuin's SqliteRowConfig.query = "history") without
    /// forcing every Nix binding to repeat them. Default empty object.
    #[must_use]
    fn baseline_adapter_config() -> serde_json::Value
    where
        Self: Sized,
    {
        serde_json::Value::Object(serde_json::Map::new())
    }
}
pub use occurrence_filter::{OccurrenceFilter, maybe_occurrence_key_string, occurrence_key_string};

/// A single event that a parser intends to publish.
///
/// This is the parser's output contract. The source host or transport
/// layer owns admission, privacy, NATS publication, and confirmation
/// tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(on(String, into))]
pub struct ParsedEventIntent {
    /// A freshly-generated `UUIDv7` identity for this intent.
    ///
    /// The transport layer uses this as the event ID it persists and
    /// references in confirmations. Derived intents reference their
    /// parent's `id` in `derived_parents`.
    #[builder(default = Id::new())]
    pub id: EventId,

    /// Which source the parser belongs to.
    pub source_id: SourceId,

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
    /// For derived intents produced via [`ParsedEventIntent::derive_from_parents`],
    /// this carries the parent's anchor verbatim (no independent material
    /// position). The transport layer uses `derived_parents` to detect derived
    /// provenance and ignores `anchor` for those intents.
    pub anchor: MaterialAnchor,

    /// An optional natural key for idempotent event creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub occurrence_key: Option<OccurrenceKey>,

    /// Privacy processing context for this event.
    pub privacy_context: crate::privacy::ProcessingContext,

    /// Parent event IDs for derived provenance.
    ///
    /// `None` means this intent carries **material provenance** — it was
    /// derived directly from source bytes.  `Some(ids)` means this intent
    /// carries **derived provenance** — it was derived from one or more
    /// already-persisted events.  The transport layer checks this field
    /// before constructing the `Provenance` variant for DB insertion.
    ///
    /// Populated by [`ParsedEventIntent::derive_from_parents`]; do not set
    /// manually unless you are constructing a derived intent explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_parents: Option<Vec<EventId>>,
}

impl ParsedEventIntent {
    /// Derive a derived event from this material-provenance intent.
    ///
    /// Given `self` (a material-provenance parsed event), builds a new
    /// `ParsedEventIntent` whose provenance is
    /// `Derived { source_event_ids: [self.id] }`.
    ///
    /// The returned intent:
    /// - Carries the parent's `source_id`, `parser_id`, `parser_version`,
    ///   `acquisition_time` (via `ts_orig`), and `anchor` (transport layer
    ///   ignores it for derived intents).
    /// - Has its own freshly-generated `id` (`UUIDv7`).
    /// - Has `derived_parents = Some(vec![self.id])` pointing to `self`.
    /// - Has `event_source` and `event_type` taken from `P::SOURCE` /
    ///   `P::EVENT_TYPE` (the *new* payload, **not** the parent's types).
    /// - Has `occurrence_key = None`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if:
    /// - `self` already has derived provenance (`derived_parents.is_some()`).
    ///   Chained derived requires explicit construction with the full parent
    ///   set — this helper is intentionally limited to single-hop derivation from
    ///   a material-provenance parent.
    /// - `self.id` would appear as both parent and child (self-referential
    ///   derived). This is structurally impossible with a freshly generated
    ///   child ID, but the check is made explicit for correctness.
    pub fn derive_from_parents<P>(&self, payload: &P) -> Result<ParsedEventIntent, ParserError>
    where
        P: crate::events::EventPayload,
    {
        // Reject derived-from-derived: chained derived needs explicit
        // construction with the complete parent set.
        if self.derived_parents.is_some() {
            return Err(SinexError::validation(
                "derive_from_parents requires a material-provenance parent; \
                 chained derived must be constructed explicitly with the full parent set",
            )
            .with_context("parent_id", self.id.to_uuid().to_string())
            .into());
        }

        let child_id: EventId = Id::new();

        // Self-referential derived is impossible with a fresh ID, but guard
        // it explicitly so the invariant is visible and testable.
        if child_id == self.id {
            return Err(SinexError::validation(
                "derive_from_parents produced a self-referential derived (child id == parent id)",
            )
            .into());
        }

        let child_payload = serde_json::to_value(payload).map_err(|e| {
            SinexError::serialization("failed to serialize derived payload")
                .with_context("event_type", P::EVENT_TYPE.as_str().to_string())
                .with_std_error(&e)
        })?;

        Ok(ParsedEventIntent {
            id: child_id,
            source_id: self.source_id.clone(),
            parser_id: self.parser_id.clone(),
            parser_version: self.parser_version.clone(),
            event_type: P::EVENT_TYPE,
            event_source: P::SOURCE,
            payload: child_payload,
            // Preserve the parent's real-world timestamp so the derived
            // event sits in the same temporal window as its material parent.
            ts_orig: self.ts_orig,
            timing: self.timing.clone(),
            // Carry the parent anchor verbatim; transport layer uses
            // derived_parents to detect derived and ignores anchor.
            anchor: self.anchor.clone(),
            occurrence_key: None,
            privacy_context: self.privacy_context,
            derived_parents: Some(vec![self.id]),
        })
    }

    /// Returns `true` if this intent carries material provenance.
    #[must_use]
    pub fn is_material(&self) -> bool {
        self.derived_parents.is_none()
    }

    /// Returns `true` if this intent carries derived provenance.
    #[must_use]
    pub fn is_synthesis(&self) -> bool {
        self.derived_parents.is_some()
    }
}

/// A natural key for idempotent event creation.
///
/// Occurrence keys allow replay to produce the same logical event
/// identity without relying on material anchor alone. They are
/// parser-defined and source-scoped.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct OccurrenceKey {
    /// The source this key is scoped to.
    pub source_id: SourceId,

    /// The key fields that identify this occurrence.
    pub fields: Vec<(String, String)>,
}

// =============================================================================
// Parser manifest
// =============================================================================

/// Metadata declared by a parser at registration time.
///
/// The manifest declares what the parser accepts, what it emits, and
/// which catalog obligations or descriptor-local verification tags apply.
/// It is the parser's public identity record.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ParserManifest {
    /// Stable parser identifier.
    pub parser_id: ParserId,

    /// Semantics version of the parser.
    pub parser_version: String,

    /// What input shape(s) the parser accepts.
    pub accepted_input_shapes: Vec<InputShapeKind>,

    /// The source this parser belongs to.
    pub source_id: SourceId,

    /// Event types the parser can emit.
    pub declared_event_types: Vec<(EventSource, EventType)>,

    /// Privacy processing contexts this parser uses.
    #[schemars(skip)]
    pub privacy_contexts: Vec<crate::privacy::ProcessingContext>,

    /// Union of semantic sensitivity-class hints declared across the parser's
    /// fields, exported for policy tooling (#1611).
    #[schemars(skip)]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sensitivity_hints: Vec<crate::privacy::SensitivityHint>,

    /// Human-readable description.
    #[serde(default)]
    pub description: String,
}

/// Field-level privacy metadata declared by a parser.
///
/// This is descriptive metadata for inventory and audit tooling. It does not
/// apply redaction by itself, and an empty set means "unavailable", not "safe".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserFieldPrivacyMetadata {
    /// Field name in the emitted payload.
    pub field_name: String,

    /// Coerced field value class, e.g. `string` or `integer`.
    pub field_type: String,

    /// Source-record class, e.g. `json_pointer`, `column_index`, or `raw_line`.
    pub field_class: String,

    /// Source-record structural key when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_shape_key: Option<String>,

    /// Effective privacy context after applying the parser default.
    pub effective_privacy_context: crate::privacy::ProcessingContext,

    /// Semantic sensitivity hints declared for this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sensitivity_hints: Vec<crate::privacy::SensitivityHint>,

    /// True when the field is excluded from emitted payloads.
    pub skip_payload: bool,

    /// True when the field participates in the parser occurrence key.
    pub occurrence_key: bool,

    /// Binding predicate that can suppress the field or whole event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suppress_if: Option<SuppressPredicate>,
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
    /// Which source this parse is for.
    pub source_id: SourceId,

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
// Re-exports
// =============================================================================

pub use declarative::{
    CarrySpec, DeclarativeParser, DeclarativeParserSpec, Discriminator, DiscriminatorCase,
    DiscriminatorFallback, FieldSource, FieldSpec, FieldTransform, FieldType, FieldValidator,
    InputFormat, StatefulCarryPolicy, StatefulDeclarativeParser, SuppressPredicate,
    TimestampFallback, TimestampFormat, TimestampSpec,
};
pub use fingerprint::{DriftAccumulator, DriftEvent, SourceRecordFingerprint};

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
#[path = "../parser_test.rs"]
mod tests;
