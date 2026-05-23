//! Parser and input-shape adapter substrate.
//!
//! This module provides the shared traits, adapters, and fixture harness
//! for the staged-source parser architecture (#1097, #1130).
//!
//! # Architecture
//!
//! ```text
//! Source Material -> InputShapeAdapter -> SourceRecord -> MaterialParser -> ParsedEventIntent
//! ```
//!
//! - **`InputShapeAdapter`** owns material access, enumeration, and cursor advancement.
//! - **`MaterialParser`** owns semantic interpretation of records.
//! - **`ParserFixtureHarness`** provides reusable test infrastructure.
//!
//! Parser authors implement `MaterialParser::parse_record()` and declare their
//! manifest. The source-worker runtime owns adapter opening, cursor persistence,
//! retry, admission, transport, and confirmation tracking.

#[cfg(feature = "messaging")]
mod adapter_node;
mod adapters;
mod declarative;
pub mod dedup;
mod fingerprint;
mod fixture;
mod weechat;

#[cfg(feature = "messaging")]
pub use adapter_node::{AdapterBackedIngestor, AdapterNodeConfig, AdapterNodeState};

pub use adapters::{
    // Adapter JSON Schema export (#1238).
    AdapterSchema,
    // Existing adapters.
    AppendOnlyCursor,
    AppendOnlyFileAdapter,
    AppendOnlyFileConfig,
    // New adapters — Phase 1B.
    ArboardBackend,
    CHAINED_PRIMARY_PREFIX,
    CHAINED_SECONDARY_PREFIX,
    // Phase 1C — ChainedAdapter: compose two adapters into one merged stream.
    ChainedAdapter,
    ChainedConfig,
    ChainedCursor,
    ChainedLeg,
    ClipboardBackend,
    ClipboardPollingAdapter,
    ClipboardPollingConfig,
    ClipboardPollingCursor,
    DbusBackend,
    DbusBus,
    DbusMessage,
    DbusStreamAdapter,
    DbusStreamConfig,
    DbusStreamCursor,
    // Phase 1F — DirectoryWalk adapter (9th input-shape adapter).
    DirectoryWalkAdapter,
    DirectoryWalkConfig,
    DirectoryWalkCursor,
    FileDropAdapter,
    FileDropConfig,
    FileDropCursor,
    FileDropEventKind,
    FileDropMoveRole,
    FileDropRecordMetadata,
    FileDropWatchBudget,
    FileDropWatchMode,
    FileDropWatchPlan,
    FileDropWatchSurvey,
    FileFingerprint,
    JOURNALCTL_BROADCAST_CAPACITY,
    JournalctlCursor,
    JournalctlStreamAdapter,
    JournalctlStreamConfig,
    JournalctlSubscriber,
    MockClipboardBackend,
    MockDbusBackend,
    SharedJournalctlStream,
    SqliteRowAdapter,
    SqliteRowConfig,
    SqliteRowCursor,
    StaticFileAdapter,
    StaticFileConfig,
    StaticFileCursor,
    UnixSocketStreamAdapter,
    UnixSocketStreamConfig,
    UnixSocketStreamCursor,
    all_adapter_schemas,
    chained_classify_record,
    choose_file_drop_watch_plan,
    records_from_journal_lines,
};
pub use declarative::{
    BindingConfig, CarrySpec, DeclarativeParser, DeclarativeParserSpec, Discriminator,
    DiscriminatorCase, DiscriminatorFallback, FieldSource, FieldSpec, FieldType, InputFormat,
    StatefulCarryPolicy, StatefulDeclarativeParser, SuppressPredicate, TimestampFallback,
    TimestampFormat, TimestampSpec,
};
pub use fingerprint::{DriftAccumulator, DriftEvent, SourceRecordFingerprint};
pub use fixture::{
    FixtureAcceptanceContract, FixtureAssertion, FixtureExpectation, FixtureSpec,
    ParserFixtureHarness, ParserTestContext,
};
pub use weechat::{WeeChatLogConfig, WeeChatLogParser};

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::{error::Error, fmt};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserManifest, SourceRecord,
};

/// Error type for parser and adapter operations.
#[derive(Debug)]
pub enum ParserError {
    Adapter(String),

    Parse(String),

    Cursor(String),

    Io(std::io::Error),

    /// `SinexError` raised by code the parser calls (privacy engine, validators,
    /// inner SDK helpers). Converted via `?` so parsers don't have to
    /// `.map_err(|e| ParserError::Parse(e.to_string()))` everywhere.
    Sinex(sinex_primitives::SinexError),

    InvalidInput(String),

    Config(String),

    MaterialNotFound(String),

    Field(String),

    Decode(String),

    Privacy(String),
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
            Self::Privacy(message) => write!(f, "privacy engine error: {message}"),
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

impl From<sinex_primitives::SinexError> for ParserError {
    fn from(error: sinex_primitives::SinexError) -> Self {
        Self::Sinex(error)
    }
}

/// Result type for parser substrate operations.
pub type ParserResult<T> = Result<T, ParserError>;

// =============================================================================
// InputShapeAdapter trait
// =============================================================================

/// Adapts a specific input shape into a stream of [`SourceRecord`]s.
///
/// Implementations own material access, enumeration, and cursor advancement.
/// The source-worker runtime calls `open()` to get a record stream and
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
    ) -> ParserResult<Option<SourceRecordFingerprint>> {
        Ok(None)
    }

    /// Compute the cursor position after consuming `record`.
    ///
    /// This is called by the runtime after each record is successfully
    /// parsed, so that checkpoints can be persisted.
    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor>;

    /// Describe an optional **parallel snapshot lane** for this adapter.
    ///
    /// Adapters that have a meaningful "whole substrate" snapshot — currently
    /// only `SqliteRowAdapter`, which can snapshot the underlying database
    /// file — return `Some(SnapshotLaneSpec)` to opt in. The
    /// [`AdapterBackedIngestor`] hosting the adapter spawns a tokio task that
    /// captures the substrate on a periodic timer, into a separate source
    /// material lineage from the per-record stream.
    ///
    /// Returns `None` by default. Snapshot lanes are independent of per-record
    /// drain: events stay anchored in their record materials.
    ///
    /// [`AdapterBackedIngestor`]: crate::parser::adapter_node::AdapterBackedIngestor
    /// [`SnapshotLaneSpec`]: crate::parser::adapters::SnapshotLaneSpec
    #[cfg(feature = "messaging")]
    fn snapshot_lane(
        &self,
        _source_unit_id: &str,
        _config: &Self::Config,
    ) -> Option<crate::parser::adapters::SnapshotLaneSpec> {
        None
    }
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
    /// The user-supplied `--node-config` JSON is merged OVER this baseline
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
