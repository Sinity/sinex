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
//! - **InputShapeAdapter** owns material access, enumeration, and cursor advancement.
//! - **MaterialParser** owns semantic interpretation of records.
//! - **ParserFixtureHarness** provides reusable test infrastructure.
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
    // Existing adapters.
    AppendOnlyCursor, AppendOnlyFileAdapter, AppendOnlyFileConfig,
    SqliteRowAdapter, SqliteRowConfig, SqliteRowCursor,
    StaticFileAdapter, StaticFileConfig, StaticFileCursor,
    // New adapters — Phase 1B.
    ArboardBackend, ClipboardBackend, ClipboardPollingAdapter, ClipboardPollingConfig,
    ClipboardPollingCursor, MockClipboardBackend,
    DbusBus, DbusBackend, DbusMessage, DbusStreamAdapter, DbusStreamConfig, DbusStreamCursor,
    MockDbusBackend,
    FileDropAdapter, FileDropConfig, FileDropCursor, FileDropEventKind,
    JournalctlCursor, JournalctlStreamAdapter, JournalctlStreamConfig,
    JournalctlSubscriber, SharedJournalctlStream,
    JOURNALCTL_BROADCAST_CAPACITY,
    records_from_journal_lines,
    UnixSocketStreamAdapter, UnixSocketStreamConfig, UnixSocketStreamCursor,
    // Phase 1F — DirectoryWalk adapter (9th input-shape adapter).
    DirectoryWalkAdapter, DirectoryWalkConfig, DirectoryWalkCursor, FileFingerprint,
    // Phase 1C — ChainedAdapter: compose two adapters into one merged stream.
    ChainedAdapter, ChainedConfig, ChainedCursor, ChainedLeg,
    chained_classify_record,
    CHAINED_PRIMARY_PREFIX, CHAINED_SECONDARY_PREFIX,
};
pub use declarative::{
    BindingConfig, CarrySpec, DeclarativeParser, DeclarativeParserSpec,
    Discriminator, DiscriminatorCase, DiscriminatorFallback,
    FieldSource, FieldSpec, FieldType,
    InputFormat, StatefulCarryPolicy, StatefulDeclarativeParser,
    SuppressPredicate, TimestampFallback, TimestampFormat, TimestampSpec,
};
pub use fingerprint::{DriftAccumulator, DriftEvent, SourceRecordFingerprint};
pub use fixture::{
    FixtureAssertion, FixtureExpectation, FixtureSpec, ParserFixtureHarness,
    ParserTestContext,
};
pub use weechat::{WeeChatLogConfig, WeeChatLogParser};

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use serde::Serialize;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserManifest, SourceRecord,
};

/// Error type for parser and adapter operations.
#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    #[error("adapter error: {0}")]
    Adapter(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("cursor error: {0}")]
    Cursor(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("material not found: {0}")]
    MaterialNotFound(String),

    #[error("field error: {0}")]
    Field(String),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("privacy engine error: {0}")]
    Privacy(String),
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
/// - Return `Ok(vec![])` to skip a record without an error.
/// - Return an error only when the record is genuinely unparseable.
#[async_trait]
pub trait MaterialParser: Send + Sync {
    /// Parser-specific configuration.
    type Config: DeserializeOwned + Serialize + Send + Sync;

    /// Return the parser's static manifest.
    fn manifest(&self) -> ParserManifest;

    /// Parse a single source record into zero or more event intents.
    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>>;
}
