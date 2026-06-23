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
//! manifest. The source runtime owns adapter opening, cursor persistence,
//! retry, admission, transport, and confirmation tracking.

#[cfg(feature = "messaging")]
mod adapter_source;
mod adapters;
mod declarative;
pub mod dedup;
mod fingerprint;
mod fixture;
mod weechat;

#[cfg(feature = "messaging")]
pub use adapter_source::{AdapterBackedSource, AdapterModuleState, AdapterSourceConfig};

pub use adapters::{
    // Adapter JSON Schema export (#1238).
    AdapterSchema,
    // ApiCursor adapter (#1746).
    ApiClient,
    ApiCursorAdapter,
    ApiCursorConfig,
    ApiCursorPosition,
    ApiFetchError,
    ApiFetchPage,
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
    DEFAULT_FILE_DROP_MAX_WATCHES,
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
    // IncrementalDump adapter (#1774).
    DumpLoader,
    EmailMboxFileAdapter,
    EmailMboxFileConfig,
    EmailMboxFileCursor,
    FileContentDropAdapter,
    FileContentDropConfig,
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
    GmailApiClient,
    GmailApiCursor,
    GmailApiCursorAdapter,
    GmailApiCursorConfig,
    GmailApiPage,
    GmailApiPageRequest,
    GmailApiRecord,
    GmailApiRecordKind,
    GmailHttpClient,
    ImapSyncAdapter,
    ImapSyncBatch,
    ImapSyncClient,
    ImapSyncConfig,
    ImapSyncCursor,
    ImapSyncMode,
    ImapSyncRecord,
    ImapSyncRecordKind,
    ImapSyncRequest,
    IncrementalDumpAdapter,
    IncrementalDumpConfig,
    IncrementalDumpCursor,
    IncrementalDumpError,
    IncrementalDumpPosition,
    JOURNALCTL_BROADCAST_CAPACITY,
    JournalctlCursor,
    JournalctlStreamAdapter,
    JournalctlStreamConfig,
    JournalctlSubscriber,
    MockClipboardBackend,
    MockDbusBackend,
    NativeImapSyncClient,
    NativeImapSyncClientConfig,
    NativeImapTlsMode,
    RetryPolicy,
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
    UnixSocketStreamMode,
    all_adapter_schemas,
    chained_classify_record,
    choose_file_drop_watch_plan,
    normalized_file_drop_watch_roots,
    records_from_journal_lines,
    survey_file_drop_watch_tree,
};
pub use declarative::{
    CarrySpec, DeclarativeParser, DeclarativeParserSpec, Discriminator, DiscriminatorCase,
    DiscriminatorFallback, FieldSource, FieldSpec, FieldType, InputFormat, StatefulCarryPolicy,
    StatefulDeclarativeParser, SuppressPredicate, TimestampFallback, TimestampFormat,
    TimestampSpec,
};
pub use fingerprint::{DriftAccumulator, DriftEvent, SourceRecordFingerprint};
pub use fixture::{
    FixtureAcceptanceContract, FixtureAssertion, FixtureExpectation, FixtureSpec,
    ParserFixtureHarness, ParserTestContext,
};
pub use weechat::{WeeChatLogConfig, WeeChatLogParser};

use async_trait::async_trait;
use futures::stream::BoxStream;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;

// =============================================================================
// Re-exports from sinex-primitives
// =============================================================================

pub use sinex_primitives::parser::{
    BindingConfig, InputShapeAdapter, InputShapeKind, MaterialParser, ParserError, ParserResult,
    SourceRecord,
};

/// Requested initial position for a continuous adapter stream when no
/// checkpoint cursor exists yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialStreamPosition {
    /// Use the adapter's natural beginning. This preserves historical/import
    /// behavior for explicit scans and for sources that do not opt into live
    /// tail startup.
    Earliest,

    /// Start at the substrate's current end for live-tail operation.
    Latest,
}

// =============================================================================
// InputShapeAdapterExt — cfg-gated methods that depend on runtime internals
// =============================================================================

/// Extension trait for [`InputShapeAdapter`] that adds messaging-gated methods
/// referencing runtime internals (`acquisition_manager`, `SnapshotLaneSpec`).
#[cfg(feature = "messaging")]
#[async_trait]
pub trait InputShapeAdapterExt: InputShapeAdapter {
    /// Open the adapter with access to runtime material acquisition.
    async fn open_with_acquisition(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
        _acquisition: Option<
            std::sync::Arc<crate::runtime::acquisition_manager::AcquisitionManager>,
        >,
    ) -> sinex_primitives::parser::ParserResult<
        BoxStream<'static, sinex_primitives::parser::ParserResult<SourceRecord>>,
    > {
        InputShapeAdapter::open(self, material_id, config, cursor).await
    }

    /// Describe an optional parallel snapshot lane for this adapter.
    fn snapshot_lane(
        &self,
        _source_id: &str,
        _config: &Self::Config,
    ) -> Option<crate::runtime::parser::adapters::SnapshotLaneSpec> {
        None
    }

    /// Return adapter config adjusted for an initial stream position.
    ///
    /// The generic source runtime owns the policy decision. Individual adapters
    /// only translate that policy into substrate-specific config when needed.
    fn configure_initial_stream_position(
        &self,
        config: &Self::Config,
        _position: InitialStreamPosition,
    ) -> sinex_primitives::parser::ParserResult<Self::Config>
    where
        Self::Config: Clone,
    {
        Ok(config.clone())
    }
}
