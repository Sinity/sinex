//! Input-shape adapter implementations.
//!
//! This module provides [`InputShapeAdapter`] implementations for all supported
//! input shapes. Each adapter lives in its own sub-module for clarity.
//!
//! # Adapters
//!
//! | Adapter | `InputShapeKind` | Cursor | Notes |
//! |---|---|---|---|
//! | [`StaticFileAdapter`] | `StaticFile` | bool (processed?) | One-shot file read |
//! | [`AppendOnlyFileAdapter`] | `AppendOnlyFile` | line + byte offset | Log-file style |
//! | [`SqliteRowAdapter`] | `SqliteQuery` | rowid | Read-only SQLite |
//! | [`FileDropAdapter`] | `FileDrop` | `()` (anchor-only) | Live inotify watcher |
//! | [`FileContentDropAdapter`] | `FileDrop` | `()` (anchor-only) | Live watcher with opt-in file-content materialization |
//! | [`JournalctlStreamAdapter`] | `Subprocess` | journal cursor string | `journalctl -f -o json` |
//! | [`DbusStreamAdapter`] | `DbusSubscription` | `()` (anchor-only) | D-Bus signals via mock or real backend |
//! | [`UnixSocketStreamAdapter`] | `UnixSocket` | `()` (anchor-only) | Line-delimited socket (e.g. Hyprland IPC) |
//! | [`ClipboardPollingAdapter`] | `Polling` | `()` (anchor-only) | Clipboard change detection |
//! | [`DirectoryWalkAdapter`] | `DirectoryWalk` | `BTreeMap<path, fingerprint>` | Recursive walk with fingerprint dedup |

pub mod adapter_schemas;
mod append_only_file;
mod chained;
mod clipboard_polling;
mod dbus_stream;
mod directory_walk;
mod file_drop;
mod journalctl_stream;
mod sqlite_row;
#[cfg(feature = "messaging")]
mod sqlite_snapshot;
mod static_file;
mod unix_socket_stream;

// Existing adapters.
pub use append_only_file::{AppendOnlyCursor, AppendOnlyFileAdapter, AppendOnlyFileConfig};
pub use chained::{
    ChainedAdapter, ChainedConfig, ChainedCursor, ChainedLeg,
    PRIMARY_PREFIX as CHAINED_PRIMARY_PREFIX, SECONDARY_PREFIX as CHAINED_SECONDARY_PREFIX,
    classify_record as chained_classify_record,
};
pub use directory_walk::{
    DirectoryWalkAdapter, DirectoryWalkConfig, DirectoryWalkCursor, FileFingerprint,
};
pub use sqlite_row::{SqliteRowAdapter, SqliteRowConfig, SqliteRowCursor};
#[cfg(feature = "messaging")]
pub use sqlite_snapshot::{SnapshotLaneSpec, SqliteSnapshotConfig, SqliteSnapshotLane};
pub use static_file::{StaticFileAdapter, StaticFileConfig, StaticFileCursor};

// New adapters.
pub use clipboard_polling::{
    ArboardBackend, ClipboardBackend, ClipboardPollingAdapter, ClipboardPollingConfig,
    ClipboardPollingCursor, MockClipboardBackend,
};
pub use dbus_stream::{
    DbusBackend, DbusBus, DbusMessage, DbusStreamAdapter, DbusStreamConfig, DbusStreamCursor,
    MockDbusBackend,
};
pub use file_drop::{
    DEFAULT_FILE_DROP_MAX_WATCHES, FileContentDropAdapter, FileContentDropConfig, FileDropAdapter,
    FileDropConfig, FileDropCursor, FileDropEventKind, FileDropMoveRole, FileDropRecordMetadata,
    FileDropWatchBudget, FileDropWatchMode, FileDropWatchPlan, FileDropWatchSurvey,
    choose_file_drop_watch_plan, normalized_file_drop_watch_roots, survey_file_drop_watch_tree,
};
pub use journalctl_stream::{
    BROADCAST_CAPACITY as JOURNALCTL_BROADCAST_CAPACITY, JournalctlCursor, JournalctlStreamAdapter,
    JournalctlStreamConfig, JournalctlSubscriber, SharedJournalctlStream,
    records_from_journal_lines,
};
pub use unix_socket_stream::{
    UnixSocketStreamAdapter, UnixSocketStreamConfig, UnixSocketStreamCursor,
};

pub use adapter_schemas::{AdapterSchema, all_adapter_schemas};

// =============================================================================
// InputShapeAdapterExt impls — default bodies for adapters that don't need
// custom open_with_acquisition or snapshot_lane behaviour
// =============================================================================

#[cfg(feature = "messaging")]
use crate::runtime::parser::InputShapeAdapterExt;

#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for append_only_file::AppendOnlyFileAdapter {}
#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for clipboard_polling::ClipboardPollingAdapter {}
#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for dbus_stream::DbusStreamAdapter {}
#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for directory_walk::DirectoryWalkAdapter {}
#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for file_drop::FileDropAdapter {}
#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for journalctl_stream::JournalctlStreamAdapter {}
#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for static_file::StaticFileAdapter {}
#[cfg(feature = "messaging")]
impl InputShapeAdapterExt for unix_socket_stream::UnixSocketStreamAdapter {}

#[cfg(feature = "messaging")]
impl<A, B> InputShapeAdapterExt for chained::ChainedAdapter<A, B>
where
    A: crate::runtime::parser::InputShapeAdapter + 'static,
    B: crate::runtime::parser::InputShapeAdapter + 'static,
{
}
