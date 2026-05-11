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
//! | [`JournalctlStreamAdapter`] | `Subprocess` | journal cursor string | `journalctl -f -o json` |
//! | [`DbusStreamAdapter`] | `DbusSubscription` | `()` (anchor-only) | D-Bus signals via mock or real backend |
//! | [`UnixSocketStreamAdapter`] | `UnixSocket` | `()` (anchor-only) | Line-delimited socket (e.g. Hyprland IPC) |
//! | [`ClipboardPollingAdapter`] | `Polling` | `()` (anchor-only) | Clipboard change detection |
//! | [`DirectoryWalkAdapter`] | `DirectoryWalk` | `BTreeMap<path, fingerprint>` | Recursive walk with fingerprint dedup |

mod append_only_file;
mod clipboard_polling;
mod dbus_stream;
mod directory_walk;
mod file_drop;
mod journalctl_stream;
mod sqlite_row;
mod static_file;
mod unix_socket_stream;

// Existing adapters.
pub use append_only_file::{AppendOnlyCursor, AppendOnlyFileAdapter, AppendOnlyFileConfig};
pub use directory_walk::{DirectoryWalkAdapter, DirectoryWalkConfig, DirectoryWalkCursor, FileFingerprint};
pub use sqlite_row::{SqliteRowAdapter, SqliteRowConfig, SqliteRowCursor};
pub use static_file::{StaticFileCursor, StaticFileAdapter, StaticFileConfig};

// New adapters.
pub use clipboard_polling::{
    ArboardBackend, ClipboardBackend, ClipboardPollingAdapter, ClipboardPollingConfig,
    ClipboardPollingCursor, MockClipboardBackend,
};
pub use dbus_stream::{
    DbusBus, DbusBackend, DbusMessage, DbusStreamAdapter, DbusStreamConfig, DbusStreamCursor,
    MockDbusBackend,
};
pub use file_drop::{FileDropAdapter, FileDropConfig, FileDropCursor, FileDropEventKind};
pub use journalctl_stream::{
    JournalctlCursor, JournalctlStreamAdapter, JournalctlStreamConfig,
    records_from_journal_lines,
};
pub use unix_socket_stream::{
    UnixSocketStreamAdapter, UnixSocketStreamConfig, UnixSocketStreamCursor,
};
