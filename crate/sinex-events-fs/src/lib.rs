pub mod filesystem;
pub mod typed_adapter;
pub mod typed_monitor;

// Re-export filesystem event types
pub use filesystem::{
    DirCreated, DirCreatedPayload, DirDeleted, DirDeletedPayload, FileCreated, FileCreatedPayload,
    FileDeleted, FileDeletedPayload, FileModified, FileModifiedPayload, FileMoved,
    FileMovedPayload, FilesystemConfig, FilesystemMonitor, FilesystemWatcher,
};

// Re-export typed monitor and adapter
pub use typed_adapter::TypedFilesystemAdapter;
pub use typed_monitor::TypedFilesystemMonitor;

use sinex_core::register_events;

// Register all filesystem event types using the macro
register_events! {
    "file.created" => (fs, FileCreatedPayload),
    "file.modified" => (fs, FileModifiedPayload),
    "file.deleted" => (fs, FileDeletedPayload),
    "file.moved" => (fs, FileMovedPayload),
    "dir.created" => (fs, DirCreatedPayload),
    "dir.deleted" => (fs, DirDeletedPayload),
}
