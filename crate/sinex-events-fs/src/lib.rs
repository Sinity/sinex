pub mod filesystem;

// Re-export filesystem event types
pub use filesystem::{
    DirCreated, DirCreatedPayload, DirDeleted, DirDeletedPayload, FileCreated, FileCreatedPayload,
    FileDeleted, FileDeletedPayload, FileModified, FileModifiedPayload, FileMoved, FileMovedPayload,
    FilesystemMonitor, FilesystemWatcher, FilesystemConfig,
};

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
