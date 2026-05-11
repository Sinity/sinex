//! Per-adapter fixture helpers.
//!
//! Each submodule provides a `build(data)` function that prepares a fixture
//! appropriate for its adapter kind. Fixtures return a `FixtureHandle` that
//! carries the adapter-specific binding parameters and cleans up on drop.

pub mod append_only_file;
pub mod clipboard;
pub mod dbus;
pub mod file_drop;
pub mod journal;
pub mod sqlite_row;
pub mod static_file;
pub mod unix_socket;

use std::path::PathBuf;

/// The binding parameters produced by a fixture — passed to the obligation
/// layer so it can configure the source-worker invocation.
#[derive(Debug, Clone)]
pub enum FixtureBinding {
    /// A filesystem path (file or watched directory).
    FilePath(PathBuf),
    /// Pre-built source records (journal, dbus, clipboard).
    InMemoryRecords(Vec<Vec<u8>>),
    /// Unix socket path.
    UnixSocketPath(PathBuf),
}

/// An active fixture that holds any tempdir/tempfile handles alive for the
/// duration of the test. Drop to clean up.
pub struct FixtureHandle {
    pub binding: FixtureBinding,
    /// Opaque cleanup resources kept alive by ownership.
    #[allow(dead_code)]
    _resources: Vec<Box<dyn std::any::Any + Send>>,
}

impl FixtureHandle {
    /// Construct a fixture handle from a binding and a set of owned resources
    /// whose Drop impls perform cleanup.
    pub fn new(binding: FixtureBinding, resources: Vec<Box<dyn std::any::Any + Send>>) -> Self {
        Self {
            binding,
            _resources: resources,
        }
    }

    /// Convenience: construct with a single owned resource.
    pub fn with_resource(binding: FixtureBinding, resource: impl std::any::Any + Send + 'static) -> Self {
        Self::new(binding, vec![Box::new(resource)])
    }

    /// Convenience: no external resources needed (in-memory only).
    pub fn in_memory(binding: FixtureBinding) -> Self {
        Self::new(binding, vec![])
    }
}
