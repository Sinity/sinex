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
use std::{fmt, io::Write};

use tempfile::NamedTempFile;

/// The binding parameters produced by a fixture — passed to the obligation
/// layer so it can configure the source invocation.
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
    #[must_use]
    pub fn new(binding: FixtureBinding, resources: Vec<Box<dyn std::any::Any + Send>>) -> Self {
        Self {
            binding,
            _resources: resources,
        }
    }

    /// Convenience: construct with a single owned resource.
    pub fn with_resource(
        binding: FixtureBinding,
        resource: impl std::any::Any + Send + 'static,
    ) -> Self {
        Self::new(binding, vec![Box::new(resource)])
    }

    /// Convenience: no external resources needed (in-memory only).
    #[must_use]
    pub fn in_memory(binding: FixtureBinding) -> Self {
        Self::new(binding, vec![])
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FileFixtureKind {
    AppendOnly,
    Static,
}

impl fmt::Display for FileFixtureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AppendOnly => f.write_str("append-only"),
            Self::Static => f.write_str("static"),
        }
    }
}

pub fn build_file_fixture(kind: FileFixtureKind, data: &[u8]) -> Result<FixtureHandle, String> {
    let mut file =
        NamedTempFile::new().map_err(|e| format!("failed to create {kind} fixture file: {e}"))?;
    file.write_all(data)
        .map_err(|e| format!("failed to write {kind} fixture data: {e}"))?;
    file.flush()
        .map_err(|e| format!("failed to flush {kind} fixture data: {e}"))?;
    let path = file.path().to_owned();
    Ok(FixtureHandle::with_resource(
        FixtureBinding::FilePath(path),
        file,
    ))
}
