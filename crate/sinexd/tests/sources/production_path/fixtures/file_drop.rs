//! File-drop fixture.
//!
//! Creates a temporary directory and writes `data` as a file into it.
//! Simulates the inotify-driven `FileDropAdapter` path: the source unit host
//! watches the directory and picks up the newly-dropped file.

use std::path::PathBuf;
use tempfile::TempDir;

use super::{FixtureBinding, FixtureHandle};

/// Build a file-drop fixture.
///
/// Creates a temporary watched directory and writes `data` as `fixture.dat`
/// inside it.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::FilePath`
/// pointing at the watched directory root. The dropped file name is always
/// `fixture.dat`.
///
/// # Errors
///
/// Returns an error if the temp dir or file cannot be created.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    let dir = TempDir::new().map_err(|e| format!("failed to create file-drop temp dir: {e}"))?;
    let drop_path: PathBuf = dir.path().join("fixture.dat");
    std::fs::write(&drop_path, data)
        .map_err(|e| format!("failed to write file-drop fixture file: {e}"))?;
    let watched_dir: PathBuf = dir.path().to_owned();
    Ok(FixtureHandle::with_resource(
        FixtureBinding::FilePath(watched_dir),
        dir,
    ))
}
