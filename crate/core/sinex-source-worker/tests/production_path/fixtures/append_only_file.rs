//! Append-only file fixture.
//!
//! Writes `data` as lines into a named temp file and returns the path.
//! The caller holds the `FixtureHandle` to keep the temp file alive.

use std::io::Write;
use tempfile::NamedTempFile;

use super::{FixtureBinding, FixtureHandle};

/// Build an append-only file fixture from raw bytes.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::FilePath`
/// pointing at a temp file containing `data`.
///
/// # Errors
///
/// Returns an error if the temp file cannot be created or written.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    let mut file =
        NamedTempFile::new().map_err(|e| format!("failed to create append-only temp file: {e}"))?;
    file.write_all(data)
        .map_err(|e| format!("failed to write fixture data: {e}"))?;
    file.flush()
        .map_err(|e| format!("failed to flush fixture data: {e}"))?;
    let path = file.path().to_owned();
    Ok(FixtureHandle::with_resource(
        FixtureBinding::FilePath(path),
        file,
    ))
}
