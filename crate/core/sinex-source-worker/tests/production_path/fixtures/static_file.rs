//! Static file fixture.
//!
//! Writes `data` to a named temp file for one-shot static parsing.
//! Functionally similar to the append-only file fixture but communicates
//! intent clearly: the file is consumed once and not expected to grow.

use std::io::Write;
use tempfile::NamedTempFile;

use super::{FixtureBinding, FixtureHandle};

/// Build a static file fixture from raw bytes.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::FilePath`
/// pointing at a temp file containing `data`.
///
/// # Errors
///
/// Returns an error if the temp file cannot be created or written.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    let mut file = NamedTempFile::new()
        .map_err(|e| format!("failed to create static fixture file: {e}"))?;
    file.write_all(data)
        .map_err(|e| format!("failed to write static fixture data: {e}"))?;
    file.flush()
        .map_err(|e| format!("failed to flush static fixture data: {e}"))?;
    let path = file.path().to_owned();
    Ok(FixtureHandle::with_resource(FixtureBinding::FilePath(path), file))
}
