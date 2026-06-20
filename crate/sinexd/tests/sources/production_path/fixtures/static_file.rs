//! Static file fixture.
//!
//! Writes `data` to a named temp file for one-shot static parsing.
//! Functionally similar to the append-only file fixture but communicates
//! intent clearly: the file is consumed once and not expected to grow.

use super::{FileFixtureKind, FixtureHandle, build_file_fixture};

/// Build a static file fixture from raw bytes.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::FilePath`
/// pointing at a temp file containing `data`.
///
/// # Errors
///
/// Returns an error if the temp file cannot be created or written.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    build_file_fixture(FileFixtureKind::Static, data)
}
