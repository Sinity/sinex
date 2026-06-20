//! Append-only file fixture.
//!
//! Writes `data` as lines into a named temp file and returns the path.
//! The caller holds the `FixtureHandle` to keep the temp file alive.

use super::{FileFixtureKind, FixtureHandle, build_file_fixture};

/// Build an append-only file fixture from raw bytes.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::FilePath`
/// pointing at a temp file containing `data`.
///
/// # Errors
///
/// Returns an error if the temp file cannot be created or written.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    build_file_fixture(FileFixtureKind::AppendOnly, data)
}
