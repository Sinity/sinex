//! Clipboard fixture.
//!
//! Builds a sequence of clipboard snapshots for `MockClipboardBackend`.
//! Each non-empty line in `data` is one clipboard state; an empty line
//! represents a `None` (empty clipboard) transition.

use super::{FixtureBinding, FixtureHandle};

/// Build a clipboard fixture from raw bytes.
///
/// `data` is treated as newline-delimited text. Each line becomes one
/// clipboard snapshot:
/// - Non-empty line → `Some(line.to_string())`
/// - Empty line → `None` (clipboard cleared)
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::InMemoryRecords`
/// containing the UTF-8 bytes of each non-None snapshot.
///
/// # Errors
///
/// Returns an error if `data` is not valid UTF-8.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    let text = std::str::from_utf8(data)
        .map_err(|e| format!("clipboard fixture data is not valid UTF-8: {e}"))?;

    let snapshots: Vec<Option<String>> = text
        .lines()
        .map(|line| {
            if line.trim().is_empty() {
                None
            } else {
                Some(line.to_string())
            }
        })
        .collect();

    // Expose non-None snapshots as record bytes for dispatch-level testing.
    let record_bytes: Vec<Vec<u8>> = snapshots
        .iter()
        .filter_map(|s| s.as_ref())
        .map(|s| s.as_bytes().to_vec())
        .collect();

    Ok(FixtureHandle::in_memory(FixtureBinding::InMemoryRecords(record_bytes)))
}

/// Build a clipboard fixture directly from snapshot strings.
///
/// `snapshots` is an ordered list of clipboard states: `Some(text)` for
/// a content change, `None` for a clear event.
pub fn build_from_snapshots(snapshots: Vec<Option<String>>) -> FixtureHandle {
    let record_bytes: Vec<Vec<u8>> = snapshots
        .iter()
        .filter_map(|s| s.as_ref())
        .map(|s| s.as_bytes().to_vec())
        .collect();
    FixtureHandle::in_memory(FixtureBinding::InMemoryRecords(record_bytes))
}
