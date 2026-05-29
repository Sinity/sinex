//! Journal fixture.
//!
//! Feeds lines through `records_from_journal_lines` and returns the resulting
//! records as in-memory bytes for the obligation layer.
//!
//! The `data` bytes are interpreted as newline-delimited JSON journal lines
//! (the same format `journalctl --output=json` produces).

use sinexd::node_sdk::parser::{ParserResult, records_from_journal_lines};
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::SourceRecord;

use super::{FixtureBinding, FixtureHandle};

/// Build a journal fixture from raw journal JSON lines.
///
/// `data` should contain newline-separated JSON objects in the format produced
/// by `journalctl --output=json`. Each non-empty line is turned into a
/// `SourceRecord` byte payload.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::InMemoryRecords`
/// containing the serialized record bytes.
///
/// # Errors
///
/// Returns an error if `data` is not valid UTF-8.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    let text = std::str::from_utf8(data)
        .map_err(|e| format!("journal fixture data is not valid UTF-8: {e}"))?;

    let lines: Vec<&str> = text.lines().collect();
    let material_id = Id::<SourceMaterial>::new();

    let records = records_from_journal_lines(material_id, &lines);

    // Flatten the records to their raw bytes for the obligation layer.
    let record_bytes: Vec<Vec<u8>> = records
        .into_iter()
        .filter_map(|r: ParserResult<SourceRecord>| r.ok())
        .map(|r| r.bytes)
        .collect();

    Ok(FixtureHandle::in_memory(FixtureBinding::InMemoryRecords(
        record_bytes,
    )))
}
