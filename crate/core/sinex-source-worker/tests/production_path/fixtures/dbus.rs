//! D-Bus fixture.
//!
//! Builds a `MockDbusBackend` from a list of pre-formed `DbusMessage` values.
//! The `data` bytes are interpreted as newline-delimited JSON-encoded
//! `DbusMessage` objects.

use sinex_node_sdk::parser::adapters::DbusMessage;

use super::{FixtureBinding, FixtureHandle};

/// Build a D-Bus fixture from raw bytes.
///
/// `data` should be newline-delimited JSON, each line a serialized `DbusMessage`.
/// Blank lines are skipped.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::InMemoryRecords`
/// containing the raw bytes of each message's body field for downstream
/// inspection.
///
/// # Errors
///
/// Returns an error if any line fails JSON deserialization.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    let text = std::str::from_utf8(data)
        .map_err(|e| format!("dbus fixture data is not valid UTF-8: {e}"))?;

    let messages: Vec<DbusMessage> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, line)| {
            serde_json::from_str(line)
                .map_err(|e| format!("dbus fixture line {i}: failed to parse DbusMessage: {e}"))
        })
        .collect::<Result<_, _>>()?;

    // Expose the message bodies as record bytes so the obligation layer can
    // feed them through the dispatch function.
    let record_bytes: Vec<Vec<u8>> = messages
        .iter()
        .map(|m| serde_json::to_vec(&m.body).unwrap_or_default())
        .collect();

    Ok(FixtureHandle::in_memory(FixtureBinding::InMemoryRecords(record_bytes)))
}

/// Build a D-Bus fixture directly from typed messages (for callers that
/// construct `DbusMessage` values in code rather than from bytes).
pub fn build_from_messages(messages: Vec<DbusMessage>) -> FixtureHandle {
    let record_bytes: Vec<Vec<u8>> = messages
        .iter()
        .map(|m| serde_json::to_vec(&m.body).unwrap_or_default())
        .collect();
    FixtureHandle::in_memory(FixtureBinding::InMemoryRecords(record_bytes))
}
