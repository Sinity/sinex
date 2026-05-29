//! D-Bus fixture.
//!
//! Builds a `MockDbusBackend` from a list of pre-formed `DbusMessage` values.
//! The `data` bytes are interpreted as newline-delimited JSON-encoded objects
//! with fields matching `DbusMessage` (interface, member, path, sender, `body_json`).

use sinexd::node_sdk::parser::DbusMessage;

use super::{FixtureBinding, FixtureHandle};

/// Build a D-Bus fixture from raw bytes.
///
/// `data` should be newline-delimited JSON, each line an object with the fields
/// of `DbusMessage`: `interface`, `member`, `path`, `sender` (optional),
/// `body_json` (arbitrary JSON value). Blank lines are skipped.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::InMemoryRecords`
/// containing the raw bytes of each message's `body_json` field for downstream
/// inspection.
///
/// # Errors
///
/// Returns an error if any line fails JSON deserialization or is missing required fields.
pub fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    let text = std::str::from_utf8(data)
        .map_err(|e| format!("dbus fixture data is not valid UTF-8: {e}"))?;

    let messages: Vec<DbusMessage> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, line)| -> Result<DbusMessage, String> {
            let v: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| format!("dbus fixture line {i}: failed to parse JSON: {e}"))?;
            Ok(DbusMessage {
                interface: v["interface"]
                    .as_str()
                    .ok_or_else(|| format!("dbus fixture line {i}: missing 'interface' field"))?
                    .to_string(),
                member: v["member"]
                    .as_str()
                    .ok_or_else(|| format!("dbus fixture line {i}: missing 'member' field"))?
                    .to_string(),
                path: v["path"]
                    .as_str()
                    .ok_or_else(|| format!("dbus fixture line {i}: missing 'path' field"))?
                    .to_string(),
                sender: v["sender"].as_str().map(std::string::ToString::to_string),
                body_json: v["body_json"].clone(),
            })
        })
        .collect::<Result<_, _>>()?;

    // Expose the message bodies as record bytes so the obligation layer can
    // feed them through the dispatch function.
    let record_bytes: Vec<Vec<u8>> = messages
        .iter()
        .map(|m| serde_json::to_vec(&m.body_json).unwrap_or_default())
        .collect();

    Ok(FixtureHandle::in_memory(FixtureBinding::InMemoryRecords(
        record_bytes,
    )))
}

/// Build a D-Bus fixture directly from typed messages (for callers that
/// construct `DbusMessage` values in code rather than from bytes).
#[must_use]
pub fn build_from_messages(messages: Vec<DbusMessage>) -> FixtureHandle {
    let record_bytes: Vec<Vec<u8>> = messages
        .iter()
        .map(|m| serde_json::to_vec(&m.body_json).unwrap_or_default())
        .collect();
    FixtureHandle::in_memory(FixtureBinding::InMemoryRecords(record_bytes))
}
