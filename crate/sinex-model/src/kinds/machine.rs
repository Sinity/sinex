//! The machine: the systemd journal and filesystem changes.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One systemd journal entry. The journal's own cursor identifies it, so an
/// entry read again (after a capture restart overlaps, or from an export) is
/// a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "journal.entry", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct JournalEntry {
    /// `__CURSOR`.
    pub cursor: String,
    /// `__REALTIME_TIMESTAMP`; `None` when the entry has none or it is malformed
    /// (the time stays unknown rather than becoming the read time).
    pub logged_at: Option<Timestamp>,
    pub boot_id: Option<String>,
    pub hostname: Option<String>,
    /// `MESSAGE`; `None` when the journal stored it as binary, which is kept
    /// in `fields` as an array of bytes.
    pub message: Option<String>,
    /// syslog priority, 0 (emergency) to 7 (debug).
    pub priority: Option<u8>,
    pub unit: Option<String>,
    pub user_unit: Option<String>,
    pub identifier: Option<String>,
    pub pid: Option<i64>,
    pub uid: Option<i64>,
    pub command: Option<String>,
    pub transport: Option<String>,
    /// Every other field, by its journal name.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub fields: Map<String, Value>,
}

/// A change to a watched file or directory, as the kernel reported it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "file.changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct FileChanged {
    pub received_at: Timestamp,
    /// `created`, `modified`, `written` (closed after writing), `removed`,
    /// `renamed`, `moved_from`, `moved_to`, `attributes`, `other`, or
    /// `unwatched` (the kernel refused a watch on this directory; see `note`).
    pub change: String,
    /// The path, when it is UTF-8 (otherwise see `path_hex`).
    pub path: Option<String>,
    /// The path's bytes in hex, when they are not UTF-8.
    pub path_hex: Option<String>,
    /// The new path, for a rename that reported both ends.
    pub to: Option<String>,
    pub is_dir: Option<bool>,
    /// Why, for an `unwatched` directory.
    pub note: Option<String>,
}
