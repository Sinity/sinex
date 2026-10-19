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

/// One record from a Sinnix capture lane, read from the capture lake
/// (architecture §5.7, phase 1; spec `port-sinnix.md` §3). The source is the
/// lane's catalog source, so lake history and native capture share a name;
/// the lane's own payload is kept whole until its native source and typed
/// event replace it, which is a parser change plus a replay. The envelope's
/// `(host, lane, seq)` is kept for gap tracking, never as identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "capture.observed", key = location, replace = repeat, time = observed_at, quality = captured)]
pub struct CaptureObserved {
    /// The envelope's `ts`: when the writer recorded the observation.
    pub observed_at: Timestamp,
    pub lane: String,
    pub host: String,
    pub seq: i64,
    /// The lane's payload, as written.
    pub payload: Value,
    /// The raw file the record describes (a frame, an audio segment), if any.
    pub raw_ref: Option<String>,
}

/// A terminal recording's session (asciinema): its start, size, command and
/// environment, from the recording's header. The recording's place is its
/// identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "terminal.session", key = location, replace = repeat, time = started_at, quality = intrinsic)]
pub struct TerminalSession {
    pub session: String,
    pub started_at: Option<Timestamp>,
    pub cols: Option<i64>,
    pub rows: Option<i64>,
    pub command: Option<String>,
    /// The header as recorded (terminal type, theme, environment).
    pub header: Value,
}

/// One event of a terminal recording: output written (`o`), input typed
/// (`i`), a resize (`r`), a marker (`m`) or the exit (`x`), at its time.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "terminal.output", key = location, replace = repeat, time = at, quality = intrinsic)]
pub struct TerminalOutput {
    pub session: String,
    pub at: Timestamp,
    pub code: String,
    pub data: String,
}
