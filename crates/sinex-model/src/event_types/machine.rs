//! Machine events: journal entries, file changes, terminal recordings and
//! what terminals showed.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One systemd journal entry. The journal's own cursor identifies it, so an
/// entry read again (after a capture restart overlaps, or from an export) is
/// a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 3, type = "journal.entry", search = words, key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
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
    /// Written by Sinex itself (its daemon, captures and session service):
    /// Kept out of figures about the machine's activity.
    #[serde(default)]
    pub from_sinex: bool,
    /// Every other field, by its journal name.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub fields: Map<String, Value>,
}

/// A change to a watched file or directory, as the kernel reported it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "file.changed", key = location, replace = repeat, time = received_at, quality = captured)]
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

/// A terminal recording's session (asciinema): its start, size, command and
/// environment, from the recording's header. The recording's place is its
/// identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "terminal.session", key = location, replace = repeat, time = started_at, quality = intrinsic)]
pub struct TerminalSession {
    pub session: String,
    pub started_at: Option<Timestamp>,
    pub cols: Option<i64>,
    pub rows: Option<i64>,
    pub command: Option<String>,
    /// The header as recorded (terminal type, theme, environment).
    pub header: Value,
}

/// A span of a terminal recording, as plain text with escape sequences
/// removed: one shell command's output where the recording kept input. A span
/// starts at a typed Enter, so it holds the command's output, the next prompt
/// and the next command line as typed up to its own Enter; it also ends at a
/// size bound, a resize or the exit. The cast stays whole as the material,
/// and the span's record is its exact byte range there, so the original
/// stream is replayable from the material; per-event detail is a read over
/// it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "terminal.output", key = location, replace = repeat, time = started_at, quality = intrinsic)]
pub struct TerminalOutput {
    pub session: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    /// Cast events in the span.
    pub events: u32,
    /// What was written to the terminal (`o` events), escape sequences
    /// removed.
    pub output: String,
    /// What was typed (`i` events), when the recording kept input.
    pub input: String,
    /// The last resize (`r`) or the exit status (`x`) the span ends with.
    pub resize: Option<String>,
    pub exit: Option<String>,
}

/// What a kitty window's scrollback held when it was captured: a snapshot in
/// the Sinnix lake (`kitty @ get-text`, every window on a timer), or the kitty
/// integration's capture of a window's new lines on a cadence and when it
/// closes. `text` holds only the lines new since the previous capture of the
/// same window, escape sequences removed; the stored material keeps the
/// capture as taken.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "scrollback.captured", key = location, replace = repeat, time = captured_at, quality = captured)]
pub struct ScrollbackCaptured {
    pub captured_at: Timestamp,
    pub host: Option<String>,
    /// The kitty process; absent in older lake snapshots.
    pub kitty_pid: Option<u32>,
    pub window_id: i64,
    /// The window's title; for a lake snapshot, as its file name spells it
    /// (the sidecar's `scrollback.window` has it exactly).
    pub title: Option<String>,
    pub cwd: Option<String>,
    /// Lines the window held at capture, scrollback and screen.
    pub lines: u64,
    /// Whether `text` continues the previous capture of this window. False for
    /// a window's first capture, and when the previous capture is not where
    /// this one starts (the window was cleared, or resized and its lines
    /// wrapped again): `text` is then the whole capture.
    pub continues: bool,
    /// Lines in `text`.
    pub new_lines: u64,
    pub text: String,
    /// Adapter metadata captured with this cwd, including repository and terminal identity.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// The window a lake scrollback snapshot was taken of, from the snapshot's
/// sidecar file: kitty's title and working directory at capture.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "scrollback.window", key = location, replace = repeat, time = captured_at, quality = captured)]
pub struct ScrollbackWindow {
    pub captured_at: Timestamp,
    pub host: Option<String>,
    pub kitty_pid: Option<u32>,
    pub window_id: i64,
    pub title: Option<String>,
    pub cwd: Option<String>,
    /// The snapshot's file name.
    pub snapshot: String,
    /// Any other fields the sidecar has.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One historical lake record. Stream derivations interpret its unchanged payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 3, type = "lake.observed", search = words, key = location, replace = repeat, time = observed_at, quality = captured)]
pub struct LakeObservation {
    pub observed_at: Timestamp,
    pub lane: String,
    pub host: String,
    pub seq: u64,
    pub payload: Value,
    pub raw_ref: Option<String>,
}
