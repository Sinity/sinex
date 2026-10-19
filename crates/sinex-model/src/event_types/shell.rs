//! Shell commands as captured (Atuin, zsh) and as derived: one fused command
//! from every source that saw it, and shell sessions.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A command run in a shell, as Atuin recorded it. Atuin updates rows in place
/// (duration and exit arrive when the command ends; `deleted_at` when the
/// operator deletes it), so a newer reading with different content replaces
/// the live one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "command.executed",
    key = ["native_id"],
    replace = newer,
    time = started_at,
    quality = intrinsic,
    removed_when = deleted_at
)]
pub struct CommandExecuted {
    /// Atuin's own row ID.
    pub native_id: String,
    pub command: String,
    pub cwd: String,
    pub session: String,
    pub hostname: String,
    pub started_at: Timestamp,
    /// `None` while the command is still running (Atuin stores -1).
    pub duration_ns: Option<i64>,
    /// `None` while the command is still running. Exit codes are signed: a
    /// signal-killed command is negative.
    pub exit_code: Option<i64>,
    pub deleted_at: Option<Timestamp>,
    pub author: Option<String>,
    pub intent: Option<String>,
    pub shell: Option<String>,
    /// Columns this parser does not know yet, kept by name.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A command from a zsh extended history file. The file has no record IDs, so
/// a record is identified by host, start second, command and its occurrence
/// number among identical pairs; a later reading of it, wherever it is read
/// from, is a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "command.imported",
    key = ["hostname", "started_at", "command", "n"],
    replace = repeat,
    time = started_at,
    quality = intrinsic
)]
pub struct CommandImported {
    pub command: String,
    pub hostname: String,
    pub started_at: Timestamp,
    /// The elapsed seconds field as written. With SHARE_HISTORY zsh writes 0
    /// before the command ends, so this is not a duration.
    pub header_elapsed: i64,
    /// Which occurrence this is among records with the same start second and
    /// command, in file order (the same command run twice in one second is
    /// two runs). With host, start and command it identifies the record
    /// wherever it is read from, so a re-read from the start (a lost position,
    /// re-staged bytes) is a repeat, not a new command.
    pub n: u32,
}

/// One command run, combined from every source that saw it (Atuin, zsh,
/// kitty). Derived by the fusion derivation with every source record as a
/// parent. Identified by its preferred source record, not by host, time and
/// text: the same command run twice in one second is two runs (zsh history
/// holds about a thousand such pairs on the operator's machine).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "command.fused",
    key = ["host", "record"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct CommandFused {
    /// The preferred member's identity: `<type>:<native id>` when the source
    /// has one (Atuin), otherwise its record location.
    pub record: String,
    pub command: String,
    pub host: String,
    pub started_at: Timestamp,
    pub cwd: Option<String>,
    pub session: Option<String>,
    pub duration_ns: Option<i64>,
    pub exit_code: Option<i64>,
    /// The sources that observed this command.
    pub observed_by: Vec<String>,
}

/// A run of shell commands on one host with no gap longer than the session
/// rule's gap between one command's end and the next command's start. Derived
/// from fused commands; identified by host and start (its end moves as it
/// grows; its start does not), so a session that grows replaces its earlier reading.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "shell.session",
    key = ["host", "started_at"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct ShellSession {
    pub host: String,
    pub started_at: Timestamp,
    /// The latest recorded end of any member command (a command without a
    /// duration counts as a point).
    pub ended_at: Timestamp,
    /// The logical day the session belongs to (`YYYY-MM-DD`).
    pub day: String,
    pub commands: u32,
    /// Commands that exited non-zero.
    pub failed: u32,
    /// Working directories in first-seen order.
    pub cwds: Vec<String>,
}
