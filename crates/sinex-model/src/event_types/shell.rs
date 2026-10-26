//! Shell commands as captured (Atuin, zsh) and the shell sessions derived
//! from fused commands.
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

/// A command from a plain or extended zsh history file. Timed records keep
/// their natural key across materials; untimed records use their material
/// location because neither text nor an unknown start identifies an occurrence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CommandImported {
    pub command: String,
    pub hostname: String,
    /// Absent in plain history; acquisition time is not a command start.
    pub started_at: Option<Timestamp>,
    /// The elapsed seconds field as written. With SHARE_HISTORY zsh writes 0
    /// before the command ends, so this is not a duration.
    pub header_elapsed: Option<i64>,
    /// Which occurrence this is among records with the same start second and
    /// command, in file order (the same command run twice in one second is
    /// two runs). Untimed records use their material location instead; this
    /// discriminator is zero and unused for them.
    /// With host, start and command it identifies the timed record
    /// wherever it is read from, so a re-read from the start (a lost position,
    /// bytes stored again) is a repeat, not a new command.
    pub n: u32,
}

impl crate::EventPayload for CommandImported {
    const EVENT_TYPE: &'static str = "command.imported";
    const KEY: crate::KeyKind = crate::KeyKind::Fields(&["hostname", "started_at", "command", "n"]);
    const REPLACE: crate::ReplaceRule = crate::ReplaceRule::SuppressRepeat;

    fn key_kind(&self) -> crate::KeyKind {
        if self.started_at.is_some() {
            Self::KEY
        } else {
            crate::KeyKind::Location
        }
    }

    fn ts_orig(&self) -> crate::TsOrig {
        crate::TsOrig::maybe(self.started_at, crate::TimeQuality::Intrinsic)
    }

    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("schemas serialize")
    }
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

#[cfg(test)]
mod tests;
