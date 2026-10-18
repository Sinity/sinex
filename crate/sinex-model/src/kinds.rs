//! Event kinds. One Rust type per stored event type; the catalog in `ALL`
//! is the single list the schema registry, NixOS options and documentation are
//! generated from.
use crate::{EventPayload, Timestamp};
use schemars::JsonSchema;

pub mod code;
pub mod desktop;
pub mod machine;
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

/// A command from a zsh extended history file. The file has no record IDs and
/// is only ever appended to, so each record location is its own identity and a
/// later reading of the same location is a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "command.imported",
    key = location,
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
}

/// One command run, combined from every source that saw it (Atuin, zsh,
/// kitty). Derived by the fusion derivation with every source record as a
/// parent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "command.fused",
    key = ["host", "started_at", "command"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct CommandFused {
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

/// A registered event type, as the schema registry stores it.
#[derive(Clone, Debug)]
pub struct KindInfo {
    pub event_type: &'static str,
    pub key: crate::KeyKind,
    pub replace: crate::ReplaceRule,
    pub json_schema: fn() -> Value,
}

const fn info<P: EventPayload>() -> KindInfo {
    KindInfo {
        event_type: P::EVENT_TYPE,
        key: P::KEY,
        replace: P::REPLACE,
        json_schema: P::payload_schema,
    }
}

/// Every event type Sinex stores.
pub const ALL: &[KindInfo] = &[
    info::<CommandExecuted>(),
    info::<CommandImported>(),
    info::<CommandFused>(),
    // Desktop
    info::<desktop::WindowFocused>(),
    info::<desktop::WindowOpened>(),
    info::<desktop::WindowClosed>(),
    info::<desktop::WindowMoved>(),
    info::<desktop::WindowTitleChanged>(),
    info::<desktop::WorkspaceSwitched>(),
    info::<desktop::MonitorFocused>(),
    info::<desktop::DesktopOther>(),
    info::<desktop::WindowSpan>(),
    info::<desktop::PresenceSpan>(),
    info::<desktop::BrowserTabActive>(),
    info::<desktop::ActivityWatchOther>(),
    // Machine
    info::<machine::JournalEntry>(),
    info::<machine::FileChanged>(),
    // Code
    info::<code::GitCommit>(),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyKind, RecordState, ReplaceRule, TimeQuality};

    fn atuin(deleted: bool) -> CommandExecuted {
        CommandExecuted {
            native_id: "01h".into(),
            command: "ls".into(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            duration_ns: None,
            exit_code: Some(-9),
            deleted_at: deleted.then(|| Timestamp::from_unix_secs(1_700_000_100).unwrap()),
            author: None,
            intent: None,
            shell: None,
            extra: Map::new(),
        }
    }

    #[test]
    fn derive_declares_identity_and_time() {
        assert_eq!(CommandExecuted::EVENT_TYPE, "command.executed");
        assert_eq!(CommandExecuted::KEY, KeyKind::Fields(&["native_id"]));
        assert_eq!(CommandExecuted::REPLACE, ReplaceRule::ReplaceWhenNewer);
        assert_eq!(CommandImported::KEY, KeyKind::Location);
        assert_eq!(CommandImported::REPLACE, ReplaceRule::SuppressRepeat);
        let a = atuin(false);
        assert_eq!(a.ts_orig().quality, TimeQuality::Intrinsic);
        assert_eq!(a.ts_orig().at, Some(a.started_at));
        assert_eq!(a.record_state(), RecordState::Present);
        assert_eq!(atuin(true).record_state(), RecordState::Removed);
    }

    #[test]
    fn json_schema_requires_declared_fields() {
        let schema = CommandImported::payload_schema();
        let required = schema["required"].as_array().unwrap();
        assert!(required.iter().any(|f| f == "command"));
        assert!(required.iter().any(|f| f == "started_at"));
    }

    #[test]
    fn catalog_lists_each_type_once() {
        let mut names: Vec<_> = ALL.iter().map(|k| k.event_type).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ALL.len());
    }
}
