//! Event kinds. One Rust type per stored event type; the catalog in `ALL`
//! is the single list the schema registry, NixOS options and documentation are
//! generated from.
use crate::{EventPayload, Timestamp};
use schemars::JsonSchema;

pub mod account;
pub mod browser;
pub mod code;
pub mod communication;
pub mod desktop;
pub mod health;
pub mod library;
pub mod machine;
pub mod media;
pub mod operator;
pub mod polylogue;
pub mod social;
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
/// from fused commands; identified by host and start ("ends move, starts do
/// not", `y8v`), so a session that grows replaces its earlier reading.
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

/// A span during which some state held: a focused window, a workspace, an
/// ActivityWatch bout, a running systemd unit. Lifted from transitions by the
/// interval derivation. Identified by its start evidence; the end may still
/// be unknown (`ended_at` is `None` while the state holds).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "state.interval",
    key = ["interval_id"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct StateInterval {
    /// `<state kind>:<subject>:<start record location or start time>`.
    pub interval_id: String,
    pub state_kind: String,
    pub subject_id: Option<String>,
    pub label: Option<String>,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub start_event_type: String,
    /// What ended it: the next transition, a fence, a stop, or the observed
    /// duration (`None` while open).
    pub end_event_type: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub attributes: std::collections::BTreeMap<String, String>,
}

/// What happened in one civil hour or logical day: commands, shell sessions
/// and time per state, each counted from the derived events of that period.
/// A count of zero means the source was read and saw nothing; a period with
/// no inputs at all has no summary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "activity.summary",
    key = ["grain", "period_start"],
    replace = newer,
    time = period_start,
    quality = intrinsic
)]
pub struct ActivitySummary {
    /// `hour` or `day`.
    pub grain: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    /// The logical day the period belongs to (`YYYY-MM-DD`).
    pub day: String,
    pub commands: u32,
    pub failed_commands: u32,
    pub sessions: u32,
    /// Seconds of shell-session time inside the period.
    pub session_seconds: i64,
    /// Working directories by command count, most first.
    pub top_cwds: Vec<(String, u32)>,
    /// Seconds per state kind inside the period (closed intervals and the
    /// elapsed part of open ones).
    pub state_seconds: std::collections::BTreeMap<String, i64>,
    /// The longest-held subjects: (state kind, label, seconds).
    pub top_subjects: Vec<(String, String, i64)>,
}

/// A registered event type, as the schema registry stores it.
#[derive(Clone, Debug)]
pub struct KindInfo {
    pub event_type: &'static str,
    pub key: crate::KeyKind,
    pub replace: crate::ReplaceRule,
    /// See [`crate::EventPayload::KEEP_KNOWN`].
    pub keep_known: &'static [&'static str],
    pub json_schema: fn() -> Value,
    /// Build an event of this type from a JSON payload, as arriving over the
    /// NATS ingress: the payload is decoded into the Rust type first, so the
    /// type's time and removal rules apply exactly as for in-process sources.
    pub build: Build,
}

/// See [`KindInfo::build`].
pub type Build = fn(
    &Value,
    &str,
    crate::Origin,
    crate::ReadingOrder,
    [u8; 32],
    crate::Class,
) -> Result<crate::Event, crate::BuildError>;

const fn info<P: EventPayload>() -> KindInfo {
    KindInfo {
        event_type: P::EVENT_TYPE,
        key: P::KEY,
        replace: P::REPLACE,
        keep_known: P::KEEP_KNOWN,
        json_schema: P::payload_schema,
        build: build_from_json::<P>,
    }
}

fn build_from_json<P: EventPayload>(
    payload: &Value,
    source: &str,
    origin: crate::Origin,
    reading_order: crate::ReadingOrder,
    record_hash: [u8; 32],
    class: crate::Class,
) -> Result<crate::Event, crate::BuildError> {
    let typed: P = serde_json::from_value(payload.clone())
        .map_err(|e| crate::BuildError::Payload(P::EVENT_TYPE, e.to_string()))?;
    crate::Event::build(&typed, source, origin, reading_order, record_hash, class)
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
    info::<desktop::PresenceInterval>(),
    info::<desktop::BrowserTabActive>(),
    info::<desktop::ActivityWatchOther>(),
    // Machine
    info::<machine::JournalEntry>(),
    info::<machine::CaptureObserved>(),
    info::<machine::FileChanged>(),
    // Code
    info::<code::GitCommit>(),
    // Derived
    info::<ShellSession>(),
    info::<StateInterval>(),
    info::<ActivitySummary>(),
    // Browsing, listening, reading, notes, communication
    info::<browser::BrowserVisit>(),
    info::<browser::BrowserUrl>(),
    info::<browser::BrowserHistoryEntry>(),
    info::<media::MusicPlayed>(),
    info::<media::BookmarkSaved>(),
    info::<media::NoteDocument>(),
    info::<communication::ChatMessage>(),
    info::<communication::MailMessage>(),
    // Polylogue's archive in primary mode
    info::<polylogue::PolylogueRows>(),
    // Account exports
    info::<account::ActionRecorded>(),
    info::<account::VideoWatched>(),
    info::<account::VideoSearched>(),
    info::<account::NoteObserved>(),
    info::<account::TaskRevised>(),
    info::<account::PurchaseObserved>(),
    info::<account::CalendarEvent>(),
    info::<communication::ConversationMessage>(),
    info::<media::BookEvent>(),
    info::<health::HealthRecord>(),
    info::<social::PostPublished>(),
    info::<social::VoteCast>(),
    info::<social::NotificationReceived>(),
    info::<social::AccountSetting>(),
    info::<operator::LogEntry>(),
    info::<operator::SteeringRecord>(),
    // Library
    info::<library::Acquisition>(),
    info::<library::Item>(),
    info::<library::Rendition>(),
    info::<library::Assignment>(),
    info::<library::Collection>(),
    info::<library::Entry>(),
    info::<library::Rating>(),
    info::<library::Comparison>(),
    info::<library::View>(),
    info::<library::Dismissal>(),
];

/// The registered type named `event_type`, for code that holds a type name
/// read from the database and needs the static one.
pub fn find(event_type: &str) -> Option<&'static KindInfo> {
    ALL.iter().find(|k| k.event_type == event_type)
}

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
    fn json_payloads_build_through_the_rust_type() {
        let kind = find("command.executed").unwrap();
        let origin = || crate::Origin::Record {
            location: crate::RecordLocation {
                material: crate::MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
        };
        let payload = serde_json::to_value(atuin(true)).unwrap();
        let event = (kind.build)(
            &payload,
            "producer:p",
            origin(),
            crate::ReadingOrder(1),
            [0; 32],
            crate::Class::Open,
        )
        .unwrap();
        assert_eq!(event.event_type, "command.executed");
        assert_eq!(event.record_state, RecordState::Removed);
        assert!(event.equivalence_key.is_some());
        let bad = serde_json::json!({"command": "ls"});
        assert!(matches!(
            (kind.build)(
                &bad,
                "producer:p",
                origin(),
                crate::ReadingOrder(1),
                [0; 32],
                crate::Class::Open
            ),
            Err(crate::BuildError::Payload("command.executed", _))
        ));
        assert!(find("no.such.type").is_none());
    }

    #[test]
    fn catalog_lists_each_type_once() {
        let mut names: Vec<_> = ALL.iter().map(|k| k.event_type).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ALL.len());
    }
}
