//! Fused records: one thing several sources observed, as one event. Each is
//! derived by a fusion rule with every source record that saw it as a
//! parent, takes its time from the most preferred source that saw it and each
//! other field from the most preferred source that has it, and lists the
//! sources in `observed_by`. A record no other source saw is fused alone, so
//! a fused kind holds every observation once, whichever sources were running.
//!
//! Each is identified by its preferred member's record (`record`), never by
//! its content and time: the same command run twice in one second is two
//! runs.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One command run, from Atuin and zsh history. zsh history holds about a
/// thousand pairs of the same command in the same second on the operator's
/// machine, which is why the record, not the text and time, identifies it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "command.fused",
    key = ["record"],
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

/// A stretch during which a window had keyboard focus, from the compositor's
/// focus changes and ActivityWatch's window spans. Focus is exclusive, so
/// whatever the sources call the window, two of them seeing focus begin at
/// the same moment saw the same focus.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "focus.fused",
    key = ["record"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct FocusFused {
    pub record: String,
    pub started_at: Timestamp,
    /// `None` while the focus holds.
    pub ended_at: Option<Timestamp>,
    /// The application: the compositor's window class or ActivityWatch's app.
    pub app: Option<String>,
    pub title: Option<String>,
    pub observed_by: Vec<String>,
}

/// One page visit, from a browser's own history, a Takeout history export
/// and the browser-navigation capture.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "visit.fused",
    key = ["record"],
    replace = newer,
    time = visited_at,
    quality = intrinsic
)]
pub struct VisitFused {
    pub record: String,
    pub url: String,
    pub title: Option<String>,
    pub visited_at: Timestamp,
    pub observed_by: Vec<String>,
}

/// One play of a track, from a streaming history export and the media
/// player's live state (live MPRIS capture and the capture lake).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "play.fused",
    key = ["record"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct PlayFused {
    pub record: String,
    pub title: String,
    pub artist: Option<String>,
    pub album: Option<String>,
    /// The player that reported it, when a live source saw it.
    pub player: Option<String>,
    pub started_at: Timestamp,
    /// When playback stopped, or the track's end when only its length is known.
    pub ended_at: Option<Timestamp>,
    pub observed_by: Vec<String>,
}

/// One clipboard or primary-selection change, from live capture and the
/// capture lake.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "clipboard.fused",
    key = ["record"],
    replace = newer,
    time = copied_at,
    quality = intrinsic
)]
pub struct ClipboardFused {
    pub record: String,
    /// `clipboard` or `primary`.
    pub selection: String,
    pub text: Option<String>,
    pub mime_type: Option<String>,
    pub copied_at: Timestamp,
    pub observed_by: Vec<String>,
}

/// One desktop notification, from live capture and the capture lake.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "notification.fused",
    key = ["record"],
    replace = newer,
    time = shown_at,
    quality = intrinsic
)]
pub struct NotificationFused {
    pub record: String,
    pub app: String,
    pub summary: String,
    pub body: Option<String>,
    pub shown_at: Timestamp,
    pub observed_by: Vec<String>,
}
