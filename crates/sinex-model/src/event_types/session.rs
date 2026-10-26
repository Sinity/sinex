//! What the operator's graphical session reports live: desktop notifications,
//! the clipboard, media players, the editor and the terminal, screen and audio
//! segments. Like the compositor's events, these exist only while they happen;
//! each received record is its own record location and a later reading of it
//! is a repeat. The time is when Sinex received the record.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A desktop notification was asked for (`org.freedesktop.Notifications.Notify`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "notification.shown", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct NotificationShown {
    pub received_at: Timestamp,
    /// The application name the sender gave.
    pub app: String,
    pub summary: String,
    pub body: String,
    pub icon: Option<String>,
    /// The notification this one replaces, when the sender gave one.
    pub replaces_id: Option<u32>,
    /// Action identifiers and labels, in the order sent.
    pub actions: Vec<String>,
    /// Hints the sender attached (urgency, category, desktop entry…), as sent.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub hints: Map<String, Value>,
    /// How long the sender asked it to stay, in milliseconds (`-1`: the
    /// server decides, `0`: until dismissed).
    pub expire_timeout_ms: i32,
    /// The sender's unique bus name.
    pub sender: String,
}

/// A notification closed (`NotificationClosed`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "notification.closed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct NotificationClosed {
    pub received_at: Timestamp,
    pub notification_id: u32,
    /// `expired`, `dismissed`, `closed` (by the sender) or `undefined`.
    pub reason: String,
}

/// The operator picked one of a notification's actions (`ActionInvoked`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "notification.action", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct NotificationAction {
    pub received_at: Timestamp,
    pub notification_id: u32,
    pub action: String,
}

/// The clipboard or the primary selection changed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "clipboard.copied", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct ClipboardCopied {
    pub received_at: Timestamp,
    /// `clipboard` or `primary`.
    pub selection: String,
    /// The types the owner offered, in its order.
    pub mime_types: Vec<String>,
    /// The type whose content was read.
    pub mime_type: Option<String>,
    /// The content when it is text; binary content stays in the stored bytes.
    pub text: Option<String>,
    pub bytes: u64,
    /// BLAKE3 of the content, so a repeated copy can be recognized.
    pub content_hash: Option<String>,
    /// The owner marked it a secret (a password manager's hint); its content
    /// was not read.
    pub secret: bool,
}

/// A media player's state changed (MPRIS `PropertiesChanged`), or was read
/// when capture started.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "player.changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct PlayerChanged {
    pub received_at: Timestamp,
    /// The player's well-known name (`org.mpris.MediaPlayer2.spotify`) when
    /// known, else its unique bus name.
    pub player: String,
    /// `Playing`, `Paused` or `Stopped`, when it changed.
    pub status: Option<String>,
    pub title: Option<String>,
    pub artists: Vec<String>,
    pub album: Option<String>,
    pub url: Option<String>,
    pub length_us: Option<i64>,
    /// `true` when this is the state read at capture start, not a change.
    pub snapshot: bool,
    /// Every changed property as sent, decoded from D-Bus types.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub properties: Map<String, Value>,
}

/// Something the editor reported (a Neovim autocommand).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "editor.reported", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct EditorReport {
    pub received_at: Timestamp,
    pub editor: String,
    /// The autocommand (`BufEnter`, `BufWritePost`, `VimLeave`…).
    pub event: String,
    pub path: Option<String>,
    pub filetype: Option<String>,
    pub cwd: Option<String>,
    pub pid: Option<u32>,
    /// Any other fields the editor sent.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// Something the terminal reported (a kitty watcher callback).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "terminal.reported", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct TerminalReport {
    pub received_at: Timestamp,
    pub terminal: String,
    /// `cmd_start`, `cmd_end`, `focus`, `close`, `title`…
    pub event: String,
    pub window_id: Option<i64>,
    pub cwd: Option<String>,
    pub cmdline: Option<String>,
    pub exit_status: Option<i64>,
    pub title: Option<String>,
    /// What the command printed, escape sequences removed (`cmd_end` with
    /// kitty's shell integration).
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A segment of screen or audio capture: the stored material is the segment
/// itself (an image or an audio file).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "media.segment", key = location, replace = repeat, time = started_at, quality = captured)]
pub struct MediaSegment {
    /// `screen` or `audio`.
    pub kind: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub mime: String,
    pub bytes: u64,
    /// The output (monitor) or audio target captured.
    pub target: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Text read from a screen segment by a local OCR engine. Derived from the
/// segment's event; recomputed when the engine version changes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "screen.text", key = ["segment"], replace = newer, time = started_at, quality = captured)]
pub struct ScreenText {
    /// The `media.segment` event this text was read from.
    pub segment: String,
    pub started_at: Timestamp,
    pub engine: String,
    pub text: String,
}

/// A page from browser history, fetched by the forward crawl. The stored
/// material is the response body; a later fetch of the same URL is a newer
/// reading.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "page.fetched", key = ["url"], replace = newer, time = fetched_at, quality = captured)]
pub struct PageFetched {
    pub url: String,
    pub final_url: String,
    pub fetched_at: Timestamp,
    pub status: u16,
    pub content_type: Option<String>,
    pub bytes: u64,
    pub title: Option<String>,
}
