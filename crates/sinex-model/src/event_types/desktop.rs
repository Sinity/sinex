//! The desktop: windows, workspaces and monitors as a compositor reports them
//! live, and ActivityWatch's watcher spans. A focus change is the same event
//! whichever compositor saw it, so the types name what happened, not the
//! program that said so.
//!
//! Live streams carry no record IDs; each received line is its own record
//! location, and a later reading of that location is a repeat. The time is
//! when Sinex received the line.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A window took keyboard focus.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "window.focused", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct WindowFocused {
    pub received_at: Timestamp,
    /// The compositor's window address; `None` when only class and title were
    /// reported before the next event arrived.
    pub window: Option<String>,
    pub class: Option<String>,
    pub title: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "window.opened", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct WindowOpened {
    pub received_at: Timestamp,
    pub window: String,
    pub workspace_id: Option<i64>,
    pub workspace_name: String,
    pub class: String,
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "window.closed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct WindowClosed {
    pub received_at: Timestamp,
    pub window: String,
}

/// A window moved to another workspace.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "window.moved", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct WindowMoved {
    pub received_at: Timestamp,
    pub window: String,
    pub workspace_id: Option<i64>,
    pub workspace_name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "window.title_changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct WindowTitleChanged {
    pub received_at: Timestamp,
    pub window: String,
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "workspace.switched", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct WorkspaceSwitched {
    pub received_at: Timestamp,
    pub workspace_id: Option<i64>,
    pub workspace_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "monitor.focused", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct MonitorFocused {
    pub received_at: Timestamp,
    pub monitor: String,
    pub workspace_name: String,
}

/// A compositor event Sinex has no type for yet, kept whole.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "desktop.other", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct DesktopOther {
    pub received_at: Timestamp,
    pub name: String,
    pub data: String,
}

/// ActivityWatch's window watcher: the focused application and title over a
/// span. ActivityWatch extends a span in place while the window stays the
/// same, and the source joins adjacent identical events, so a newer reading
/// of the span replaces the older one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "window.span", key = ["bucket", "span_id"], replace = newer, time = started_at, quality = intrinsic)]
pub struct WindowSpan {
    pub bucket: String,
    /// The row ID of the first ActivityWatch event in this span.
    pub span_id: i64,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub app: String,
    /// The title with animation glyphs removed (see the ActivityWatch source).
    pub title: String,
    /// How many ActivityWatch events this span combines.
    pub merged: u32,
}

/// ActivityWatch's AFK watcher: whether the operator was at the machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "presence.span", key = ["bucket", "row"], replace = newer, time = started_at, quality = intrinsic)]
pub struct PresenceSpan {
    pub bucket: String,
    pub row: i64,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    /// `afk` or `not-afk`, as reported.
    pub status: String,
}

/// Whether the operator was at the machine over an interval, judged from
/// ActivityWatch's AFK watcher and corroborating input (`y0o3.13`, defect 2).
/// The watcher misses idle transitions under load, so a long not-afk stretch
/// with no input is `uncertain`, never silently `present`. Derived per
/// logical day by the presence derivation; the reported spans stay as read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "presence.interval", key = ["day", "started_at"], replace = newer, time = started_at, quality = intrinsic)]
pub struct PresenceInterval {
    /// The logical day this interval was computed for (intervals are cut at
    /// day boundaries).
    pub day: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    /// `present`, `absent` or `uncertain`.
    pub state: String,
    /// Present, but with less input than the dense threshold: a real but
    /// quiet stretch (reading, a call), not an artifact.
    pub sparse: bool,
    /// ActivityWatch's status for the span (`afk`, `not-afk`).
    pub reported: String,
    /// Corroborating input events inside the interval.
    pub evidence: u32,
}

/// An ActivityWatch event from a watcher Sinex has no type for yet, kept whole.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "activitywatch.other", key = ["bucket", "row"], replace = newer, time = started_at, quality = intrinsic)]
pub struct ActivityWatchOther {
    pub bucket: String,
    pub row: i64,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub data: serde_json::Value,
}

/// ActivityWatch's browser watcher: the active tab over a span.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "browser.tab_active", key = ["bucket", "row"], replace = newer, time = started_at, quality = intrinsic)]
pub struct BrowserTabActive {
    pub bucket: String,
    pub row: i64,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub browser: String,
    pub url: String,
    pub title: String,
    pub incognito: Option<bool>,
    pub audible: Option<bool>,
    pub tab_count: Option<i64>,
}

/// One sample of a window-sampling tracker (arbtt): every open window, which
/// one was active, and how long input had been idle, taken every `rate_ms`.
/// The sample's place in the log is its identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "window.sampled", key = location, replace = repeat, time = sampled_at, quality = intrinsic)]
pub struct WindowSampled {
    pub sampled_at: Timestamp,
    /// The sampling interval the tracker recorded.
    pub rate_ms: i64,
    /// Milliseconds since the last keyboard or mouse input.
    pub idle_ms: i64,
    /// The active window's title and program, when one was active.
    pub active_title: Option<String>,
    pub active_program: Option<String>,
    pub desktop: Option<String>,
    pub windows: Vec<SampledWindow>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SampledWindow {
    pub active: bool,
    pub title: String,
    pub program: String,
}
