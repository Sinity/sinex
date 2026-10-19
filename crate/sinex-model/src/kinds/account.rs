//! Account exports: activity an online account recorded about the operator
//! (Google My Activity, YouTube history), and what the operator kept there
//! (Keep notes, Tasks, purchases, calendars).
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One entry of an account's activity log (Google My Activity): a search, a
/// visit, an app action. The instant is the identity across exports: each
/// export writes its times in the zone of the export day, and restates titles,
/// so neither the time text nor the title can be.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "action.recorded",
    key = ["service", "at", "title_url", "unlinked_text"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct ActionRecorded {
    /// The service heading the entry: `Search`, `Chrome`, `Maps`.
    pub service: String,
    pub at: Timestamp,
    /// The words before the link (`Searched for`, `Visited`); a field, never
    /// part of the identity, since Google rewords it between exports.
    pub verb: Option<String>,
    /// The linked text, or the whole first line when nothing is linked.
    pub title: String,
    pub title_url: Option<String>,
    /// For an entry without a link, its first line; the identity then rests
    /// on it beside the instant, so two unlinked actions in one second stay
    /// apart.
    pub unlinked_text: Option<String>,
    /// Lines between the title and the time.
    pub lines: Vec<String>,
    pub products: Vec<String>,
    pub locations: Vec<String>,
    pub details: Vec<String>,
    /// The time zone label the export printed (`UTC`, `CEST`); the export
    /// day's zone, kept for reference.
    pub zone_label: String,
}

/// A video watched on a video service (YouTube watch history).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "video.watched",
    key = ["service", "video_id", "watched_at"],
    replace = newer,
    time = watched_at,
    quality = intrinsic,
    keep_known = ["title", "channel", "channel_url"]
)]
pub struct VideoWatched {
    pub service: String,
    /// `None` when the entry links no video: a removed video Google lists
    /// without a link, or a link to something other than a video.
    pub video_id: Option<String>,
    pub watched_at: Timestamp,
    pub url: Option<String>,
    /// `None` when the export names no title: the video is gone and Google
    /// writes `Watched <url>`. A later export that drops a title an earlier
    /// one had does not erase it (`keep_known`).
    pub title: Option<String>,
    pub channel: Option<String>,
    pub channel_url: Option<String>,
    pub products: Vec<String>,
    pub details: Vec<String>,
}

/// A search on a video service (YouTube search history).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "video.searched",
    key = ["service", "searched_at", "query"],
    replace = newer,
    time = searched_at,
    quality = intrinsic
)]
pub struct VideoSearched {
    pub service: String,
    pub searched_at: Timestamp,
    pub query: String,
    pub url: Option<String>,
    pub products: Vec<String>,
}

/// A note kept in a notes service (Google Keep). Keep writes no note ID and
/// names files after titles, so the creation time is the identity; a later
/// edit is a newer reading.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "note.observed",
    key = ["service", "created_at"],
    replace = newer,
    time = created_at,
    quality = intrinsic,
    removed_when = trashed_at
)]
pub struct NoteObserved {
    pub service: String,
    pub created_at: Timestamp,
    pub edited_at: Option<Timestamp>,
    pub title: Option<String>,
    pub text: Option<String>,
    /// Checklist items, in order.
    pub items: Vec<NoteItem>,
    pub labels: Vec<String>,
    pub color: Option<String>,
    pub pinned: Option<bool>,
    pub archived: Option<bool>,
    /// The edit time when the note is in the trash; the export gives no time
    /// of trashing.
    pub trashed_at: Option<Timestamp>,
    /// Attached files, by the name Takeout wrote them under.
    pub attachments: Vec<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NoteItem {
    pub text: String,
    pub checked: bool,
}

/// A task in a task list (Google Tasks); a later export's reading with a newer
/// `updated` replaces it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "task.revised",
    key = ["service", "task_id"],
    replace = newer,
    time = created_at,
    quality = intrinsic,
    removed_when = deleted_at
)]
pub struct TaskRevised {
    pub service: String,
    pub task_id: String,
    pub list_id: String,
    pub list_title: Option<String>,
    pub title: Option<String>,
    pub notes: Option<String>,
    pub status: Option<String>,
    pub parent: Option<String>,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    pub due: Option<String>,
    pub completed_at: Option<Timestamp>,
    pub deleted_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// An order, install, library item or purchase an account store recorded
/// (Google Play, Purchases). The time is the wrapped item's own field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "purchase.observed",
    key = ["service", "record_kind", "native_id"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct PurchaseObserved {
    pub service: String,
    /// The wrapper the item came in: `orderHistory`, `install`,
    /// `purchaseHistory`, `libraryDoc`.
    pub record_kind: String,
    /// The provider's ID, or a hash of the item when it has none.
    pub native_id: String,
    pub at: Option<Timestamp>,
    pub title: Option<String>,
    pub item: Value,
}

/// One event of a calendar (RFC 5545 `VEVENT`), from an export or a calendar
/// API. A recurring series is one event with its rule; a changed occurrence is
/// its own event with a recurrence ID. Expanding a rule into occurrences is a
/// derivation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "calendar.event",
    key = ["calendar", "uid", "recurrence_id"],
    replace = newer,
    time = start,
    quality = intrinsic,
    removed_when = cancelled_at
)]
pub struct CalendarEvent {
    /// The calendar it belongs to (the export's calendar name).
    pub calendar: String,
    pub uid: String,
    /// For one changed occurrence of a series, the original start it replaces,
    /// as written.
    pub recurrence_id: Option<String>,
    pub summary: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    /// A date without a time: the event spans whole days.
    pub all_day: bool,
    /// The zone the times were written in (`Europe/Warsaw`, `UTC`), or
    /// `floating` for local times the export ties to no zone, read in the
    /// configured zone.
    pub zone: String,
    pub status: Option<String>,
    pub rrule: Option<String>,
    pub exdates: Vec<String>,
    pub organizer: Option<String>,
    pub attendees: Vec<String>,
    pub created_at: Option<Timestamp>,
    pub last_modified: Option<Timestamp>,
    pub sequence: Option<i64>,
    /// The last-modified time when the status is `CANCELLED`.
    pub cancelled_at: Option<Timestamp>,
    /// Properties not modelled above, by name, as written.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One row of an account data export that no more specific type describes (a
/// Samsung account export's alarms, settings and backups): the service, the
/// table and every column as the export wrote it, a JSON column decoded. The
/// row's own ID column is the identity, or a hash of the row without one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "account.record",
    key = ["service", "table", "native_id"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct AccountRecord {
    pub service: String,
    pub table: String,
    pub native_id: String,
    /// The row's own time (creation, or the export's timestamp column).
    pub at: Option<Timestamp>,
    pub values: Map<String, Value>,
}

/// A location fix an account recorded (Google Location History): where the
/// device was, how sure it was, and what it thought the operator was doing.
/// The device and the instant identify it across exports.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "location.sample",
    key = ["provider", "device", "at"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct LocationSample {
    pub provider: String,
    /// The export's device tag; `None` in older exports, which carry none.
    pub device: Option<String>,
    pub at: Timestamp,
    pub latitude: f64,
    pub longitude: f64,
    /// Meters, as the provider reported it.
    pub accuracy_m: Option<f64>,
    pub altitude_m: Option<f64>,
    pub velocity_mps: Option<f64>,
    pub heading_deg: Option<f64>,
    /// Activity guesses with their confidences, as exported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<Value>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
