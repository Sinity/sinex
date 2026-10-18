//! Browser history: Chromium-family `History` databases and Takeout exports.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One visit in a Chromium-family browser's `History` database (`visits`
/// joined with `urls`). Chromium writes the visit when it starts and fills in
/// its duration when the page is left, so a newer reading replaces the live
/// one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "browser.visit",
    key = ["profile", "visit_id"],
    replace = newer,
    time = visited_at,
    quality = intrinsic
)]
pub struct BrowserVisit {
    /// The profile directory, such as `google-chrome/Default`.
    pub profile: String,
    /// Chromium's `visits.id`.
    pub visit_id: i64,
    /// `None` when the visit's `urls` row is gone.
    pub url: Option<String>,
    pub title: Option<String>,
    pub visited_at: Timestamp,
    /// The visit this one came from (a link click); `None` when it had none.
    pub from_visit: Option<i64>,
    /// Chromium's page transition as stored: the core type in the low byte,
    /// qualifier flags above it.
    pub transition: i64,
    /// How long the page stayed open; `None` while it is still open (0).
    pub duration_us: Option<i64>,
    /// Columns this parser does not know yet, kept by name.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One row of a Chromium-family browser's `urls` table: a page and its
/// running counts, updated in place as it is visited again.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "browser.url",
    key = ["profile", "url_id"],
    replace = newer,
    time = last_visited_at,
    quality = intrinsic
)]
pub struct BrowserUrl {
    pub profile: String,
    /// Chromium's `urls.id`.
    pub url_id: i64,
    pub url: String,
    pub title: Option<String>,
    pub visit_count: i64,
    pub typed_count: i64,
    /// `None` when Chromium stored 0 (never).
    pub last_visited_at: Option<Timestamp>,
    pub hidden: bool,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One entry of a Google Takeout Chrome history export. It has no ID, so the
/// URL and time identify it, and a later export of the same entry is a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "browser.history_entry",
    key = ["url", "visited_at"],
    replace = repeat,
    time = visited_at,
    quality = intrinsic
)]
pub struct BrowserHistoryEntry {
    pub url: String,
    pub title: Option<String>,
    pub visited_at: Timestamp,
    /// Takeout's `page_transition`, such as `LINK` or `TYPED`.
    pub page_transition: Option<String>,
    /// The synced device the visit came from.
    pub client_id: Option<String>,
    pub favicon_url: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
