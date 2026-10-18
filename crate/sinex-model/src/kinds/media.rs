//! Listening history and saved reading: Spotify plays, bookmarks, notes.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One play of a track, episode or audiobook chapter from a Spotify streaming
/// history export. Spotify records when playback stopped (`ts`, or `endTime`
/// in the older account-data export) and how long it played; the start is
/// derived from those two.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "music.played",
    key = ["ended_at", "played_ms", "track_uri", "episode_uri", "track_name", "artist_name"],
    replace = repeat,
    time = started_at,
    quality = intrinsic
)]
pub struct MusicPlayed {
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub played_ms: i64,
    pub track_uri: Option<String>,
    pub track_name: Option<String>,
    pub artist_name: Option<String>,
    pub album_name: Option<String>,
    pub episode_uri: Option<String>,
    pub episode_name: Option<String>,
    pub show_name: Option<String>,
    pub audiobook_title: Option<String>,
    pub audiobook_chapter_title: Option<String>,
    pub platform: Option<String>,
    pub conn_country: Option<String>,
    pub reason_start: Option<String>,
    pub reason_end: Option<String>,
    pub shuffle: Option<bool>,
    /// Spotify's own skip flag; `None` when the export does not say.
    pub skipped: Option<bool>,
    pub offline: Option<bool>,
    pub incognito_mode: Option<bool>,
    /// Fields this parser does not model (`ip_addr`, `offline_timestamp`, new
    /// fields), kept by name.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A saved bookmark in a bookmarking service (Raindrop). The service's ID is
/// the identity, so an edited or moved bookmark is a newer reading; one in the
/// trash is removed at the source.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "bookmark.saved",
    key = ["service", "native_id"],
    replace = newer,
    time = created_at,
    quality = intrinsic,
    removed_when = trashed_at
)]
pub struct BookmarkSaved {
    pub service: String,
    pub native_id: String,
    pub url: String,
    pub title: Option<String>,
    pub note: Option<String>,
    pub excerpt: Option<String>,
    /// The collection or folder it is filed in.
    pub folder: Option<String>,
    pub tags: Vec<String>,
    pub created_at: Option<Timestamp>,
    pub updated_at: Option<Timestamp>,
    pub cover: Option<String>,
    /// Highlighted passages, as the service exported them.
    pub highlights: Option<Value>,
    pub favorite: Option<bool>,
    /// When it went to the trash. The trash export gives no time, so the last
    /// update stands for it.
    pub trashed_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A note file in a notes vault (Obsidian, plain Markdown folders). The vault
/// and the path inside it identify it; an edit is a newer reading, and a file
/// missing from a completed scan is removed at the source.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "note.document",
    key = ["vault", "path"],
    replace = newer,
    time = modified_at,
    quality = file_mtime,
    removed_when = vanished_at
)]
pub struct NoteDocument {
    pub vault: String,
    /// Relative to the vault root.
    pub path: String,
    /// The file's text; `None` for a vanished file.
    pub text: Option<String>,
    pub size: u64,
    pub modified_at: Option<Timestamp>,
    /// When a completed scan first found the file gone.
    pub vanished_at: Option<Timestamp>,
}
