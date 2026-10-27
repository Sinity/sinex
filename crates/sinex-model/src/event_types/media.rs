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
#[event(version = 2,
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
#[event(version = 2,
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
#[event(version = 2,
    type = "note.document",
    key = ["vault", "path"],
    replace = newer,
    time = modified_at,
    quality = file_mtime,
    removed_when = removed_at
)]
pub struct NoteDocument {
    pub vault: String,
    /// Relative to the vault root.
    pub path: String,
    /// The file's text; `None` for a removed file.
    pub text: Option<String>,
    pub size: u64,
    pub modified_at: Option<Timestamp>,
    /// When a completed scan first found the file gone.
    pub removed_at: Option<Timestamp>,
    /// Absent for a removed file and for a note without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The exact front-matter string, including an explicitly empty ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note_id: Option<String>,
    /// Sorted string tags from the parsed header and known body. An invalid
    /// front_matter means header tags remain unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wikilinks: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub front_matter: Option<NoteFrontMatter>,
}

/// The parsed YAML header, separate from the exact original note text.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum NoteFrontMatter {
    Absent,
    Parsed {
        value: Value,
    },
    Invalid {
        kind: NoteFrontMatterError,
        /// One-based coordinates in the note, when the parser supplies them.
        line: Option<u64>,
        column: Option<u64>,
    },
}

/// Structural diagnostics contain no copied note text or YAML snippets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NoteFrontMatterError {
    Yaml,
    UnsupportedValue,
    Limit,
    Unclosed,
}

/// A book shelved or read in a reading tracker (Goodreads). Dates are days,
/// so these serve long-horizon interest, not daily behaviour.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "book.status",
    key = ["service", "book_id", "action"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct BookStatus {
    pub service: String,
    pub book_id: String,
    /// `shelved` (added) or `read` (finished).
    pub action: String,
    /// The day, at midnight UTC; the export gives no time of day.
    pub at: Option<Timestamp>,
    pub title: String,
    pub author: Option<String>,
    pub additional_authors: Option<String>,
    pub isbn: Option<String>,
    pub isbn13: Option<String>,
    /// The operator's rating, 1-5; `None` when unrated (the export writes 0).
    pub my_rating: Option<i64>,
    pub average_rating: Option<f64>,
    pub publisher: Option<String>,
    pub binding: Option<String>,
    pub pages: Option<i64>,
    pub year_published: Option<i64>,
    pub original_year: Option<i64>,
    pub shelves: Vec<String>,
    pub exclusive_shelf: Option<String>,
    pub review: Option<String>,
    pub private_notes: Option<String>,
    pub read_count: Option<i64>,
    pub owned_copies: Option<i64>,
}
