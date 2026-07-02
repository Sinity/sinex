//! Document library index payloads.
//!
//! Metadata-only inventory events for the local document library
//! (`/realm/data/libraries/doc/` and sibling directories).  Content
//! extraction is out of scope here — that is the domain of
//! `DocumentParsedPayload` / `DocumentChunkedPayload` in `document.rs`.
//!
//! One `document.indexed` event is emitted per file discovered by the
//! `docs-library-index` source.  Fields are derived from filename
//! heuristics and filesystem metadata; no document bytes are read.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::temporal::Timestamp;

/// Inventory event emitted once per file in the document library.
///
/// `path` is relative to the configured library root so events are
/// portable across host renames.  `content_hash` (BLAKE3 hex) provides
/// stable identity: the same file re-imported after a rename produces
/// the same hash and can be deduplicated on `(path, content_hash)`.
///
/// Filename heuristics (`title_hint`, `author_hint`, `year_hint`,
/// `external_id`) are best-effort only.  Callers must treat `None` as
/// "unknown" and `Some` as an unverified hint.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "docs-library", event_type = "document.indexed")]
pub struct LibraryDocumentIndexedPayload {
    /// Path relative to the library root (e.g. `"Aaron Clarey - ... (2013).epub"`).
    pub path: String,

    /// Bare filename with extension.
    pub filename: String,

    /// Lowercase file extension without the leading dot (e.g. `"pdf"`, `"epub"`).
    pub extension: String,

    /// File size in bytes.
    pub byte_size: u64,

    /// File modification time according to the local filesystem.
    pub mtime: Timestamp,

    /// BLAKE3 hex digest of the file contents, if computed.
    ///
    /// The adapter may supply this via the anchor; the parser falls back
    /// to `None` when not available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,

    /// Heuristic title extracted from the filename.
    ///
    /// Derived by stripping the author prefix (everything up to ` - `),
    /// the year suffix, and the extension.  `None` when the heuristic
    /// does not fire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title_hint: Option<String>,

    /// Heuristic author extracted from the filename.
    ///
    /// Derived by taking the segment before the first ` - ` separator.
    /// `None` when the separator is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_hint: Option<String>,

    /// Four-digit year extracted from the filename, if present.
    ///
    /// Matches the first `(\d{4})` group in the range 1900–2030.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub year_hint: Option<u16>,

    /// External identifier found in the filename.
    ///
    /// A 32-character hex sequence is interpreted as an MD5 / Anna's Archive
    /// identifier.  `None` when no such sequence is found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
}

#[cfg(test)]
#[path = "library_test.rs"]
mod tests;
