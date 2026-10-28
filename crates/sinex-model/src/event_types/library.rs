//! The media library: what a library scan saw, and the records library requests
//! write. Every library record is an ordinary typed event; current values
//! (which tag applies, which property value is shown, a collection's order) are
//! read from them, never stored beside them.
//!
//! An item follows its root and original relative path across revisions and
//! moves. Its opaque ID is deterministic; equal bytes at unrelated paths are
//! separate items.
use crate::{EventId, Timestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A file whose facts changed in a scan. Earlier visible changes remain
/// evidence for the item and its original rendition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.copy",
    key = ["host", "root", "path", "scan"],
    replace = newer,
    time = scanned_at,
    quality = intrinsic,
    removed_when = vanished_at,
    owns(field = content, size = size, when = stored)
)]
pub struct FileCopy {
    /// The material identifying this captured scan part.
    pub scan: String,
    /// Capture order within the root.
    pub part_index: u32,
    /// The root and original relative path, kept across moves.
    pub item_key: String,
    /// Deterministic opaque identifier of `item_key`.
    pub item: String,
    pub scanned_at: Timestamp,
    pub host: String,
    /// The library root the scan walked.
    pub root: String,
    /// The file's absolute path.
    pub path: String,
    /// `blake3:<hex>`, the content store's name for the bytes.
    pub content: String,
    pub size: u64,
    /// Hex digests other catalogues name files by (Hydrus, boorus).
    pub sha256: String,
    pub md5: String,
    /// Sniffed from the bytes; `application/octet-stream` when unknown. A file
    /// Sinex cannot read is still stored and described.
    pub media_type: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
    /// Whether the scan stored the bytes in the content store (a root can be
    /// indexed by reference only).
    pub stored: bool,
    pub device: Option<u64>,
    pub inode: Option<u64>,
    pub modified_at: Option<Timestamp>,
    /// The earlier path matched by device and inode, or by a unique content
    /// match among missing paths.
    pub moved_from: Option<String>,
    /// Paths that could be the same item; no move is inferred when several match.
    pub move_candidates: Vec<String>,
    /// The earlier bytes at this path, when its content changed.
    pub previous_content: Option<String>,
    /// When a completed scan first found the file gone.
    pub vanished_at: Option<Timestamp>,
}

/// One revision of an item. The live reading is the current revision; the
/// original renditions describe its revisions. New bytes at the same item key
/// make a new revision; equal bytes at an unrelated key are a different item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.item",
    key = ["item"],
    replace = newer,
    time = at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct Item {
    /// Opaque identifier computed from the item key.
    pub item: String,
    pub revision: String,
    /// The revision this one replaces.
    pub replaces: Option<String>,
    /// The original rendition's bytes, `blake3:<hex>`.
    pub content: String,
    pub media_type: String,
    /// Copies that belong to the item, as `host:path`.
    pub copies: Vec<String>,
    /// A human label; the file name at intake when nothing better is known.
    pub label: Option<String>,
    pub by: String,
    pub at: Timestamp,
    pub withdrawn_at: Option<Timestamp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RenditionRole {
    Original,
    Crop,
    Preview,
    Transcode,
}

/// Bytes standing for an item revision: its original, or something made
/// from it. A preview never answers a request for the original.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.rendition",
    key = ["rendition"],
    replace = newer,
    time = at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct Rendition {
    pub rendition: String,
    pub item: String,
    pub revision: String,
    /// The first visible scan part observing this revision.
    pub part_index: u32,
    pub role: RenditionRole,
    pub content: String,
    pub media_type: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
    /// The rendition it was made from; `None` for an original.
    pub made_from: Option<String>,
    /// How it was made: the tool and its settings.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub made_with: Map<String, Value>,
    pub by: String,
    pub at: Timestamp,
    pub withdrawn_at: Option<Timestamp>,
}

/// A rendered preview submitted as evidence for a rendition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "library.preview",
    key = ["rendition"],
    replace = newer,
    time = at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct Preview {
    pub rendition: String,
    pub item: String,
    pub revision: String,
    pub role: RenditionRole,
    pub content: String,
    pub media_type: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
    /// The rendition it was made from; `None` for an original.
    pub made_from: Option<String>,
    /// How it was made: the tool and its settings.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub made_with: Map<String, Value>,
    pub by: String,
    pub at: Timestamp,
    pub withdrawn_at: Option<Timestamp>,
}

/// What an assignment, entry or view is about: an item (following its
/// revisions), one revision, one rendition, or a part of a rendition.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Target {
    pub item: String,
    /// Fixed revision; `None` follows the item.
    pub revision: Option<String>,
    pub rendition: Option<String>,
    /// A selector inside the rendition: `t=12.5-40` (seconds, half-open) or
    /// `xywh=10,10,200,120` (pixels).
    pub part: Option<String>,
}

/// Who an assigned value comes from, highest first in the selection policy.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    /// The operator's own value; wins over everything, an empty one included.
    Override,
    /// Brought in from another application or site.
    Import,
    /// A model's suggestion.
    Proposal,
}

/// One value given to one field of a target: a tag (`field = "tag"`), or a
/// property. A property can be set to empty (`value = None`), which stops
/// fallback to lower levels. Withdrawing an assignment is a newer reading of
/// it with `withdrawn_at` set.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.assignment",
    key = ["assignment"],
    replace = newer,
    time = at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct Assignment {
    pub assignment: String,
    pub target: Target,
    /// `tag`, or the property's name.
    pub field: String,
    pub value: Option<String>,
    pub level: Level,
    /// Where an imported value came from: a sidecar file, a post URL.
    pub source_ref: Option<String>,
    pub by: String,
    pub at: Timestamp,
    pub withdrawn_at: Option<Timestamp>,
}

/// A collection: an ordered list of entries, optionally with a saved query that
/// offers suggestions. A fixed selection is a collection without a query whose
/// entries were added from a query's results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.collection",
    key = ["collection"],
    replace = newer,
    time = at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct Collection {
    pub collection: String,
    pub name: String,
    pub saved_query: Option<String>,
    /// The query the entries were selected from, for a fixed selection.
    pub selected_from: Option<String>,
    pub by: String,
    pub at: Timestamp,
    pub withdrawn_at: Option<Timestamp>,
}

/// One entry in a collection. Each insertion is a new entry, so the same item
/// can appear twice; removing an entry withdraws it and keeps it inspectable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.entry",
    key = ["entry"],
    replace = newer,
    time = at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct Entry {
    pub entry: String,
    pub collection: String,
    /// The item, with the revision fixed when it was added.
    pub target: Target,
    /// Sort position; an entry inserted between two takes a value between
    /// theirs.
    pub position: f64,
    pub by: String,
    pub at: Timestamp,
    pub withdrawn_at: Option<Timestamp>,
}

/// A rating on a named scale (`stars`, `favourite`), one per rater.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.rating",
    key = ["item", "scale", "by"],
    replace = newer,
    time = at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct Rating {
    pub item: String,
    pub scale: String,
    pub value: f64,
    /// The top of the scale (5 stars, 1 for a like).
    pub of: f64,
    pub level: Level,
    pub by: String,
    pub at: Timestamp,
    pub withdrawn_at: Option<Timestamp>,
}

/// One pairwise judgement: `winner` is better than `loser` on `scale`. A
/// ranking is fitted from all of them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.comparison",
    key = ["comparison"],
    replace = newer,
    time = at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct Comparison {
    pub comparison: String,
    pub scale: String,
    pub winner: String,
    pub loser: String,
    pub by: String,
    pub at: Timestamp,
    pub withdrawn_at: Option<Timestamp>,
}

/// One time something was opened for viewing or playing. The target is fixed
/// when the request is made.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.view",
    key = ["view"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct View {
    pub view: String,
    pub target: Target,
    /// The program it was handed to.
    pub player: String,
    pub started_at: Timestamp,
    /// How long it was open; `None` while it still is, or when unknown.
    pub seconds: Option<f64>,
    /// Imported view counts carry how many views the record stands for.
    pub count: u32,
    pub level: Level,
    pub by: String,
}

/// A collection suggestion the operator dismissed; remembered for that
/// collection and query revision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.dismissal",
    key = ["collection", "item", "query"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct Dismissal {
    pub collection: String,
    pub item: String,
    /// The saved query's text when the suggestion was seen.
    pub query: String,
    pub by: String,
    pub at: Timestamp,
}

crate::payload_enum! {
    /// Whatever one line of a library request writes.
    #[derive(Clone, Debug, PartialEq)]
    pub enum LibraryRecord {
        Preview(Preview),
        Assignment(Assignment),
        Collection(Collection),
        Entry(Entry),
        Rating(Rating),
        Comparison(Comparison),
        View(View),
        Dismissal(Dismissal),
    }
}

/// The event types library requests write.
pub const REQUEST_TYPES: &[&str] = &[
    "library.preview",
    "library.assignment",
    "library.collection",
    "library.entry",
    "library.rating",
    "library.comparison",
    "library.view",
    "library.dismissal",
];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "library.filename_hints", key = ["copy"], replace = newer)]
pub struct FilenameHints {
    pub copy: EventId,
    pub path: String,
    pub content: String,
    pub author_hint: Option<String>,
    pub title_hint: Option<String>,
    pub year_hint: Option<u16>,
    pub external_id_hint: Option<String>,
    pub rule_version: u32,
}

/// A root's identity includes the host and its resolved path.
pub fn item_key(host: &str, root: &str, relative: &str) -> String {
    serde_json::to_string(&[host, root, relative]).expect("strings serialize")
}

/// An item's identity depends on its first key, never its content.
pub fn item_id(key: &str) -> String {
    format!("library:{}", blake3::hash(key.as_bytes()).to_hex())
}
