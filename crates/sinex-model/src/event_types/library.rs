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
#[event(version = 3,
    type = "library.copy",
    key = ["host", "root", "path", "scan"],
    replace = repeat,
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
/// An item is a derived reading of the scans, not a curation record: the
/// `library.items` derivation recomputes it when its copies change, and the
/// newer reading replaces the live one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 3,
    type = "library.item",
    key = ["item"],
    replace = newer,
    time = at,
    quality = intrinsic
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RenditionRole {
    Original,
    Crop,
    Preview,
    Transcode,
}

/// Bytes standing for an item revision: its original, read from the scans
/// by the `library.items` derivation, or something made from it (a preview,
/// written by a preview request citing the stored bytes). A preview never
/// answers a request for the original.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 3,
    type = "library.rendition",
    key = ["rendition"],
    replace = repeat,
    time = at,
    quality = intrinsic
)]
pub struct Rendition {
    pub rendition: String,
    pub item: String,
    pub revision: String,
    /// The first visible scan part observing this revision; for a rendition
    /// made from another, that rendition's.
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
}

/// What a curation record or view is about: an item (following its
/// revisions), one revision, one rendition, or a part of a rendition.
#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct Target {
    pub item: String,
    /// Fixed revision; `None` follows the item.
    pub revision: Option<String>,
    pub rendition: Option<String>,
    /// A selector inside the rendition: `t=12.5-40` (seconds, half-open) or
    /// `xywh=10,10,200,120` (pixels).
    pub part: Option<String>,
}

/// What a curation record states. The kind decides what `key` and `value`
/// hold and which records count as the same statement (its uniqueness key).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CurationKind {
    /// `key` is the tag. `value` is absent when the tag applies and `false`
    /// when the author rejects it (an imported or proposed tag that is wrong).
    Tag,
    /// `key` is the property; `value` its value, `null` for an explicit empty.
    Property,
    /// `value` is the text.
    Note,
    /// `key` is the scale; `value` a number in its range or one of its choices.
    Rating,
    /// `key` is the scale; `value` holds `order`, the items best first with
    /// ties as groups, each with its revision fixed, and `blind`.
    Comparison,
    /// One entry of a collection: `key` is the collection's root, `value`
    /// holds the entry's order key, and the target fixes the revision.
    Entry,
    /// `key` is the collection's name.
    Collection,
    /// `key` is the scale's name; `value` holds its question and its range
    /// or choices.
    Scale,
    /// `key` is the property's name; `value` holds its value type
    /// (`text`, `number`, `boolean` or `choice`) and its choices.
    PropertyDefinition,
    /// `key` is the tag; `value` holds its namespace and description.
    TagDefinition,
    /// `key` is `alias` or `implies`; `value` is the two tags: an alias's in
    /// sorted order, an implication's as `[tag, implied]`.
    TagRelation,
    /// A collection suggestion turned down: `key` is the collection's root,
    /// `value` the saved query's revision it was offered under, and the
    /// target the suggested item.
    Dismissal,
    /// Withdraws what `withdraws` names.
    Withdrawal,
}

impl CurationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CurationKind::Tag => "tag",
            CurationKind::Property => "property",
            CurationKind::Note => "note",
            CurationKind::Rating => "rating",
            CurationKind::Comparison => "comparison",
            CurationKind::Entry => "entry",
            CurationKind::Collection => "collection",
            CurationKind::Scale => "scale",
            CurationKind::PropertyDefinition => "property_definition",
            CurationKind::TagDefinition => "tag_definition",
            CurationKind::TagRelation => "tag_relation",
            CurationKind::Dismissal => "dismissal",
            CurationKind::Withdrawal => "withdrawal",
        }
    }
}

/// Where a curated value comes from. With the author it decides which value
/// is shown: the operator's authored value, then an agent's, then imported
/// values, then proposals.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ValueOrigin {
    /// Written by its author through a library request.
    Authored,
    /// Brought in from another application's or site's records; `cites`
    /// names them.
    Imported,
    /// A method's suggestion; `cites` names the method and its version.
    Proposed,
}

/// One revision of an authored library record: a tag, a property value, a
/// note, a rating, a comparison, a collection entry, a collection, a scale or
/// a withdrawal. A revision never changes. Which revisions are current
/// follows from `revises` and `withdraws` alone (the descendant rule in the
/// shared `authored` crate), never from time or from a newer reading.
///
/// `author`, `via` and `at` come from the request's manifest, which the daemon
/// fills from the authenticated caller; no request line supplies them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "library.curation",
    key = ["root", "rev"],
    replace = repeat,
    time = at,
    quality = intrinsic
)]
pub struct Curation {
    /// The record's identity: the ID of its first revision.
    pub root: String,
    /// This revision: `<request>` or `<request>.<n>`.
    pub rev: String,
    /// The revisions of the same record this one revises, as `<root>@<rev>`.
    pub revises: Vec<String>,
    /// What this record withdraws: `<root>` (every revision) or `<root>@<rev>`.
    pub withdraws: Vec<String>,
    pub kind: CurationKind,
    pub target: Option<Target>,
    pub key: Option<String>,
    pub value: Option<Value>,
    pub origin: ValueOrigin,
    /// The imported records cited, or the proposing method and its version.
    pub cites: Option<String>,
    pub author: String,
    /// The channel the request arrived on.
    pub via: String,
    pub at: Timestamp,
    /// The request that wrote this revision, and the digest of its content,
    /// so a retried request is answered without writing again.
    pub request: String,
    pub request_digest: String,
    /// Hex digests of the uniqueness slots the record occupies.
    pub slots: Vec<String>,
}

/// One act of a library curation request, as the operator or an agent asks
/// for it. The daemon turns acts into curation records: it finds what each
/// act revises or withdraws and refuses the whole request when any act is
/// refused. `expects` names the revision the caller saw (`<root>@<rev>`);
/// a write against a revision that is no longer current is refused.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "act", rename_all = "snake_case", deny_unknown_fields)]
pub enum Act {
    /// Apply a tag. Any non-empty label is a tag.
    Tag {
        target: Target,
        tag: String,
        #[serde(default)]
        expects: Option<String>,
    },
    /// Withdraw your own application of a tag.
    Untag {
        target: Target,
        tag: String,
    },
    /// State that a tag does not apply, above imported and proposed values.
    Reject {
        target: Target,
        tag: String,
    },
    /// Set a property; `null` is an explicit empty, which stops imported and
    /// proposed values showing.
    Set {
        target: Target,
        property: String,
        value: Value,
        #[serde(default)]
        expects: Option<String>,
    },
    Note {
        target: Target,
        text: String,
    },
    /// Rate on a defined scale: a number in its range or one of its choices.
    Rate {
        target: Target,
        scale: String,
        value: Value,
        #[serde(default)]
        expects: Option<String>,
    },
    /// Insert an entry into a collection (made when it does not exist), at
    /// the end or next to an entry.
    Collect {
        target: Target,
        collection: String,
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        before: Option<String>,
    },
    /// Withdraw one entry; other entries of the same item stay.
    Uncollect {
        entry: String,
    },
    /// Move an entry next to another.
    Move {
        entry: String,
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        before: Option<String>,
    },
    /// Order two or more items on a scale, best first; a group is a tie.
    Judge {
        scale: String,
        order: Vec<Vec<Target>>,
        #[serde(default)]
        blind: bool,
    },
    /// Define a scale, or revise its question; a range only widens and
    /// choices are only added.
    DefineScale {
        name: String,
        question: String,
        #[serde(default)]
        range: Option<(f64, f64)>,
        #[serde(default)]
        choices: Option<Vec<String>>,
    },
    /// Declare a property's value type; values set afterwards are checked
    /// against it. Choices are only added on a redefinition.
    DefineProperty {
        name: String,
        value_type: PropertyType,
        #[serde(default)]
        choices: Option<Vec<String>>,
    },
    /// Declare a tag's namespace and description.
    DefineTag {
        tag: String,
        #[serde(default)]
        namespace: Option<String>,
        #[serde(default)]
        description: Option<String>,
    },
    /// Relate two tags: `alias` makes them one shown tag, `implies` adds the
    /// second wherever the first applies. An implication closing a cycle is
    /// refused.
    Relate {
        relation: TagRelation,
        tag: String,
        other: String,
    },
    /// Turn down a collection suggestion offered by a saved query revision.
    Dismiss {
        collection: String,
        target: Target,
        query: String,
    },
    /// Withdraw a record (`<root>`) or one revision (`<root>@<rev>`).
    /// Withdrawing a withdrawal restores what it withdrew. With `expects`,
    /// refused unless that revision of the record is current.
    Withdraw {
        record: String,
        #[serde(default)]
        expects: Option<String>,
    },
    /// Withdraw everything a request wrote; refused while a later record
    /// revises or withdraws any of it.
    Undo {
        request: String,
    },
}

/// The type of a declared property's values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PropertyType {
    Text,
    Number,
    Boolean,
    Choice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TagRelation {
    Alias,
    Implies,
}

/// One time something was opened for viewing or playing. The target is fixed
/// when the request is made.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 3,
    type = "library.view",
    key = ["view"],
    replace = repeat,
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
    pub by: String,
}

crate::payload_enum! {
    /// Whatever one line of a library request writes.
    #[derive(Clone, Debug, PartialEq)]
    pub enum LibraryRecord {
        Rendition(Rendition),
        Curation(Curation),
        View(View),
    }
}

/// The event types library requests write.
pub const REQUEST_TYPES: &[&str] = &["library.rendition", "library.curation", "library.view"];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "library.filename_hints", key = ["copy"], replace = newer)]
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

/// The text of a document in the library: a page of a PDF, or the whole of a
/// text, HTML, Markdown, Word (`.docx`) or OpenDocument (`.odt`) file, read
/// from its stored bytes. Each captured copy has its own text and parents,
/// including when another copy has identical bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "document.text",
    key = ["record", "part"],
    replace = newer,
    time = modified_at,
    quality = file_mtime
)]
pub struct DocumentText {
    /// The captured copy's stable source record.
    pub record: String,
    /// The BLAKE3 name of the document's bytes.
    pub content: String,
    /// The path of the copy it was read from.
    pub path: String,
    /// `pdf`, `html`, `markdown`, `text`, `docx` or `odt`.
    pub format: String,
    /// The page, from 1, for a PDF; 0 for a document read whole.
    pub part: u64,
    pub title: Option<String>,
    pub text: String,
    /// What read it: the PDF reader's version line, or `sinexd` for the
    /// formats read in Sinex itself.
    pub reader: String,
    /// The copy's modification time.
    pub modified_at: Option<Timestamp>,
}
