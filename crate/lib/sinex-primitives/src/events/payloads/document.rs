//! Document ingestion + synthesis event payloads.
//!
//! `DocumentIngestedPayload` is the material-provenance event emitted by
//! `sinex-document-ingestor` when a file is staged into the source-material
//! registry. The `DocumentParsedPayload` and `DocumentChunkedPayload` events
//! are synthesis-provenance outputs of the document-layer parser (issue
//! [#733], design doc `docs/architecture/document-layer-v1.md`): a document
//! is the queryable text unit derived from one source — for v1, either a
//! Dendron markdown file or a single terminal command's canonicalized
//! output.
//!
//! [#733]: https://github.com/Sinity/sinex/issues/733

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_macros::EventPayload;

use crate::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "document-ingestor", event_type = "document.ingested")]
pub struct DocumentIngestedPayload {
    pub file_path: String,
    pub source_material_id: String,
    pub size_bytes: u64,
    pub mime_type: Option<String>,
    pub encoding: Option<String>,
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl DocumentIngestedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            file_path: "/test/document.txt".into(),
            source_material_id: "test-material-id".into(),
            size_bytes: 0,
            mime_type: None,
            encoding: None,
        }
    }
}

/// Corpus that produced a document. v1 ships exactly two — see the document
/// layer design doc's "Two corpora, no more" table.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DocumentKind {
    /// Dendron-style markdown file under a configured vault root.
    DendronMarkdown,
    /// Captured stdout/stderr from a single terminal command, transduced via
    /// `command.canonical`.
    TerminalOutput,
}

impl DocumentKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            DocumentKind::DendronMarkdown => "dendron_markdown",
            DocumentKind::TerminalOutput => "terminal_output",
        }
    }
}

/// Synthesis event emitted once per parsed document.
///
/// Provenance is `from_parents([parent_event_id])`:
/// - For Dendron, the parent is the originating `document.ingested` event.
/// - For terminal output, the parent is the `command.canonical` event.
///
/// The corresponding chunks are emitted as `document.chunked` events whose
/// own provenance threads back to *this* `document.parsed` event ID
/// (chunks are derived from the document, not directly from source bytes).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "document-parser", event_type = "document.parsed")]
pub struct DocumentParsedPayload {
    /// Deterministic document identity. UUIDv5 over `(NS_DOCUMENTS,
    /// source_unit || "/" || natural_key)` — replay against the same source
    /// produces the same id.
    pub document_id: Uuid,
    /// Which corpus this document belongs to (see `DocumentKind`).
    pub kind: DocumentKind,
    /// Vault-relative path for Dendron, stringified parent event id for
    /// terminal output. Together with `kind` this is the natural key.
    pub natural_key: String,
    /// Bumped when the parser's output schema changes; downstream consumers
    /// (entities, embeddings) key off `(document_id, extraction_version)`
    /// for invalidation. v1 ships at 1.
    pub extraction_version: u32,
    /// Number of `document.chunked` events emitted alongside this
    /// `document.parsed`. Used by the projection writer for completeness
    /// checks; not load-bearing for replay.
    pub chunk_count: u32,
    /// Total bytes of *post-redaction* extracted text across all chunks.
    pub text_byte_len: u64,
    /// Kind-specific structured fields. For Dendron: `{ frontmatter,
    /// wikilinks, title }`. For terminal: `{ command, exit_code, shell }`.
    /// Schema-less by design — see the design doc's "Open questions" item
    /// on frontmatter schema.
    pub side_data: JsonValue,
}

#[cfg(any(test, feature = "testing"))]
impl DocumentParsedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            document_id: Uuid::nil(),
            kind: DocumentKind::DendronMarkdown,
            natural_key: "test/note.md".into(),
            extraction_version: 1,
            chunk_count: 0,
            text_byte_len: 0,
            side_data: JsonValue::Object(serde_json::Map::new()),
        }
    }
}

/// Synthesis event emitted once per chunk of a parsed document.
///
/// Provenance is `from_parents([document_parsed_event_id])` — chunks are
/// derived from the document, not directly from the source material. This
/// keeps replay scoped: re-extraction of one document re-emits its chunks
/// without touching siblings.
///
/// `byte_offset_*` are offsets into the post-redaction extracted text (the
/// `text` column of `core.document_chunks`), suitable for in-document chunk
/// navigation. `source_anchor_*` are pre-redaction byte offsets into the
/// raw source material — populated for Dendron, `None` for terminal output
/// (which has no byte-stream source). See the design doc's "Anchor-byte
/// semantics" section.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "document-parser", event_type = "document.chunked")]
pub struct DocumentChunkedPayload {
    pub document_id: Uuid,
    pub chunk_index: u32,
    /// Post-privacy-redaction chunk content.
    pub text: String,
    pub byte_offset_start: u64,
    /// Exclusive end offset.
    pub byte_offset_end: u64,
    pub source_anchor_start: Option<u64>,
    pub source_anchor_end: Option<u64>,
}

#[cfg(any(test, feature = "testing"))]
impl DocumentChunkedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            document_id: Uuid::nil(),
            chunk_index: 0,
            text: "test chunk".into(),
            byte_offset_start: 0,
            byte_offset_end: 10,
            source_anchor_start: None,
            source_anchor_end: None,
        }
    }
}
