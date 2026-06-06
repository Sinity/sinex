//! Knowledge graph event payloads.
//!
//! Two groups:
//!
//! - **Vault observation** — material-provenance events emitted by the
//!   `knowledgebase-vault` source when it mirrors an Obsidian-style PKM
//!   vault into sinex. Each `.md` file becomes one `knowledgebase`/`note.observed`
//!   event.
//!
//! - **Graph automata** — derived events emitted by currently wired knowledge
//!   graph automata. Today that surface is intentionally limited to rule-based
//!   tag application (`knowledge-graph`/`knowledge.tag_applied`). Entity mention,
//!   resolution-candidate, merge, and relation-proposal schemas are not declared
//!   here until a producer exists.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Uuid;

// ── Vault note observation ──────────────────────────────────────────────

/// Emitted once per `.md` file observed in the knowledgebase vault.
///
/// This is a periodic mirror / federated snapshot — sinex observes the vault's
/// state without claiming ownership. Re-imports re-publish only when the note's
/// content has changed (BLAKE3 hash of body differs).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "knowledgebase",
    event_type = "note.observed",
    version = "1.0.0"
)]
pub struct KnowledgeNoteObservedPayload {
    /// Path relative to vault root (e.g. `permanent.concept.foo.md`).
    pub path: String,
    /// Note title — from front-matter `title:` field, otherwise the stem of
    /// the filename (dendron-style `area.subarea.note`).
    pub title: String,
    /// Front-matter fields as an opaque JSON object.
    /// All YAML scalars, sequences, and mappings are preserved verbatim so
    /// downstream consumers can use whatever fields they need without schema
    /// coupling.
    pub front_matter: serde_json::Value,
    /// Tags collected from both front-matter (`tags:` list) and inline body
    /// `#tag` syntax. Deduplicated and sorted.
    pub tags: Vec<String>,
    /// `[[wikilink]]` references found in the note body. Deduplicated and
    /// sorted. Brackets and alias suffixes (`|alias`) are stripped.
    pub wikilinks: Vec<String>,
    /// BLAKE3 hex digest of the note body (everything after the front-matter
    /// delimiter). Used for change detection between imports.
    pub body_text_hash: String,
    /// Byte size of the note body (everything after front-matter).
    pub body_byte_size: u64,
    /// File modification time as an RFC 3339 string, if available from the OS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime: Option<String>,
}

#[cfg(any(test, feature = "testing"))]
impl KnowledgeNoteObservedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            path: "permanent.concept.test.md".into(),
            title: "test".into(),
            front_matter: serde_json::json!({"id": "permanent.concept.test"}),
            tags: vec!["concept".into()],
            wikilinks: vec![],
            body_text_hash: "0".repeat(64),
            body_byte_size: 0,
            mtime: None,
        }
    }
}

// ── Tag applied ────────────────────────────────────────────────────────

/// Emitted when a tag is applied to an entity, either by rule-based
/// automaton or by operator confirmation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "knowledge-graph",
    event_type = "knowledge.tag_applied",
    version = "1.0.0"
)]
pub struct KnowledgeTagAppliedPayload {
    pub entity_id: Uuid,
    pub tag_name: String,
    pub tag_source: String,
}

#[cfg(any(test, feature = "testing"))]
impl KnowledgeTagAppliedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            entity_id: Uuid::nil(),
            tag_name: "test-tag".into(),
            tag_source: "rule".into(),
        }
    }
}
