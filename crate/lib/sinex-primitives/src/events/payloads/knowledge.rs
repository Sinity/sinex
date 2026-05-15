//! Knowledge graph event payloads.
//!
//! Two groups:
//!
//! - **Vault observation** — material-provenance events emitted by the
//!   `knowledgebase-vault` source unit when it mirrors an Obsidian-style PKM
//!   vault into sinex. Each `.md` file becomes one `knowledgebase`/`note.observed`
//!   event.
//!
//! - **Graph automata** — synthesis events emitted by knowledge graph automata:
//!   entity mention extraction, linking, merging, relation proposals, and tag
//!   lifecycle. All carry synthesis provenance from the parent event that
//!   triggered them.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Uuid;
use crate::domain::EntityTypeName;

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

// ── Entity mention ─────────────────────────────────────────────────────

/// Emitted when an entity is mentioned in a parsed document or event payload.
/// These are raw signals before resolution — the entity resolver consumes them.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "knowledge-graph",
    event_type = "knowledge.entity_mention",
    version = "1.0.0"
)]
pub struct KnowledgeEntityMentionPayload {
    /// Raw text span as it appeared in the source.
    pub raw_text: String,
    /// Guessed entity type before resolution.
    pub guessed_type: EntityTypeName,
    /// Source position context (field name, byte offset, etc.).
    pub source_context: serde_json::Value,
}

#[cfg(any(test, feature = "testing"))]
impl KnowledgeEntityMentionPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            raw_text: "test".into(),
            guessed_type: EntityTypeName::new("concept"),
            source_context: serde_json::Value::Object(serde_json::Map::new()),
        }
    }
}

// ── Entity linked ──────────────────────────────────────────────────────

/// Emitted when a resolved entity is linked to a specific document or event,
/// establishing a durable association for retrieval.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "knowledge-graph",
    event_type = "knowledge.entity_linked",
    version = "1.0.0"
)]
pub struct KnowledgeEntityLinkedPayload {
    pub entity_id: Uuid,
    pub target_event_id: Uuid,
    pub link_kind: String,
}

#[cfg(any(test, feature = "testing"))]
impl KnowledgeEntityLinkedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            entity_id: Uuid::nil(),
            target_event_id: Uuid::nil(),
            link_kind: "mention".into(),
        }
    }
}

// ── Entity merged ──────────────────────────────────────────────────────

/// Emitted when two entities are deduplicated/merged. The source entity is
/// absorbed into the target; its `is_merged` flag is set in `core.entities`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "knowledge-graph",
    event_type = "knowledge.entity_merged",
    version = "1.0.0"
)]
pub struct KnowledgeEntityMergedPayload {
    /// The surviving entity.
    pub survivor_entity_id: Uuid,
    /// The entity being absorbed.
    pub absorbed_entity_id: Uuid,
    /// Merge strategy used.
    pub merge_strategy: String,
}

#[cfg(any(test, feature = "testing"))]
impl KnowledgeEntityMergedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            survivor_entity_id: Uuid::nil(),
            absorbed_entity_id: Uuid::nil(),
            merge_strategy: "exact_match".into(),
        }
    }
}

// ── Relation proposed ──────────────────────────────────────────────────

/// Emitted when a relationship between two entities is detected.
/// Downstream automata promote or reject based on confidence thresholds.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "knowledge-graph",
    event_type = "knowledge.relation_proposed",
    version = "1.0.0"
)]
pub struct KnowledgeRelationProposedPayload {
    pub source_entity_id: Uuid,
    pub target_entity_id: Uuid,
    pub relation_type: String,
    pub confidence: f64,
}

#[cfg(any(test, feature = "testing"))]
impl KnowledgeRelationProposedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            source_entity_id: Uuid::nil(),
            target_entity_id: Uuid::nil(),
            relation_type: "related_to".into(),
            confidence: 0.8,
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

// ── Tag confirmed ──────────────────────────────────────────────────────

/// Emitted when a proposed tag is confirmed, either by operator action or
/// by confidence threshold crossing.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "knowledge-graph",
    event_type = "knowledge.tag_confirmed",
    version = "1.0.0"
)]
pub struct KnowledgeTagConfirmedPayload {
    pub entity_id: Uuid,
    pub tag_name: String,
    pub confirmed_by: String,
}

#[cfg(any(test, feature = "testing"))]
impl KnowledgeTagConfirmedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            entity_id: Uuid::nil(),
            tag_name: "test-tag".into(),
            confirmed_by: "operator".into(),
        }
    }
}

// ── Entity resolution candidate ────────────────────────────────────────

/// Emitted by the entity resolver when it finds a candidate match that
/// needs operator confirmation before merging.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "knowledge-graph",
    event_type = "knowledge.entity_resolution_candidate",
    version = "1.0.0"
)]
pub struct KnowledgeEntityResolutionCandidatePayload {
    pub existing_entity_id: Uuid,
    pub candidate_entity_id: Uuid,
    pub match_confidence: f64,
    pub match_reason: String,
}

#[cfg(any(test, feature = "testing"))]
impl KnowledgeEntityResolutionCandidatePayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            existing_entity_id: Uuid::nil(),
            candidate_entity_id: Uuid::nil(),
            match_confidence: 0.7,
            match_reason: "name_similarity".into(),
        }
    }
}
