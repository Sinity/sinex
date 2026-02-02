//! PKM (Personal Knowledge Management) types

use crate::domain::{Entity, EntityRelation, EntityTypeName, RelationType, UserId};
use crate::events::{Event, SourceMaterial};
use crate::ids::Id;
use crate::JsonValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─────────────────────────────────────────────────────────────
// pkm.create_note
// ─────────────────────────────────────────────────────────────

/// Request: `pkm.create_note`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateNoteRequest {
    /// Event ID to attach note to
    pub event_id: Id<Event<JsonValue>>,
    /// Base64-encoded note content
    pub content: String,
    /// Optional tags
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Creator identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<UserId>,
}

/// Response: `pkm.create_note`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateNoteResponse {
    pub annotation_id: Id<Event<JsonValue>>,
}

// ─────────────────────────────────────────────────────────────
// pkm.create_entities_from_list
// ─────────────────────────────────────────────────────────────

/// Entity definition for batch creation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityDefinition {
    pub name: String,
    pub entity_type: EntityTypeName,
}

/// Request: `pkm.create_entities_from_list`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEntitiesRequest {
    /// Source material ID
    pub source_material_id: Id<SourceMaterial>,
    /// List of entities to create
    pub entities: Vec<EntityDefinition>,
    /// Creator identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<UserId>,
}

/// Response: `pkm.create_entities_from_list`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEntitiesResponse {
    pub entity_ids: Vec<Id<Entity>>,
}

// ─────────────────────────────────────────────────────────────
// pkm.link_entities
// ─────────────────────────────────────────────────────────────

/// Request: `pkm.link_entities`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkEntitiesRequest {
    /// Source entity ID
    pub from_entity_id: Id<Entity>,
    /// Target entity ID
    pub to_entity_id: Id<Entity>,
    /// Relationship type
    pub relation_type: RelationType,
    /// Optional metadata
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// Source material ID for provenance
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_material_id: Option<Id<SourceMaterial>>,
}

/// Response: `pkm.link_entities`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkEntitiesResponse {
    pub relation_id: Id<EntityRelation>,
}
