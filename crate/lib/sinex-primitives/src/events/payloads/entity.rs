//! Entity intelligence pipeline event payloads.
//!
//! Defines payload types for stages 1-4 of the entity intelligence pipeline:
//! 1. Entity Extraction (Stage 1, issue #331)
//! 2. Entity Resolution (Stage 2, issue #934)
//! 3. Relation Extraction (Stage 3, issue #934)
//! 4. Entity Enrichment (Stage 4, issue #934)
//!
//! All events in this module are synthesis-provenance events derived from
//! upstream pipeline stages or document parsing -- none carry source_material_id.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::domain::{EntityTypeName, RelationType};
use crate::Timestamp;
use crate::Uuid;

// ============================================================================
// Stage 1: Entity Extraction
// ============================================================================

/// Raw entity extracted from document text by the entity extractor.
///
/// This is the initial output of Stage 1 -- a lightweight signal carrying the
/// entity type and the raw text span as it appeared in the source document.
/// Downstream stages (resolver) canonicalize the name and assign a
/// deterministic UUIDv5 identity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "entity-extractor",
    event_type = "entity.extracted",
    version = "1.0.0"
)]
pub struct EntityExtractedPayload {
    /// Classification of the extracted entity (e.g. tool, url, file, person).
    pub entity_type: EntityTypeName,
    /// The raw text span as it appeared in the source document.
    pub raw_name: String,
    /// Extraction confidence in [0.0, 1.0].
    pub confidence: f64,
}

// ============================================================================
// Stage 2: Entity Resolution
// ============================================================================

/// Resolved entity with a deterministic UUIDv5 identity and canonical name.
///
/// The entity resolver (Stage 2) consumes `entity.extracted` events and emits
/// one `entity.resolved` per unique `(entity_type, canonical_name)` pair.
/// The UUIDv5 is deterministically derived from that pair so that replay
/// against the same source always produces identical IDs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "entity-resolver",
    event_type = "entity.resolved",
    version = "1.0.0"
)]
pub struct EntityResolvedPayload {
    /// Deterministic entity identity. UUIDv5 over `(entity_type, canonical_name)`.
    pub entity_id: Uuid,
    /// Type-aware canonicalized name (e.g., lowercased tool, normalized URL host).
    pub canonical_name: String,
    /// Normalized entity type classification.
    pub entity_type: EntityTypeName,
    /// The raw name as originally extracted, preserved for audit.
    pub original_name: String,
}

// ============================================================================
// Stage 3: Relation Extraction
// ============================================================================

/// A typed relationship between two resolved entities.
///
/// The relation extractor (Stage 3) consumes `entity.resolved` events and emits
/// `entity.related` events when entities co-occur in the same source window or
/// when explicit relationships are found (e.g., tool to project from CWD, website
/// to topic from page title).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "relation-extractor",
    event_type = "entity.related",
    version = "1.0.0"
)]
pub struct EntityRelatedPayload {
    /// The source entity of the relationship.
    pub source_entity_id: Uuid,
    /// The target entity of the relationship.
    pub target_entity_id: Uuid,
    /// Semantic type of the relationship (e.g. works_on, co_occurs_with).
    pub relation_type: RelationType,
    /// Confidence score for this relationship in [0.0, 1.0].
    pub confidence: f64,
}

// ============================================================================
// Stage 4: Entity Enrichment
// ============================================================================

/// Refined classification label for an entity.
///
/// Produced by the entity enricher (Stage 4) based on temporal signals,
/// co-occurrence patterns, and source context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EntityCategory {
    /// Command-line tool or binary.
    Tool,
    /// Software project or repository.
    Project,
    /// Web domain or URL.
    Website,
    /// Document or file.
    Document,
    /// Person or user identity.
    Person,
}

/// Enriched entity with temporal statistics and category refinement.
///
/// The entity enricher (Stage 4) consumes `entity.resolved` events and emits
/// periodically enriched snapshots that include first/last seen timestamps,
/// occurrence frequency, an active-hours histogram, and a refined category.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "entity-enricher",
    event_type = "entity.enriched",
    version = "1.0.0"
)]
pub struct EntityEnrichedPayload {
    /// The resolved entity identity this enrichment snapshot describes.
    pub entity_id: Uuid,
    /// Canonical name of the entity (pass-through from resolved).
    pub canonical_name: String,
    /// Entity type classification (pass-through from resolved).
    pub entity_type: EntityTypeName,
    /// Refined category assigned by the enricher.
    pub refined_category: EntityCategory,
    /// Earliest observation timestamp.
    pub first_seen: Timestamp,
    /// Most recent observation timestamp.
    pub last_seen: Timestamp,
    /// Total number of observations.
    pub occurrence_count: u64,
    /// Active-hours histogram: maps hour-of-day (0-23) to occurrence count.
    pub active_hours: BTreeMap<u8, u64>,
}

// ============================================================================
// Test helpers
// ============================================================================

#[cfg(any(test, feature = "testing"))]
impl EntityExtractedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            entity_type: EntityTypeName::new("tool"),
            raw_name: "test-tool".into(),
            confidence: 1.0,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl EntityResolvedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            entity_id: Uuid::nil(),
            canonical_name: "test-entity".into(),
            entity_type: EntityTypeName::new("tool"),
            original_name: "Test Entity".into(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl EntityRelatedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            source_entity_id: Uuid::nil(),
            target_entity_id: Uuid::nil(),
            relation_type: RelationType::new("co_occurs_with"),
            confidence: 1.0,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl EntityEnrichedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            entity_id: Uuid::nil(),
            canonical_name: "test-entity".into(),
            entity_type: EntityTypeName::new("tool"),
            refined_category: EntityCategory::Tool,
            first_seen: crate::temporal::now(),
            last_seen: crate::temporal::now(),
            occurrence_count: 1,
            active_hours: BTreeMap::new(),
        }
    }
}
