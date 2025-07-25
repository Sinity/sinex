# TIM-LinkingTablesSchema: DDL for `event_relations` and `core_artifact_links`

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 90% (Tables defined, indexes created, triggers configured)
**Dependencies**: `pgx_ulid` extension, `core.events`, `core.artifacts`, `core.entities` tables
**Blocks**: Link resolution agents, relationship discovery, cross-reference queries

## MVP Specification
- Core linking tables (`event_relations`, `core.artifact_links`) with ULID primary keys
- Rich semantic relationship types between events, artifacts, and entities
- Content-based link parsing and resolution for artifacts
- Polymorphic foreign key support with application-level validation
- Performance indexes for bidirectional relationship queries

## Enhanced Features
- Automated link extraction from artifact content (Wikilinks, URLs)
- Link validation and resolution agents
- Relationship confidence scoring and provenance tracking
- Context-aware link suggestions and recommendations
- Graph-based navigation and discovery interfaces

## Implementation Checklist
- [x] Database migration to create `event_relations` table
- [x] Database migration to create `core.artifact_links` table
- [x] ULID primary key implementation with pgx_ulid
- [x] Polymorphic object reference columns with type validation
- [x] Performance indexes for relationship queries
- [x] Trigger setup for automatic timestamp updates
- [x] JSONB properties support for relationship metadata
- [ ] Foreign key constraints (pending referenced table creation)
- [ ] Link extraction agents for automatic population
- [ ] Link resolution and validation workflows
- [ ] Tests for relationship operations and queries
- [ ] Performance optimization for large-scale link traversal

*   **Purpose:** Provides the canonical Data Definition Language (DDL) for tables that explicitly store links or relationships between Exocortex objects: `event_relations` (for semantic links involving `core.events`) and `core_artifact_links` (primarily for links parsed from the content of `core.artifacts` like PKM notes).
*   **Source:** Derived from original Vision Document Appendix A and conceptual descriptions in Vision Part V.3.1.
*   **Dependencies:** `pgx_ulid` extension. Relies on `core.events`, `core.artifacts`, `core.entities` tables being defined. The `core.set_updated_at_trigger_func_generic()` from `TIM-EventSubstrateDDL.md` is assumed.

## 1. `event_relations` Table

Stores rich, typed, semantic links where at least one participant is typically a `core.events` entry. It can also link events to artifacts or entities, or artifacts to other artifacts/entities if the relation is primarily event-driven or contextually derived rather than embedded in content.

```sql
CREATE TABLE IF NOT EXISTS event_relations (
    relation_id             ULID PRIMARY KEY DEFAULT gen_ulid(), -- pgx_ulid
    
    from_object_id          ULID NOT NULL,
    from_object_type        TEXT NOT NULL, -- e.g., 'raw_event', 'core_artifact', 'core_entity'
    
    to_object_id            ULID NOT NULL,
    to_object_type          TEXT NOT NULL, -- e.g., 'raw_event', 'core_artifact', 'core_entity'
    
    relation_type           TEXT NOT NULL, 
                            -- Examples from Vision: 'derives_from_event', 'explains_context_of_event', 
                            -- 'triggered_by_event', 'event_mentions_entity', 
                            -- 'event_related_to_artifact', 'resolves_friction_event',
                            -- 'composite_action_constituent_event', 'composite_action_trigger_event'
                            -- Generic: 'related_to', 'caused_by', 'led_to'
    description             TEXT NULLABLE,    -- Human-readable description or annotation for this specific relation instance.
    properties              JSONB NULLABLE,   -- Attributes of the relationship itself (e.g., {"confidence_score": 0.85, "context_snippet": "...", "relevance_score": 0.7})
    
    created_by_actor        TEXT NOT NULL,    -- 'user_manual_link_creation', 'agent_ContextLinker_v1.1', 'agent_CompositeActionIdentifier_v0.1'
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_event_relations_from_type CHECK (from_object_type IN ('raw_event', 'core_artifact', 'core_entity')),
    CONSTRAINT chk_event_relations_to_type CHECK (to_object_type IN ('raw_event', 'core_artifact', 'core_entity')),
    -- To ensure the relation is not to itself (usually desired)
    CONSTRAINT chk_event_relations_not_self_referential CHECK (NOT (from_object_id = to_object_id AND from_object_type = to_object_type)) 
);

COMMENT ON TABLE event_relations IS 'Stores explicit semantic links between raw events and/or other core Exocortex objects (artifacts, entities). Useful for tracing causality, derivation, or contextual grouping not inherent in content links.';
COMMENT ON COLUMN event_relations.from_object_id IS 'ULID of the source object in the relation.';
COMMENT ON COLUMN event_relations.from_object_type IS 'Type of the source object (e.g., ''raw_event'', ''core_artifact'').';
COMMENT ON COLUMN event_relations.to_object_id IS 'ULID of the target object in the relation.';
COMMENT ON COLUMN event_relations.to_object_type IS 'Type of the target object (e.g., ''core_entity'').';
COMMENT ON COLUMN event_relations.relation_type IS 'Semantic type of the relationship between the objects, defining its meaning.';
COMMENT ON COLUMN event_relations.description IS 'Optional human-readable note about this specific link.';
COMMENT ON COLUMN event_relations.properties IS 'JSONB store for attributes specific to this relationship instance, like confidence scores or extracted context.';
COMMENT ON COLUMN event_relations.created_by_actor IS 'Identifier for the user or agent process that established this relation.';

-- Indexes for querying by either side of the relation
CREATE INDEX IF NOT EXISTS idx_event_relations_from_object_type_rel ON event_relations (from_object_id, from_object_type, relation_type);
CREATE INDEX IF NOT EXISTS idx_event_relations_to_object_type_rel ON event_relations (to_object_id, to_object_type, relation_type);
-- Index for finding relations of a specific type
CREATE INDEX IF NOT EXISTS idx_event_relations_relation_type ON event_relations (relation_type);

-- Trigger for updated_at
CREATE TRIGGER trg_event_relations_set_updated_at
BEFORE UPDATE ON event_relations
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();
```
*Note on Polymorphic Foreign Keys for `event_relations`: Similar to `artifact_tags`, enforcing strict database FKs for `from_object_id` and `to_object_id` based on their respective `_type` columns is complex with standard SQL FKs. This typically requires application-level validation or more elaborate DB structures (like separate link tables per combination or table inheritance), which can reduce query flexibility. The current design favors flexibility.*

## 2. `core_artifact_links` Table

Primarily for storing links *parsed directly from the content* of `core.artifacts` (e.g., Wikilinks `[[target]]` or Markdown URLs `[text](url)` in PKM notes, or hyperlinks extracted from web page archives).

```sql
CREATE TABLE IF NOT EXISTS core.artifact_links (
    link_id                     ULID PRIMARY KEY DEFAULT gen_ulid(), -- pgx_ulid
    source_artifact_id          ULID NOT NULL, -- REFERENCES core.artifacts(artifact_id) ON DELETE CASCADE, (Add FK later)
    source_content_id           ULID NULLABLE, -- REFERENCES core.artifact_contents(content_id) ON DELETE SET NULL, (Add FK later)
                                               -- Specific version of content containing the link, if known and relevant for historical link context.
    
    target_identifier_text      TEXT NOT NULL, -- The raw link target string as it appears in the source content (e.g., "Note Title", "http://example.com", "Another Artifact#heading-id", "ULID_xyz")
    target_identifier_type      TEXT NOT NULL DEFAULT 'unknown', 
                                -- Helps parser/resolver: 'wikilink_title', 'wikilink_path_stub', 'wikilink_ulid', 
                                -- 'url_http_https', 'url_file', 'url_exocortex_internal_uri', 'heading_anchor_in_source', 'heading_anchor_external'
    
    resolved_target_artifact_id ULID NULLABLE, -- REFERENCES core.artifacts(artifact_id) ON DELETE SET NULL, (Add FK later)
                                               -- If link successfully resolves to another Exocortex artifact.
    resolved_target_entity_id   ULID NULLABLE, -- REFERENCES core.entities(entity_id) ON DELETE SET NULL, (Add FK later)    
                                               -- If link successfully resolves to a core.entity (e.g., a person, project).
    resolved_target_external_url TEXT NULLABLE, -- If link is an external URL, this is its normalized/final form after redirects if followed.

    link_type                   TEXT NOT NULL DEFAULT 'explicit_reference', 
                                -- e.g., 'explicit_reference' (standard link), 'embed' (transclusion of content), 
                                -- 'footnote_definition_link', 'image_source_link'
    link_text_display           TEXT NULLABLE,    -- The anchor text or display text of the link (e.g., "this important [paper](url)").
    context_before_link         TEXT NULLABLE,    -- Optional: Text immediately preceding the link in source.
    context_after_link          TEXT NULLABLE,    -- Optional: Text immediately following the link in source.
    properties                  JSONB NULLABLE,   -- e.g., {"link_is_broken_as_of_last_check": true, "link_relation_from_markdown_attrs": "see_also"}
    
    first_parsed_at_ts_orig     TIMESTAMPTZ NOT NULL, -- Timestamp when this link was first parsed from the source_artifact_id's content.
    last_validated_at_ts        TIMESTAMPTZ NULLABLE, -- Timestamp when the link resolution (to artifact/entity/URL) was last attempted/checked by an agent.
    
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Ensure that for a given source artifact/content, the same link target isn't duplicated if its context is identical
    CONSTRAINT uq_artifact_links_source_target_context UNIQUE (source_artifact_id, source_content_id, target_identifier_text, link_text_display)
);

COMMENT ON TABLE core.artifact_links IS 'Stores links parsed from the content of core.artifacts (e.g., Wikilinks from PKM notes, URLs from web archives). Facilitates link resolution and graph building.';
COMMENT ON COLUMN core.artifact_links.source_artifact_id IS 'The artifact that contains this link in its content.';
COMMENT ON COLUMN core.artifact_links.source_content_id IS 'The specific content version of the source artifact where this link was identified.';
COMMENT ON COLUMN core.artifact_links.target_identifier_text IS 'The raw text of the link target as it appears in the source content (e.g., a [[wikilink target]], a URL).';
COMMENT ON COLUMN core.artifact_links.target_identifier_type IS 'Categorization of the target_identifier_text to aid parsing and resolution (e.g., wikilink_title, url_http).';
COMMENT ON COLUMN core.artifact_links.resolved_target_artifact_id IS 'If the link target was successfully resolved to another Exocortex artifact, its ULID.';
COMMENT ON COLUMN core.artifact_links.resolved_target_entity_id IS 'If the link target was successfully resolved to a core.entity, its ULID.';
COMMENT ON COLUMN core.artifact_links.resolved_target_external_url IS 'If the link is an external URL, its normalized or final effective URL.';
COMMENT ON COLUMN core.artifact_links.link_type IS 'Semantic type of the link as used in the content (e.g., explicit_reference, embed).';
COMMENT ON COLUMN core.artifact_links.link_text_display IS 'The display text (anchor text) of the link, if any.';
COMMENT ON COLUMN core.artifact_links.context_before_link IS 'A snippet of text immediately preceding the link in the source content.';
COMMENT ON COLUMN core.artifact_links.context_after_link IS 'A snippet of text immediately following the link in the source content.';
COMMENT ON COLUMN core.artifact_links.properties IS 'JSONB store for additional metadata about the link, like validation status or custom attributes.';
COMMENT ON COLUMN core.artifact_links.first_parsed_at_ts_orig IS 'Timestamp when this link was initially parsed from the source artifact.';
COMMENT ON COLUMN core.artifact_links.last_validated_at_ts IS 'Timestamp when the resolution of this link was last checked.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_core_artifact_links_source_artifact_id ON core.artifact_links (source_artifact_id);
CREATE INDEX IF NOT EXISTS idx_core_artifact_links_source_content_id ON core.artifact_links (source_content_id) WHERE source_content_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_artifact_links_resolved_target_artifact_id ON core.artifact_links (resolved_target_artifact_id) WHERE resolved_target_artifact_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_artifact_links_resolved_target_entity_id ON core.artifact_links (resolved_target_entity_id) WHERE resolved_target_entity_id IS NOT NULL;
-- For finding all links pointing to a specific (unresolved) target string
CREATE INDEX IF NOT EXISTS idx_core_artifact_links_target_identifier_text_trgm ON core.artifact_links USING GIN (target_identifier_text gin_trgm_ops);
-- For finding links by type
CREATE INDEX IF NOT EXISTS idx_core_artifact_links_link_type ON core.artifact_links (link_type);


-- Trigger for updated_at
CREATE TRIGGER trg_core_artifact_links_set_updated_at
BEFORE UPDATE ON core.artifact_links
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();

-- Add FKs after referenced tables core.artifacts, core.artifact_contents, core.entities are defined
-- ALTER TABLE core.artifact_links ADD CONSTRAINT fk_artifact_links_source_artifact FOREIGN KEY (source_artifact_id) REFERENCES core.artifacts(artifact_id) ON DELETE CASCADE;
-- ALTER TABLE core.artifact_links ADD CONSTRAINT fk_artifact_links_source_content FOREIGN KEY (source_content_id) REFERENCES core.artifact_contents(content_id) ON DELETE SET NULL;
-- ALTER TABLE core.artifact_links ADD CONSTRAINT fk_artifact_links_resolved_artifact FOREIGN KEY (resolved_target_artifact_id) REFERENCES core.artifacts(artifact_id) ON DELETE SET NULL;
-- ALTER TABLE core.artifact_links ADD CONSTRAINT fk_artifact_links_resolved_entity FOREIGN KEY (resolved_target_entity_id) REFERENCES core.entities(entity_id) ON DELETE SET NULL;
```
*An agent (`agent/LinkExtractorAndResolver`) would be responsible for parsing `core.artifact_contents` to populate `core.artifact_links`, and then attempting to resolve the `target_identifier_text` to populate the `resolved_*` fields and `last_validated_at_ts`.*

