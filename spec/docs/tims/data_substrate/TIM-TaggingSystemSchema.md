# TIM-TaggingSystemSchema: DDL for Universal Tagging System (`core.tags`, `artifact_tags`)

*   **Purpose:** Provides the canonical Data Definition Language (DDL) for the Exocortex universal tagging system, comprising `core.tags` for tag definitions and `artifact_tags` for linking tags to various Exocortex objects.
*   **Source:** Derived from original Vision Document Appendix A and conceptual descriptions in Vision Part II.2.4.
*   **Dependencies:** `pgx_ulid` extension. The `core.set_updated_at_trigger_func_generic()` from `TIM-EventSubstrateDDL.md` is assumed.

## 1. `core.tags` Table

Defines canonical tags, supporting hierarchy and aliases. This table was also previously outlined; this version aligns with Vision's original intent.

```sql
CREATE TABLE IF NOT EXISTS core.tags (
    tag_id                  ULID PRIMARY KEY DEFAULT gen_ulid(), -- pgx_ulid
    tag_name                TEXT UNIQUE NOT NULL, 
                            -- Canonical tag name, e.g., "project.exocortex.docs", "status.review", "topic.ai.llm"
                            -- Convention: dot-separated for implied hierarchy, but the parent_tag_id defines formal hierarchy.
                            -- Tag names should be normalized (e.g., lowercase, dashes instead of spaces).
    description             TEXT NULLABLE, -- A detailed description of what this tag represents or when it should be used.
    parent_tag_id           ULID NULLABLE REFERENCES core.tags(tag_id) ON DELETE SET NULL, -- For establishing explicit hierarchical relationships (e.g., "llm" is child of "ai").
    aliases                 TEXT[] NULLABLE, -- Array of alternative names or synonyms for this tag (e.g., "AI" for "artificial_intelligence").
    properties              JSONB NULLABLE,  -- e.g., {"color_hex": "#FF0000", "icon_identifier": "flag_icon", "is_status_tag": true} for UI hints or special behaviors.
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE core.tags IS 'Canonical definitions for all tags used in the system. Tags can be hierarchical and have aliases.';
COMMENT ON COLUMN core.tags.tag_name IS 'The unique, canonical, and normalized name of the tag (e.g., project.research.topic).';
COMMENT ON COLUMN core.tags.description IS 'Detailed explanation of the tag''s meaning and usage guidelines.';
COMMENT ON COLUMN core.tags.parent_tag_id IS 'If this tag is part of a formal hierarchy, this points to its parent tag_id.';
COMMENT ON COLUMN core.tags.aliases IS 'Array of alternative names or synonyms by which this tag might be referred.';
COMMENT ON COLUMN core.tags.properties IS 'JSONB store for additional metadata about the tag, like UI hints or behavioral flags.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_core_tags_name_fts ON core.tags USING GIN (to_tsvector('english', tag_name || ' ' || coalesce(description, ''))); -- Search name and desc
CREATE INDEX IF NOT EXISTS idx_core_tags_parent_id ON core.tags (parent_tag_id) WHERE parent_tag_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_tags_aliases_gin ON core.tags USING GIN (aliases) WHERE aliases IS NOT NULL;

-- Trigger for updated_at
CREATE TRIGGER trg_core_tags_set_updated_at
BEFORE UPDATE ON core.tags
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();
```

## 2. `artifact_tags` Table (Polymorphic Join Table)

Links tags from `core.tags` to various Exocortex objects (artifacts, events, blobs, entities). The name `artifact_tags` is historical but its scope is broader.

```sql
CREATE TABLE IF NOT EXISTS artifact_tags (
    target_object_id        ULID NOT NULL, -- ULID of the tagged item
    target_object_type      TEXT NOT NULL, -- Enum-like: 'core_artifact', 'raw_event', 'core_blob', 'core_entity', 'event_annotation'
    tag_id                  ULID NOT NULL REFERENCES core.tags(tag_id) ON DELETE CASCADE,
    
    assigned_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    assigner_actor          TEXT NOT NULL, -- e.g., 'user_manual_neovim', 'agent_AutoTagger_v1.2', 'rule_based_ingestion_pipeline_X'
    confidence_score        FLOAT NULLABLE, -- If assigned by an agent/automated process (range 0.0 to 1.0)
    assignment_context      JSONB NULLABLE, -- Optional: e.g., {"source_text_snippet": "...", "rule_id_triggered": "X", "llm_prompt_id_used": "ULID"}
    
    PRIMARY KEY (target_object_id, target_object_type, tag_id) -- Ensures a tag is applied only once to a specific object instance
);
COMMENT ON TABLE artifact_tags IS 'Many-to-many join table linking tags from core.tags to various Exocortex objects (artifacts, events, blobs, entities, etc.).';
COMMENT ON COLUMN artifact_tags.target_object_id IS 'ULID of the object being tagged.';
COMMENT ON COLUMN artifact_tags.target_object_type IS 'Type of the object being tagged (e.g., ''core_artifact'', ''raw_event''). This defines which table target_object_id refers to.';
COMMENT ON COLUMN artifact_tags.tag_id IS 'FK to the core.tag being applied.';
COMMENT ON COLUMN artifact_tags.assigner_actor IS 'Identifier for the user, agent, or process that assigned this tag.';
COMMENT ON COLUMN artifact_tags.confidence_score IS 'Confidence of an automated tagging process in this assignment.';
COMMENT ON COLUMN artifact_tags.assignment_context IS 'JSONB store for additional context about why or how this tag was assigned.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_artifact_tags_tag_id_object_type ON artifact_tags (tag_id, target_object_type);
CREATE INDEX IF NOT EXISTS idx_artifact_tags_target_object_id_type_tag ON artifact_tags (target_object_id, target_object_type, tag_id); -- Covers PK lookups and some queries
CREATE INDEX IF NOT EXISTS idx_artifact_tags_assigner_actor ON artifact_tags (assigner_actor);
```
*Note on Polymorphism and FKs for `artifact_tags`: As stated before, true database-level FKs from `target_object_id` to the specific tables based on `target_object_type` are not straightforward with this polymorphic design. Integrity is typically enforced at the application layer or through periodic checks. For stricter integrity at the DB level, separate join tables for each taggable object type (e.g., `core_artifact_to_core_tags`, `raw_event_to_core_tags`) would be needed, but this reduces flexibility in querying "all objects with tag X."*

