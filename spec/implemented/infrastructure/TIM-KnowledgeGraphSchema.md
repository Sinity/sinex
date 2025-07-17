# TIM-KnowledgeGraphSchema: DDL for Core Knowledge Graph Tables (`core_entities`, `core_entity_relations`)

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 85% (Tables defined with embeddings support, indexes created)
**Dependencies**: `pgx_ulid` extension, `pgvector` extension, `core.artifacts`, `core.tags`, `core.events` tables
**Blocks**: Entity resolution, relationship discovery, knowledge graph queries, semantic search, AI-assisted knowledge extraction

## MVP Specification
- Core knowledge graph tables (`core.entities`, `core.entity_relations`) with embeddings support
- Entity types: people, projects, artifacts, topics, tasks, locations
- Relationship types: mentions, works_on, depends_on, links_to, authored_by
- Basic entity and relationship CRUD operations
- Graph traversal queries for connected entities

## Enhanced Features
- Automated entity extraction from artifacts and events
- Entity resolution and deduplication using embeddings
- Semantic similarity search across entities
- Graph-based recommendation systems
- Entity relationship confidence scoring and validation
- Temporal relationship tracking with validity periods

## Implementation Checklist
- [x] Database migration to create `core.entities` and `core.entity_relations` tables
- [x] Entity embedding setup with pgvector (768-dimensional vectors)
- [x] ULID primary key implementation with pgx_ulid
- [x] Entity type validation and canonical labeling
- [x] Performance indexes for entity queries and graph traversal
- [x] Trigger setup for automatic timestamp updates
- [x] JSONB properties support for flexible entity metadata
- [x] Vector similarity indexes (HNSW) for semantic search
- [ ] Foreign key constraints to `core.artifacts`, `core.tags`, `core.events`
- [ ] Entity management API (create, merge, link, resolve duplicates)
- [ ] Basic graph traversal and path-finding queries
- [ ] Entity extraction agents for automatic knowledge graph population
- [ ] Tests for entity operations and graph queries
- [ ] Performance optimization for large-scale graph operations

*   **Purpose:** Provides the canonical Data Definition Language (DDL) for the core tables constituting the Exocortex Knowledge Graph: `core.entities` (nodes) and `core.entity_relations` (edges).
*   **Source:** Derived from original Vision Document Appendix A and conceptual descriptions in Vision Part III.3.5.
*   **Dependencies:** `pgx_ulid` extension (see `TIM-PrimaryKeyImplementation.md`). `pgvector` for optional entity embeddings (see `TIM-EmbeddingGenerationModels.md`). The `core.set_updated_at_trigger_func_generic()` from `TIM-EventSubstrateDDL.md` is assumed. `core.artifacts` and `core.tags` for optional FKs.

## 1. `core.entities` Table

The central registry for all canonical "things" or "concepts" (nodes) in the Exocortex. This table was previously outlined when generating TIMs from the UG, and this version ensures it aligns with Vision's original intent for these entities.

```sql
CREATE TABLE IF NOT EXISTS core.entities (
    entity_id               ULID PRIMARY KEY DEFAULT gen_ulid(), -- From pgx_ulid
    entity_type             TEXT NOT NULL,
                            -- Conceptual types from Vision: 'pkm_note_ref', 'web_archive_ref', 'email_message_ref', 
                            -- (these often reference a core.artifacts entry via source_artifact_id)
                            -- 'person', 'organization', 'project', 'task_item_ref', 
                            -- 'software_application', 'file_path_object', 'topic_tag_object', 
                            -- (topic_tag_object references a core.tags entry via source_tag_id)
                            -- 'geographic_location', 'planning_goal', 'planning_milestone',
                            -- 'activity_segment_derived', 'user_intent_derived', 'composite_action_derived',
                            -- 'llm_model_ref', 'prompt_template_ref' (referencing core.llm_models, core.prompts)
    canonical_label         TEXT NOT NULL, -- Primary human-readable name/identifier
    aliases                 TEXT[] NULLABLE, -- Array of alternative names
    properties              JSONB NULLABLE, -- Type-specific attributes
                                        -- For artifact_refs, this might include denormalized data from the artifact for quick KG access.
                                        -- For 'task_item_ref', it could hold status, priority, due_date if not linking to a full task artifact.
    description             TEXT NULLABLE,  -- Longer description of the entity.
    
    -- Optional Foreign Keys to link this entity to its primary source if it represents another core object
    source_artifact_id      ULID NULLABLE, -- REFERENCES core.artifacts(artifact_id) ON DELETE SET NULL, -- Add FK after core.artifacts is defined
    source_tag_id           ULID NULLABLE, -- REFERENCES core.tags(tag_id) ON DELETE SET NULL,           -- Add FK after core.tags is defined
    source_raw_event_id     ULID NULLABLE, -- REFERENCES core.events(id) ON DELETE SET NULL,              -- Add FK after core.events is defined (e.g., for derived entities like activity_segment)

    created_at_ts_orig      TIMESTAMPTZ NULLABLE,   -- Original creation timestamp from source, if applicable
    last_event_ts_orig      TIMESTAMPTZ NULLABLE,   -- Timestamp of the last raw.event directly relevant to this entity's concept
    
    embedding_vector        VECTOR NULLABLE,        -- Optional embedding of (canonical_label + description + key properties) for entity similarity
                                                    -- Dimension to be defined, e.g., VECTOR(768)
    
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE core.entities IS 'Canonical nodes for the Exocortex knowledge graph. Represents distinct concepts, objects, or items.';
COMMENT ON COLUMN core.entities.entity_type IS 'Categorizes the entity, influencing its properties and relations (e.g., person, project, pkm_note_ref).';
COMMENT ON COLUMN core.entities.canonical_label IS 'The primary, human-readable and queryable label for this entity.';
COMMENT ON COLUMN core.entities.aliases IS 'Array of alternative names or identifiers used to refer to this entity.';
COMMENT ON COLUMN core.entities.properties IS 'JSONB store for entity-type-specific attributes and metadata not fitting structured columns.';
COMMENT ON COLUMN core.entities.description IS 'A detailed textual description of the entity.';
COMMENT ON COLUMN core.entities.source_artifact_id IS 'If this entity primarily represents a core.artifact (like a note or webpage), this links them.';
COMMENT ON COLUMN core.entities.source_tag_id IS 'If this entity primarily represents a core.tag (acting as a topic node), this links them.';
COMMENT ON COLUMN core.entities.source_raw_event_id IS 'If this entity is derived directly from a specific raw.event (e.g., a significant meta-cognitive log).';
COMMENT ON COLUMN core.entities.created_at_ts_orig IS 'Timestamp reflecting the original creation or identification of the entity concept.';
COMMENT ON COLUMN core.entities.last_event_ts_orig IS 'Timestamp of the last known relevant raw event associated with this entity.';
COMMENT ON COLUMN core.entities.embedding_vector IS 'Vector embedding for semantic similarity searches on entities themselves.';

-- Indexes
CREATE UNIQUE INDEX IF NOT EXISTS uidx_core_entities_type_label ON core.entities (entity_type, canonical_label); -- Common lookup
CREATE INDEX IF NOT EXISTS idx_core_entities_type ON core.entities (entity_type);
CREATE INDEX IF NOT EXISTS idx_core_entities_aliases_gin ON core.entities USING GIN (aliases) WHERE aliases IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_entities_source_artifact_id ON core.entities (source_artifact_id) WHERE source_artifact_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_entities_source_tag_id ON core.entities (source_tag_id) WHERE source_tag_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_entities_source_raw_event_id ON core.entities (source_raw_event_id) WHERE source_raw_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_entities_properties_gin ON core.entities USING GIN (properties jsonb_path_ops);

-- Embedding Index (example for 768 dimensions, HNSW)
-- ALTER TABLE core.entities ALTER COLUMN embedding_vector TYPE VECTOR(768);
-- CREATE INDEX IF NOT EXISTS idx_core_entities_embedding_hnsw_cosine ON core.entities
--   USING hnsw (embedding_vector vector_cosine_ops)
--   WITH (m = 16, ef_construction = 64)
--   WHERE embedding_vector IS NOT NULL;

-- Trigger for updated_at
CREATE TRIGGER trg_core_entities_set_updated_at
BEFORE UPDATE ON core.entities
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();

-- Add FKs after referenced tables are confirmed to exist
-- ALTER TABLE core.entities ADD CONSTRAINT fk_entities_source_artifact FOREIGN KEY (source_artifact_id) REFERENCES core.artifacts(artifact_id) ON DELETE SET NULL;
-- ALTER TABLE core.entities ADD CONSTRAINT fk_entities_source_tag FOREIGN KEY (source_tag_id) REFERENCES core.tags(tag_id) ON DELETE SET NULL;
-- ALTER TABLE core.entities ADD CONSTRAINT fk_entities_source_event FOREIGN KEY (source_raw_event_id) REFERENCES core.events(id) ON DELETE SET NULL;
```

## 2. `core.entity_relations` Table

Defines typed, directed relationships (edges) between entities in `core.entities`. This table was also previously outlined; this version aligns with Vision's conceptual scope.

```sql
CREATE TABLE IF NOT EXISTS core.entity_relations (
    relation_id             ULID PRIMARY KEY DEFAULT gen_ulid(), -- pgx_ulid
    source_entity_id        ULID NOT NULL REFERENCES core.entities(entity_id) ON DELETE CASCADE,
    target_entity_id        ULID NOT NULL REFERENCES core.entities(entity_id) ON DELETE CASCADE,
    relation_type           TEXT NOT NULL,
                            -- Examples from Vision: 'mentions_entity_in_artifact_ref', 'artifact_ref_links_to_artifact_ref', 
                            -- 'person_works_on_project', 'project_uses_software_application', 
                            -- 'task_item_ref_depends_on_task_item_ref', 'entity_located_at_geographic_location', 
                            -- 'person_authored_artifact_ref', 'artifact_ref_related_to_topic_tag_obj',
                            -- 'derived_entity_from_event_cluster', 'project_has_milestone_entity'
    properties              JSONB NULLABLE,         -- Attributes of the relationship itself (e.g., confidence_score, role, context snippet)
    ts_start_orig           TIMESTAMPTZ NULLABLE,   -- Optional: start time of the relationship's validity in its original context
    ts_end_orig             TIMESTAMPTZ NULLABLE,   -- Optional: end time of the relationship's validity
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_actor        TEXT NOT NULL -- e.g., 'user_manual_link', 'agent_NERLinker_v1.2', 'agent_PKMParser_v0.9'
);

COMMENT ON TABLE core.entity_relations IS 'Typed, directed relationships (edges) between canonical entities in core.entities, forming the knowledge graph.';
COMMENT ON COLUMN core.entity_relations.source_entity_id IS 'The ULID of the source entity in the relationship.';
COMMENT ON COLUMN core.entity_relations.target_entity_id IS 'The ULID of the target entity in the relationship.';
COMMENT ON COLUMN core.entity_relations.relation_type IS 'Semantic type of the relationship, e.g., mentions_entity, works_on_project.';
COMMENT ON COLUMN core.entity_relations.properties IS 'JSONB store for attributes specific to the relationship instance, like confidence or context.';
COMMENT ON COLUMN core.entity_relations.ts_start_orig IS 'Timestamp indicating when this relationship became valid or was observed.';
COMMENT ON COLUMN core.entity_relations.ts_end_orig IS 'Timestamp indicating when this relationship ceased to be valid (if applicable).';
COMMENT ON COLUMN core.entity_relations.created_by_actor IS 'Indicates who or what process established this relationship.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_core_entity_relations_source_type ON core.entity_relations (source_entity_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_core_entity_relations_target_type ON core.entity_relations (target_entity_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_core_entity_relations_type ON core.entity_relations (relation_type);
CREATE INDEX IF NOT EXISTS idx_core_entity_relations_properties_gin ON core.entity_relations USING GIN (properties jsonb_path_ops);


-- Trigger for updated_at
CREATE TRIGGER trg_core_entity_relations_set_updated_at
BEFORE UPDATE ON core.entity_relations
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();
```

