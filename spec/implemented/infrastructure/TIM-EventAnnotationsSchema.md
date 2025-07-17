# TIM-EventAnnotationsSchema: DDL for `event_annotations` Table

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 90% (Full database schema, indexes, vector support, triggers - missing Rust models and CRUD API layer)
**Dependencies**: `pgx_ulid` extension, `pgvector` extension, `core.events` table
**Blocks**: Annotation agents, event interpretation workflows, collaborative tagging

## MVP Specification
- Core event annotations table with ULID primary keys
- Support for both textual and structured (JSONB) annotations
- Actor identification for annotation provenance
- Annotation type categorization system
- Vector embeddings for semantic search across annotations

## Enhanced Features
- Automated annotation generation by AI agents
- Collaborative annotation workflows with conflict resolution
- Annotation confidence scoring and validation
- Full-text search across annotation content
- Temporal annotation tracking and versioning

## Implementation Checklist
- [x] Database migration to create `event_annotations` table - `migrations/20250103120013_create_event_relations_and_annotations.sql`
- [x] ULID primary key implementation with pgx_ulid - `gen_ulid()` default
- [x] Foreign key constraint to `core.events` table - CASCADE DELETE implemented
- [x] Support for textual content and JSONB metadata - `content TEXT` + `metadata JSONB`
- [x] Actor identification and annotation type classification - `created_by` + `annotation_type` fields
- [x] Vector embedding support with pgvector integration - `migrations/20250103120012_create_llm_and_embeddings_tables.sql`
- [x] Performance indexes for annotation queries - event_id, type, created_at indexes
- [x] Full-text search indexes for textual content - GIN tsvector index
- [x] Vector similarity indexes (IVFFLAT) for semantic search - Separate event_embeddings table
- [x] Trigger setup for automatic timestamp updates - `set_event_annotations_updated_at` trigger
- [ ] Rust models for event annotations in sinex-db
- [ ] Annotation management API and validation logic
- [ ] Automated annotation agents and workflows
- [ ] Tests for annotation operations and searches

*   **Purpose:** Provides the canonical Data Definition Language (DDL) for the `event_annotations` table. This table allows users and agents to attach flexible, evolving metadata, comments, flags, or preliminary interpretations directly to individual `core.events` entries without altering the immutable event itself.
*   **Source:** Derived from conceptual descriptions in Vision Document Part V.3.3.
*   **Dependencies:** `pgx_ulid` extension. Relies on `core.events` table being defined. `pgvector` for optional annotation embeddings. The `core.set_updated_at_trigger_func_generic()` from `TIM-EventSubstrateDDL.md` is assumed.

## 1. `event_annotations` Table

Stores annotations linked to specific `core.events`.

```sql
CREATE TABLE IF NOT EXISTS event_annotations (
    annotation_id           ULID PRIMARY KEY DEFAULT gen_ulid(), -- pgx_ulid
    target_event_id         ULID NOT NULL REFERENCES core.events(id) ON DELETE CASCADE, -- The event being annotated
    
    annotator_actor         TEXT NOT NULL, 
                            -- Identifier for who/what created the annotation.
                            -- e.g., 'user_sinex_manual_review', 'agent_PatternDetector_v0.2', 
                            -- 'llm_gpt4_summary_promptX', 'feedback_loop_correction_v1'
    annotation_type         TEXT NOT NULL, 
                            -- Categorizes the annotation's purpose or nature.
                            -- e.g., 'user_comment', 'agent_flag_for_review', 'llm_generated_summary_chunk', 
                            -- 'correction_proposal', 'preliminary_tag_suggestion', 
                            -- 'inferred_relation_proposal', 'debug_note', 'importance_score_heuristic'
    
    content_text            TEXT NULLABLE,          -- For free-form textual annotations (comments, summaries, notes).
    content_jsonb           JSONB NULLABLE,         -- For structured annotations, e.g.:
                                                    -- {"tags_proposed": ["#project_alpha", "#bug"], "certainty": 0.7}
                                                    -- {"suggested_link": {"to_object_id": "ULID_artifact", "to_object_type": "core_artifact", "relation_type": "relevant_to"}}
                                                    -- {"importance_score": 0.85, "reason": "Contains error keywords"}
    
    -- Optional: embedding of the annotation's content_text for similarity searches *on annotations themselves*
    embedding_vector        VECTOR NULLABLE,        -- Dimension depends on model used for annotation embeddings, e.g. VECTOR(384)

    ts_created              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(), -- If annotations themselves can be edited

    CONSTRAINT chk_event_annotation_content_defined CHECK (content_text IS NOT NULL OR content_jsonb IS NOT NULL)
);

COMMENT ON TABLE event_annotations IS 'Stores user and agent annotations layered on top of core.events entries, providing a flexible way to add evolving insights or operational metadata.';
COMMENT ON COLUMN event_annotations.target_event_id IS 'The raw.event ULID that this annotation pertains to.';
COMMENT ON COLUMN event_annotations.annotator_actor IS 'Identifier for the user, agent, or specific process that created the annotation.';
COMMENT ON COLUMN event_annotations.annotation_type IS 'Categorizes the type/purpose of the annotation (e.g., user_comment, agent_flag_for_review).';
COMMENT ON COLUMN event_annotations.content_text IS 'Free-form textual content of the annotation (e.g., a user''s note about an event).';
COMMENT ON COLUMN event_annotations.content_jsonb IS 'Structured (JSONB) content of the annotation, for more complex metadata or proposals.';
COMMENT ON COLUMN event_annotations.embedding_vector IS 'Optional vector embedding of the annotation''s textual content for similarity search on annotations.';
COMMENT ON COLUMN event_annotations.updated_at IS 'Timestamp of the last modification to this annotation (if editable).';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_event_annotations_target_event_id ON event_annotations (target_event_id); -- Main lookup
CREATE INDEX IF NOT EXISTS idx_event_annotations_actor_type ON event_annotations (annotator_actor, annotation_type); -- Find annotations by source/type
CREATE INDEX IF NOT EXISTS idx_event_annotations_type ON event_annotations (annotation_type);
-- Full-text search on textual annotations
CREATE INDEX IF NOT EXISTS idx_event_annotations_content_text_fts ON event_annotations USING GIN (to_tsvector('english', content_text)) WHERE content_text IS NOT NULL;
-- GIN index for querying structured JSONB annotations
CREATE INDEX IF NOT EXISTS idx_event_annotations_content_jsonb_gin ON event_annotations USING GIN (content_jsonb jsonb_path_ops) WHERE content_jsonb IS NOT NULL;

-- For pgvector (HNSW as per ADR-005) on annotation embeddings
-- Example assuming 384 dimensions:
-- ALTER TABLE event_annotations ALTER COLUMN embedding_vector TYPE VECTOR(384);
-- CREATE INDEX IF NOT EXISTS idx_event_annotations_embedding_hnsw_cosine ON event_annotations
--   USING hnsw (embedding_vector vector_cosine_ops)
--   WITH (m = 16, ef_construction = 64)
--   WHERE embedding_vector IS NOT NULL;

-- Trigger for updated_at
CREATE TRIGGER trg_event_annotations_set_updated_at
BEFORE UPDATE ON event_annotations
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();
```

