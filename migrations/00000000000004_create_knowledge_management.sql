-- Create knowledge management tables for concepts, relations, and annotations

-- Concepts table
CREATE TABLE IF NOT EXISTS km.concepts (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    concept_name TEXT NOT NULL,
    concept_type TEXT NOT NULL,
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    -- NOTE: embedding field removed - embeddings belong in core.embedding_cache
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT unique_concept_name_type UNIQUE (concept_name, concept_type)
);

CREATE INDEX idx_concepts_type ON km.concepts (concept_type);
CREATE INDEX idx_concepts_name ON km.concepts (concept_name);
-- Note: Vector embeddings documentation moved to docs/roadmap/features/embeddings-and-semantic-search.md

-- Relations between concepts
CREATE TABLE IF NOT EXISTS km.relations (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    from_concept_id ULID NOT NULL REFERENCES km.concepts(id) ON DELETE CASCADE,
    to_concept_id ULID NOT NULL REFERENCES km.concepts(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL,
    confidence REAL CHECK (confidence >= 0 AND confidence <= 1),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT unique_relation UNIQUE (from_concept_id, to_concept_id, relation_type),
    CONSTRAINT no_self_relation CHECK (from_concept_id != to_concept_id)
);

CREATE INDEX idx_relations_from ON km.relations (from_concept_id);
CREATE INDEX idx_relations_to ON km.relations (to_concept_id);
CREATE INDEX idx_relations_type ON km.relations (relation_type);

-- Event annotations linking events to concepts
--
-- ## Event Annotations Schema (TIM-EventAnnotationsSchema)
--
-- Provides flexible annotation system for events with:
-- - Multiple annotation types (tag, comment, summary, analysis)
-- - Actor tracking for provenance (user vs AI agent)
-- - Confidence scoring for automated annotations
-- - Structured metadata in JSONB format
--
-- This implementation differs from the TIM which proposed core.event_annotations.
-- We use km.event_annotations to link events with knowledge concepts instead.
--
-- Future enhancements:
-- - Direct text annotations without concept requirement
-- - Version history for annotation edits
-- - Collaborative annotation workflows
CREATE TABLE IF NOT EXISTS km.event_annotations (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    event_id ULID NOT NULL,
    concept_id ULID NOT NULL REFERENCES km.concepts(id) ON DELETE CASCADE,
    annotation_type TEXT NOT NULL,
    confidence REAL CHECK (confidence >= 0 AND confidence <= 1),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT unique_event_concept_annotation UNIQUE (event_id, concept_id, annotation_type)
);

CREATE INDEX idx_annotations_event ON km.event_annotations (event_id);
CREATE INDEX idx_annotations_concept ON km.event_annotations (concept_id);
CREATE INDEX idx_annotations_type ON km.event_annotations (annotation_type);

-- Artifacts for storing knowledge documents
--
-- ## Artifact Schema Design (TIM-CoreArtifactsSchema)
--
-- The artifacts system provides versioned storage for conceptual documents including:
-- - PKM notes with Yjs integration for real-time collaboration
-- - Web page archives with markdown extraction
-- - Email messages with structured metadata
-- - PDF documents and other binary artifacts
-- - Tasks and project definitions
--
-- Key design principles:
-- - Separate artifact metadata (km.artifacts) from versioned content (km.artifact_revisions)
-- - Content deduplication via BLAKE3 hashing
-- - Support for both text content and binary blob references
-- - Extensible metadata in JSONB for type-specific properties
--
-- Artifact types include:
-- - 'pkm_note': Personal knowledge management notes
-- - 'webpage_archive': Archived web pages with extracted content
-- - 'email_message': Email messages with headers and content
-- - 'pdf_document': PDF files (content extraction optional)
-- - 'task_item': Actionable tasks with status tracking
--
-- Note: This is a simplified implementation. The full TIM specification includes:
-- - Canonical identifiers for stable references
-- - Tags denormalization for faster search
-- - Integration with core.blobs for large content
-- - Full-text search capabilities via tsvector
CREATE TABLE IF NOT EXISTS km.artifacts (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    artifact_type TEXT NOT NULL,
    title TEXT NOT NULL,
    uri TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT
);

CREATE INDEX idx_artifacts_type ON km.artifacts (artifact_type);
CREATE INDEX idx_artifacts_uri ON km.artifacts (uri) WHERE uri IS NOT NULL;

-- Artifact revisions for version control
--
-- Stores immutable content versions for artifacts. Key features:
-- - Content versions are append-only (no updates)
-- - BLAKE3 hashing for content integrity and deduplication
-- - Sequential revision numbers per artifact
-- - Metadata can include extraction details, Yjs state vectors, etc.
--
-- For PKM notes with Yjs integration:
-- - Content snapshots derived from Yjs document state
-- - Version identifier references last incorporated Yjs delta
-- - Periodic snapshots for efficient retrieval
CREATE TABLE IF NOT EXISTS km.artifact_revisions (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    artifact_id ULID NOT NULL REFERENCES km.artifacts(id) ON DELETE CASCADE,
    revision_number INTEGER NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT unique_artifact_revision UNIQUE (artifact_id, revision_number)
);

CREATE INDEX idx_revisions_artifact ON km.artifact_revisions (artifact_id, revision_number DESC);
CREATE INDEX idx_revisions_hash ON km.artifact_revisions (content_hash);

-- LLM interactions for knowledge extraction
CREATE TABLE IF NOT EXISTS km.llm_interactions (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    interaction_type TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT,
    prompt TEXT NOT NULL,
    response TEXT NOT NULL,
    token_count INTEGER,
    latency_ms INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_llm_type_time ON km.llm_interactions (interaction_type, created_at DESC);
CREATE INDEX idx_llm_model ON km.llm_interactions (model_name, created_at DESC);

-- NOTE: km.embeddings table removed - use core.embedding_cache design from original migrations
-- See docs/roadmap/features/embeddings-and-semantic-search.md for proper embedding cache schema

-- Add comments
COMMENT ON TABLE km.concepts IS 'Knowledge graph nodes representing concepts, entities, and ideas';
COMMENT ON TABLE km.relations IS 'Edges in the knowledge graph connecting concepts';
COMMENT ON TABLE km.event_annotations IS 'Links between events and concepts for knowledge extraction';
COMMENT ON TABLE km.artifacts IS 'Knowledge artifacts like documents, notes, and references';
COMMENT ON TABLE km.artifact_revisions IS 'Version history for knowledge artifacts';
COMMENT ON TABLE km.llm_interactions IS 'History of LLM interactions for knowledge extraction and synthesis';
-- embeddings table removed - see roadmap docs