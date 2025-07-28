-- ============================================================================
-- Create Core Artifact Tables
-- ============================================================================
--
-- This migration creates the core.artifacts tables that the code expects,
-- with all features from the original design.
--

-- ============================================================================
-- Core Artifacts Table
-- ============================================================================
--
-- Central registry of all knowledge artifacts in the system. Each artifact
-- represents a conceptual "thing" that has meaning to the user.
--
CREATE TABLE IF NOT EXISTS core.artifacts (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    type TEXT NOT NULL CHECK (type IN ('note', 'webpage', 'email', 'file', 'document', 'code', 'media', 'pkm_note', 'task_item')),
    title TEXT NOT NULL,
    source_url TEXT,
    original_path TEXT,
    mime_type TEXT,
    size_bytes BIGINT,
    checksum TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ, -- Soft delete support
    created_from_event_id ULID REFERENCES core.events(event_id),
    blob_id ULID REFERENCES core.blobs(id)
);

CREATE INDEX idx_core_artifacts_type ON core.artifacts(type);
CREATE INDEX idx_core_artifacts_created_at ON core.artifacts(created_at);
CREATE INDEX idx_core_artifacts_updated_at ON core.artifacts(updated_at);
CREATE INDEX idx_core_artifacts_metadata ON core.artifacts USING gin(metadata);
CREATE INDEX idx_core_artifacts_deleted_at ON core.artifacts(deleted_at) WHERE deleted_at IS NULL;
CREATE INDEX idx_core_artifacts_blob_id ON core.artifacts(blob_id) WHERE blob_id IS NOT NULL;

-- ============================================================================
-- Artifact Contents
-- ============================================================================
--
-- Versioned content storage for artifacts with full history tracking.
--
CREATE TABLE IF NOT EXISTS core.artifact_contents (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    artifact_id ULID NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    version INTEGER NOT NULL DEFAULT 1,
    content TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'text/plain',
    extracted_text TEXT,
    word_count INTEGER,
    char_count INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_from_event_id ULID REFERENCES core.events(event_id),
    UNIQUE(artifact_id, version)
);

CREATE INDEX idx_artifact_contents_artifact_id ON core.artifact_contents(artifact_id);
CREATE INDEX idx_artifact_contents_content_search ON core.artifact_contents USING gin(to_tsvector('english', content));
CREATE INDEX idx_artifact_contents_extracted_search ON core.artifact_contents USING gin(to_tsvector('english', extracted_text)) WHERE extracted_text IS NOT NULL;

-- ============================================================================
-- Artifact Tags (many-to-many relationship)
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.artifact_tags (
    artifact_id ULID NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    tag_id ULID NOT NULL REFERENCES core.tags(id) ON DELETE CASCADE,
    tagged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tagged_from_event_id ULID REFERENCES core.events(event_id),
    PRIMARY KEY (artifact_id, tag_id)
);

CREATE INDEX idx_artifact_tags_tag ON core.artifact_tags(tag_id);

-- ============================================================================
-- Artifact Relations
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.artifact_relations (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    from_artifact_id ULID NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    to_artifact_id ULID NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT no_self_artifact_relations CHECK (from_artifact_id != to_artifact_id),
    CONSTRAINT unique_artifact_relation UNIQUE(from_artifact_id, to_artifact_id, relation_type)
);

CREATE INDEX idx_artifact_relations_from ON core.artifact_relations(from_artifact_id);
CREATE INDEX idx_artifact_relations_to ON core.artifact_relations(to_artifact_id);
CREATE INDEX idx_artifact_relations_type ON core.artifact_relations(relation_type);

-- ============================================================================
-- Cross-references between artifacts and events
-- ============================================================================

-- Artifacts derived from events
CREATE TABLE IF NOT EXISTS core.artifact_event_sources (
    artifact_id ULID NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    event_id ULID NOT NULL REFERENCES core.events(event_id) ON DELETE CASCADE,
    derivation_type TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (artifact_id, event_id)
);

CREATE INDEX idx_artifact_event_sources_event ON core.artifact_event_sources(event_id);

-- Events that reference artifacts
CREATE TABLE IF NOT EXISTS core.event_artifact_refs (
    event_id ULID NOT NULL REFERENCES core.events(event_id) ON DELETE CASCADE,
    artifact_id ULID NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    reference_type TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, artifact_id)
);

CREATE INDEX idx_event_artifact_refs_artifact ON core.event_artifact_refs(artifact_id);

-- ============================================================================
-- Artifact Embeddings (for semantic search)
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.artifact_embeddings (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    artifact_id ULID NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    artifact_content_id ULID REFERENCES core.artifact_contents(id) ON DELETE CASCADE,
    embedding_model_id ULID NOT NULL REFERENCES core.embedding_models(id),
    chunk_index INTEGER NOT NULL DEFAULT 0,
    chunk_text TEXT NOT NULL,
    embedding vector(1536) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_artifact_chunk_embedding UNIQUE(artifact_id, embedding_model_id, chunk_index)
);

CREATE INDEX idx_artifact_embeddings_artifact ON core.artifact_embeddings(artifact_id);
CREATE INDEX idx_artifact_embeddings_vector ON core.artifact_embeddings 
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- ============================================================================
-- Event Embeddings (for semantic search)
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.event_embeddings (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    event_id ULID NOT NULL REFERENCES core.events(event_id) ON DELETE CASCADE,
    embedding_model_id ULID NOT NULL REFERENCES core.embedding_models(id),
    embedded_text TEXT NOT NULL,
    embedding vector(1536) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_event_embedding UNIQUE(event_id, embedding_model_id)
);

CREATE INDEX idx_event_embeddings_event ON core.event_embeddings(event_id);
CREATE INDEX idx_event_embeddings_vector ON core.event_embeddings 
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- ============================================================================
-- Add triggers
-- ============================================================================
CREATE TRIGGER set_artifacts_updated_at 
    BEFORE UPDATE ON core.artifacts 
    FOR EACH ROW 
    EXECUTE FUNCTION set_current_timestamp();

-- ============================================================================
-- Add comments
-- ============================================================================
COMMENT ON TABLE core.artifacts IS 'Central registry of all knowledge artifacts in the system';
COMMENT ON TABLE core.artifact_contents IS 'Versioned content storage for artifacts';
COMMENT ON TABLE core.artifact_tags IS 'Many-to-many mapping of tags to artifacts';
COMMENT ON TABLE core.artifact_relations IS 'Relationships between knowledge artifacts';
COMMENT ON TABLE core.artifact_event_sources IS 'Links artifacts to their source events';
COMMENT ON TABLE core.event_artifact_refs IS 'Links events to artifacts they reference';
COMMENT ON TABLE core.artifact_embeddings IS 'Vector embeddings for knowledge artifacts';
COMMENT ON TABLE core.event_embeddings IS 'Vector embeddings for events';