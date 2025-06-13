-- ============================================================================
-- Knowledge Management Core Tables
-- ============================================================================

-- Create core namespace if it doesn't exist
CREATE SCHEMA IF NOT EXISTS core;

-- Artifacts table: Conceptual documents/items (PKM notes, web pages, emails, files)
CREATE TABLE IF NOT EXISTS core.artifacts (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    type TEXT NOT NULL CHECK (type IN ('note', 'webpage', 'email', 'file', 'document', 'code', 'media')),
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
    -- Foreign keys
    created_from_event_id ulid REFERENCES raw.events(id),
    blob_id ulid -- References core.blobs when created
);

-- Artifact contents: Versioned textual content
CREATE TABLE IF NOT EXISTS core.artifact_contents (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    artifact_id ulid NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    version INTEGER NOT NULL DEFAULT 1,
    content TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'text/plain', -- markdown, html, plain, etc.
    extracted_text TEXT, -- For searchable text from binary formats
    word_count INTEGER,
    char_count INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_from_event_id ulid REFERENCES raw.events(id),
    UNIQUE(artifact_id, version)
);

-- Entities: Knowledge graph nodes (persons, projects, topics, organizations)
CREATE TABLE IF NOT EXISTS core.entities (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    type TEXT NOT NULL CHECK (type IN ('person', 'project', 'topic', 'organization', 'location', 'concept', 'tool', 'event')),
    name TEXT NOT NULL,
    canonical_name TEXT NOT NULL, -- Normalized/standardized name
    aliases TEXT[] NOT NULL DEFAULT '{}',
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    merged_into_id ulid REFERENCES core.entities(id), -- For entity deduplication
    CONSTRAINT unique_canonical_name_per_type UNIQUE(type, canonical_name)
);

-- Entity relations: Knowledge graph edges
CREATE TABLE IF NOT EXISTS core.entity_relations (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    from_entity_id ulid NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    to_entity_id ulid NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL, -- 'works_on', 'knows', 'located_in', 'part_of', etc.
    strength FLOAT DEFAULT 1.0 CHECK (strength >= 0 AND strength <= 1),
    metadata JSONB NOT NULL DEFAULT '{}',
    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_from_event_id ulid REFERENCES raw.events(id),
    CONSTRAINT no_self_relations CHECK (from_entity_id != to_entity_id)
);

-- Tags: Flexible categorization system
CREATE TABLE IF NOT EXISTS core.tags (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    color TEXT, -- Hex color for UI
    icon TEXT, -- Icon identifier for UI
    parent_id ulid REFERENCES core.tags(id), -- Hierarchical tags
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Artifact tags: Many-to-many relationship
CREATE TABLE IF NOT EXISTS core.artifact_tags (
    artifact_id ulid NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    tag_id ulid NOT NULL REFERENCES core.tags(id) ON DELETE CASCADE,
    tagged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tagged_from_event_id ulid REFERENCES raw.events(id),
    PRIMARY KEY (artifact_id, tag_id)
);

-- Blobs: Git-annex managed large files
CREATE TABLE IF NOT EXISTS core.blobs (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    annex_key TEXT UNIQUE NOT NULL, -- Git-annex key
    original_filename TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    mime_type TEXT,
    checksum_sha256 TEXT NOT NULL,
    checksum_md5 TEXT,
    storage_backend TEXT NOT NULL DEFAULT 'git-annex', -- Future: s3, local, etc.
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_verified_at TIMESTAMPTZ,
    verification_status TEXT CHECK (verification_status IN ('pending', 'verified', 'missing', 'corrupted'))
);

-- ============================================================================
-- Indexes
-- ============================================================================

-- Artifacts indexes
CREATE INDEX idx_artifacts_type ON core.artifacts(type);
CREATE INDEX idx_artifacts_created_at ON core.artifacts(created_at);
CREATE INDEX idx_artifacts_updated_at ON core.artifacts(updated_at);
CREATE INDEX idx_artifacts_metadata ON core.artifacts USING gin(metadata);
CREATE INDEX idx_artifacts_deleted_at ON core.artifacts(deleted_at) WHERE deleted_at IS NULL;
CREATE INDEX idx_artifacts_blob_id ON core.artifacts(blob_id) WHERE blob_id IS NOT NULL;

-- Artifact contents indexes
CREATE INDEX idx_artifact_contents_artifact_id ON core.artifact_contents(artifact_id);
CREATE INDEX idx_artifact_contents_content_search ON core.artifact_contents USING gin(to_tsvector('english', content));
CREATE INDEX idx_artifact_contents_extracted_search ON core.artifact_contents USING gin(to_tsvector('english', extracted_text)) WHERE extracted_text IS NOT NULL;

-- Entities indexes
CREATE INDEX idx_entities_type ON core.entities(type);
CREATE INDEX idx_entities_canonical_name ON core.entities(canonical_name);
CREATE INDEX idx_entities_aliases ON core.entities USING gin(aliases);
CREATE INDEX idx_entities_name_search ON core.entities USING gin(to_tsvector('english', name));

-- Entity relations indexes
CREATE INDEX idx_entity_relations_from ON core.entity_relations(from_entity_id);
CREATE INDEX idx_entity_relations_to ON core.entity_relations(to_entity_id);
CREATE INDEX idx_entity_relations_type ON core.entity_relations(relation_type);
CREATE INDEX idx_entity_relations_valid ON core.entity_relations(valid_from, valid_until);

-- Tags indexes
CREATE INDEX idx_tags_parent ON core.tags(parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX idx_tags_name ON core.tags(name);

-- Blobs indexes
CREATE INDEX idx_blobs_annex_key ON core.blobs(annex_key);
CREATE INDEX idx_blobs_checksum_sha256 ON core.blobs(checksum_sha256);
CREATE INDEX idx_blobs_verification ON core.blobs(verification_status, last_verified_at);

-- ============================================================================
-- Triggers
-- ============================================================================

-- Updated_at triggers
CREATE TRIGGER set_artifacts_updated_at 
    BEFORE UPDATE ON core.artifacts 
    FOR EACH ROW 
    EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();

CREATE TRIGGER set_entities_updated_at 
    BEFORE UPDATE ON core.entities 
    FOR EACH ROW 
    EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();

-- ============================================================================
-- Comments
-- ============================================================================

COMMENT ON TABLE core.artifacts IS 'Central registry of all knowledge artifacts in the system';
COMMENT ON TABLE core.artifact_contents IS 'Versioned content storage for artifacts';
COMMENT ON TABLE core.entities IS 'Knowledge graph nodes representing real-world entities';
COMMENT ON TABLE core.entity_relations IS 'Knowledge graph edges representing relationships';
COMMENT ON TABLE core.tags IS 'Hierarchical tagging system for categorization';
COMMENT ON TABLE core.artifact_tags IS 'Many-to-many mapping of tags to artifacts';
COMMENT ON TABLE core.blobs IS 'Registry of git-annex managed binary files';