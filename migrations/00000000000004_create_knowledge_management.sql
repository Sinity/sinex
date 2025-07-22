-- Create knowledge management tables for concepts, relations, and annotations

-- Concepts table
CREATE TABLE IF NOT EXISTS km.concepts (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    concept_name TEXT NOT NULL,
    concept_type TEXT NOT NULL,
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    embedding vector(1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT unique_concept_name_type UNIQUE (concept_name, concept_type)
);

CREATE INDEX idx_concepts_type ON km.concepts (concept_type);
CREATE INDEX idx_concepts_name ON km.concepts (concept_name);
CREATE INDEX idx_concepts_embedding ON km.concepts USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

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

-- Embeddings cache
CREATE TABLE IF NOT EXISTS km.embeddings (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    content_hash TEXT NOT NULL UNIQUE,
    content_type TEXT NOT NULL,
    embedding vector(1536) NOT NULL,
    model_name TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_embeddings_hash ON km.embeddings (content_hash);
CREATE INDEX idx_embeddings_vector ON km.embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Add comments
COMMENT ON TABLE km.concepts IS 'Knowledge graph nodes representing concepts, entities, and ideas';
COMMENT ON TABLE km.relations IS 'Edges in the knowledge graph connecting concepts';
COMMENT ON TABLE km.event_annotations IS 'Links between events and concepts for knowledge extraction';
COMMENT ON TABLE km.artifacts IS 'Knowledge artifacts like documents, notes, and references';
COMMENT ON TABLE km.artifact_revisions IS 'Version history for knowledge artifacts';
COMMENT ON TABLE km.llm_interactions IS 'History of LLM interactions for knowledge extraction and synthesis';
COMMENT ON TABLE km.embeddings IS 'Cache of vector embeddings for semantic search';