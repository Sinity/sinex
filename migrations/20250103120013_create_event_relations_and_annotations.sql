-- ============================================================================
-- Event Relations and Annotations
-- ============================================================================

-- Event relations: Relationships between events
CREATE TABLE IF NOT EXISTS core.event_relations (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    from_event_id ulid NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
    to_event_id ulid NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL, -- 'caused_by', 'followed_by', 'related_to', 'parent_of', etc.
    confidence FLOAT DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    detected_by TEXT NOT NULL, -- 'user', 'temporal_analysis', 'causal_inference', etc.
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT no_self_event_relations CHECK (from_event_id != to_event_id),
    CONSTRAINT unique_event_relation UNIQUE(from_event_id, to_event_id, relation_type)
);

-- Event annotations: User annotations and notes on events
CREATE TABLE IF NOT EXISTS core.event_annotations (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    event_id ulid NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
    annotation_type TEXT NOT NULL, -- 'note', 'correction', 'context', 'importance', etc.
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT NOT NULL DEFAULT 'user' -- Future: user system integration
);

-- Artifact relations: Relationships between artifacts
CREATE TABLE IF NOT EXISTS core.artifact_relations (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    from_artifact_id ulid NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    to_artifact_id ulid NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL, -- 'references', 'derived_from', 'supersedes', 'part_of', etc.
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT no_self_artifact_relations CHECK (from_artifact_id != to_artifact_id),
    CONSTRAINT unique_artifact_relation UNIQUE(from_artifact_id, to_artifact_id, relation_type)
);

-- Event clusters: Groups of related events
CREATE TABLE IF NOT EXISTS core.event_clusters (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    name TEXT NOT NULL,
    cluster_type TEXT NOT NULL, -- 'session', 'workflow', 'project', 'incident', etc.
    summary TEXT,
    time_start TIMESTAMPTZ NOT NULL,
    time_end TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Event cluster membership
CREATE TABLE IF NOT EXISTS core.event_cluster_members (
    cluster_id ulid NOT NULL REFERENCES core.event_clusters(id) ON DELETE CASCADE,
    event_id ulid NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
    role TEXT, -- 'start', 'end', 'key_event', etc.
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (cluster_id, event_id)
);

-- ============================================================================
-- Cross-references between artifacts and events
-- ============================================================================

-- Artifacts derived from events
CREATE TABLE IF NOT EXISTS core.artifact_event_sources (
    artifact_id ulid NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    event_id ulid NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
    derivation_type TEXT NOT NULL, -- 'created_from', 'extracted_from', 'mentioned_in', etc.
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (artifact_id, event_id)
);

-- Events that reference artifacts
CREATE TABLE IF NOT EXISTS core.event_artifact_refs (
    event_id ulid NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
    artifact_id ulid NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    reference_type TEXT NOT NULL, -- 'accessed', 'modified', 'created', 'deleted', etc.
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, artifact_id)
);

-- ============================================================================
-- Indexes
-- ============================================================================

-- Event relations indexes
CREATE INDEX idx_event_relations_from ON core.event_relations(from_event_id);
CREATE INDEX idx_event_relations_to ON core.event_relations(to_event_id);
CREATE INDEX idx_event_relations_type ON core.event_relations(relation_type);
CREATE INDEX idx_event_relations_confidence ON core.event_relations(confidence) WHERE confidence < 1.0;

-- Event annotations indexes
CREATE INDEX idx_event_annotations_event ON core.event_annotations(event_id);
CREATE INDEX idx_event_annotations_type ON core.event_annotations(annotation_type);
CREATE INDEX idx_event_annotations_created ON core.event_annotations(created_at);
CREATE INDEX idx_event_annotations_search ON core.event_annotations USING gin(to_tsvector('english', content));

-- Artifact relations indexes
CREATE INDEX idx_artifact_relations_from ON core.artifact_relations(from_artifact_id);
CREATE INDEX idx_artifact_relations_to ON core.artifact_relations(to_artifact_id);
CREATE INDEX idx_artifact_relations_type ON core.artifact_relations(relation_type);

-- Event clusters indexes
CREATE INDEX idx_event_clusters_type ON core.event_clusters(cluster_type);
CREATE INDEX idx_event_clusters_time ON core.event_clusters(time_start, time_end);
CREATE INDEX idx_event_cluster_members_event ON core.event_cluster_members(event_id);

-- Cross-reference indexes
CREATE INDEX idx_artifact_event_sources_event ON core.artifact_event_sources(event_id);
CREATE INDEX idx_event_artifact_refs_artifact ON core.event_artifact_refs(artifact_id);

-- ============================================================================
-- Triggers
-- ============================================================================

CREATE TRIGGER set_event_annotations_updated_at 
    BEFORE UPDATE ON core.event_annotations 
    FOR EACH ROW 
    EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();

CREATE TRIGGER set_event_clusters_updated_at 
    BEFORE UPDATE ON core.event_clusters 
    FOR EACH ROW 
    EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();

-- ============================================================================
-- Comments
-- ============================================================================

COMMENT ON TABLE core.event_relations IS 'Discovered or defined relationships between events';
COMMENT ON TABLE core.event_annotations IS 'User annotations and notes on individual events';
COMMENT ON TABLE core.artifact_relations IS 'Relationships between knowledge artifacts';
COMMENT ON TABLE core.event_clusters IS 'Grouped collections of related events';
COMMENT ON TABLE core.event_cluster_members IS 'Membership of events in clusters';
COMMENT ON TABLE core.artifact_event_sources IS 'Links artifacts to their source events';
COMMENT ON TABLE core.event_artifact_refs IS 'Links events to artifacts they reference';