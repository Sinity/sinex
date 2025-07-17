-- Migration: Create GitOps schema registry for JSON schema management
-- This table stores schemas deployed from the Git repository (single source of truth)

-- Create schema registry table
CREATE TABLE IF NOT EXISTS sinex_schemas.schema_registry (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    schema_id               TEXT NOT NULL,           -- e.g., "v1/filesystem/file_created.json"
    version                 TEXT NOT NULL,           -- e.g., "v1", "v2"
    schema_content          JSONB NOT NULL,          -- The full JSON Schema definition
    
    -- Metadata
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    deployed_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deployed_by             TEXT,                    -- User or system that deployed
    git_commit_sha          TEXT,                    -- Git commit SHA for traceability
    
    -- Schema validation metadata
    draft_version           TEXT GENERATED ALWAYS AS (schema_content->>'$schema') STORED,
    schema_title            TEXT GENERATED ALWAYS AS (schema_content->>'title') STORED,
    schema_description      TEXT GENERATED ALWAYS AS (schema_content->>'description') STORED,
    
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique schema_id per version
    CONSTRAINT uq_schema_registry_id_version UNIQUE (schema_id, version)
);

COMMENT ON TABLE sinex_schemas.schema_registry IS 'GitOps-managed JSON Schema registry. Schemas are deployed from Git repository.';
COMMENT ON COLUMN sinex_schemas.schema_registry.schema_id IS 'Schema identifier matching the $id field, e.g., v1/filesystem/file_created.json';
COMMENT ON COLUMN sinex_schemas.schema_registry.version IS 'Schema version extracted from path, e.g., v1, v2';
COMMENT ON COLUMN sinex_schemas.schema_registry.schema_content IS 'Complete JSON Schema definition including $schema, $id, properties, etc.';
COMMENT ON COLUMN sinex_schemas.schema_registry.is_active IS 'Whether this schema version is currently active for validation';
COMMENT ON COLUMN sinex_schemas.schema_registry.git_commit_sha IS 'Git commit SHA from which this schema was deployed';

-- Indexes for efficient lookups
CREATE INDEX idx_schema_registry_schema_id_active 
    ON sinex_schemas.schema_registry (schema_id, is_active) 
    WHERE is_active = TRUE;

CREATE INDEX idx_schema_registry_version_active 
    ON sinex_schemas.schema_registry (version, is_active) 
    WHERE is_active = TRUE;

CREATE INDEX idx_schema_registry_deployed_at 
    ON sinex_schemas.schema_registry (deployed_at DESC);

-- Function to get active schema by ID
CREATE OR REPLACE FUNCTION sinex_schemas.get_active_schema(p_schema_id TEXT)
RETURNS JSONB AS $$
BEGIN
    RETURN (
        SELECT schema_content
        FROM sinex_schemas.schema_registry
        WHERE schema_id = p_schema_id
          AND is_active = TRUE
        ORDER BY deployed_at DESC
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION sinex_schemas.get_active_schema IS 'Retrieves the active schema content for a given schema_id';

-- Function to validate JSON against a schema from the registry
CREATE OR REPLACE FUNCTION sinex_schemas.validate_against_registry(
    p_schema_id TEXT,
    p_json_data JSONB
)
RETURNS BOOLEAN AS $$
DECLARE
    v_schema JSONB;
BEGIN
    v_schema := sinex_schemas.get_active_schema(p_schema_id);
    
    IF v_schema IS NULL THEN
        RAISE EXCEPTION 'No active schema found for schema_id: %', p_schema_id;
    END IF;
    
    RETURN jsonb_matches_schema(v_schema::json, p_json_data);
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION sinex_schemas.validate_against_registry IS 'Validates JSON data against a schema from the registry';

-- Updated trigger for raw.events to use schema registry
CREATE OR REPLACE FUNCTION raw.validate_event_payload_with_registry()
RETURNS TRIGGER AS $$
DECLARE
    v_schema_id TEXT;
BEGIN
    -- If no schema is specified, validation is skipped
    IF NEW.payload_schema_id IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Get schema_id from event_payload_schemas table
    SELECT CONCAT(schema_version, '/', event_source, '/', event_type, '.json')
    INTO v_schema_id
    FROM sinex_schemas.event_payload_schemas
    WHERE id = NEW.payload_schema_id
      AND is_active = TRUE;
    
    IF v_schema_id IS NULL THEN
        RAISE EXCEPTION 'Schema ID % not found or inactive', NEW.payload_schema_id;
    END IF;
    
    -- Validate against schema registry
    IF NOT sinex_schemas.validate_against_registry(v_schema_id, NEW.payload) THEN
        RAISE EXCEPTION 'Event payload does not conform to schema %', v_schema_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Migration tracking view
CREATE VIEW sinex_schemas.schema_deployment_status AS
SELECT 
    sr.schema_id,
    sr.version,
    sr.is_active,
    sr.deployed_at,
    sr.git_commit_sha,
    COUNT(DISTINCT eps.id) as linked_event_schemas,
    sr.schema_title,
    sr.draft_version
FROM sinex_schemas.schema_registry sr
LEFT JOIN sinex_schemas.event_payload_schemas eps 
    ON CONCAT(eps.schema_version, '/', eps.event_source, '/', eps.event_type, '.json') = sr.schema_id
GROUP BY sr.id, sr.schema_id, sr.version, sr.is_active, sr.deployed_at, 
         sr.git_commit_sha, sr.schema_title, sr.draft_version
ORDER BY sr.deployed_at DESC;

COMMENT ON VIEW sinex_schemas.schema_deployment_status IS 'Overview of deployed schemas and their usage';