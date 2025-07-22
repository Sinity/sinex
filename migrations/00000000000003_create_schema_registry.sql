-- Create schema registry for event payload validation

-- Event payload schemas table
CREATE TABLE IF NOT EXISTS sinex_schemas.event_payload_schemas (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    schema_name TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    schema_content JSONB NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    event_types TEXT[] NOT NULL,
    description TEXT,
    examples JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at TIMESTAMPTZ,
    deprecation_reason TEXT,
    CONSTRAINT unique_schema_name_version UNIQUE (schema_name, schema_version)
);

CREATE INDEX idx_schemas_active ON sinex_schemas.event_payload_schemas (schema_name, schema_version) WHERE is_active = true;
CREATE INDEX idx_schemas_event_types ON sinex_schemas.event_payload_schemas USING GIN (event_types);

-- Schema compatibility tracking
CREATE TABLE IF NOT EXISTS sinex_schemas.schema_compatibility (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    from_schema_id ULID NOT NULL REFERENCES sinex_schemas.event_payload_schemas(id),
    to_schema_id ULID NOT NULL REFERENCES sinex_schemas.event_payload_schemas(id),
    compatibility_type TEXT NOT NULL CHECK (compatibility_type IN ('backward', 'forward', 'full', 'none')),
    migration_strategy JSONB,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_schema_pair UNIQUE (from_schema_id, to_schema_id),
    CONSTRAINT no_self_reference CHECK (from_schema_id != to_schema_id)
);

CREATE INDEX idx_schema_compat_from ON sinex_schemas.schema_compatibility (from_schema_id);
CREATE INDEX idx_schema_compat_to ON sinex_schemas.schema_compatibility (to_schema_id);

-- GitOps schema management
CREATE TABLE IF NOT EXISTS sinex_schemas.gitops_schema_sources (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    repository_url TEXT NOT NULL,
    branch TEXT NOT NULL DEFAULT 'main',
    path_pattern TEXT NOT NULL,
    sync_enabled BOOLEAN NOT NULL DEFAULT true,
    last_sync_at TIMESTAMPTZ,
    last_sync_commit TEXT,
    sync_frequency_minutes INTEGER NOT NULL DEFAULT 60,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_repo_branch_path UNIQUE (repository_url, branch, path_pattern)
);

CREATE INDEX idx_gitops_sources_sync ON sinex_schemas.gitops_schema_sources (sync_enabled, last_sync_at) WHERE sync_enabled = true;

-- Schema validation results cache
CREATE TABLE IF NOT EXISTS sinex_schemas.validation_cache (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    event_id ULID NOT NULL,
    schema_id ULID NOT NULL REFERENCES sinex_schemas.event_payload_schemas(id),
    is_valid BOOLEAN NOT NULL,
    validation_errors JSONB,
    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_event_schema_validation UNIQUE (event_id, schema_id)
);

CREATE INDEX idx_validation_cache_event ON sinex_schemas.validation_cache (event_id);
CREATE INDEX idx_validation_cache_schema ON sinex_schemas.validation_cache (schema_id);
CREATE INDEX idx_validation_cache_invalid ON sinex_schemas.validation_cache (schema_id, validated_at DESC) WHERE is_valid = false;

-- Add comments
COMMENT ON TABLE sinex_schemas.event_payload_schemas IS 'Registry of JSON schemas for validating event payloads';
COMMENT ON TABLE sinex_schemas.schema_compatibility IS 'Tracks compatibility relationships between schema versions';
COMMENT ON TABLE sinex_schemas.gitops_schema_sources IS 'Configuration for syncing schemas from Git repositories';
COMMENT ON TABLE sinex_schemas.validation_cache IS 'Cache of event payload validation results';