-- Create processor_manifests table for unified processor tracking
-- This table unifies tracking for both ingestors and automata as "processors"
-- Implements the Deep Symmetry principle from the comprehensive plan

CREATE TABLE IF NOT EXISTS sinex_schemas.processor_manifests (
    id SERIAL PRIMARY KEY,
    
    -- Processor identification
    processor_name TEXT NOT NULL,
    processor_type TEXT NOT NULL CHECK (processor_type IN ('ingestor', 'automaton')),
    
    -- Version information
    version TEXT NOT NULL,
    git_commit_sha TEXT,
    rust_version TEXT,
    build_timestamp TIMESTAMPTZ,
    
    -- Processor metadata
    description TEXT,
    status TEXT NOT NULL DEFAULT 'development' CHECK (status IN ('development', 'stable', 'deprecated')),
    
    -- Capabilities
    produces_event_types TEXT[] DEFAULT '{}',
    consumes_event_types TEXT[] DEFAULT '{}',
    
    -- Health tracking
    last_heartbeat_ts TIMESTAMPTZ,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Timestamps
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Ensure unique processor builds
    UNIQUE (processor_name, version, git_commit_sha)
);

-- Add foreign key to events table for provenance tracking (conditional)
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'events') THEN
        ALTER TABLE core.events ADD COLUMN IF NOT EXISTS processor_manifest_id INTEGER 
            REFERENCES sinex_schemas.processor_manifests(id);
    END IF;
END $$;

-- Create indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_processor_manifests_name_type 
    ON sinex_schemas.processor_manifests (processor_name, processor_type);

CREATE INDEX IF NOT EXISTS idx_processor_manifests_status 
    ON sinex_schemas.processor_manifests (status, processor_type);

CREATE INDEX IF NOT EXISTS idx_processor_manifests_heartbeat 
    ON sinex_schemas.processor_manifests (last_heartbeat_ts DESC);

-- Create index on events table (conditional)
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'events') THEN
        CREATE INDEX IF NOT EXISTS idx_events_processor_manifest_id 
            ON core.events (processor_manifest_id);
    END IF;
END $$;

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION sinex_schemas.update_processor_manifests_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER processor_manifests_updated_at
    BEFORE UPDATE ON sinex_schemas.processor_manifests
    FOR EACH ROW
    EXECUTE FUNCTION sinex_schemas.update_processor_manifests_updated_at();

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON sinex_schemas.processor_manifests TO sinex;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA sinex_schemas TO sinex;

-- Comments for documentation
COMMENT ON TABLE sinex_schemas.processor_manifests IS 'Unified registry for both ingestor and automaton processors implementing Deep Symmetry';
COMMENT ON COLUMN sinex_schemas.processor_manifests.processor_name IS 'Name of the processor (e.g., sinex-fs-watcher, sinex-command-canonicalizer)';
COMMENT ON COLUMN sinex_schemas.processor_manifests.processor_type IS 'Type of processor: ingestor or automaton';
COMMENT ON COLUMN sinex_schemas.processor_manifests.version IS 'Semantic version of the processor';
COMMENT ON COLUMN sinex_schemas.processor_manifests.git_commit_sha IS 'Git commit SHA for precise build tracking';
COMMENT ON COLUMN sinex_schemas.processor_manifests.rust_version IS 'Rust compiler version used for build';
COMMENT ON COLUMN sinex_schemas.processor_manifests.build_timestamp IS 'When this processor version was built';
COMMENT ON COLUMN sinex_schemas.processor_manifests.produces_event_types IS 'Array of event types this processor produces';
COMMENT ON COLUMN sinex_schemas.processor_manifests.consumes_event_types IS 'Array of event types this processor consumes';
COMMENT ON COLUMN sinex_schemas.processor_manifests.last_heartbeat_ts IS 'Last heartbeat timestamp for health monitoring';
COMMENT ON COLUMN sinex_schemas.processor_manifests.last_seen IS 'Last time this processor was active';

-- Insert some example processors for testing
INSERT INTO sinex_schemas.processor_manifests (
    processor_name, processor_type, version, description, status,
    produces_event_types, consumes_event_types
) VALUES 
    ('sinex-fs-watcher', 'ingestor', '0.1.0', 'Filesystem event ingestor', 'stable',
     ARRAY['fs.file.created', 'fs.file.modified', 'fs.file.deleted', 'fs.dir.created', 'fs.dir.deleted'],
     ARRAY[]::TEXT[]),
    ('sinex-terminal-satellite', 'ingestor', '0.1.0', 'Terminal command and output ingestor', 'stable',
     ARRAY['shell.command.executed', 'shell.session.started', 'shell.output.captured'],
     ARRAY[]::TEXT[]),
    ('sinex-command-canonicalizer', 'automaton', '0.1.0', 'Command canonicalization automaton', 'stable',
     ARRAY['command.canonical'],
     ARRAY['shell.command.executed']),
    ('sinex-health-aggregator', 'automaton', '0.1.0', 'Health monitoring aggregator', 'development',
     ARRAY['system.health.summary'],
     ARRAY['satellite.heartbeat'])
ON CONFLICT (processor_name, version, git_commit_sha) DO NOTHING;