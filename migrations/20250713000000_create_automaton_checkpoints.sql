-- Create automaton checkpoints table for satellite architecture
-- This table stores checkpoint state for automaton satellites processing Redis Streams

CREATE TABLE IF NOT EXISTS core.automaton_checkpoints (
    id UUID PRIMARY KEY DEFAULT gen_ulid(),
    
    -- Automaton identification
    automaton_name TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    consumer_name TEXT NOT NULL,
    
    -- Checkpoint state
    last_processed_id TEXT, -- Redis Stream message ID
    processed_count BIGINT NOT NULL DEFAULT 0,
    last_activity TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Automaton-specific state data (JSON)
    state_data JSONB,
    
    -- Checkpoint schema version for future evolution
    checkpoint_version INTEGER NOT NULL DEFAULT 1,
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Ensure unique checkpoint per automaton/group/consumer
    UNIQUE(automaton_name, consumer_group, consumer_name)
);

-- Index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_automaton_checkpoints_lookup 
ON core.automaton_checkpoints (automaton_name, consumer_group, consumer_name);

-- Index for monitoring queries
CREATE INDEX IF NOT EXISTS idx_automaton_checkpoints_activity 
ON core.automaton_checkpoints (last_activity DESC);

-- Index for automaton queries
CREATE INDEX IF NOT EXISTS idx_automaton_checkpoints_automaton 
ON core.automaton_checkpoints (automaton_name, last_activity DESC);

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION core.update_automaton_checkpoints_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER automaton_checkpoints_updated_at
    BEFORE UPDATE ON core.automaton_checkpoints
    FOR EACH ROW
    EXECUTE FUNCTION core.update_automaton_checkpoints_updated_at();

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON core.automaton_checkpoints TO sinex;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA core TO sinex;

-- Comments for documentation
COMMENT ON TABLE core.automaton_checkpoints IS 'Checkpoint state for automaton satellites processing Redis Streams';
COMMENT ON COLUMN core.automaton_checkpoints.automaton_name IS 'Name of the automaton (e.g., canonical-command-synthesizer)';
COMMENT ON COLUMN core.automaton_checkpoints.consumer_group IS 'Redis Streams consumer group name';
COMMENT ON COLUMN core.automaton_checkpoints.consumer_name IS 'Redis Streams consumer name (usually hostname-pid)';
COMMENT ON COLUMN core.automaton_checkpoints.last_processed_id IS 'Last processed Redis Stream message ID';
COMMENT ON COLUMN core.automaton_checkpoints.processed_count IS 'Total number of messages processed by this automaton';
COMMENT ON COLUMN core.automaton_checkpoints.state_data IS 'Automaton-specific state data as JSON';
COMMENT ON COLUMN core.automaton_checkpoints.checkpoint_version IS 'Schema version for checkpoint evolution';