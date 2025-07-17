-- Add unified checkpoint data column for stream processor architecture
-- This migration adds support for the unified checkpoint system that supports
-- both ingestors and automata with a single Checkpoint enum type

-- Add new column for unified checkpoint data
ALTER TABLE core.automaton_checkpoints 
ADD COLUMN IF NOT EXISTS checkpoint_data JSONB;

-- Add index for checkpoint data queries
CREATE INDEX IF NOT EXISTS idx_automaton_checkpoints_checkpoint_data 
ON core.automaton_checkpoints USING GIN (checkpoint_data);

-- Update comment to reflect unified usage
COMMENT ON TABLE core.automaton_checkpoints IS 'Unified checkpoint state for both ingestor and automaton stream processors';
COMMENT ON COLUMN core.automaton_checkpoints.checkpoint_data IS 'Unified checkpoint data (version 2+) supporting external positions, event IDs, stream IDs, and timestamps';

-- Grant permissions for new column
GRANT SELECT, INSERT, UPDATE ON core.automaton_checkpoints TO sinex;