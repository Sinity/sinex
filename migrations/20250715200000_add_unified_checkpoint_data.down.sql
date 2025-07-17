-- Revert unified checkpoint data column

-- Drop index
DROP INDEX IF EXISTS idx_automaton_checkpoints_checkpoint_data;

-- Remove column
ALTER TABLE core.automaton_checkpoints 
DROP COLUMN IF EXISTS checkpoint_data;

-- Revert comments
COMMENT ON TABLE core.automaton_checkpoints IS 'Checkpoint state for automaton satellites processing Redis Streams';
COMMENT ON COLUMN core.automaton_checkpoints.checkpoint_data IS NULL;