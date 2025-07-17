-- Rollback automaton checkpoints table

-- Drop trigger and function
DROP TRIGGER IF EXISTS automaton_checkpoints_updated_at ON core.automaton_checkpoints;
DROP FUNCTION IF EXISTS core.update_automaton_checkpoints_updated_at();

-- Drop indexes
DROP INDEX IF EXISTS core.idx_automaton_checkpoints_lookup;
DROP INDEX IF EXISTS core.idx_automaton_checkpoints_activity;
DROP INDEX IF EXISTS core.idx_automaton_checkpoints_automaton;

-- Drop table
DROP TABLE IF EXISTS core.automaton_checkpoints;