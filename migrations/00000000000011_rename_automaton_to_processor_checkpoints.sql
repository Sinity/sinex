-- ============================================================================
-- Rename automaton_checkpoints to processor_checkpoints
-- ============================================================================
--
-- This migration renames the automaton_checkpoints table to processor_checkpoints
-- to better reflect that it's used by all processor types (ingestors, automata, 
-- and system processors), not just automata.
--

-- Rename the table
ALTER TABLE core.automaton_checkpoints RENAME TO processor_checkpoints;

-- Rename the column
ALTER TABLE core.processor_checkpoints RENAME COLUMN automaton_name TO processor_name;

-- Update the constraint name
ALTER TABLE core.processor_checkpoints 
    DROP CONSTRAINT unique_automaton_consumer,
    ADD CONSTRAINT unique_processor_consumer UNIQUE (processor_name, consumer_group, consumer_name);

-- Rename indexes to match new naming
ALTER INDEX idx_automaton_checkpoints_updated RENAME TO idx_processor_checkpoints_updated;
ALTER INDEX idx_automaton_checkpoints_automaton RENAME TO idx_processor_checkpoints_processor;
ALTER INDEX idx_automaton_checkpoints_consumer RENAME TO idx_processor_checkpoints_consumer;

-- Update table comment
COMMENT ON TABLE core.processor_checkpoints IS 'Processing state for all event processors (ingestors, automata, system) to enable reliable restarts';
COMMENT ON COLUMN core.processor_checkpoints.processor_name IS 'Name of the processor (any type: ingestor, automaton, or system)';