-- Down migration: Revert processor_checkpoints back to automaton_checkpoints

-- Update table comment back
COMMENT ON TABLE core.processor_checkpoints IS 'Processing state for event automata to enable reliable restarts';
COMMENT ON COLUMN core.processor_checkpoints.processor_name IS NULL;

-- Rename indexes back
ALTER INDEX idx_processor_checkpoints_consumer RENAME TO idx_automaton_checkpoints_consumer;
ALTER INDEX idx_processor_checkpoints_processor RENAME TO idx_automaton_checkpoints_automaton;
ALTER INDEX idx_processor_checkpoints_updated RENAME TO idx_automaton_checkpoints_updated;

-- Update the constraint name back
ALTER TABLE core.processor_checkpoints 
    DROP CONSTRAINT unique_processor_consumer,
    ADD CONSTRAINT unique_automaton_consumer UNIQUE (processor_name, consumer_group, consumer_name);

-- Rename the column back
ALTER TABLE core.processor_checkpoints RENAME COLUMN processor_name TO automaton_name;

-- Rename the table back
ALTER TABLE core.processor_checkpoints RENAME TO automaton_checkpoints;