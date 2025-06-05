-- Down migration for 20250103120002_create_raw_events

DROP TABLE IF EXISTS raw.events CASCADE;
DROP FUNCTION IF EXISTS core.set_updated_at_trigger_func_generic() CASCADE;