-- Migration: Create raw.events table (core event substrate)
-- Up Migration

-- Note: ulid extension must be enabled before this migration (see 20250103115900_enable_ulid_extension.sql)

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION core.set_updated_at_trigger_func_generic()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION core.set_updated_at_trigger_func_generic() IS 'Generic trigger function to set current timestamp on updated_at column.';

-- Create raw.events table
CREATE TABLE IF NOT EXISTS raw.events (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    source                  TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    ts_ingest               TIMESTAMPTZ NOT NULL DEFAULT now(),
    ts_orig                 TIMESTAMPTZ, -- Can be NULL if truly unknown, but highly encouraged
    host                    TEXT NOT NULL,
    ingestor_version        TEXT,
    payload_schema_id       ULID REFERENCES sinex_schemas.event_payload_schemas(id) ON DELETE SET NULL ON UPDATE CASCADE,
    payload                 JSONB NOT NULL
);

COMMENT ON TABLE raw.events IS 'Universal log for all captured raw events before promotion or detailed structuring. Immutable by principle.';
COMMENT ON COLUMN raw.events.id IS 'Globally unique identifier for the event using ULID (Universally Unique Lexicographically Sortable Identifier).';
COMMENT ON COLUMN raw.events.source IS 'Canonical identifier for the event origin/producer (e.g., "desktop.hyprland.ipc_ingestor", "sinex.pkm.sync_agent").';
COMMENT ON COLUMN raw.events.event_type IS 'Type string for the event, often namespaced by source (e.g., "window_focused", "note_updated", "agent.heartbeat").';
COMMENT ON COLUMN raw.events.ts_ingest IS 'Timestamp of ingestion into this table (database server time). Primary TimescaleDB partitioning key.';
COMMENT ON COLUMN raw.events.ts_orig IS 'Original timestamp from the source system/sensor when the event occurred; best effort for accuracy.';
COMMENT ON COLUMN raw.events.host IS 'Identifier of the machine or device where the event originated.';
COMMENT ON COLUMN raw.events.ingestor_version IS 'Version of the ingestor code/binary that produced this event.';
COMMENT ON COLUMN raw.events.payload_schema_id IS 'Foreign key to sinex_schemas.event_payload_schemas, identifying the schema for the payload. Null if ad-hoc/unknown.';
COMMENT ON COLUMN raw.events.payload IS 'Complete raw event data as JSONB. May contain a "_provenance" sub-object for lineage details.';

-- Indexes for raw.events
-- Primary Key index is created automatically

-- For common filtering and sorting by time of occurrence
CREATE INDEX IF NOT EXISTS idx_raw_events_ts_orig_desc ON raw.events (ts_orig DESC NULLS LAST);

-- For querying by source, type, and then time (common for agent processing)
CREATE INDEX IF NOT EXISTS idx_raw_events_source_type_ts_ingest_desc ON raw.events (source, event_type, ts_ingest DESC);

-- For querying by host and time
CREATE INDEX IF NOT EXISTS idx_raw_events_host_ts_ingest_desc ON raw.events (host, ts_ingest DESC);

-- For finding events by their payload schema
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_schema_id ON raw.events (payload_schema_id) WHERE payload_schema_id IS NOT NULL;

-- GIN index for querying JSONB payload content
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_gin_path_ops ON raw.events USING GIN (payload jsonb_path_ops);