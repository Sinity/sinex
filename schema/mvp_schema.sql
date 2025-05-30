-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create raw schema
CREATE SCHEMA IF NOT EXISTS raw;

-- Create events table
CREATE TABLE IF NOT EXISTS raw.events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source TEXT NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL,
    provenance JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('raw.events', 'ts_ingest', if_not_exists => TRUE);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_events_source ON raw.events (source);
CREATE INDEX IF NOT EXISTS idx_events_ts_ingest ON raw.events (ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_events_payload ON raw.events USING GIN (payload);
CREATE INDEX IF NOT EXISTS idx_events_provenance ON raw.events USING GIN (provenance);

-- Composite index for source + time queries
CREATE INDEX IF NOT EXISTS idx_events_source_ts ON raw.events (source, ts_ingest DESC);

-- Add comments for documentation
COMMENT ON SCHEMA raw IS 'Raw event storage for all ingestors';
COMMENT ON TABLE raw.events IS 'Universal event storage table for all system events';
COMMENT ON COLUMN raw.events.id IS 'Unique identifier for each event';
COMMENT ON COLUMN raw.events.source IS 'Source system/ingestor that generated the event (e.g., hyprland, browser, filesystem)';
COMMENT ON COLUMN raw.events.ts_ingest IS 'Timestamp when the event was ingested into the database';
COMMENT ON COLUMN raw.events.payload IS 'Event-specific data in JSON format';
COMMENT ON COLUMN raw.events.provenance IS 'Metadata about event origin, processing, and lineage';