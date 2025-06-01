-- Migration: Convert raw.events to TimescaleDB hypertable
-- Up Migration

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Convert raw.events to hypertable
-- This must be done AFTER the table is created
SELECT create_hypertable(
  'raw.events',
  'ts_ingest',
  if_not_exists => TRUE,
  chunk_time_interval => INTERVAL '1 day', -- Daily chunks for balanced performance
  migrate_data => TRUE
);

-- Set compression policy (compress chunks older than 7 days)
-- This is based on TIM-TimescaleDBConfiguration recommendations
SELECT add_compression_policy('raw.events', INTERVAL '7 days', if_not_exists => TRUE);

-- Configure compression settings
ALTER TABLE raw.events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source,event_type',
  timescaledb.compress_orderby = 'ts_ingest DESC'
);

COMMENT ON TABLE raw.events IS 'Universal log for all captured raw events (TimescaleDB hypertable). Immutable by principle.';