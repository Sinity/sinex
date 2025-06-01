-- Sinex Phase 2 Master Schema
-- This file contains the complete database schema for Phase 2
-- It is designed to be idempotent and can be run multiple times

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ULID Support (custom implementation since no native ULID type)
CREATE DOMAIN ULID AS BYTEA CHECK(octet_length(VALUE) = 16);

CREATE OR REPLACE FUNCTION generate_ulid() RETURNS ULID AS $$
DECLARE
   timestamp_ms BIGINT;
   random_bytes BYTEA;
BEGIN
   timestamp_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
   random_bytes := gen_random_bytes(10);
   RETURN substring((E'\\x' || lpad(to_hex(timestamp_ms), 12, '0'))::bytea FROM 1 FOR 6) || random_bytes;
END $$ LANGUAGE plpgsql;

-- Convert ULID to text for display purposes
CREATE OR REPLACE FUNCTION ulid_to_text(ulid_val ULID) RETURNS TEXT AS $$
DECLARE
    -- Crockford's Base32 alphabet (excluding I, L, O, U to avoid confusion)
    alphabet TEXT := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    result TEXT := '';
    bytes BYTEA;
    i INT;
    val BIGINT;
BEGIN
    bytes := ulid_val;
    
    -- Convert 16 bytes to Base32
    -- This is a simplified version - a proper implementation would handle the full conversion
    -- For now, we'll use hex representation
    result := encode(bytes, 'hex');
    
    RETURN result;
END $$ LANGUAGE plpgsql;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS sinex_schemas;

-- Drop existing tables if they exist (for clean rebuild)
DROP TABLE IF EXISTS raw.events CASCADE;
DROP TABLE IF EXISTS sinex_schemas.event_payload_schemas CASCADE;
DROP TABLE IF EXISTS sinex_schemas.agent_manifests CASCADE;

-- Create event_payload_schemas table first (referenced by raw.events)
CREATE TABLE sinex_schemas.event_payload_schemas (
    id                      ULID PRIMARY KEY DEFAULT generate_ulid(),
    event_source            TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    schema_version          TEXT NOT NULL, -- e.g., "v1.0", "v1.1_beta"
    json_schema_definition  JSONB NOT NULL, -- The actual JSON Schema object
    description             TEXT,
    created_at              TIMESTAMPTZ DEFAULT now(),
    is_active               BOOLEAN DEFAULT TRUE,
    UNIQUE (event_source, event_type, schema_version)
);

CREATE INDEX idx_eps_source_type_active ON sinex_schemas.event_payload_schemas (event_source, event_type, is_active);

-- Create revised raw.events table
CREATE TABLE raw.events (
    id                      ULID PRIMARY KEY DEFAULT generate_ulid(),
    source                  TEXT NOT NULL,          -- e.g., "hyprland", "terminal.kitty", "sinex"
    event_type              TEXT NOT NULL,          -- e.g., "window_focused", "command_executed", "agent.heartbeat"
    ts_ingest               TIMESTAMPTZ DEFAULT now(), -- Timestamp of ingestion into DB
    ts_orig                 TIMESTAMPTZ,            -- Timestamp from the source system
    host                    TEXT NOT NULL,          -- Hostname of event origin
    ingestor_version        TEXT,                   -- Version of the ingestor binary/script
    payload_schema_id       ULID,                   -- FK to sinex_schemas.event_payload_schemas(id)
    payload                 JSONB NOT NULL          -- The actual event data
);

-- Create indexes for raw.events
CREATE INDEX idx_raw_events_source_type_ts ON raw.events (source, event_type, ts_ingest DESC);
CREATE INDEX idx_raw_events_ts_orig ON raw.events (ts_orig DESC);
CREATE INDEX idx_raw_events_payload_gin ON raw.events USING GIN (payload);
CREATE INDEX idx_raw_events_host ON raw.events (host);
CREATE INDEX idx_raw_events_payload_schema_id ON raw.events (payload_schema_id);

-- Create agent_manifests table
CREATE TABLE sinex_schemas.agent_manifests (
    agent_name              TEXT PRIMARY KEY,       -- e.g., "hyprland-ingestor", "kitty-ingestor"
    description             TEXT,
    version                 TEXT NOT NULL,
    status                  TEXT DEFAULT 'development', -- e.g., development, stable, deprecated
    config_schema_id        ULID REFERENCES sinex_schemas.event_payload_schemas(id), -- Schema for its own config file (optional)
    produces_event_types    JSONB,                  -- JSON object: {"source_A": ["type1", "type2"], "source_B": ["type3"]}
    repo_url                TEXT,                   -- Link to its source code
    last_seen_heartbeat     TIMESTAMPTZ,            -- Updated by a monitoring process
    registered_at           TIMESTAMPTZ DEFAULT now()
);

-- Add foreign key constraint
ALTER TABLE raw.events
ADD CONSTRAINT fk_payload_schema
FOREIGN KEY (payload_schema_id) REFERENCES sinex_schemas.event_payload_schemas(id);

-- Insert initial schema definitions for Phase 2 event types

-- Hyprland event schemas
INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition, description)
VALUES 
('hyprland', 'window_focused', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["app_class", "app_name", "pid", "window_title", "workspace_id"],
  "properties": {
    "app_class": {"type": "string"},
    "app_name": {"type": "string"},
    "pid": {"type": "integer"},
    "window_title": {"type": "string"},
    "workspace_id": {"type": "integer"},
    "workspace_name": {"type": "string"}
  }
}'::jsonb, 'Hyprland window focus change event'),

('hyprland', 'workspace_changed', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["workspace_id", "workspace_name"],
  "properties": {
    "workspace_id": {"type": "integer"},
    "workspace_name": {"type": "string"},
    "monitor": {"type": "string"}
  }
}'::jsonb, 'Hyprland workspace change event'),

('hyprland', 'clipboard_changed', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["mime_types"],
  "properties": {
    "mime_types": {
      "type": "array",
      "items": {"type": "string"}
    },
    "size_bytes": {"type": "integer"}
  }
}'::jsonb, 'Clipboard content change event'),

('hyprland', 'state_snapshot', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["clients", "workspaces", "monitors", "active_window"],
  "properties": {
    "clients": {"type": "array"},
    "workspaces": {"type": "array"},
    "monitors": {"type": "array"},
    "active_window": {"type": "object"}
  }
}'::jsonb, 'Full Hyprland state snapshot');

-- Terminal event schemas
INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition, description)
VALUES 
('terminal.kitty', 'command_executed', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["command_string", "cwd", "exit_code", "ts_start_orig", "ts_end_orig"],
  "properties": {
    "command_string": {"type": "string"},
    "cwd": {"type": "string"},
    "exit_code": {"type": "integer"},
    "ts_start_orig": {"type": "string", "format": "date-time"},
    "ts_end_orig": {"type": "string", "format": "date-time"}
  }
}'::jsonb, 'Terminal command execution event');

-- Filesystem event schemas
INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition, description)
VALUES 
('filesystem', 'file_created', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["path", "object_type"],
  "properties": {
    "path": {"type": "string"},
    "object_type": {"type": "string", "enum": ["file", "directory"]},
    "blake3_hash": {"type": "string"}
  }
}'::jsonb, 'File creation event'),

('filesystem', 'file_modified', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["path", "object_type"],
  "properties": {
    "path": {"type": "string"},
    "object_type": {"type": "string", "enum": ["file", "directory"]},
    "blake3_hash": {"type": "string"}
  }
}'::jsonb, 'File modification event'),

('filesystem', 'file_deleted', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["path", "object_type"],
  "properties": {
    "path": {"type": "string"},
    "object_type": {"type": "string", "enum": ["file", "directory"]}
  }
}'::jsonb, 'File deletion event'),

('filesystem', 'file_renamed', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["path", "new_path", "object_type"],
  "properties": {
    "path": {"type": "string"},
    "new_path": {"type": "string"},
    "object_type": {"type": "string", "enum": ["file", "directory"]},
    "blake3_hash": {"type": "string"}
  }
}'::jsonb, 'File rename event');

-- Sinex agent event schemas
INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition, description)
VALUES 
('sinex', 'agent.heartbeat', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["agent_name", "status", "uptime_seconds", "events_processed_session", "dlq_size", "version"],
  "properties": {
    "agent_name": {"type": "string"},
    "status": {"type": "string", "enum": ["running", "degraded", "erroring"]},
    "uptime_seconds": {"type": "integer"},
    "events_processed_session": {"type": "integer"},
    "dlq_size": {"type": "integer"},
    "version": {"type": "string"}
  }
}'::jsonb, 'Agent heartbeat event'),

('sinex', 'agent.error', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["agent_name", "error_message", "error_context", "severity"],
  "properties": {
    "agent_name": {"type": "string"},
    "error_message": {"type": "string"},
    "error_context": {"type": "string"},
    "severity": {"type": "string", "enum": ["warning", "error", "critical"]},
    "original_event_id_if_related": {"type": "string"}
  }
}'::jsonb, 'Agent error event'),

('sinex', 'agent.dlq_event_written', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["agent_name", "failed_event_source", "failed_event_type", "dlq_file_path", "failure_reason"],
  "properties": {
    "agent_name": {"type": "string"},
    "failed_event_source": {"type": "string"},
    "failed_event_type": {"type": "string"},
    "dlq_file_path": {"type": "string"},
    "failure_reason": {"type": "string"}
  }
}'::jsonb, 'DLQ event written notification'),

('sinex', 'schema.change', 'v1.0', '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["change_description", "applied_by"],
  "properties": {
    "change_description": {"type": "string"},
    "applied_by": {"type": "string"}
  }
}'::jsonb, 'Schema change event');

-- Create a view for easier ULID display
CREATE OR REPLACE VIEW raw.events_readable AS
SELECT 
    ulid_to_text(id) as id_text,
    source,
    event_type,
    ts_ingest,
    ts_orig,
    host,
    ingestor_version,
    ulid_to_text(payload_schema_id) as payload_schema_id_text,
    payload
FROM raw.events;

-- Grant permissions (adjust as needed)
GRANT ALL ON SCHEMA raw TO sinex;
GRANT ALL ON SCHEMA sinex_schemas TO sinex;
GRANT ALL ON ALL TABLES IN SCHEMA raw TO sinex;
GRANT ALL ON ALL TABLES IN SCHEMA sinex_schemas TO sinex;
GRANT ALL ON ALL SEQUENCES IN SCHEMA raw TO sinex;
GRANT ALL ON ALL SEQUENCES IN SCHEMA sinex_schemas TO sinex;