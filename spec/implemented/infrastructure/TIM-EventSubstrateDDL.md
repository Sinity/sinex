# TIM-EventSubstrateDDL: Core DDL for `raw.events` and Foundational Schema Objects

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 95% (Core schema fully deployed, TimescaleDB integration operational)
**Dependencies**: PostgreSQL, pgx_ulid extension, TimescaleDB
**Blocks**: All event ingestion, promotion pipelines, AI processing

## MVP Specification
- Complete raw.events table with ULID primary keys
- Core schema organization (raw, core, sinex_schemas)
- Essential indexes for performance
- JSONB payload storage with GIN indexing
- Updated_at trigger function

## Enhanced Features
- Advanced TimescaleDB chunk management
- Automated retention policies
- Cross-partition query optimization
- Schema evolution support
- Advanced JSONB query patterns

## Implementation Checklist
- [x] Database migrations
- [x] Core table structure (raw.events)
- [x] Schema organization
- [x] Primary and performance indexes
- [x] ULID integration
- [x] TimescaleDB hypertable setup
- [x] Trigger functions
- [x] Documentation
- [ ] Retention policy automation
- [ ] Query optimization analysis

*   **Purpose:** Provides the canonical Data Definition Language (DDL) for the `raw.events` table and closely related foundational schema objects necessary for the event substrate.
*   **Source:** Derived from original Vision Document Appendix A and refined based on decisions in ADRs and other TIMs.
*   **Dependencies:** `pgx_ulid` extension (see `TIM-PrimaryKeyImplementation.md`). Assumes TimescaleDB is available for hypertable conversion (see `TIM-TimescaleDBConfiguration.md`).

## 1. Core Schemas

These schemas organize database objects.

```sql
CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS 'Schema for raw, immutable event data (raw.events).';

CREATE SCHEMA IF NOT EXISTS sinex_schemas;
COMMENT ON SCHEMA sinex_schemas IS 'Schema for Exocortex system schemas, like event payload definitions and agent manifests.';

CREATE SCHEMA IF NOT EXISTS core;
COMMENT ON SCHEMA core IS 'Schema for core structured data: artifacts, entities, blobs, tags, etc.';

-- Domain schemas (e.g., domain_hyprland, domain_system_metrics) would be created by their respective promotion agents or setup scripts.
```

## 2. `raw.events` Table

The immutable, append-only log for all captured data.

```sql
CREATE TABLE IF NOT EXISTS raw.events (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(), -- From pgx_ulid
    source                  TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    ts_ingest               TIMESTAMPTZ NOT NULL DEFAULT now(),
    ts_orig                 TIMESTAMPTZ, -- Can be NULL if truly unknown, but highly encouraged
    host                    TEXT NOT NULL,
    ingestor_version        TEXT,
    payload_schema_id       ULID NULLABLE REFERENCES sinex_schemas.event_payload_schemas(id) ON DELETE SET NULL ON UPDATE CASCADE,
    payload                 JSONB NOT NULL
);

COMMENT ON TABLE raw.events IS 'Universal log for all captured raw events before promotion or detailed structuring. Immutable by principle.';
COMMENT ON COLUMN raw.events.id IS 'Globally unique, time-sortable ULID for the event (pgx_ulid).';
COMMENT ON COLUMN raw.events.source IS 'Canonical identifier for the event origin/producer (e.g., "desktop.hyprland.ipc_ingestor", "sinex.pkm.sync_agent").';
COMMENT ON COLUMN raw.events.event_type IS 'Type string for the event, often namespaced by source (e.g., "window_focused", "note_updated", "agent.heartbeat").';
COMMENT ON COLUMN raw.events.ts_ingest IS 'Timestamp of ingestion into this table (database server time). Primary TimescaleDB partitioning key.';
COMMENT ON COLUMN raw.events.ts_orig IS 'Original timestamp from the source system/sensor when the event occurred; best effort for accuracy.';
COMMENT ON COLUMN raw.events.host IS 'Identifier of the machine or device where the event originated.';
COMMENT ON COLUMN raw.events.ingestor_version IS 'Version of the ingestor code/binary that produced this event.';
COMMENT ON COLUMN raw.events.payload_schema_id IS 'ULID Foreign key to sinex_schemas.event_payload_schemas, identifying the schema for the payload. Null if ad-hoc/unknown.';
COMMENT ON COLUMN raw.events.payload IS 'Complete raw event data as JSONB. May contain a "_provenance" sub-object for lineage details (e.g., "agent_id_if_generated", "input_event_id_if_derived").';

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

-- GIN index for querying JSONB payload content.
-- jsonb_path_ops is generally preferred if specific paths are often queried.
-- jsonb_ops is more general but can be larger/slower for specific path lookups.
-- Choose based on expected query patterns. For general flexibility:
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_gin_path_ops ON raw.events USING GIN (payload jsonb_path_ops);
-- If full JSONB search is common:
-- CREATE INDEX IF NOT EXISTS idx_raw_events_payload_gin_ops ON raw.events USING GIN (payload);

-- TimescaleDB Hypertable Conversion (from TIM-TimescaleDBConfiguration.md)
-- Ensure this is run AFTER table creation and basic index setup if table might already have data.
-- SELECT create_hypertable(
--   'raw.events',
--   'ts_ingest',
--   if_not_exists => TRUE,
--   chunk_time_interval => INTERVAL '1 day', -- Adjust as needed
--   migrate_data => TRUE
-- );
```
*Note: The `payload_schema_id` FK constraint to `sinex_schemas.event_payload_schemas` is defined here, assuming `sinex_schemas.event_payload_schemas` (from `TIM-EventSchemaRegistry.md`) is created first.*

## 3. Function for `updated_at` Timestamps

A generic trigger function to automatically update `updated_at` columns. Used by various `core` tables.

```sql
CREATE OR REPLACE FUNCTION core.set_updated_at_trigger_func_generic()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION core.set_updated_at_trigger_func_generic() IS 'Generic trigger function to set current timestamp on updated_at column.';
```

