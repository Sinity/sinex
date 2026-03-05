## Database Schema

### Core Tables (`core.*`)

- `core.events` - Primary event storage (TimescaleDB hypertable)
- `core.blobs` - Binary blob metadata
- `core.source_materials` - Raw source data references
- `core.processors` - Registered node metadata
- `core.embeddings` - Vector embeddings for semantic search (pgvector)

**TimescaleDB Configuration**: The `core.events` hypertable uses `id` as the partition column with `uuid_extract_timestamp` as the time partition function. IDs are UUIDv7 (stored as `uuid`) and `ts_ingest` is generated from `id`.

### Knowledge Graph (`entities.*`)

- `entities.entities` - Graph nodes
- `entities.entity_relations` - Graph edges

### Event Schemas (`sinex_schemas.*`)

- `sinex_schemas.event_payload_schemas` - JSON schema registry

### Raw/Staging (`raw.*`)

- Staging tables for batch ingest operations

### Audit (`audit.*`)

- Archived/soft-deleted events and tombstone records

### Metrics (`metrics.*`)

- Declared but currently unused; reserved for future operational metrics

### Telemetry (`sinex_telemetry.*`)

- Self-observation continuous aggregates: gateway stats, stream stats, node stats, assembly stats, health views
- Created by migration `m20250117_000011`

### All Schemas Summary

| Schema | Purpose | Status |
|--------|---------|--------|
| `public` | PostgreSQL default (extensions) | Active |
| `core` | Primary event storage | Active |
| `raw` | Staging/ingest data | Active |
| `audit` | Archived/tombstoned events | Active |
| `entities` | Knowledge graph | Active |
| `sinex_schemas` | JSON schema registry | Active |
| `metrics` | Operational metrics | Reserved |
| `sinex_telemetry` | Continuous aggregates for observability | Active |

### Schema Details

- Full schema: `crate/lib/sinex-schema/src/schema/`
- Migrations: `crate/lib/sinex-schema/src/migrations/`
- Design doc: `crate/lib/sinex-schema/docs/schema_design.md`
