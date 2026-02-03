## Database Schema

### Core Tables (`core.*`)

- `core.events` - Primary event storage (TimescaleDB hypertable)
- `core.blobs` - Binary blob metadata
- `core.source_materials` - Raw source data references
- `core.processors` - Registered node metadata
- `core.embeddings` - Vector embeddings for semantic search (pgvector)

**TimescaleDB Configuration**: The `core.events` hypertable uses `id` (ULID) as the time dimension with `ulid_to_timestamptz()` partition function. This provides optimal partitioning (primary key = partition key) but prevents TimescaleDB continuous aggregates, which require native timestamp types. Current state tracking uses standard PostgreSQL materialized views instead. See `docs/current/analysis/timescaledb-ulid-continuous-aggregates.md` for details.

### Knowledge Graph (`entities.*`)

- `entities.entities` - Graph nodes
- `entities.entity_relations` - Graph edges

### Event Schemas (`sinex_schemas.*`)

- `sinex_schemas.event_payload_schemas` - JSON schema registry

### Schema Details

- Full schema: `crate/lib/sinex-schema/src/schema/`
- Migrations: `crate/lib/sinex-schema/src/migrations/`
- Design doc: `crate/lib/sinex-schema/docs/schema_design.md`
