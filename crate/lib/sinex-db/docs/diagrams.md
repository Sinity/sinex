# Database Architecture Diagrams

## Schema Overview

```
Schemas:
  - core                  (main event + metadata tables)
  - raw                   (ingest ledger + source registry)
  - audit                 (archived events + tombstones)
  - entities              (knowledge graph)
  - sinex_schemas         (schema registry + manifests)

┌─────────────────────────────────────────────────────────────────────┐
│                       core.events (Hypertable)                       │
│                                                                       │
│  Columns:                                                             │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ id                    UUIDv7 PRIMARY KEY                         │  │
│  │ source                TEXT NOT NULL                            │  │
│  │ event_type            TEXT NOT NULL                            │  │
│  │ host                  TEXT NOT NULL                            │  │
│  │ payload               JSONB NOT NULL                           │  │
│  │ ts_orig               TIMESTAMPTZ                              │  │
│  │ ts_coided             TIMESTAMPTZ GENERATED FROM UUIDv7 id     │  │
│  │ ts_persisted          TIMESTAMPTZ NOT NULL DEFAULT NOW()       │  │
│  │ source_material_id    UUIDv7                                     │  │
│  │ anchor_byte           BIGINT                                   │  │
│  │ offset_start          BIGINT                                   │  │
│  │ offset_end            BIGINT                                   │  │
│  │ offset_kind           TEXT                                     │  │
│  │ source_event_ids      UUIDv7[]                                   │  │
│  │ associated_blob_ids   UUIDv7[]                                   │  │
│  │ payload_schema_id     UUIDv7                                     │  │
│  │ ingestor_version      TEXT                                     │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  Partitioning (TimescaleDB Hypertable):                               │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ - Partition by: id (partition_func=uuid_extract_timestamp)     │  │
│  │ - Chunk interval: 7 days (default)                             │  │
│  │ - Automatic chunk creation                                     │  │
│  │ - Partition pruning on time-range queries                      │  │
│  │                                                                 │  │
│  │ Chunks (auto-created):                                         │  │
│  │   _hyper_1_1_chunk  (2025-01-01 to 2025-01-08)                 │  │
│  │   _hyper_1_2_chunk  (2025-01-08 to 2025-01-15)                 │  │
│  │   _hyper_1_3_chunk  (2025-01-15 to 2025-01-22)  ← active       │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  Indexes:                                                             │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ PRIMARY KEY (id)                                               │  │
│  │ CREATE INDEX ix_events_ts_coided ON core.events(ts_coided)    │  │
│  │ CREATE INDEX idx_events_source ON core.events(source)         │  │
│  │ CREATE INDEX idx_events_event_type ON core.events(event_type) │  │
│  │ CREATE INDEX idx_events_payload_gin ON core.events            │  │
│  │   USING GIN (payload jsonb_path_ops)                          │  │
│  │ CREATE INDEX idx_events_source_material                        │  │
│  │   ON core.events(source_material_id) WHERE source_material_id  │  │
│  │   IS NOT NULL                                                  │  │
│  └───────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│              raw.source_material_registry                            │
│                                                                       │
│  Purpose: Tracks source materials and their provenance roots         │
│                                                                       │
│  Columns:                                                             │
│  - id (UUIDv7)                                                          │
│  - material_type (text, binary, structured)                           │
│  - content_hash (SHA256)                                              │
│  - size_bytes                                                         │
│  - storage_path (Git Annex key)                                       │
│  - created_at                                                         │
│                                                                       │
│  Storage Backend: Git Annex                                           │
│  - Large files (>1MB) stored in annex                                 │
│  - Deduplication via content hash                                     │
│  - Symlinks in .git/annex/objects/                                    │
└───────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                        core.blobs                                     │
│                                                                       │
│  Purpose: Binary data attached to events (screenshots, recordings)    │
│                                                                       │
│  Columns:                                                             │
│  - id (UUIDv7)                                                          │
│  - mime_type                                                          │
│  - size_bytes                                                         │
│  - content_hash                                                       │
│  - storage_backend (postgres, filesystem, s3)                         │
│  - data (BYTEA, nullable)  ← Small blobs inline                       │
│  - external_path           ← Large blobs external                     │
└───────────────────────────────────────────────────────────────────────┘
```

## Repository Pattern Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    sinex-core/db/repositories/                       │
│                                                                       │
│  Base Trait: Repository<'a>                                          │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ trait Repository<'a> {                                         │  │
│  │     fn pool(&self) -> &'a PgPool;                              │  │
│  │     fn new(pool: &'a PgPool) -> Self;                          │  │
│  │ }                                                               │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  Concrete Repositories:                                               │
│  ┌────────────────────────────────────────────────────────────┬────┐│
│  │ EventRepository<'a>                                        │ ⭐ ││
│  │ - insert(event) -> Event                                   │    ││
│  │ - insert_batch(events) -> Vec<Event>                       │    ││
│  │ - get_by_id(id) -> Option<Event>                           │    ││
│  │ - search(filters) -> Vec<Event>                            │    ││
│  │ - get_events_over_time(range, interval) -> Vec<Bucket>     │    ││
│  └────────────────────────────────────────────────────────────┴────┘│
│  ┌────────────────────────────────────────────────────────────┐    ││
│  │ SourceMaterialRepository<'a>                               │    ││
│  │ - insert(material) -> SourceMaterial                       │    ││
│  │ - get_by_id(id) -> Option<SourceMaterial>                  │    ││
│  │ - get_by_hash(hash) -> Option<SourceMaterial>              │    ││
│  └────────────────────────────────────────────────────────────┘    ││
│  ┌────────────────────────────────────────────────────────────┐    ││
│  │ CheckpointRepository<'a>                                   │    ││
│  │ - get_latest(node) -> Option<Checkpoint>                   │    ││
│  │ - save(checkpoint) -> ()                                   │    ││
│  └────────────────────────────────────────────────────────────┘    ││
│                                                                       │
│  DbPoolExt Trait (Ergonomic Access):                                 │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ impl DbPoolExt for PgPool {                                   │  │
│  │     fn events(&self) -> EventRepository<'_> { ... }           │  │
│  │     fn source_materials(&self) -> SourceMaterialRepository { }│  │
│  │     fn checkpoints(&self) -> CheckpointRepository { ... }     │  │
│  │ }                                                              │  │
│  │                                                                │  │
│  │ Usage:                                                         │  │
│  │   let event = pool.events().get_by_id(id).await?;             │  │
│  │   let materials = pool.source_materials().search(q).await?;   │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## SQLX Compile-Time Validation

```
Flow during `cargo build`:
┌───────────────────────────────────────────────────────────────────┐
│ 1. SQLX Macros (sqlx::query!, query_as!)                          │
│    ↓                                                               │
│ 2. Connect to DATABASE_URL at compile-time                        │
│    ↓                                                               │
│ 3. Execute PREPARE query                                          │
│    - Check syntax                                                  │
│    - Check table/column existence                                  │
│    - Infer result types                                            │
│    ↓                                                               │
│ 4. Generate Rust struct matching result                           │
│    ↓                                                               │
│ 5. Type-check bindings                                             │
│    - Parameter types match                                         │
│    - Nullability correct                                           │
│    ↓                                                               │
│ 6. Compile succeeds OR fails with helpful error                   │
└───────────────────────────────────────────────────────────────────┘

Benefits:
✅ Typos caught at compile-time (not runtime!)
✅ Schema changes break build (immediate feedback)
✅ Nullability enforced (no unexpected NULLs)
✅ Zero runtime overhead (all validation done at build time)
```

## TimescaleDB Features

```
1. Hypertable Partitioning
   - Automatic chunk creation based on time
   - Partition pruning (query only relevant chunks)
   - Efficient time-range queries

2. time_bucket() Function
   SELECT time_bucket('1 hour', ts_coided) as hour,
          COUNT(*) as event_count
   FROM core.events
   WHERE ts_coided >= NOW() - INTERVAL '24 hours'
   GROUP BY hour
   ORDER BY hour;

3. Continuous Aggregates (Future)
   - Pre-compute materialized views
   - Automatically refresh
   - Fast dashboard queries

4. Compression (Future)
   - Compress old chunks
   - Save storage
   - Still queryable

5. Retention Policies (Planned)
   SELECT add_retention_policy('core.events', INTERVAL '90 days');
```

## See Also

- Patterns: [patterns.md](./patterns.md)
- Schema design: `crate/lib/sinex-schema/docs/schema_design.md`
