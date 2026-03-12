## Database Schema

### Core Tables (`core.*`)

| Table | Purpose |
|-------|---------|
| `core.events` | Primary event storage (TimescaleDB hypertable, UUIDv7 partitioned) |
| `core.blobs` | Content-addressed binary blob metadata |
| `core.node_manifests` | Node registration, version tracking, and heartbeat health |
| `core.entities` | Knowledge graph nodes |
| `core.entity_relations` | Knowledge graph edges |
| `core.event_annotations` | Rich structured annotations on events |
| `core.tags` | Tag definitions with hierarchy support |
| `core.tagged_items` | Polymorphic many-to-many tag associations |
| `core.event_embeddings` | Vector embeddings for semantic search (pgvector) |
| `core.embedding_models` | Registry of ML embedding model metadata |
| `core.embedding_cache` | Cache for computed embedding vectors |
| `core.event_clusters` | Clusters of semantically similar events |
| `core.event_cluster_members` | Junction table for cluster membership |
| `core.event_tombstones` | Lightweight skeletons for permanently purged events |
| `core.operations_log` | Audit trail of high-level system operations |

**TimescaleDB Configuration**: The `core.events` hypertable uses native UUIDv7 time partitioning on `id` (`by_range('id')`). `ts_coided` is a generated timestamptz (stored) derived from `id` for query ergonomics, and continuous aggregates bucket on `id`.

### Event Schemas (`sinex_schemas.*`)

| Table | Purpose |
|-------|---------|
| `sinex_schemas.event_payload_schemas` | Central JSON schema registry for event validation |
| `sinex_schemas.validation_cache` | Cached validation results for (event, schema) pairs |
| `sinex_schemas.gitops_schema_sources` | Git repositories as sources of truth for schemas |
| `sinex_schemas.dlq_events` | Dead-letter queue for events that failed automaton processing |

### Raw/Staging (`raw.*`)

| Table | Purpose |
|-------|---------|
| `raw.source_material_registry` | Manifest of all external data artifacts (provenance roots) |
| `raw.temporal_ledger` | High-precision append-only log tracking when source materials were observed |

### Audit (`audit.*`)

| Table | Purpose |
|-------|---------|
| `audit.archived_events` | Immutable archive of deleted/superseded events |

### Metrics (`metrics.*`)

- Declared but currently unused; reserved for future operational metrics

### Telemetry (`sinex_telemetry.*`)

Self-observation continuous aggregates and views, created by declarative SQL in `sinex-schema`:

| View | Type | Bucket | Source |
|------|------|--------|--------|
| `gateway_stats_1h` | Continuous aggregate | 1h | Gateway request stats, latency, rate limits |
| `stream_stats_1h` | Continuous aggregate | 1h | JetStream fill %, message counts |
| `assembly_stats_1h` | Continuous aggregate | 1h | Material assembly completion stats |
| `node_stats_1h` | Continuous aggregate | 1h | Node events processed, latency, queue depth |
| `metric_counters_1h` | Continuous aggregate | 1h | Named metric counter totals |
| `ingestd_batch_stats_1h` | Continuous aggregate | 1h | Batch size, latency, deferred/failed counts |
| `current_window_focus` | Continuous aggregate | 5m | Desktop window focus tracking |
| `command_frequency_hourly` | Continuous aggregate | 1h | Shell command execution frequency |
| `file_activity_summary` | Continuous aggregate | 1h | Filesystem event counts by directory |
| `current_system_state` | Continuous aggregate | 5m | CPU, memory, disk, systemd units |
| `current_health` | Regular view | — | Latest health status per source |
| `current_device_state` | Materialized view | — | Latest systemd/udev state by unit |
| `recent_activity_summary` | Regular view | — | Union of recent focus, system, commands |

### All Schemas Summary

| Schema | Purpose | Status |
|--------|---------|--------|
| `public` | PostgreSQL default (extensions) | Active |
| `core` | Primary event storage + knowledge graph + embeddings | Active |
| `raw` | Source material registry + temporal ledger | Active |
| `audit` | Archived/tombstoned events | Active |
| `sinex_schemas` | JSON schema registry + validation cache + DLQ | Active |
| `metrics` | Operational metrics | Reserved |
| `sinex_telemetry` | Continuous aggregates for observability | Active |

### Schema Details

- Full schema: `crate/lib/sinex-schema/src/schema/`
- Apply engine: `crate/lib/sinex-schema/src/apply.rs`
- Design doc: `crate/lib/sinex-schema/docs/schema_design.md`
