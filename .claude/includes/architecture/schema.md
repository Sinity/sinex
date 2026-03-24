## Database Schema

`core.events` is a TimescaleDB hypertable partitioned by UUIDv7 `id` (`by_range('id')`). `ts_coided` is a generated stored column derived from `id` for query ergonomics.

### Schema Map

| Schema | Key Tables | Purpose |
|--------|------------|---------|
| `core` | `events`, `blobs`, `node_manifests`, `entities`, `entity_relations`, `event_annotations`, `tags`, `tagged_items`, `event_embeddings`, `event_tombstones`, `operations_log` | Primary storage + knowledge graph + embeddings |
| `raw` | `source_material_registry`, `temporal_ledger` | Provenance roots + observation timestamps |
| `audit` | `archived_events` | Immutable archive of deleted/superseded events (replay target) |
| `sinex_schemas` | `event_payload_schemas`, `validation_cache`, `dlq_events` | JSON schema registry + DLQ |
| `sinex_telemetry` | 10 continuous aggregates, 3 views | Self-observation (see below) |
| `metrics` | (empty) | Reserved for future operational metrics |

### Telemetry CAs (Continuous Aggregates)

| CA | Bucket | What it tracks |
|----|--------|----------------|
| `ingestd_batch_stats_1h` | 1h | Batch size, latency, deferred/failed counts |
| `gateway_stats_1h` | 1h | Request stats, latency, rate limits |
| `node_stats_1h` | 1h | Events processed, latency, queue depth per node |
| `stream_stats_1h` | 1h | JetStream fill %, message counts |
| `command_frequency_hourly` | 1h | Shell command execution frequency |
| `file_activity_summary` | 1h | Filesystem event counts by directory |
| `current_window_focus` | 5m | Desktop window focus tracking |
| `current_system_state` | 5m | CPU, memory, disk, systemd units |

CAs bucket on `id` (UUIDv7 time), not `ts_orig`. **Historical imports are invisible to CAs** until `CALL refresh_continuous_aggregate()` is run manually.

### Schema Convergence

Schema evolution uses declarative convergence (`sinex-schema apply`), not migrations. The apply engine diffs desired state against actual DB state and converges: adding columns, indexes, constraints, functions. Named CHECK constraints are converged; inline column CHECKs are not.

- Schema source: `crate/lib/sinex-schema/src/schema/`
- Apply engine: `crate/lib/sinex-schema/src/apply.rs`
- Design: `crate/lib/sinex-schema/docs/schema_design.md`
