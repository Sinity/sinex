## Database Schema

`core.events` is a TimescaleDB hypertable partitioned by UUIDv7 `id` (`by_range('id')`). `ts_coided` is a generated stored column derived from `id` for query ergonomics.

### Schema Map

| Schema | Key Tables | Purpose |
|--------|------------|---------|
| `core` | `events`, `blobs`, `node_manifests`, `entities`, `entity_relations`, `event_annotations`, `tags`, `tagged_items`, `event_embeddings`, `event_tombstones`, `operations_log` | Primary storage + knowledge graph + embeddings |
| `raw` | `source_material_registry`, `temporal_ledger` | Provenance roots + observation timestamps |
| `audit` | `archived_events` | Immutable archive of deleted/superseded events (replay target) |
| `sinex_schemas` | `event_payload_schemas`, `validation_cache`, `dlq_events` | JSON schema registry + DLQ |
| `sinex_telemetry` | 12 views + 1 materialized view | Self-observation and activity read models (see below) |
| `metrics` | (empty) | Reserved for future operational metrics |

### Telemetry Surface

There are no continuous aggregates currently — all rollups live as ordinary
views or, for `current_device_state`, a single materialized view. The bucket
column below describes the time grain of each rollup, not a TimescaleDB
refresh policy.

| Relation | Type | Bucket | What it tracks |
|----------|------|--------|----------------|
| `ingestd_batch_stats_1h` | View | 1h | Batch size, latency, deferred/failed counts |
| `gateway_stats_1h` | View | 1h | Request stats, latency, rate limits |
| `node_stats_1h` | View | 1h | Events processed, latency, queue depth per node |
| `stream_stats_1h` | View | 1h | JetStream fill %, message counts |
| `metric_counters_1h` | View | 1h | Named metric counter totals |
| `assembly_stats_1h` | View | 1h | Material assembler state-machine activity |
| `current_health` | View | now | Latest health-aggregated reports per component |
| `command_frequency_hourly` | View | 1h | Shell command execution frequency by `ts_orig` |
| `file_activity_summary` | View | 1h | Filesystem event counts by directory by `ts_orig` |
| `current_window_focus` | View | 5m | Desktop window focus tracking by `ts_orig` |
| `current_system_state` | View | 5m | CPU, memory, disk, systemd units by `ts_orig` |
| `recent_activity_summary` | View | now | Cross-source activity rollup by `ts_orig` |
| `current_device_state` | Materialized view | now | Latest device state observation |

Source of truth: `TELEMETRY_VIEW_RELATIONS`,
`TELEMETRY_MATERIALIZED_VIEW_RELATIONS`, and `TELEMETRY_CONTINUOUS_AGGREGATES`
in `crate/lib/sinex-schema/src/apply.rs`.

### Schema Convergence

Schema evolution uses declarative convergence (`sinex-schema apply`), not migrations. The apply engine diffs desired state against actual DB state and converges: adding columns, indexes, constraints, functions. Named CHECK constraints are converged; inline column CHECKs are not.

- Schema source: `crate/lib/sinex-schema/src/schema/`
- Apply engine: `crate/lib/sinex-schema/src/apply.rs`
- Design: `crate/lib/sinex-schema/docs/schema_design.md`
