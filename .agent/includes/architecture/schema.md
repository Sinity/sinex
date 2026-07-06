## Database Schema

`core.events` is a TimescaleDB hypertable partitioned by UUIDv7 `id` (`by_range('id')`). `ts_coided` is a generated stored column derived from `id` for query ergonomics.

### Schema Map

| Schema | Key Tables | Purpose |
|--------|------------|---------|
| `core` | `events`, `blobs`, `node_manifests`, `entities`, `entity_relations`, `event_annotations`, `tags`, `tagged_items`, `event_embeddings`, `event_tombstones`, `operations_log` | Primary storage + knowledge graph + embeddings |
| `reflection` | `events` | Self-observation lane: same event model as `core.events` (LIKE shape, own hypertable) but its own stream, retention (30d + 7d compression), and read models — operator life-events never share retention policy with rebuildable telemetry. Events route here by `SourceRole::Reflection` via `EventStorageLane` |
| `raw` | `source_material_registry`, `temporal_ledger` | Provenance roots + observation timestamps |
| `audit` | `archived_events` | Immutable archive of deleted/superseded events (replay target) |
| `sinex_schemas` | `event_payload_schemas`, `validation_cache`, `dlq_events` | JSON schema registry + DLQ |
| `sinex_telemetry` | 9 continuous aggregates + 2 views + 1 materialized view | Self-observation and activity read models (see below) |
| `metrics` | (empty) | Reserved for future operational metrics |

### Telemetry Surface

Per issue #952 (closed), hot-path 1h/5m rollups bucketed on UUIDv7 `id` are
TimescaleDB continuous aggregates with hourly (or 5-minute for 5m buckets)
refresh policies. `current_health` and `recent_activity_summary` remain
ordinary views (one is a point-in-time aggregate over health events, the other
unions across CAs); `current_device_state` remains a regular materialized view
(latest-observation lookup, refreshed explicitly).

| Relation | Type | Bucket | What it tracks |
|----------|------|--------|----------------|
| `event_engine_batch_stats_1h` | Continuous aggregate | 1h | Batch size, latency, deferred/failed counts |
| `gateway_stats_1h` | Continuous aggregate | 1h | Request stats, latency, rate limits |
| `node_stats_1h` | Continuous aggregate | 1h | Events processed, latency, queue depth per runtime module |
| `stream_stats_1h` | Continuous aggregate | 1h | JetStream fill %, message counts |
| `metric_counters_1h` | Continuous aggregate | 1h | Named metric counter totals |
| `assembly_stats_1h` | Continuous aggregate | 1h | Material assembler state-machine activity |
| `command_frequency_hourly` | Continuous aggregate | 1h | Shell command execution frequency by UUIDv7 bucket |
| `file_activity_summary` | Continuous aggregate | 1h | Filesystem event counts by directory |
| `current_window_focus` | Continuous aggregate | 5m | Desktop window focus tracking |
| `current_system_state` | Continuous aggregate | 5m | CPU, memory, disk, systemd units |
| `current_health` | View | now | Latest health-aggregated reports per component |
| `recent_activity_summary` | View | now | Cross-source activity rollup (depends on CAs above) |
| `current_device_state` | Materialized view | now | Latest device state observation |

Source of truth: `TELEMETRY_VIEW_RELATIONS`,
`TELEMETRY_MATERIALIZED_VIEW_RELATIONS`, and `TELEMETRY_CONTINUOUS_AGGREGATES`
in `crate/sinex-schema/src/apply.rs`.

### Schema Convergence

Schema evolution uses declarative convergence (`sinex-schema apply`), not migrations. The apply engine diffs desired state against actual DB state and converges: adding columns, indexes, constraints, functions. Named CHECK constraints are converged; inline column CHECKs are not.

- Schema source: `crate/sinex-schema/src/defs/`
- Apply engine: `crate/sinex-schema/src/apply.rs`
- Strict diff: `crate/sinex-schema/src/strict_diff.rs`
- Design: `crate/sinex-db/docs/schema/schema_design.md`

**Drift detection**: `apply::diff` reports the categories it converges
(missing tables, columns, named constraints, indexes, triggers, views,
continuous aggregates). For categories the convergence engine does NOT
reconcile — trigger function bodies that survived a manual edit,
DEFAULT changes on existing columns, inline CHECKs, FK
ON DELETE / ON UPDATE actions, TimescaleDB hypertable settings (chunk
interval + retention policy presence) — call `strict_diff::check_strict`
(or run the `schema-strict-diff` binary against `DATABASE_URL`).
Comments / table descriptions remain a non-goal per #556. Issues #578
and #579 track real source-vs-live drift the strict diff caught on
fresh-apply state.
