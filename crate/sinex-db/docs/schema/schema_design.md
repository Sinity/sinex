# Database Schema Design

This document describes the current schema design used by Sinex.

## Design Goals

- preserve immutable event history
- enforce provenance integrity at the database boundary
- support high-throughput ingestion with time-series query performance
- keep schema evolution explicit and declarative

## Identifier Model

All identifiers are native `PostgreSQL` `uuid`.

- event IDs, entity IDs, schema IDs, and relationship IDs use UUID columns
- list relationships use `uuid[]` where arrays are intentional
- query bindings and casts should remain UUID-native

## Schema Inventory

Sinex partitions its relational surface across seven namespaces, each with a distinct role:

| Schema | Key Tables / Views | Purpose |
|--------|---------------------|---------|
| `core` | `events`, `blobs`, `node_manifests`, `entities`, `entity_relations`, `event_annotations`, `tags` | Primary storage + knowledge graph |
| `raw` | `source_material_registry`, `temporal_ledger` | Provenance roots + observation timestamps |
| `audit` | `archived_events` | Immutable archive (replay target) |
| `sinex_schemas` | `event_payload_schemas`, `validation_cache`, `dlq_events` | Schema registry + DLQ |
| `sinex_telemetry` | hourly operator views, activity/status views, one materialized device-state view | Self-observation |
| `metrics` | via schema registry | Operational metrics |
| `public` | default | `PostgreSQL` default schema |

Schema evolution uses **declarative convergence** (`sinex-schema apply`), not migrations. The apply engine diffs desired state against actual DB state and converges. Schema-source status and gitops integration details: [`gitops-schema-sources-status.md`](gitops-schema-sources-status.md); apply-engine mechanics: [`apply.md`](apply.md).

## Event Storage Model

`core.events` is the central append-only log.

- `ts_coided` is the canonical ingest timestamp
- `ts_persisted` records when the row was written to storage
- `TimescaleDB` uses native `UUIDv7` time partitioning on `id` (`by_range('id')`)
- `ts_coided` is generated from `id` (`uuid_extract_timestamp(id)`) and remains the canonical query timestamp for ordering
- indexes prioritize source/event-type/time filters and replay paths

## Provenance Model

Event provenance is explicit and enforced.

- external provenance: `source_material_id`
- internal provenance: `source_event_ids`
- XOR constraints enforce exactly one provenance path per event

## Validation and Search

- payload validation uses `pg_jsonschema`
- semantic retrieval uses `pgvector`
- text and pattern filters use `pg_trgm`-assisted indexes where needed

## Operational Constraints

- append-only guarantees are enforced by trigger/constraint logic
- destructive operations are limited to explicit retention/archive workflows
- declarative schema changes are validated through repository tooling before deploy

### Operations Log

`core.operations_log` records every significant data-altering action (replay, restore, stage) with parameters, timing, and links to affected events. Replay on its own is not inspectable from event state alone — the operations log is the separate audit trail that makes "how did this state change over time" a queryable question. It complements the append-only discipline of `core.events` rather than weakening it.

## Query Guidance

- bind UUID parameters directly (`$1::uuid`, `$1::uuid[]`)
- keep ordering explicit for deterministic replay (`ORDER BY ts_coided DESC, id DESC`)
- prefer index-aligned predicates for source/type/time paths
