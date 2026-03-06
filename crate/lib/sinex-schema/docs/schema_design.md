# Database Schema Design

This document describes the current schema design used by Sinex.

## Design Goals

- preserve immutable event history
- enforce provenance integrity at the database boundary
- support high-throughput ingestion with time-series query performance
- keep schema evolution explicit and declarative

## Identifier Model

All identifiers are native PostgreSQL `uuid`.

- event IDs, entity IDs, schema IDs, and relationship IDs use UUID columns
- list relationships use `uuid[]` where arrays are intentional
- query bindings and casts should remain UUID-native

## Event Storage Model

`core.events` is the central append-only log.

- `ts_coided` is the canonical ingest timestamp
- TimescaleDB partitions by `id` using `uuid_extract_timestamp(id)`
- `ts_coided` remains generated from `id` for query semantics; generated columns are not used as hypertable partition dimensions
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

## Query Guidance

- bind UUID parameters directly (`$1::uuid`, `$1::uuid[]`)
- keep ordering explicit for deterministic replay (`ORDER BY ts_coided DESC, id DESC`)
- prefer index-aligned predicates for source/type/time paths
