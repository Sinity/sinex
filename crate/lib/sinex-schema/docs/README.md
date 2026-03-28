# Sinex Schema

The single source of truth for the Sinex database schema, implemented with `sea-query` schema declarations plus a declarative `apply()` convergence engine.

## Core Components

- **Schema Definitions**: `src/schema/*.rs` defines tables, columns, and constraints using type-safe builders.
- **Schema Apply Engine**: `src/apply.rs` converges a database to the declared schema idempotently.
- **Identifiers**: canonical schema uses native `UUID` columns. Generated IDs use `PostgreSQL` `UUIDv7` functions where defaults are needed.

## Key Features

- **Provenance**: every event tracks external source material or parent events via an XOR constraint.
- **Immutability**: `core.events` is append-only, enforced by triggers.
- **TimescaleDB**: `core.events` is a hypertable partitioned by `id` using `uuid_extract_timestamp(id)`; `ts_coided` remains a stored generated timestamp derived from `UUIDv7` `id` for query ergonomics.
- **Self-Observation**: continuous aggregates track ingest-time system telemetry, and event-time
  views power user-facing activity read models.

## Documentation

- `apply.md`: schema apply strategy and operational checks.
- `schema_design.md`: current schema patterns and constraints.
- `architecture.md`: crate-level architectural decisions and integration points.
