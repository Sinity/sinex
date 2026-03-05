# Sinex Schema

The single source of truth for the Sinex database schema, implemented with `sea-orm-migration` and `sea-query`.

## Core Components

- **Schema Definitions**: `src/schema/*.rs` defines tables, columns, and constraints using type-safe builders.
- **Migrations**: `src/migrations/` contains the ordered history of database changes.
- **Identifiers**: canonical schema uses native `UUID` columns. Generated IDs use PostgreSQL UUIDv7 functions where defaults are needed.

## Key Features

- **Provenance**: every event tracks external source material or parent events via an XOR constraint.
- **Immutability**: `core.events` is append-only, enforced by triggers.
- **TimescaleDB**: `core.events` is a hypertable partitioned by `id` with `uuid_extract_timestamp(id)` as the time partition function.
- **Self-Observation**: continuous aggregates track system health and metrics.

## Documentation

- `migrations.md`: migration strategy and operational checks.
- `schema_design.md`: current schema patterns and constraints.
- `architecture.md`: crate-level architectural decisions and integration points.
