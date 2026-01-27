# Sinex Schema

The single source of truth for the Sinex database schema, implemented using `sea-orm-migration` and `sea-query`.

## Core Components

-   **Schema Definitions**: `src/schema/*.rs` defines tables, columns, and constraints using type-safe builders.
-   **Migrations**: `src/migrations/` contains the ordered history of database changes.
-   **ULID**: `src/ulid.rs` provides the robust, monotonic ULID implementation used as primary keys.

## Key Features

-   **Provenance**: Every event tracks its source material or parent events via XOR constraints.
-   **Immutability**: `core.events` is append-only, enforced by triggers.
-   **TimescaleDB**: `core.events` is a hypertable partitioned by ULID timestamp.
-   **Self-Observation**: Continuous aggregates track system health and metrics.

## Documentation

-   `migrations.md`: Analysis of migration history and safety.
-   `ulid.md`: Deep dive into ULID generation and conversion.
-   `schema_design.md`: Architectural decisions behind the schema.