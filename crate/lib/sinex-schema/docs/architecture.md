# Sinex Schema Architecture

This document summarizes the architectural decisions used in `sinex-schema`.

## Overview

`sinex-schema` is the canonical schema definition crate for Sinex. It owns:

- table/constraint/index definitions via `sea-query`
- migration ordering and execution via `sea-orm-migration`
- record structs used by integration layers
- schema-level invariants that must hold independent of application behavior

## Core Design

### 1. Programmatic Schema Source of Truth

All structural database elements are defined in Rust so migration SQL and compile-time schema references stay aligned.

### 2. Native UUID Identifier Model

IDs are stored as PostgreSQL `uuid`.

- Primary and foreign keys bind as native UUID values.
- Default ID generation uses UUIDv7 database functions where appropriate.
- Query code should bind UUID parameters directly, with no custom ID-cast layers.

### 3. Time-Series Partitioning on UUIDv7 IDs

`core.events` uses TimescaleDB hypertables with `id` as the partition column and
`uuid_extract_timestamp(id)` as the time partition function.

- `ts_coided` is generated from `id` for query ergonomics and explicit event ordering
- time semantics remain aligned to UUIDv7 creation timestamp extraction
- deterministic replay remains explicit through query ordering (for example `ORDER BY ts_coided DESC, id DESC`)

### 4. Constraint-First Integrity

Critical invariants are encoded in database constraints and triggers.

- provenance XOR rules
- append-only protections for immutable event data
- typed indexes for time, provenance, and payload-access patterns

## Integration Boundaries

### Migrations

- one canonical base migration for clean provisioning
- incremental migrations for ongoing changes
- repo tooling (`xtask`) for readiness and consistency checks

### Extensions

Schema and operational paths assume extension support consistent with repository infrastructure policy, including `timescaledb`, `pg_jsonschema`, `vector`, and `pg_trgm`.

## Testing

Schema behavior is verified through:

- migration readiness checks
- repository integration tests against real PostgreSQL + TimescaleDB environments
- targeted constraint/index tests for invariant enforcement
