# Event Schema Registry

The Schema Registry manages JSON Schemas used to validate event payloads. it ensures that all ingested events adhere to defined structures and enables content-addressed deduplication and historical validation.

## Architecture

The system consists of three main components:

1. **Schema Management**: Persistence and lifecycle of schemas in PostgreSQL (`sinex_schemas.event_payload_schemas`).
2. **Schema Cache**: A read-optimized layer for high-throughput lookup during ingestion.
3. **Validation Cache**: A database-level cache (`sinex_schemas.validation_cache`) that stores the results of previous validations to avoid redundant compute.

## Content-Addressed Storage

Schemas are identified by a BLAKE3 content hash of their metadata (source, event type, version) and the JSON schema itself.

- **Deduplication**: Identical schemas produce the same hash, preventing redundant storage.
- **Idempotency**: Registering the same schema multiple times is a safe, no-op operation.
- **Versioning**: Only one schema can be "active" for a given source and event type at any time. Registering a new version automatically deactivates the previous one.

## Validation Workflow

### In-Process Validation
The `EventValidator` uses the `jsonschema` crate to perform validation within the application process. Active schemas are loaded and compiled into memory for maximum performance.

### Validation Caching
To optimize performance for replayed or re-processed events:
1. The system checks the `validation_cache` for an existing `(event_id, schema_id)` entry.
2. If found, the cached result is returned immediately.
3. If not found, the payload is validated, and the result is persisted to the cache.

## Schema Discovery & Sync

Schemas are typically defined as Rust structs using the `EventPayload` derive macro.

- **Compile-time Discovery**: The macro registers schema metadata using the `inventory` crate.
- **Automated Runtime Sync**: preflight / ingest startup collects all registered schemas and synchronizes them with the database registry.
- **Manual Registration**: Dynamic schemas can also be registered at runtime via the `register_schema` API.

## Historical Validation

Every event record can store a `payload_schema_id`. This allows the system to validate old events against the specific schema version that was active when they were first ingested, even if the "current" schema for that event type has since changed.
