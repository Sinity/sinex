# Core Types and Constants

This file catalogues the type families exported from `sinex-core`. Each section points to the
module that owns the implementation and highlights the invariants enforced by the type system.

## Identifiers & Namespacing

- [`sinex_schema::uuid::Uuid`](../../sinex-schema/docs/uuid.md) – canonical identifier used across all tables.
- `types::ids` – strongly-typed wrappers (`EventId`, `BlobId`, `OperationId`, etc.) that prevent mixing domains.
- `types::domain::EventSource` / `EventType` – validated wrappers for the `source` and `event_type` columns.
- `environment::SinexEnvironment` – scopes database schemas, JetStream subjects, sockets, and filesystem paths.

## Events & Payloads

- `types::events::Event` – logical representation of a persisted event, including provenance metadata.
- `types::events::payloads::*` – typed payload structs for each canonical event family (e.g. filesystem, terminal).
- `payloads::*` re-export – convenience namespace for consumers that prefer a flattened import path.
- Constants under `types::events::constants` – common subject names, DLQ identifiers, and validation helpers.

## Errors & Results

- `types::error::SinexError` – application-wide error enum with rich context (database, IO, validation, etc.).
- `types::error::Result<T>` / `SinexResult<T>` – crate-level aliases to reduce boilerplate.
- `types::error::context` helpers – attach tracing metadata while preserving original error kinds.

## Database Integration

- `db::pool` – connection pool builders and the `DbPoolExt` trait for accessing repositories.
- `db::repositories::*` – repository traits and concrete implementations for events, blobs, checkpoints, operations log, etc.
- `DbTransaction` alias – ergonomic wrapper for `sqlx::Transaction`.

## Validation & Utilities

- `validation::path::validate_path` – ensures filesystem input respects sandbox rules.
- `validation::json::validate_json` – JSON schema enforcement helpers used by ingest and services.
- `types::utils` – date/time helpers, UUIDv7 conversions, and serde helpers shared across crates.

## Working with these Types

- Always prefer the typed wrappers (`EventId`, `EventSource`, etc.) over raw strings—repositories and services assume validated input.
- Repository methods return strongly-typed records (e.g. `EventRecord`) that mirror the database schema; map them to domain objects in the caller.
- When introducing new event families or payloads, define the struct under `types::events::payloads`, add serde derives, and update the taxonomy in `crate/lib/sinex-schema/docs/event-taxonomy.md`.
