# Sinex Macros

Procedural macro toolkit that keeps Sinex ergonomics consistent across crates.

## Core Macros

The primary macro in production use is `#[derive(EventPayload)]`, which powers over 100 event types in `sinex-core`.

### `#[derive(EventPayload)]`

Automatically implements the `EventPayload` trait with `SOURCE` and `EVENT_TYPE` constants, generates builder methods, and registers the schema.

```rust
#[derive(EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.created")]
pub struct FileCreatedPayload { ... }
```

## Experimental / Unused Macros

The following macros are implemented but currently unused or deprecated:

-   `#[with_context]`: Error context enrichment (Non-functional, see BUG-020).
-   `#[derive(ValidateRecord)]`: Schema validation (Non-functional, see BUG-019).
-   `db_query!`: Database query helper.
-   `db_transaction!`: Transaction wrapper.
-   `event_registry!`: Legacy event registration.
-   `typed_event_envelope`: Typed enum envelope.
-   `define_id_type!`: Typed ID generation (superseded by generic `Id<T>`).

See `docs/usage_audit.md` for detailed analysis.