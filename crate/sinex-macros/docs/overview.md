# `sinex-macros` Overview

This crate exists because Rust proc macros must live in a dedicated `proc-macro` crate.

## Exported Macros

### `#[derive(EventPayload)]`

Derives `sinex_primitives::events::EventPayload` for struct payloads.

Supported attributes:

- `#[event_payload(source = "...", event_type = "...")]` (required)
- `#[event_payload(version = "...")]` (optional, defaults to `"1.0.0"`)

Generated behavior:

- `impl EventPayload` with:
  - `SOURCE`
  - `EVENT_TYPE`
  - `VERSION`
- fluent setters:
  - `with_<field>(value)` for each named struct field
  - `Option<T>` fields accept `impl Into<T>` and are wrapped in `Some(...)`
- schema registry inventory entry for non-generic payload structs

Notes:

- Derive supports struct generics for the `EventPayload` impl and builder methods.
- Generic payloads do not register a single inventory schema entry because schema registration is monomorphic.
- Enums and unions are rejected.
