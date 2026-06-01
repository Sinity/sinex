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

### `#[derive(SinexConfig)]`

Derives a `from_env()` constructor for environment-driven configuration
structs.

Supported struct attributes:

- `#[sinex_config(prefix = "...", context = "...")]` (required)
- `#[sinex_config(fallible)]` (optional, makes `from_env()` return
  `Result<Self, SinexError>`)
- `#[sinex_config(normalize_fn = "...")]` (optional post-construction
  normalization)

Supported field attributes:

- `#[sinex_config(env = "FULL_ENV_KEY")]`
- `#[sinex_config(default = LITERAL)]`
- `#[sinex_config(default_expr = "EXPR")]`
- `#[sinex_config(default_fn = "function_name")]`
- `#[sinex_config(parser = path::to::fn)]`
- `#[sinex_config(duration_secs)]`
- `#[sinex_config(nested)]`
- `#[sinex_config(nested_fallible)]`
- `#[sinex_config(skip)]`

Generated behavior:

- infers strict or lenient `sinex_primitives::env::*` helpers from field types
- preserves explicit env-key namespaces through `env = "..."`
- parses `std::time::Duration` from positive second counts when
  `duration_secs` is present
- delegates nested config fields through their own `from_env()` constructors
- calls an optional normalize method after construction

Notes:

- This derive is for declarative env-to-field mapping. Configs with runtime
  constructor arguments, private-field factories, file-plus-env overlay
  semantics, or nested map parsing with bespoke validation stay hand-rolled.
- See `sinex_config.md` for the full grammar and the current migration audit.
