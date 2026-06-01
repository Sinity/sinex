# `#[derive(SinexConfig)]`

Generates `pub fn from_env() -> Self` (or `pub fn from_env() -> Result<Self,
SinexError>` in `fallible` mode) that reads each non-`skip` field from an
environment variable using `sinex_primitives::env::*` helpers.

## Required struct attributes

```rust,ignore
#[derive(SinexConfig)]
#[sinex_config(prefix = "SINEX_DB", context = "database pool")]
pub struct PoolConfig { /* ... */ }
```

- `prefix` (required) — env-key namespace. Combined with each field's
  uppercased name (or its `env = ...` override) to form the final key.
  Example: `prefix = "SINEX_DB"` + field `max_connections` →
  `SINEX_DB_MAX_CONNECTIONS`.
- `context` (required) — passed to env helpers for warn-log context.
  Operators reading service logs see this string when a parse fails.

## Field attributes

| Attribute | Purpose |
|---|---|
| `#[sinex_config(env = "MY_ENV_VAR")]` | Override the full env-var name (default: `{prefix}_{FIELD_UPPER}`) |
| `#[sinex_config(default = LITERAL)]` | Literal default for fields whose type doesn't otherwise have one |
| `#[sinex_config(default_expr = "EXPR")]` | Non-literal default (e.g. `"Seconds::from_secs(30)"`) |
| `#[sinex_config(parser = path::to::fn)]` | Custom parser `fn(&str) -> Result<T, _>`; requires a default |
| `#[sinex_config(duration_secs)]` | Parse a positive integer env value into `std::time::Duration::from_secs`; requires a default |
| `#[sinex_config(skip)]` | Leave at `Default::default()`; no env read |

## Type-driven helper inference

| Field type | Helper |
|---|---|
| `bool` | `env::bool_or(key, default, context)` — default `false` if not specified |
| `String` | `env::var_or(key, default, context)` — default `""` if not specified |
| `Option<String>` | `env::var_optional(key, context)` |
| `Option<PathBuf>` | `env::path_optional(key, context)` |
| `Option<T>` (other) | `env::parse_optional(key, context)` |
| `PathBuf` | `env::path_optional(...).unwrap_or_else(|| default)` — requires default |
| `std::time::Duration` + `duration_secs` | In infallible mode, `env::parse_optional::<u64>(...).map(Duration::from_secs).unwrap_or(default)` with zero/invalid values falling back to default; in `fallible` mode, strict parsing with zero rejected as configuration error |
| Other `T: FromStr` | `env::parse_or(key, default, context)` — requires default |

## Examples

```rust,ignore
#[derive(SinexConfig)]
#[sinex_config(prefix = "SINEX_DB", context = "database pool")]
pub struct PoolConfig {
    #[sinex_config(default = 20)]
    pub max_connections: u32,

    #[sinex_config(default = 1)]
    pub min_connections: u32,

    #[sinex_config(default_expr = "Seconds::from_secs(30)")]
    pub acquire_timeout_secs: Seconds,

    pub alt_cert: Option<PathBuf>,

    #[sinex_config(skip)]
    pub computed_runtime_field: Option<String>,
}
```

Generated:

```rust,ignore
impl PoolConfig {
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            max_connections: sinex_primitives::env::parse_or(
                "SINEX_DB_MAX_CONNECTIONS", 20, "database pool"),
            min_connections: sinex_primitives::env::parse_or(
                "SINEX_DB_MIN_CONNECTIONS", 1, "database pool"),
            acquire_timeout_secs: sinex_primitives::env::parse_or(
                "SINEX_DB_ACQUIRE_TIMEOUT_SECS",
                Seconds::from_secs(30),
                "database pool"),
            alt_cert: sinex_primitives::env::path_optional(
                "SINEX_DB_ALT_CERT", "database pool"),
            computed_runtime_field: ::std::default::Default::default(),
        }
    }
}
```

## What this does NOT do

- Does not call `validate()` (chain explicitly after `from_env()`).
- Does not replace CLI parsing (`clap`).
- Does not handle conditional fields ("if `MODE=advanced` read additional
  vars"). Those structs stay hand-rolled.
- Does not handle runtime-argument constructors (`from_env(component: &str)`),
  private-field factories, `Arc` construction, or nested map parsing with
  bespoke validation. Those configs stay hand-rolled and should document why.
- Does not log resolved values — env helpers already trace/warn as needed.
- Does not redact sensitive fields. Prefer `Option<String>` and avoid
  trace-logging the result.

## Migration

Conversion sweeps are tracked on umbrella issue #1589. Each converted
struct deletes bespoke env parsing and gains a consistent env-key naming
convention.

Permanent hand-rolled exceptions in `sinexd`:

| Config | Why it is not derived |
|---|---|
| `NativeMessagingConfig` | Loads raw strings, then dispatches through `from_raw()` to populate private parsed fields, role maps, trusted-extension maps, and rate-limiter state. |
| `SelfObserverConfig` | `from_env(component: &str)` takes a runtime component name; the derive intentionally generates only zero-argument `from_env()`. |
| `HealthAggregatorConfig` | Parses `SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS` as a JSON map and validates component interval values as a unit. |

## Design

See `thoughtspace/crystal/decisions/sinex-config-derive.md` for the full
design including rejected alternatives, risks, and the field-inference
rationale.
