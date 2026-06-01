# `#[derive(SinexConfig)]`

Generates `pub fn from_env(&self) -> Self` (a `Self`-returning associated
function, no receiver) that reads each non-`skip` field from an environment
variable using `sinex_primitives::env::*` helpers.

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
| `#[sinex_config(duration = "secs")]` | Parse an integer env var into `std::time::Duration`; units: `millis`, `secs`, `minutes`, `hours`, `days`; requires a default |
| `#[sinex_config(nonzero)]` | With `duration`, treat `0` as invalid and fall back to the default |
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
| `Duration` + `duration = "..."` | integer env var converted through the declared unit — requires default |
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

    #[sinex_config(default_expr = "Duration::from_secs(30)", duration = "secs", nonzero)]
    pub connect_timeout: Duration,

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
            connect_timeout: match sinex_primitives::env::parse_optional::<u64>(
                "SINEX_DB_CONNECT_TIMEOUT",
                "database pool",
            ) {
                Some(units) if units > 0 => Duration::from_secs(units),
                _ => Duration::from_secs(30),
            },
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
- Does not replace loaders whose shape is not a zero-argument declarative
  struct construction, such as runtime-argument loaders, private constructor
  funnels, `Arc` construction, or nested map parsing. In sinexd, that keeps
  `SelfObserverConfig`, `NativeMessagingConfig`, and `HealthAggregatorConfig`
  as intentional manual exceptions.
- Does not log resolved values — env helpers already trace/warn as needed.
- Does not redact sensitive fields. Prefer `Option<String>` and avoid
  trace-logging the result.

## Migration

20+ structs in the workspace hand-roll `from_env()`. Conversion sweeps
are tracked on umbrella issue #1239. Each converted struct deletes ~5-15
LoC and gains a consistent env-key naming convention.

## Design

See `thoughtspace/crystal/decisions/sinex-config-derive.md` for the full
design including rejected alternatives, risks, and the field-inference
rationale.
