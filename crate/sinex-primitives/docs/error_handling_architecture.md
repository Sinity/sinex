# Error Handling Architecture

Sinex uses one typed library error, `SinexError`, with explicit internal and
public projections.

## Shape

`SinexError` remains an enum because many call sites need domain-specific
pattern matching. Every variant maps to `SinexErrorKind`, a stable
machine-readable kind exposed through `kind()` and serialized in public API
payloads.

Each variant contains `ErrorDetails`:

- `message`: primary diagnostic text
- `context`: ordered key/value metadata for internal debugging
- `sources`: legacy string source messages for display compatibility
- `source_chain`: structured causal nodes captured from typed errors
- `backtrace`: optional text captured only when requested

The structured source chain is cloneable and serializable. It stores type names
and messages rather than owning concrete error values, so `SinexError` can cross
process/logging boundaries without losing its source shape.

## Conversion Rules

At a boundary from another error type into Sinex:

1. Choose the `SinexError` variant that describes the semantic failure domain.
2. Attach safe, queryable context with `with_context`.
3. Preserve typed causality with `with_error_source(&error)`.
4. Use `with_std_error(&dyn_error)` only when the concrete type is unavailable.
5. Use `with_source("...")` only for string-only context with no typed source.

Database and serialization conversions classify well-known error kinds before
capturing the source chain. For example, SQL unique violations become
`AlreadyExists`, FK/check/not-null violations become `Validation`, pool timeout
becomes `Timeout`, and unknown SQLx errors remain `Database`.

## Backtraces

Ordinary construction does not capture a backtrace. Typed source capture records
a backtrace only when `SINEX_ERROR_BACKTRACE` or `RUST_BACKTRACE` is set to a
non-empty, non-`0` value. Call sites may use `with_backtrace()` for an explicit
diagnostic capture when the extra allocation is justified.

## Public Boundary

`Display`, `Debug`, serde serialization of `SinexError`, and `ErrorDetails` are
internal diagnostic surfaces. They may contain SQL, paths, URLs, source messages,
and other private context.

External surfaces must use `public_payload()`:

- `kind` / `kind_name`: stable machine-readable category
- `message`: `client_message()` with internal variants generalized
- `status_code`: stable HTTP-like mapping
- `context`: whitelist-only safe keys

JSON-RPC responses include this public projection in `error.data`. The CLI reads
the stable category/status fields from that projection rather than parsing
human-readable messages. Full internal errors are only exposed by the gateway
when the `dev-errors` feature is enabled.
