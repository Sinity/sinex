# SinexError

`SinexError` is the typed error boundary for Sinex library code. It preserves:

- a stable `SinexErrorKind` for machine-readable classification
- ordered key/value context for internal diagnostics
- structured source chains captured from `std::error::Error`
- optional backtrace text when explicitly requested
- a sanitized `PublicError` projection for API and CLI output

## Construction

Use the variant constructor that matches the failure domain, then attach
structured context:

```rust
use sinex_primitives::error::{Result, SinexError};

fn validate_event_type(event_type: &str) -> Result<()> {
    if event_type.trim().is_empty() {
        return Err(SinexError::validation("event_type must not be empty")
            .with_context("field", "event_type")
            .with_context("reason", "blank"));
    }
    Ok(())
}
```

Prefer typed source capture at conversion boundaries:

```rust
# use sinex_primitives::error::SinexError;
# fn db_call() -> std::result::Result<(), sqlx::Error> { Err(sqlx::Error::RowNotFound) }
let result = db_call().map_err(|error| {
    SinexError::database("failed to fetch event")
        .with_context("operation", "events.get")
        .with_error_source(&error)
});
```

`with_source("...")` remains available for string-only context when no typed
error exists, but new conversion code should use `with_error_source` or
`with_std_error` so `source_chain()` and `std::error::Error::source()` remain
usable.

## Classification

Use `kind()` for programmatic classification and `status_code()` for HTTP-like
status mapping:

```rust
# use sinex_primitives::error::{SinexError, SinexErrorKind};
let error = SinexError::permission_denied("token lacks write access");
assert_eq!(error.kind(), SinexErrorKind::PermissionDenied);
assert_eq!(error.kind().as_str(), "permission_denied");
assert_eq!(error.status_code(), 403);
```

Avoid parsing `Display` text. Display is full-fidelity diagnostic output and may
include private context and source details.

## Public Projection

External responses must use `client_message()` or `public_payload()`.
`public_payload()` contains the stable kind, safe message, status code, and a
whitelisted subset of context keys. It never includes source chains, backtraces,
paths, SQL text, tokens, URLs, or arbitrary private context.

```rust
# use sinex_primitives::error::SinexError;
let error = SinexError::database("SELECT secret FROM auth_tokens")
    .with_context("operation", "events.query")
    .with_context("path", "/home/sinity/.ssh/id_ed25519");

let public = error.public_payload();
assert_eq!(public.kind_name, "database");
assert_eq!(public.message, "A database error occurred");
assert!(public.context.contains_key("operation"));
assert!(!public.context.contains_key("path"));
```

Internal serde serialization of `SinexError` and `ErrorDetails` is intentionally
full fidelity for logs, tests, and durable diagnostics. It is not the public API
format.
