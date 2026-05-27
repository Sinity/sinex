# Error Handling Architecture

Sinex uses a centralized, context-rich error handling system built around the `SinexError` enum. this system is designed for maximum observability while maintaining strict security boundaries.

## Error Taxonomy

Errors are classified into over 20 specialized variants, each representing a specific failure domain:
- **Infrastructure**: `Database`, `Network`, `IO`, `Timeout`.
- **Logic & State**: `Validation`, `InvalidState`, `AlreadyExists`, `NotFound`.
- **Security**: `PermissionDenied`, `Security`, `Authentication`.
- **Lifecycle**: `Initialization`, `Configuration`, `Service`, `Shutdown`.

## Context Enrichment

Every `SinexError` wraps an `ErrorDetails` struct that supports arbitrary metadata:
- **Key-Value Context**: Developers can attach structured information (e.g., `table_name`, `operation`, `retry_count`) using the `with_context` method.
- **Ordered Metadata**: The system uses `IndexMap` to ensure that context information is logged in the same order it was added, facilitating deterministic debugging.
- **Causal Chains**: Errors support a `sources` chain, allowing developers to wrap lower-level errors (like `sqlx::Error`) while preserving the original failure reason for root cause analysis.

## Security & Sanitization

To prevent leaking sensitive information into logs or across service boundaries, `SinexError` implements a multi-layer sanitization strategy during serialization:

1. **Path Stripping**: Internal file system paths (e.g., `/home/user/project/...`) are automatically stripped from error messages in release builds.
2. **Context Whitelisting**: Only context keys explicitly marked as "safe" (e.g., `status_code`, `duration_ms`) are included in serialized payloads.
3. **Internal vs. External Messages**: The system distinguishes between internal display (full details) and serialized wire format (sanitized details).

## Ergonomics & Patterns

- **Fluent API**: Errors are constructed using a builder-like pattern: `SinexError::database("Query failed").with_context("table", "events")`.
- **Type Alias**: A global `Result<T>` alias simplifies method signatures across the codebase.
- **Categorization Methods**: Errors provide boolean checks like `is_retryable()` and `is_client_error()`, which are used by the `node-sdk` to drive automatic retry logic and circuit breaking.
- **HTTP Mapping**: The `status_code()` method automatically maps error variants to appropriate HTTP status codes (e.g., 404 for `NotFound`), ensuring consistent API behavior.
