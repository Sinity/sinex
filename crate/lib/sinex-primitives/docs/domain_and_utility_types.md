# Domain and Utility Types

The Sinex core provides a robust type system that enforces domain constraints and prevents common programming errors through type-safe wrappers and centralized validation.

## Domain String Types

Domain strings (e.g., `EventSource`, `EventType`, `SanitizedPath`) are implemented as newtype wrappers around `Cow<'static, str>`.

- **Type Safety**: Prevents accidental mixing of semantically different strings (e.g., passing a `HostName` where an `EventSource` is expected).
- **Validation-by-Construction**: Validated types (like `SanitizedPath` and `Blake3Hash`) enforce structural constraints during construction.
- **Normalization**: Types like `SanitizedPath` automatically normalize separators and remove redundant components (e.g., `..` and `.`).

## Type-Safe IDs (`Id<T>`)

All system identifiers use the `Id<T>` wrapper, which is a phantom-typed ULID.

- **Lexicographical Ordering**: ULIDs ensure that IDs are roughly chronologically ordered, which is beneficial for database indexing and event replaying.
- **Entity Separation**: The type parameter `T` (e.g., `Id<Event>`, `Id<Blob>`) ensures that IDs from different domains cannot be used interchangeably at compile-time.

## Error Handling (`SinexError`)

The `SinexError` type provides structured, context-rich error reporting across the entire system.

- **Categorized Variants**: Errors are grouped into high-level categories (e.g., `Database`, `Validation`, `Security`, `Network`).
- **Context Enrichment**: The `ErrorDetails` system allows developers to attach arbitrary key-value pairs to errors, making logs significantly more informative.
- **Safe Serialization**: Sensitive context data is automatically filtered out during serialization to prevent leaking secrets into logs or over the wire.

## Specialized Collections & Units

- **NonEmptyVec**: A wrapper that guarantees at least one element, used primarily for synthesis provenance (must have at least one parent event).
- **Type-Safe Units**: Dedicated types for `Bytes`, `Seconds`, and `Nanoseconds` prevent units-of-measure errors (e.g., adding seconds to milliseconds).
- **Pagination & TimeRange**: Standardized types for query parameters ensure consistent API behavior across all repositories.

## Validation Subsystem

The validation module provides the underlying logic for domain type constraints:
- **Path Security**: Prevents path traversal attacks and enforces restricted directory access.
- **JSON Integrity**: Protects against "Billion Laughs" style resource exhaustion by enforcing depth and size limits on JSON payloads.
- **Composable Chains**: Validation rules can be composed into chains for complex multi-field verification.
