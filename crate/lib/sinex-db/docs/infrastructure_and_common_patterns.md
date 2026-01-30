# Infrastructure & Common Patterns

The `sinex-core` data layer is built on a set of shared traits and utilities that ensure consistent behavior, error handling, and transaction management across all repositories.

## Repository Traits

Repositories generally implement a standard interface to maintain consistency:

- **Repository Trait**: Defines the base contract, requiring a database pool and a constructor (`new`).
- **Enhanced Repository**: Provides generic CRUD operations (e.g., `count_all`, `exists_by_id`) using metadata from the `TableDef` trait. This reduces boilerplate for standard table operations.
- **Transaction Support**: A factory pattern for creating transaction-bound versions of repositories. This allows operations to be composed into larger atomic units.

## Error Handling (`db_error`)

The `db_error` utility transforms raw `sqlx` errors into structured `SinexError` types with rich context:

- **Categorization**: Errors are classified into domain-specific types such as `not_found`, `unique_violation`, or `foreign_key_violation`.
- **Contextualization**: The utility captures the specific database operation and error codes, making logs more actionable for forensics.
- **Source Preservation**: The original database error message is preserved in the `source` field for deep debugging.

## Common Data Types

Shared DTOs (Data Transfer Objects) are centralized in the `common` module to ensure compatibility between repositories and services:

- **TimeBucketResult**: A standardized structure for time-series aggregations, used across all analytics queries.
- **EventSearchFilters**: A comprehensive filter object that supports composite filtering by source, type, host, time range, and JSONB content.
- **DbResult**: A semantic alias for `Result<T, SinexError>`, used consistently throughout the data layer.

## Transaction & Performance Utilities

- **REPEATABLE READ Isolation**: High-integrity operations (like entity merges and schema registration) use `REPEATABLE READ` isolation to prevent phantom reads and ensure a consistent view of the database.
- **Statement Timeouts**: Infrastructure is available to set local statement timeouts for long-running analytics queries, protecting the connection pool from exhaustion.
- **ULID-UUID Bridging**: Lossless bidirectional conversion between Rust ULIDs and PostgreSQL UUIDs ensures that the system can leverage Postgres's native UUID performance while maintaining the benefits of ULID timestamps in the application layer.
