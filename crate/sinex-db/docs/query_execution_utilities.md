# Query Execution & Transaction Helpers

The Sinex data layer provides a set of utilities to simplify common database operations, ensure consistent error handling, and implement resilient transaction patterns.

## Transaction Management

The system provides several wrappers around `sqlx::Transaction` to ensure proper commit/rollback behavior and ergonomic async usage.

### Standard Transactions (`with_transaction`)
A closure-based wrapper that handles the boilerplate of beginning a transaction, executing a block of code, and automatically committing on success or rolling back on error.

### Resilient Transactions (`with_retry_transaction_idempotent`)
For operations that are safe to retry (e.g., updating a non-unique counter), this helper implements exponential backoff and automatic retry for transient database errors like deadlocks or serialization failures.

- **Idempotency Marker**: Callers must provide an `IdempotentTransaction` marker, serving as a type-level proof that the operation is safe to execute multiple times.
- **Explicit Rollback**: The retry loop explicitly rolls back failed attempts before retrying, ensuring the database connection is in a clean state before returning to the pool.

## Dynamic Query Building

While the system prefers compile-time validated SQL (`sqlx::query!`), some scenarios require dynamic filtering. The `WhereBuilder` provides a type-safe way to construct complex `WHERE` clauses at runtime.

- **SQL Injection Prevention**: Identifiers are automatically quoted and escaped, and all values are bound using parameterized `$1, $2...` placeholders.
- **Fluent API**: Supports chaining conditions (`and`, `or`) with various comparison operators (`Comparison::Eq`, `Comparison::Lt`, `Comparison::Like`).

## Error Conversion (`db_error`)

To maintain domain-level abstraction, the system provides a centralized `db_error` utility that maps raw `sqlx` errors to structured `SinexError` types.

- **Contextualization**: Prepends operation-specific context (e.g., "fetch event by id") to the error message.
- **Classification**: Distinguishes between record-not-found (404), constraint violations (409), and generic database failures (500).

## UUIDv7-UUID Integration

The query helpers re-export essential UUIDv7 conversion utilities, facilitating the lossless bridging between application-layer UUIDv7 IDs and database-layer UUIDs. This ensures that the performance benefits of native UUID storage are maintained without sacrificing the chronological ordering benefits of UUIDv7 IDs in Rust.
