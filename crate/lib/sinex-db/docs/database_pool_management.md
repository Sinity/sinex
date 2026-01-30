# Database Pool Management

Sinex uses a centralized, global database connection pool managed via the `pool.rs` module. This singleton pattern ensures that database resources are shared efficiently across the process while providing a simple API for all system components.

## Initialization & Lifecycle

The connection pool is lazily initialized on the first request using a **Double-Checked Locking** pattern with an asynchronous `RwLock`.

1. **Fast Path**: A read lock checks if the pool already exists. If it does, the pool is returned immediately.
2. **Expensive Path**: If the pool is absent, it is created using the provided (or default) configuration. This step happens outside the lock to avoid blocking other tasks.
3. **Synchronization**: After creation, a write lock is acquired, the existence is re-checked (to handle concurrent initialization), and the new pool is stored globally.

## Configuration & Tuning

The pool is highly configurable via environment variables (`SINEX_DB_*`) or the `PoolConfig` struct:

- **Connection Limits**: Default `max_connections` is 100, with a baseline `min_connections` of 10.
- **Timeouts**: The system enforces separate timeouts for connection acquisition (default 30s), idle connections (300s), and individual SQL statements (60s).
- **Safety Checks**: On startup, the pool optionally validates its `max_connections` against the PostgreSQL `max_connections` setting to prevent resource exhaustion at the database level.

## Observability & Performance

- **Acquisition Monitoring**: The system tracks the time taken to acquire connections from the pool. A warning is logged if acquisition exceeds a configurable threshold (default 100ms).
- **Pool Metrics**: Metrics such as pool size and idle connection count are captured during acquisition failure or slow-down to assist in capacity planning.
- **Statement Timeouts**: Every connection is automatically configured with a `statement_timeout`, protecting the pool from being exhausted by runaway or non-optimized queries.

## Testing & Isolation

To support isolated integration tests, the system provides a `reset_pool_for_tests` utility (available only under the `testing` feature). This allows test suites to gracefully close all connections and reset the global state between runs, ensuring a clean environment for every test.
