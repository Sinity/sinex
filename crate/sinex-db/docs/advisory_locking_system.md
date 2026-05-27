# Distributed Advisory Locking

Sinex utilizes PostgreSQL advisory locks for cross-process coordination, ensuring that critical operations like schema migrations or single-writer tasks are executed by only one node at a time.

## Core Mechanism

Advisory locks are application-level locks tied to a 64-bit identifier. Unlike row or table locks, they do not lock any physical database objects but instead act as a signaling mechanism that multiple system instances agree to respect.

- **Key-to-ID Mapping**: String-based lock keys (e.g., `"ingestd:migrations"`) are deterministically hashed to 64-bit integers using the **BLAKE3** algorithm.
- **Session Scoping**: All advisory locks in Sinex are session-scoped. They are tied to a specific database connection and are automatically released by PostgreSQL if that connection is closed, protecting the system against deadlocks caused by process crashes or network partitions.

## RAII & Automatic Cleanup

The system employs an **RAII (Resource Acquisition Is Initialization)** pattern to manage lock lifecycles safely:

1. **Acquisition**: When a lock is acquired via `try_acquire`, it returns an `AdvisoryLock` struct wrapped in a `ResourceGuard`.
2. **Connection Pinning**: The `AdvisoryLock` struct holds the specific `PoolConnection` used to acquire the lock, preventing it from being returned to the pool and reused until the lock is released.
3. **Automatic Release**: When the `ResourceGuard` is dropped (either normally or during a panic), a background task is triggered to execute `pg_advisory_unlock` and return the connection to the pool.

## Acquisition Strategies

- **Non-Blocking (`try_acquire`)**: Attempts to acquire the lock and returns `None` immediately if another process already holds it. This is used for "best-effort" coordination.
- **Polling Wait (`acquire_or_wait`)**: Retries acquisition at a fixed interval (10ms) until the lock is acquired or a timeout is reached. This is used for critical startup tasks like schema migrations.

## Usage Patterns

Advisory locks are primarily used for:
- **Migration Coordination**: Ensuring that only one instance of `sinex-ingestd` applies schema updates at a time.
- **Singleton Services**: Coordinating tasks that should only run on a single node in a cluster (e.g., certain maintenance or archival jobs).

## Observability

Lock operations are fully instrumented with `tracing`. Every acquisition attempt records the lock key, facilitating debugging of contention issues and providing clear audit trails for cross-process synchronization.
