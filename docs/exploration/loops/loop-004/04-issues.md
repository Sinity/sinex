# Loop 004 - Concrete Issues

1) Cascade analysis holds a database transaction open for the full analysis without a timeout
- Evidence: `CascadeAnalyzer::analyze_cascades()` in `crate/core/sinex-gateway/src/cascade_analyzer.rs` begins a transaction and runs multiple steps before commit/rollback.
- Impact: long analyses can hold a connection and transaction open, potentially starving other RPC handlers and increasing lock contention.

2) Advisory locks reserve a pool connection for the lock lifetime
- Evidence: `AdvisoryLock` stores a `PoolConnection` in `crate/lib/sinex-core/src/db/advisory_lock.rs` and releases on guard drop.
- Impact: during migrations/schema sync, one pool slot is held for the advisory lock; with small pools this can affect availability.
