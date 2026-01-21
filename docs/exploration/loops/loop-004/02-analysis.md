# Loop 004 - Pool Usage and Long-Running Operations

Scope
- Gateway and services with explicit pool acquisition or transactions.
- Long-lived connection usage.

Explicit Connection Usage
- Gateway uses a single long-lived transaction for cascade analysis.
  - `crate/core/sinex-gateway/src/cascade_analyzer.rs` `analyze_cascades()` begins a transaction, runs multiple steps, then commits/rolls back.
- Core replay state machine uses short transactions for state transitions.
  - `crate/lib/sinex-core/src/db/replay/state_machine.rs` `transition()` begins and commits a transaction around a single transition.
- Advisory locks hold a pooled connection for the lifetime of the lock.
  - `crate/lib/sinex-core/src/db/advisory_lock.rs` `AdvisoryLock` stores a `PoolConnection` and releases on guard drop.

Implicit Pool Usage
- Most service queries use SQLx macros on `&Pool`, which acquire/release connections per query.
  - No explicit `pool.acquire()` in gateway services beyond cascade analyzer.

Findings
- The only runtime path holding a connection across multiple awaits in gateway code is cascade analysis (intentionally transactional).
- Advisory locks intentionally reserve a connection for the duration of the lock; used during ingestd migrations.

Risks
- Cascade analysis can hold a connection and transaction open across multiple queries without a timeout; large analyses could monopolize a connection.
- Advisory lock usage can reduce pool availability while migrations or schema sync run, especially with small pool sizes.

Opportunities
- Consider a dedicated pool or per-request timeout for cascade analysis to avoid starving other RPC handlers.
- Document that advisory locks reserve a connection for their lifetime and ensure pool sizes account for this.
