# Loop 007 - Concrete Issues

1) Replay execute retry can downgrade a completed operation to failed
- Evidence: `ReplayExecutionEngine::run_operation()` errors if state != Approved, and `ReplayExecutionEngine::execute()` calls `mark_failed()` on any error (`crate/core/sinex-gateway/src/replay_control.rs`).
- Impact: if a client retries `Execute` after a timeout and the operation already completed, the retry can mark it Failed (`crate/lib/sinex-core/src/db/replay/state_machine.rs`).

2) Advisory lock acquisition/release is not session-affine
- Evidence: `acquire_execution_lock()` uses `pg_try_advisory_lock` via a pooled connection without retaining it, and `release_execution_lock()` uses `pg_advisory_unlock` via another pooled connection (`crate/lib/sinex-core/src/db/replay/state_machine.rs`).
- Impact: locks may remain held by the original session and never released, causing repeated execute attempts to fail with "already executing".
