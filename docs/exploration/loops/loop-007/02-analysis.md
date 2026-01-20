# Loop 007 - Replay Control Idempotency Under Retries

Scope
- Replay control request handling and state transitions.
- Behavior under request retries/timeouts.

Request Path
- Requests are handled synchronously in the replay control server.
  - `crate/core/sinex-gateway/src/replay_control.rs` `ReplayControlServer::spawn()` receives messages and awaits `handle_message()` per request.
- Client requests have a hardcoded 30s timeout; server execution is not cancelled on timeout.
  - `crate/core/sinex-gateway/src/replay_control.rs` `ReplayControlClient::send()` uses `tokio::time::timeout(Duration::from_secs(30), ...)`.

Idempotency Map (evidence-based)
- Plan: creates a new operation every call (non-idempotent).
  - `crate/lib/sinex-core/src/db/replay/state_machine.rs` `create_operation()` always inserts a new operation.
- Preview: re-runs queries and updates preview metadata; state moves Planning -> Previewed once.
  - `crate/lib/sinex-core/src/db/replay/state_machine.rs` `update_preview()` sets state to Previewed only if current state is Planning; otherwise it updates preview without state rollback.
- Approve: requires Previewed; retry after approval errors.
  - `crate/lib/sinex-core/src/db/replay/state_machine.rs` `approve()` returns error unless state == Previewed.
- Execute: requires Approved; retry when state != Approved triggers error and then marks failed.
  - `crate/core/sinex-gateway/src/replay_control.rs` `ReplayExecutionEngine::run_operation()` errors if state != Approved.
  - `ReplayExecutionEngine::execute()` calls `mark_failed()` on any error.
  - `crate/lib/sinex-core/src/db/replay/state_machine.rs` `mark_failed()` unconditionally writes Failed state.
- Cancel: idempotent for terminal states.
  - `crate/lib/sinex-core/src/db/replay/state_machine.rs` `cancel()` returns Ok if already terminal.
- Status/List: read-only.

Concurrency/Locking Considerations
- Execution lock uses PostgreSQL advisory locks but does not keep the acquiring connection alive.
  - `crate/lib/sinex-core/src/db/replay/state_machine.rs` `acquire_execution_lock()` uses `pg_try_advisory_lock` via the pool without retaining the connection.
- Lock release uses a pooled connection that may not be the same session as the lock holder.
  - `release_execution_lock()` uses `pg_advisory_unlock` via the pool with no session affinity.

Findings
- Replay control requests are not broadly idempotent; retries can create duplicate plans or fail approvals.
- `execute` retries can incorrectly mark completed operations as failed if the server already moved beyond Approved.
- Advisory lock usage does not ensure release on the same session, risking stuck locks under retries or crashes.

Risks
- Client timeouts followed by retries can cause state regression (Completed -> Failed) due to `mark_failed()` on execute errors.
- Stuck advisory locks can block future executions indefinitely without explicit cleanup.

Opportunities
- Add idempotent safeguards: treat execute on terminal states as a no-op success.
- Bind advisory locks to a retained connection or use an explicit lock table with row-level locks.
