# Loop 003 - NATS Request/Response Timeout Map

Scope
- NATS request/response usage and timeouts.
- Retry/backoff behavior around NATS subscriptions.

Request/Response Usage
- Replay control is the only production use of NATS request/response.
  - `crate/core/sinex-gateway/src/replay_control.rs` uses `client.request(...)` in `ReplayControlClient::send()`.
  - Test-only helper uses `async_nats::Request::new().timeout(...)` via `send_request()`.
- No other code paths use `request()` or `send_request()`.
  - Search results only show replay control usage.

Timeouts
- Replay control client enforces a 30s timeout on requests.
  - `crate/core/sinex-gateway/src/replay_control.rs` wraps `client.request()` in `tokio::time::timeout(Duration::from_secs(30), ...)` and records errors.
- Test helper uses explicit NATS request timeout (for controlled tests).
  - `crate/core/sinex-gateway/src/replay_control.rs` `send_with_timeout()` sets `Request::timeout(Some(timeout))`.
- Coordination handoff waits have explicit timeouts.
  - `crate/lib/sinex-node-sdk/src/coordination.rs` `wait_for_handoff_ready()` wraps `sub.next()` in `tokio::time::timeout(timeout, ...)`.

Retry/Backoff
- Replay control subscription retries with exponential backoff on subscribe failures.
  - `crate/core/sinex-gateway/src/replay_control.rs` `ReplayControlServer::spawn()` loops with backoff up to `REPLAY_CONTROL_SUBSCRIBE_ATTEMPTS`.
- No explicit retry for failed request/response aside from manual callers (none found).

Observations
- Replay control request processing is sequential within the subscription loop.
  - `ReplayControlServer::spawn()` awaits `handle_message()` per message; long request handling can block subsequent requests.
- Client-side timeout does not cancel server-side execution.
  - A timed-out request may still execute, which can lead to ambiguous outcomes if clients retry externally.

Findings
- Only replay control uses NATS request/response, and it has a hardcoded 30s timeout.
- Coordination handoff wait uses an explicit timeout, avoiding indefinite waiting.
- Subscription retry behavior exists for replay control but not for other NATS operations.

Risks
- If replay operations exceed 30s, the client will time out and report failure even if the server eventually completes.
- Serial request handling in replay control can become a bottleneck under concurrent RPC usage.

Opportunities
- Consider configurable replay control request timeout via env or config.
- Consider concurrent handling or a bounded worker pool for replay control requests if load grows.
