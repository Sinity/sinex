# Loop 005 - Next Analysis Brainstorm

- NATS subject namespace consistency across core/services/nodes (env and namespace handling).
- Replay control idempotency under retries/timeouts.
- RPC input validation boundaries: which handlers validate and which rely on downstream?
- Event schema registration vs usage: identify event types without schemas that are still emitted.
