# Loop 004 - Next Analysis Brainstorm

- Schema broadcast cache lifecycle: where is it read, refreshed, and invalidated?
- Replay control idempotency under retries/timeouts.
- NATS subject namespace consistency (environment and namespace handling across core/services/nodes).
- Validation boundary audit for RPC inputs (which handlers validate vs rely on downstream).
