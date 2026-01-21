# Loop 003 - Next Analysis Brainstorm

- Pool usage with long-running queries: identify connections held across await points in gateway services.
- Schema broadcast cache lifecycle: check updates, invalidation, and usage in node runtime.
- Replay control execution idempotency: determine what happens if a request times out and is retried.
- NATS subject namespace consistency: ensure coordination subjects are consistently namespaced.
