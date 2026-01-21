Next analysis brainstorm
- Task lifetime audit: enumerate tokio::spawn sites and ensure they are joined/aborted during shutdown.
- Retry vs fail-fast policy mapping for NATS/DB operations.
- Observability coverage: map metrics/log fields across ingestd and gateway.
