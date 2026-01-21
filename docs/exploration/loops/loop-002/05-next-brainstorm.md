# Loop 002 - Next Analysis Brainstorm

- NATS request/response timeouts: find missing timeouts or inconsistent defaults across clients.
- Pool acquisition and long-running queries: map where pool connections are held across awaits.
- Schema broadcast cache lifecycle: ensure it is invalidated on schema updates and shutdown.
- Replay control backpressure: assess queue sizes and failure behavior under load.
