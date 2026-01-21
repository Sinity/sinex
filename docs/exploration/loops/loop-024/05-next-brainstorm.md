Next analysis brainstorm
- Channel topology and backpressure audit across ingestd, node SDK, and services.
- Task lifetime analysis: enumerate tokio::spawn sites, identify orphan tasks, and check graceful shutdown handling.
- Error-to-response mapping: trace SinexError (and other error types) to JSON-RPC or API responses in gateway/services.
