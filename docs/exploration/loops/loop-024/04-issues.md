Concrete issues to handle
- Preserve structured context when converting between NodeError and SinexError (`crate/lib/sinex-node-sdk/src/lib.rs:307-355`). Current `to_string()` conversions drop context and sources, making origin tracing and telemetry weaker.
- Add context for DB connection failures in ingestd (currently `?` uses `From<sqlx::Error>`). Consider `.with_operation("service.connect_db")` and include database host/namespace (`crate/core/sinex-ingestd/src/service.rs:48-64`).
- Decide whether JetStream consumer and MaterialAssembler failures should escalate beyond logging (e.g., trigger shutdown or expose health check failures). Both are currently logged and silently continue (`crate/core/sinex-ingestd/src/service.rs:260-357`).
- Gateway main returns errors without explicit logging; if CLI UX is poor, add `tracing::error!` on failures to ensure visibility (`crate/core/sinex-gateway/src/main.rs:80-119`).
- Unused SinexError variants (`AlreadyExists`, `PermissionDenied`, `ResourceExhausted`, `ChannelSend`, `ChannelReceive`, `MaxRetriesExceeded`) should either be implemented in production paths or removed to reduce dead surface area.
