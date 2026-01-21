Error origin tracing

Summary
- SinexError is rich (message, context, sources, status_code), but many conversions flatten errors to strings, and several variants are never constructed in production.
- Ingestd and gateway top-level handlers log/return errors, but background task failures often only log and never propagate.

Core error definition and context
- `crate/lib/sinex-core/src/types/error.rs` defines `SinexError` variants with `ErrorDetails` (message + context + sources) and helper methods like `with_operation`, `with_path`, and `status_code()`.
- Display formatting includes context and source chain, so the information is only preserved if the error remains a `SinexError` and is not stringified.

Ingestd (sinex-ingestd)
- Entry point logs errors and exits on failure in `crate/core/sinex-ingestd/src/main.rs:95` ("Service failed: {e}"). This uses Display, so `ErrorDetails` context should appear if preserved.
- Error origins:
  - DB pool connection uses `?` on sqlx errors in `crate/core/sinex-ingestd/src/service.rs:48-64`, which relies on `impl From<sqlx::Error> for SinexError` and drops contextual information (no operation or connection string).
  - NATS connection adds operation and context in `crate/core/sinex-ingestd/src/service.rs:70-79` via `.with_operation("service.connect_nats")` and `.with_context("nats_url", ...)`.
  - Schema sync errors are wrapped as `SinexError::service` with `.with_operation("service.schema_sync")` in `crate/core/sinex-ingestd/src/service.rs:88-100`.
  - Schema broadcast uses `SinexError::kv` with explicit messages in `crate/core/sinex-ingestd/src/service.rs:582-613`.
- Error propagation gaps:
  - `JetStreamConsumer::run()` errors are logged inside a spawned task and not propagated to `IngestService::run()` (`crate/core/sinex-ingestd/src/service.rs:260-289`). This means the daemon can continue running with a failed consumer, and only logs reveal the failure.
  - `MaterialAssembler` failures are logged and then the task returns without propagating (`crate/core/sinex-ingestd/src/service.rs:312-357`).

Gateway (sinex-gateway)
- `ServiceContainer::new` constructs `SinexError` for configuration and service setup failures (`crate/core/sinex-gateway/src/service_container.rs:44-200`), using `with_operation` and `with_source` in most cases.
- `main` wraps errors into `color_eyre` without direct logging (`crate/core/sinex-gateway/src/main.rs:80-119`). Failures are returned to the CLI with generic context ("Failed to initialize services"), so structured `SinexError` context may only appear if the caller prints the report.
- The `SinexError::status_code()` mapping is defined but unused in production (only referenced in tests), so HTTP/JSON-RPC response mapping is likely custom elsewhere.

Node SDK conversions
- `NodeError <-> SinexError` conversions in `crate/lib/sinex-node-sdk/src/lib.rs:307-355` call `e.to_string()` in both directions. This flattens `ErrorDetails` context/sources and loses structured metadata, making origin tracing harder downstream.

Unused SinexError variants (no origin sites found in production)
- `AlreadyExists`, `PermissionDenied`, `ResourceExhausted`, `ChannelSend`, `ChannelReceive`, `MaxRetriesExceeded` are only referenced in `crate/lib/sinex-core/src/types/error.rs` and tests, suggesting their origin paths are currently unused in production.

