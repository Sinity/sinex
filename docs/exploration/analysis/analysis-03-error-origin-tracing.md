# Error Origin Tracing

Scope
- Identify where SinexError is constructed, how context is attached, and where it surfaces to users.

Method
- rg "SinexError::" and inspect ingestd and gateway error paths.

Error taxonomy
- SinexError has a broad variant set with structured ErrorDetails and optional context/source chains (crate/lib/sinex-core/src/types/error.rs:45-117).

Primary construction sites
- ingestd service and schema broadcast use explicit SinexError::service/network/kv/validation with operation context (crate/core/sinex-ingestd/src/service.rs:499-613).
- ingestd JetStream consumer validates inbound events and returns SinexError::validation on envelope or schema errors; NoSchema and SchemaNotFound are accepted with warnings (crate/core/sinex-ingestd/src/jetstream_consumer.rs:778-811).

User-facing surfaces
- RPC layer wraps internal errors as JSON-RPC errors by stringifying the error; structured context in SinexError is not preserved in the JSON-RPC response (crate/core/sinex-gateway/src/rpc_server.rs:714-727).

Observations
- Most core errors accumulate context via with_operation/with_source, which is good for logs and internal diagnostics.
- The RPC boundary collapses errors to strings; clients lose structured fields like error type and context keys.

Follow-ups
- Consider exposing SinexError fields in RPC error data for better client-side diagnostics.
- Track which SinexError variants are actually emitted to logs vs mapped to generic "Internal error" to avoid losing actionable details.
