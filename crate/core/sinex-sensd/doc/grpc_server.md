# gRPC Server

`grpc_server.rs` exposes optional control-plane APIs for sensd. Operators use it
to inspect sensor status, trigger replays, and fetch metrics.

- Wraps internal job manager state for read-only access.
- Enforces authentication/authorization boundaries.
- Streams health summaries compatible with the dashboards detailed in
  `docs/architecture/SystemOperations_And_Integrity_Architecture.md`.
