# sinexd API Documentation

## Service Role

The `sinexd::api` module serves as the **hardened external interface** for the
Sinex platform, mediating access for CLI tools, browser extensions, and other
external clients. It acts as a **Zero-Trust Boundary**, enforcing
authentication, authorization, and rate limiting on all incoming traffic before
it reaches internal services.

## Architectural Patterns

- **Zero-Trust Boundary**: The only component exposed to untrusted clients. The
  event engine, source contracts, and automata operate within the trusted runtime.
- **Failure Isolation**: API failure should not stop source collection or event
  persistence inside `sinexd`.
- **Protocol Stack**: Supports JSON-RPC (CLI) and Length-prefixed JSON (browser extensions) via a unified dispatch layer.
- **Replay Orchestration**: Manages complex replay operations via a background task engine, keeping the RPC interface responsive.

## Core Documentation

- `architecture.md` – API role, separation rationale, security posture
- `overview.md` – High-level architecture and usage guidance
- `rpc_server.md` – JSON-RPC methods, request/response schema, safety notes
- `interaction_and_query.md` – query/read-path architecture, CLI boundary, and service-layer split

## Configuration

- `environment.md` – Gateway-specific environment variables
- `transport_security.md` – TLS and authentication requirements

## Implementation Details

- `cascade_analyzer.md` – Cascade planning algorithms and performance
- `replay_control.md` – Distributed replay orchestration
- `replay_state_machine.md` – State machine lifecycle and transitions
- `native_messaging.md` – Browser extension protocol and security
- `rate_limit.md` – Per-token rate limiting strategy
- `coordination.md` – API lifecycle, hot-reload, and distributed coordination

## See Also

- Global security posture: `README.md#security`
- Deployment config: `nixos/modules/README.md`
