# Sinex Gateway Documentation

## Service Role

The Gateway serves as the **hardened external interface** for the Sinex platform, mediating access for CLI tools, browser extensions, and other external clients. It acts as a **Zero-Trust Boundary**, enforcing authentication, authorization, and rate limiting on all incoming traffic before it reaches internal services.

## Architectural Patterns

- **Zero-Trust Boundary**: The only component exposed to untrusted clients. All other services (ingestd, automatons) operate within a trusted internal network.
- **Failure Isolation**: Gateway failure does not stop data collection (ingestd runs independently).
- **Protocol Stack**: Supports JSON-RPC (CLI) and Length-prefixed JSON (browser extensions) via a unified dispatch layer.
- **Replay Orchestration**: Manages complex replay operations via a background task engine, keeping the RPC interface responsive.

## Core Documentation

- `architecture.md` – Service role, separation rationale, security posture
- `overview.md` – High-level architecture and usage guidance
- `rpc_server.md` – JSON-RPC methods, request/response schema, safety notes

## Configuration

- `environment.md` – Gateway-specific environment variables
- `transport_security.md` – TLS and authentication requirements

## Implementation Details

- `cascade_analyzer.md` – Cascade planning algorithms and performance
- `replay_control.md` – Distributed replay orchestration
- `replay_state_machine.md` – State machine lifecycle and transitions
- `native_messaging.md` – Browser extension protocol and security
- `rate_limit.md` – Per-token rate limiting strategy

## See Also

- Global security: `docs/current/architecture/security-architecture.md`
- Global config: `docs/current/configuration/environment-variables.md`
