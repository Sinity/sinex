# Security

> Status: canonical
> Last Verified: 2026-03-12 (code review)

Sinex is primarily a single-user, local-first system. Its security model is built around:

- capture-time privacy filtering
- strict transport requirements on the control plane
- process isolation via NixOS/systemd hardening
- explicit, auditable lifecycle operations instead of silent background deletion

This document is the canonical current-state security reference.

## Threat Model

### In Scope

- accidental disclosure through logs, exports, or captured payloads
- unauthorized access to gateway RPC or `PostgreSQL`
- malicious or malformed event injection
- resource exhaustion via NATS, gateway, or database overload
- compromise of one service process expanding into broader system access

### Out of Scope

- multi-user tenancy
- physical access attacks
- generic host-compromise mitigation beyond normal NixOS host hardening
- blanket at-rest database encryption as a default product requirement

## Trust Boundaries

| Boundary | Current model |
|----------|---------------|
| User ↔ local system | trusted single-user host |
| Nodes ↔ ingestd | NATS transport; TLS available and enforceable, per-node ACLs not yet in place |
| ingestd ↔ `PostgreSQL` | single-writer application path, but DB login-role separation is not fully wired |
| Gateway ↔ clients | TLS-only RPC with bearer-token auth; mTLS required for non-loopback binds |
| Stored events ↔ exports/read APIs | controlled by gateway auth, privacy filtering at capture time, and host-level disk protection |

## Implemented Controls

### Privacy and Input Boundaries

- All text ingestion boundaries are expected to route through `privacy::engine().process()` with the right `ProcessingContext`.
- The unified privacy engine is already implemented in `sinex_primitives::privacy`.
- It covers secrets, PII, infrastructure identifiers, and window-title rules, with `Redact`, `Encrypt`, `Hash`, `Suppress`, and `Mask` strategies.
- Event payloads also pass schema and structural validation before persistence.

### Transport and Access Control

- Gateway RPC is TLS-only.
- Non-loopback gateway binds require mTLS.
- Gateway startup requires a non-empty RPC token source.
- Bearer tokens are checked with constant-time comparison.
- Tokens carry `readonly|write|admin` roles and RPC dispatch enforces per-method RBAC.
- Gateway rate limiting is implemented per token.

### Process and Runtime Isolation

- NixOS services run with strong systemd hardening, including `NoNewPrivileges` and `ProtectSystem=strict`.
- Services run under a dedicated `sinex` system user.
- Resource limits are applied through systemd/cgroup controls.

### Data-Path Controls

- Canonical event persistence goes through `sinex-ingestd`, not arbitrary direct writes from nodes.
- Event persistence remains append-only with provenance tracking.
- Lifecycle changes are intended to be explicit and auditable.
- Automatic retention policies for `core.events` are intentionally not part of the current model.

## Partial Controls and Gaps

| Area | State | What is missing |
|------|-------|-----------------|
| `PostgreSQL` role separation | Partial | Grant roles exist, but default deployment still uses one shared login role for core services |
| NATS TLS | Partial | NixOS now has a typed TLS surface for NATS clients, but TLS is not yet the universal default everywhere |
| NATS authorization | Missing | No per-node credentials or subject-level ACL isolation yet |
| Secrets wiring | Partial | Gateway/admin token handling is integrated; broader service secret handling is still uneven |
| Syscall filtering | Missing | `SystemCallFilter` hardening is not yet applied in the NixOS service modules |
| Audit logging | Missing | No strong application-level “who accessed what” trail yet |

## Current Policy

These are deliberate policy choices, not unfinished work disguised as future intent:

- blanket database encryption is not the baseline security model
- host full-disk encryption and capture-time privacy controls are the intended baseline
- automatic retention for the core event log is rejected; lifecycle changes should be explicit

## Near-Term Priorities

1. Wire distinct `PostgreSQL` login roles onto the existing grant-role scaffolding.
2. Enforce TLS consistently for NATS connections now that the NixOS/client wiring path is explicit.
3. Finish consistent secret delivery for remaining runtime services.
4. Add syscall filtering to the NixOS service layer.
5. Add meaningful audit logging for sensitive RPC/data-access paths.

## Contributor Guardrails

- Do not add new ingestion paths that bypass `privacy::engine().process()`.
- Do not document or test with real credentials; use obvious placeholders.
- Keep new externally reachable surfaces TLS-only by default.
- Treat database-role expansion as a security change, not just a convenience refactor.
- Update this document when a gap is actually closed.

## References

- [Core Architecture](architecture/Core_Architecture.md)
- [System Operations And Integrity Architecture](architecture/SystemOperations_And_Integrity_Architecture.md)
- [Gateway Coordination](architecture/gateway-coordination.md)
- [Environment Variables](configuration/environment-variables.md)
- [TLS / NixOS Integration](configuration/tls-nixos-integration.md)
