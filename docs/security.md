# Security

> Status: canonical
> Last Verified: 2026-03-12 (code review)

Sinex is primarily a single-user, local-first system. Its security model is built around capture-time privacy filtering, hardened control-plane transport, NixOS/systemd isolation, and explicit lifecycle operations instead of silent background deletion.

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

| Area | Implemented control | Owning detail |
|------|---------------------|---------------|
| Privacy and input boundaries | Text ingestion is expected to pass through `privacy::engine().process()` with the right `ProcessingContext`; payloads also pass schema/structural validation before persistence | `crate/lib/sinex-primitives/src/privacy/`, `crate/core/sinex-ingestd/docs/validator.md` |
| Gateway transport and access control | RPC is TLS-only; non-loopback binds require mTLS; startup requires token source; bearer-token auth uses constant-time comparison; tokens carry `readonly|write|admin` roles; rate limiting is per token | `crate/core/sinex-gateway/docs/transport_security.md`, `crate/core/sinex-gateway/docs/environment.md` |
| Process/runtime isolation | Services run as a dedicated `sinex` user under hardened systemd units with resource limits | `nixos/modules/README.md` |
| Data-path controls | Canonical persistence goes through `sinex-ingestd`; event history remains append-only with provenance; lifecycle changes are explicit and auditable | `crate/core/sinex-ingestd/docs/architecture.md`, `crate/lib/sinex-db/docs/data_lifecycle.md` |

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
- [Gateway Coordination](../../crate/core/sinex-gateway/docs/coordination.md)
- [Gateway Transport Security](../../crate/core/sinex-gateway/docs/transport_security.md)
- [Ingestd Transport Security](../../crate/core/sinex-ingestd/docs/transport_security.md)
- [NixOS Module Surface](../../nixos/modules/README.md)
