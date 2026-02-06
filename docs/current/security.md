# Security Posture

> Last Verified: 2026-02-03 (manual review)

*Pair with `docs/current/architecture/security-architecture.md` for the broader threat model.*

## Current Strengths

- **Input validation:** `sinex_primitives::types::validation` enforces path sanitisation,
  JSON depth limits, and command-injection guards. Adversarial tests cover
  traversal, null-byte injection, Unicode edge cases, and SQLi attempts.
- **Process isolation:** NixOS/unit files apply strict systemd hardening
  (NoNewPrivileges, `ProtectSystem=strict`, per-service cgroups, capability
  bounding).
- **Transport security:** Gateway RPC is TLS-only, even on loopback; non-loopback
  binds require mTLS. Unix sockets are reserved for external integrations
  (e.g., Hyprland/Kitty) rather than Sinex control-plane traffic.
- **Token authentication:** Bearer token auth with constant-time comparison and
  live reload from file. Gateway rejects requests without valid tokens.
- **Rate limiting:** Per-token rate limiting via governor crate (100 req/sec default,
  configurable). Protects against runaway clients and abuse.

## Gaps

| Area | Status | Notes |
|------|--------|-------|
| Authorization / Roles | **Missing** | Token auth exists, but no role differentiation. All valid tokens have full access. |
| Encryption at rest | **Missing** | pgsodium integration planned but not implemented; data relies solely on full-disk encryption. |
| NATS transport | **Partial** | Gateway RPC is TLS-only; NATS connections can use TLS but it's not enforced by default. |
| Secrets management | **Planned** | agenix workflow defined, but services still read plain env vars. Need rotation policy. |
| Data cleanup tooling | **Missing** | No automated tooling for deleting old events or redacting sensitive data. |

## Near-Term Tasks

1. Introduce role-based authorization: read-only tokens for query clients,
   write tokens for ingestors, admin tokens for management operations.
2. Integrate pgsodium for column/key encryption (payload archives, operations
   log) and document key management expectations.
3. Enforce TLS for NATS connections; update CLI helpers for CA configuration.
4. Finalise agenix integration: secrets encoded once, exposed via
   `/run/agenix/...` with rotation hooks.
5. Add data lifecycle tooling: time-based retention policies, selective deletion,
   and export utilities for personal data management.

## Guardrails for Contributors

- Never hard-code credentials in tests or docs. Use
  `postgresql://sinex:<PLACEHOLDER>@localhost/sinex_dev` style examples and call
  out that they are placeholders.
- Keep all new ingress TLS-only by default; require an explicit security review
  before exposing non-loopback listeners.
- Update this file whenever a gap moves from red to green—call out the commit
  or module that closed it.

Security is a product story, not a subproject. Track these items alongside core
feature work.
