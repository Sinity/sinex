# Security & Privacy Posture
> Last Verified: 2025-12-02 (manual review)

*Source material: 2025-07-23 security analysis (Redis-era); updated to track
post-JetStream priorities. Pair this with
`docs/current/architecture/security-architecture.md` for the broader threat model.*

## Current Strengths

- **Input validation:** `sinex_core::db::security` enforces path sanitisation,
  JSON depth limits, and command-injection guards. Adversarial tests cover
  traversal, null-byte injection, Unicode edge cases, and SQLi attempts.
- **Process isolation:** NixOS/unit files apply strict systemd hardening
  (NoNewPrivileges, `ProtectSystem=strict`, per-service cgroups, capability
  bounding).
- **Local IPC surface:** satellites communicate via Unix domain sockets by
  default; nothing binds to TCP without an explicit enable.

## Priority Gaps

| Area | Status | Notes |
|------|--------|-------|
| Authentication / Authorization | **Missing** | No API keys, roles, or user management. All services share the same database role. |
| Encryption at rest | **Missing** | pgsodium integration planned but not implemented; data relies solely on full-disk encryption. |
| Transport security | **Missing** | gRPC, CLI helpers, and any future web APIs lack TLS. |
| Secrets management | **Planned** | agenix workflow defined, but services still read plain env vars. Need rotation policy and usage audit. |
| Privacy tooling | **Missing** | No PII detection, redaction, or GDPR/RTBF strategies; immutable log complicates compliance. |
| Rate limiting / abuse prevention | **Missing** | Even after auth, ingress needs quota + DoS protection. |

## Near-Term Tasks

1. Introduce service accounts with scoped credentials (`DATABASE_URL`
   namespacing, least-privilege roles) and migrate satellites off the shared
   superuser.
2. Integrate pgsodium for column/key encryption (payload archives, operations
   log) and document key management expectations.
3. Terminate TLS at the gateway and adopt mTLS for satellite RPC.
4. Finalise agenix integration: secrets encoded once, exposed via
   `/run/agenix/...` with rotation hooks.
5. Ship privacy hygiene tooling: optional redaction pipelines, consent tracking
   for human data, explicit docs on immutable-log implications.

## Guardrails for Contributors

- Never hard-code credentials in tests or docs. Use
  `postgresql://sinex:<PLACEHOLDER>@localhost/sinex_dev` style examples and call
  out that they are placeholders.
- Keep all new ingress behind Unix sockets by default; require an explicit
  security review before exposing TCP listeners.
- Update this file (and `docs/vision/manifesto.md`) whenever a gap moves from
  red to green—call out the commit or module that closed it.

Security is a product story, not a subproject. Track these items alongside core
feature work so that the “cognitive prosthesis” does not become a liability.
