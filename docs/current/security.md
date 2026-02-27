# Security Posture

> Last Verified: 2026-02-19 (manual review)

*Pair with `docs/current/architecture/security-architecture.md` for the broader threat model.*

## Current Strengths

- **Input validation:** `sinex_primitives::types::validation` enforces path sanitisation,
  JSON depth limits, and command-injection guards. Adversarial tests cover
  traversal, null-byte injection, Unicode edge cases, and SQLi attempts.
- **Privacy engine:** `sinex_primitives::privacy::engine()` provides a unified
  `PrivacyEngine` (initialized via `OnceLock` from `PrivacyConfig::from_env()`) applied
  at every ingestion boundary — journal messages, D-Bus payloads, terminal commands, and
  window titles. 31+ rules span 5 categories: 17 secret detectors (AWS keys, GitHub PATs,
  Slack tokens, JWTs, Google API keys, Azure connection strings, URL credentials, generic
  `PASSWORD=` assignments, etc.), 5 PII detectors (credit card via Luhn, SSN, email,
  phone, IBAN via mod-97), 5 infrastructure detectors (IPv4, IPv6, MAC, hostname, home
  path), and 4 window-title privacy rules. Five strategies are available: Redact (lossy),
  Encrypt (XChaCha20-Poly1305, reversible), Hash (BLAKE3 MAC, correlatable), Suppress
  (drop field), and Mask (partial obscure). Processing is context-aware across 8 contexts
  (Command, Clipboard, WindowTitle, Journal, Dbus, Notification, Document, Metadata).
  Configuration via `SINEX_PRIVACY_*` env vars or TOML file at
  `$SINEX_STATE_DIR/privacy.toml`.
- **Process isolation:** NixOS/unit files apply strict systemd hardening
  (NoNewPrivileges, `ProtectSystem=strict`, per-service cgroups, capability bounding).
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
| Data cleanup tooling | **Missing** | No automated tooling for deleting old events or redacting sensitive data post-ingestion. |
| Redaction configuration | ✅ **Implemented** | Unified privacy engine with TOML config (`$SINEX_STATE_DIR/privacy.toml`), per-rule overrides, category filtering, and context-aware processing via `SINEX_PRIVACY_*` env vars. |

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
- All ingestion boundaries must route text through `privacy::engine().process()`
  with the appropriate `ProcessingContext`. Do not add new ingestion paths without
  privacy processing.
- Update this file whenever a gap moves from red to green — call out the commit
  or module that closed it.

Security is a product story, not a subproject. Track these items alongside core
feature work.
