Status: canonical
Last Verified: 2026-02-19 (code review)
> **Purpose:** Canonical threat model + control-plane reference; pair with `docs/current/security.md` for current posture updates.

# Security Architecture

## Overview

Sinex implements defense-in-depth for data at rest, in transit, and during
processing. The system primarily runs as a single-user deployment on the local
host; the threat model reflects that assumption explicitly.

Coordinate changes here with `docs/current/security.md` (live posture) and
`docs/current/architecture/Core_Architecture.md` (JetStream pipeline expectations).

---

## Current Security Implementation

### Process Isolation

✅ **Implemented via NixOS systemd hardening** (`nixos/modules/node-services.nix`):

- `NoNewPrivileges = true` — prevents privilege escalation
- `ProtectSystem = "strict"` — makes system directories read-only
- `PrivateTmp = true` — isolates temporary directories
- `MemoryMax` / `CPUQuota` — resource limits per service

⚠️ **Not yet applied**: `SystemCallFilter` — the nix modules do not currently
restrict the syscall surface. This would meaningfully reduce the blast radius of
a compromised service binary.

### Access Control

⚠️ **Partial — needs hardening**:

- NixOS units run as the dedicated `sinex` system user with local peer
  authentication to PostgreSQL (no password in transit).
- All services currently share the same PostgreSQL role; **role separation and
  scoped credentials are outstanding work** (tracked under Implementation Priorities).
- Gateway RPC requires TLS + bearer token auth (`SINEX_RPC_TOKEN` or file-based
  equivalents). The gateway refuses to start without a token.
- JetStream subjects still lack fine-grained authorisation; any client with NATS
  access can publish to any subject.

### Gateway Authentication

✅ **Implemented** (`crate/core/sinex-gateway/src/rpc_server.rs`):

- Gateway **refuses to start** unless `SINEX_RPC_TOKEN`, `SINEX_RPC_TOKEN_FILE`,
  or `SINEX_GATEWAY_ADMIN_TOKEN_FILE` is set and non-empty.
- All JSON-RPC clients must send `Authorization: Bearer <token>`.
- Token is loaded in priority order:
  `SINEX_GATEWAY_ADMIN_TOKEN_FILE` → `SINEX_RPC_TOKEN_FILE` → `SINEX_RPC_TOKEN`.
- Bearer token parsing and constant-time comparison are enforced for every request.

### Network / Transport Security

✅ **Gateway TCP**: any TCP bind requires TLS cert/key; plaintext is disallowed.  
✅ **NATS TLS**: when `SINEX_NATS_REQUIRE_TLS=1`, plaintext connections are
  rejected at config validation (`crate/lib/sinex-primitives/src/nats.rs`).  
⚠️ **NATS auth**: no per-node credentials or JetStream subject ACLs yet. All
  nodes share the same NATS connection.  
⚠️ **mTLS**: `SINEX_GATEWAY_TLS_CLIENT_CA` enables mTLS for non-local binds but
  is not enforced by default; loopback binding remains the default.

### Input Validation

✅ **Multi-layer validation**:

- JSON Schema validation for event payloads (strict mode configurable via
  `SINEX_INGESTD_STRICT_VALIDATION`).
- ULID format validation at ingest.
- SQL injection prevention via `sqlx::QueryBuilder` throughout.
- Database URL passwords redacted in preflight and logs
  (`crate/lib/sinex-node-sdk/src/preflight/`).
- Terminal command `argv` scrubbing and all ingestion boundaries route through
  `privacy::engine().process()` with the appropriate `ProcessingContext`. The unified
  privacy engine provides 31+ rules (secrets, PII, infrastructure, window-title),
  structural validation (Luhn for credit cards, mod-97 for IBAN), and 5 strategies
  (Redact, Encrypt, Hash, Suppress, Mask).

⚠️ **Partial**: Event payloads not matched by privacy engine rules are captured
verbatim — mitigation is pgsodium field encryption (see below).

### Event Payload Scrubbing

✅ **Implemented via unified privacy engine** (`sinex_primitives::privacy`):

- 31+ rules: 17 secret detectors, 5 PII (credit card/Luhn, SSN, email, phone,
  IBAN/mod-97), 5 infrastructure (IPv4, IPv6, MAC, hostname, home path), 4 window-title
- 5 strategies: Redact, Encrypt (XChaCha20-Poly1305), Hash (BLAKE3 MAC), Suppress, Mask
- 8 processing contexts: Command, Clipboard, WindowTitle, Journal, Dbus, Notification,
  Document, Metadata
- Configuration: `SINEX_PRIVACY_*` env vars or TOML at `$SINEX_STATE_DIR/privacy.toml`
- Per-rule match stats tracking; CLI via `xtask privacy catalog|test|decrypt|key|stats`

---

## Unimplemented Security Features

### Database Encryption (pgsodium)

❌ **Not Implemented** — critical gap

pgsodium provides field-level transparent encryption for:

- Sensitive event payloads (passwords, API keys captured in commands or URLs)
- Personal information (file paths, window titles, notes)
- Knowledge-graph content

See [Database Encryption Roadmap](../../planning/roadmap/features/database-encryption-pgsodium.md).

### Audit Logging

❌ **Not Implemented** — no structured record of data access exists

Without audit logging:

- Cannot detect unauthorized data access after the fact
- Cannot satisfy "who read what and when" for compliance
- Replay of sensitive data is undetectable

Minimum viable: PostgreSQL `log_statement = 'mod'` + structured log forwarding.
Better: application-level `access_log` events for sensitive RPC methods.

### PostgreSQL Role Separation

❌ **Not Implemented** — all services share one role

Current risk: a compromised ingestd can `SELECT` from any table including
knowledge-graph content. Mitigation: separate `sinex_ingest`, `sinex_read`,
`sinex_admin` roles with `GRANT` scoping.

### Secrets Management (agenix)

⚠️ **Partial**:

- ✅ Implemented in user's NixOS host config for host secrets (SSH keys, etc.)
- ✅ `SINEX_RPC_TOKEN` and API keys are injected from the host environment
- ❌ Not integrated into the Sinex project's own NixOS modules
- ❌ No pgsodium master key lifecycle management

Current approach may be sufficient while pgsodium is unimplemented.

---

## Security Model

### Trust Boundaries

1. **User ↔ System**: Full trust (single-user system)
2. **Nodes ↔ ingestd**: NATS (no per-node creds yet); TLS enforced when
   `SINEX_NATS_REQUIRE_TLS=1`
3. **ingestd ↔ Database**: single PostgreSQL role via peer auth; **no column-level
   access control yet** (pgsodium + role separation are the mitigations)
4. **Automata ↔ JetStream**: durable consumer isolation per subject, but no ACL
   enforcement
5. **External APIs**: API keys injected from host environment

### Data Classification

| Level | Examples | Protection |
|-------|----------|-----------|
| Public | System metrics, heartbeats | None required |
| Private | File paths, window titles, command history | DB access control (pending) |
| Sensitive | Passwords, API keys, personal notes | pgsodium field encryption (pending) |
| Critical | DB encryption keys, RPC tokens | agenix / file permissions |

### Threat Model

**In Scope**:

- Accidental data exposure via logs or exports
- Unauthorized access to the PostgreSQL database
- Memory disclosure (process crash dumps, `/proc` reads)
- Malicious event injection (spoofed source or event_type)
- Resource exhaustion (JetStream flooding, DB overload)

**Out of Scope** (single-user assumption):

- Multi-user access control
- Network-based remote exploitation (local-only binding default)
- Physical access attacks
- Supply chain attacks

---

## Implementation Priorities

### 🚨 Critical (Do First)

1. **Enable pgsodium encryption**
   - No workaround for data-at-rest exposure without it
   - Prerequisite for storing sensitive event payloads safely
   - Requires key management strategy (agenix integration)

2. **Implement audit logging**
   - Minimum: `log_statement = 'mod'` in PostgreSQL config
   - Better: application-level `sinex.audit.*` events for RPC access to sensitive data

### ⚠️ Important (Do Soon)

1. **PostgreSQL role separation**
   - `sinex_ingest` for ingestd (INSERT only on `core.events`)
   - `sinex_read` for gateway read queries
   - `sinex_admin` for migrations and schema management

2. **Syscall filter hardening**
   - Add `SystemCallFilter` to `nixos/modules/node-services.nix`
   - Use `@system-service` allowlist as starting point
   - Test each binary for required syscalls before deploying

3. **NATS per-node credentials**
   - Issue JWT/NKEY credentials per node to allow JetStream subject ACLs
   - Prevents a compromised node from publishing to other nodes' subjects

### 📋 Nice to Have

1. **agenix integration into Sinex NixOS modules**
   - Manage `SINEX_RPC_TOKEN` and future pgsodium keys via agenix secrets

---

## Threat Model Summary

### Information Disclosure

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Unauthorized DB access | Peer auth + role separation | Peer auth ✅, roles ❌ |
| Sensitive payload in logs | pgsodium + log scrubbing | ❌ |
| Filesystem eavesdropping | LUKS FDE (NixOS host config) | Out of Sinex scope |
| Network service exposure | Localhost binding + TLS + token auth | ✅ |
| Sensitive data in events | Privacy engine (31+ rules, 5 strategies, context-aware) | ✅ |
| Evdev keylogging | Opt-in + privilege separation | ✅ |

### Tampering

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Database corruption | Append-only events + backups + checksums | ✅ |
| Git-annex tampering | Content-addressed storage | ✅ |
| Binary tampering | NixOS immutability + version control | ✅ |
| Malicious event injection | JSON Schema validation + ULID format check | ✅ |

### Denial of Service

| Threat | Mitigation | Status |
|--------|-----------|--------|
| DB overload | Connection pooling + query timeouts | ✅ |
| JetStream flooding | `max_ack_pending` + backpressure + DLQ | ✅ |
| Gateway overload | Rate limiting (NATS KV distributed) + concurrency limit | ✅ |
| Resource exhaustion | Systemd `MemoryMax` / `CPUQuota` | ✅ |

### Privilege Escalation

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Process privilege escalation | `NoNewPrivileges` | ✅ |
| SQL injection | Parameterized queries only | ✅ |
| Path traversal | Input validation + `SanitizedPath` type | ✅ |
| Syscall exploitation | `SystemCallFilter` | ❌ not yet applied |

---

## Pre-Deployment Checklist

Items owned by the **Sinex project** (not the host NixOS config):

| Item | Status |
|------|--------|
| `SINEX_RPC_TOKEN` set and non-empty | Required — gateway refuses to start otherwise |
| Gateway TLS cert/key configured | Required for any non-loopback bind |
| `SINEX_NATS_REQUIRE_TLS=1` set in production | Manual step |
| pgsodium configured with secure key | ❌ Not yet implemented |
| PostgreSQL role separation applied | ❌ Not yet implemented |
| Audit logging enabled | ❌ Not yet implemented |

Items owned by the **NixOS host configuration** (outside Sinex scope):

- LUKS full-disk encryption
- Firewall rules (`nftables` / `iptables`)
- Regular OS security updates
- Backup strategy

---

## References

- [Database Encryption with pgsodium](../../planning/roadmap/features/database-encryption-pgsodium.md)
- [Gateway Coordination](./gateway-coordination.md) — distributed rate limiting and TLS details
- [Network Security](./network-security.md) — NATS TLS enforcement details
- NixOS module: `nixos/modules/node-services.nix` — systemd hardening settings

For an up-to-date checklist of implemented controls and open gaps, see [Security & Privacy Posture](../security.md).
