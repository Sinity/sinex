# Deployment Topology

Status: design record. Companion to `runtime-boundaries.md`; supersedes
target-vision/report/deployment.md as the canonical sinex-side authority.

Where `runtime-boundaries.md` defines the process topology and trust
edges, this doc defines the host wiring: service users, file permissions,
systemd ordering and hardening, agenix secret inventory, and the
preflight contract. It is the operator-facing record for "what does a
correctly deployed sinex host look like?"

## Reference Deployment

`sinnix-prime` runs `services.sinex.enable = true; provisionDatabase =
true`. The deployment includes nine sinex systemd units plus PostgreSQL
and NATS:

| Service | Role | Notes |
|---|---|---|
| `sinex-ingestd` | Durable writer | COPY-protocol batched writes; envelope validation |
| `sinex-gateway` | JSON-RPC + SSE | Token-suffix RBAC |
| `sinex-fs-ingestor` | Filesystem domain | SDK buffered append streams (metadata only) |
| `sinex-terminal-ingestor` | Terminal domain | Continuous watchers; bootstraps from live tail |
| `sinex-desktop-ingestor` | Desktop domain | Target-user Hyprland bridge under hardening |
| `sinex-system-ingestor` | System/journal domain | Bounded historical import; deterministic UUIDv7 ids |
| `sinex-browser-ingestor` | Browser domain | Snapshot mode (no startup replay) |
| `sinex-document-ingestor` | Document domain | Snapshot/historical only |
| `sinex-process` | Domain automata | 6 per-automaton units (canonicalizer, analytics, health, session-detector, hourly summarizer, daily summarizer) |
| `sinex-schema-apply` | Declarative convergence | `Type=oneshot`, exits cleanly |

The legacy per-domain ingestor surface is transitional. Source-worker
migration (`runtime-boundaries.md`, #1125, #1126) replaces them with
per-source-unit units of one `sinex-source-worker` binary.

## Service User Permission Model

| User | UID | Owns | Reads | Writes | Notes |
|---|---|---|---|---|---|
| `sinex` | 991 | PostgreSQL data dir (delegated), CAS root, agenix run dir | `/realm/project/*` (world-readable), journald, Hyprland socket, Atuin DB (post ACL-mask), browser history roots | Sinex state dirs | `ProtectHome=true`; no broad access to `/home/sinity` |
| `sinity` | 1000 | `/home/sinity` (0700), `/realm/data/` | own data | own data | Data owner; never runs sinex services directly |
| `root` | 0 | ACL bridge oneshots only | n/a | n/a | Used for one-shot setup units; long-running services never run as root |

`/home/sinity` is hidden from sinex services by `ProtectHome=true`.
Access to specific paths under the target user's home (Hyprland socket,
Atuin DB, browser history) is mediated by explicit ACL bridges, not by
broad home visibility. This satisfies the T2 (unauthorized local access)
mitigation: a misbehaving sinex service cannot exfiltrate arbitrary user
files.

## Systemd Ordering

```
postgresql ─┐
            ├─► sinex-schema-apply ─► sinex-preflight ─┬─► sinex-ingestd ─► (all ingestors)
nats ───────┘                                          └─► sinex-gateway
                                                       │
nats-bootstrap (coreRequires of ingestd/gateway, guarded by preflight)
```

Properties:

- `sinex-schema-apply` runs `sinex-schema apply` as a `Type=oneshot`. The
  unit exits cleanly when convergence is complete.
- `sinex-preflight` (`Type=oneshot`) gates ingestd and gateway. A preflight
  failure with `failure_action = abort` (the default) prevents the
  writers and the query surface from coming up; the operator sees the
  failure in `systemctl status` rather than discovering it through a
  silently degraded ingest path.
- `sinex-ingestd` is the ordering parent for every domain ingestor and
  `sinex-process`. Ingestors do not start before ingestd is ready.
- NATS sits parallel to PostgreSQL; both are upstream of preflight.

## Service Hardening

All sinex services use a shared hardening profile (see
`nixos/modules/lib/systemd-hardening.nix`). Baseline knobs:

| Directive | Value |
|---|---|
| `Type` | `notify` (long-running) or `oneshot` |
| `WatchdogSec` | `60` |
| `ProtectSystem` | `strict` |
| `ProtectHome` | `true` |
| `PrivateTmp` | `true` |
| `NoNewPrivileges` | `true` |
| `MemoryDenyWriteExecute` | `true` |
| `LockPersonality` | `true` |
| `RestrictNamespaces` | `true` |
| `RestrictRealtime` | `true` |
| `RestrictSUIDSGID` | `true` |
| `RemoveIPC` | `true` |
| `ProtectKernelTunables` | `true` |
| `ProtectKernelModules` | `true` |
| `ProtectKernelLogs` | `true` |
| `ProtectClock` | `true` |
| `ProtectControlGroups` | `true` |
| `SystemCallArchitectures` | `native` |
| `SystemCallFilter` | `@system-service ~@privileged` |
| `RestrictAddressFamilies` | `AF_UNIX AF_INET AF_INET6` |
| `UMask` | `0077` |
| `Restart` | `on-failure` |

`ReadWritePaths` and `ReadOnlyPaths` are unit-specific and live in the
NixOS module per service.

The historical "hardening gap" inventory (closed in #990) is no longer the
operator's frame of reference. To verify the live hardening profile,
inspect the rendered NixOS unit rather than treating any vision document
or older gap list as current.

## Preflight Contract

`sinex-preflight verify` runs seven read-only phases. Each phase
classifies as pass / warn / fail; `failure_action` (per phase) governs
whether a failure aborts startup, warns, or is ignored.

| # | Phase | What it verifies |
|---|---|---|
| 1 | `database` | Connectivity, schema access, transactions, concurrent queries |
| 2 | `extensions` | `timescaledb`, `pg_jsonschema`, `vector`, `pg_trgm` present |
| 3 | `migrations` | Schema dry-run (check-only, no writes) |
| 4 | `resources` | Filesystem permissions, systemd hardening audit |
| 5 | `configuration` | Environment variable validation, secret file accessibility |
| 6 | `services` | NATS connectivity, JetStream stream existence |
| 7 | `integration` | End-to-end DB + service integration check |

All phases are read-only by contract: preflight cannot mutate live state.
Phases can be skipped per-deployment via
`lifecycle.preflight.skip` in the NixOS module; this is escape-valve
machinery, not a normal operating mode.

### False-Readiness Surfaces

Preflight passing is a necessary condition, not a sufficient one.
Specifically:

1. `/health` and `/ready` are not the same endpoint. Deployment checks
   must use readiness, not liveness.
2. A gateway ACK is not proof of persistence. The smoke test only passes
   when the persisted event is queryable.
3. Database-backed readiness alone is still not persistence proof. Only a
   persisted smoke event round-trips end-to-end.
4. Preflight verifies the unit contract, not live serving semantics. It
   cannot substitute for a real rebuild plus a persisted smoke event.

Operator runbook for a new host bring-up should always include a
post-rebuild smoke event probe; preflight is not a stand-in.

## Agenix Secret Inventory

Eight agenix-managed secrets decrypt to `/run/agenix/` (0400, sinex user).

| Secret | Purpose | Required when |
|---|---|---|
| `sinex-local-db` | PostgreSQL scram-sha-256 password | All non-dev modes |
| `sinex-gateway-admin-token` | Gateway admin RPC bearer | Gateway admin operations |
| `sinex-nats-ca` | NATS CA cert | NATS TLS in production |
| `sinex-nats-client-cert` | NATS client cert | NATS mTLS |
| `sinex-nats-client-key` | NATS client key | NATS mTLS |
| `sinex-nats-token` | NATS bearer token | NATS auth in token mode |
| `sinex-nats-client-creds` | NATS user credentials file | NATS auth in creds mode |
| `sinex-nats-client-nkey` | NATS NKey seed | NATS auth in NKey mode |

Dev mode (trust auth, no TLS) requires none. Production requires
`sinex-local-db` and `sinex-gateway-admin-token` unconditionally; NATS
secrets are conditional on the chosen auth mode.

The `sinex-privacy-key` agenix secret is owned by `at-rest-encryption.md`
and is not enumerated here; it is not a transport secret.

## Known Gaps

| Gap | Tracking |
|---|---|
| VM coverage lags runtime-target model | #318 |
| Source-worker drain / in-flight material shutdown | #1125 |
| Source operation status surfaces | #1124 |

The historical operational-gap inventory (backup/WAL/compression/
telemetry/hardening/CAS/watcher recovery) was closed in the May 2026
audit cleanup. Live regressions should be filed as concrete issues
rather than tracked in this document.

## What This Doc Does Not Own

- Process topology and trust edges: `runtime-boundaries.md`.
- Threat assumptions behind these controls: `threat-model.md`.
- Key generation/rotation lifecycle: `at-rest-encryption.md`.
- Operator-facing data verbs (export/delete/audit): `gdpr-rights-surface.md`.
- Private-mode toggles: `runtime-private-mode.md`.

## Related

- `docs/architecture/runtime-boundaries.md`
- `docs/architecture/threat-model.md`
- `docs/architecture/at-rest-encryption.md`
- `docs/architecture/gdpr-rights-surface.md`
- Source: `nixos/modules/lib/systemd-hardening.nix`,
  `nixos/modules/database.nix`,
  `nixos/modules/preflight-verification.nix`
- Issues: #318, #1124, #1125, #1442
