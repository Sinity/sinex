# Sinex

**Local-first event capture for your machine. Query your digital history like a database.**

[Quick Start](#quick-start) · [Architecture](#architecture) · [Security](#security) · [Deployment & Operations](#deployment--operations)

---

## What Is Sinex?

Sinex captures local activity as typed, timestamped events and stores them in
an append-only PostgreSQL event log. The runtime is built around Rust services,
NATS JetStream transport, and a NixOS deployment surface.

The system is meant for grounded local analytics, replayable derived state, and
operator-visible automation, not opaque cloud processing.

Example payoff:

```sql
-- What was I researching when that build failed?
SELECT v.url
FROM commands c
JOIN visits v ON v.ts BETWEEN c.ts AND c.ts + interval '5 min'
WHERE c.command LIKE 'cargo test%'
  AND c.exit_code = 1
  AND v.domain = 'stackoverflow.com';
```

## Current Surfaces

| Surface | Current Owner |
|---------|---------------|
| Capture | crate-local ingestors under `crate/nodes/` |
| Query / control | `sinex-gateway` + `sinexctl` |
| Persistence | `sinex-ingestd` + PostgreSQL |
| Derived state | automata and replay-aware node runtime |
| Deployment | NixOS modules + systemd |
| Extension | `sinex-node-sdk` |

## Architecture

```text
Ingestors          Automata             Clients
  fs, terminal,      analytics,           CLI, browser
  desktop, system    derived nodes        extension
       │                 │                    │
       ▼                 ▼                    │
  ┌────────────────────────────────────────┐  │
  │         NATS JetStream                 │  │
  │       (event transport)                │  │
  └──────────────┬─────────────────────────┘  │
                 │                            │
                 ▼                            │
        ┌────────────────┐                    │
        │  sinex-ingestd │ validate, persist  │
        └───────┬────────┘                    │
                │                             │
                ▼                             │
        ┌────────────────┐                    │
        │   PostgreSQL   │ TimescaleDB,       │
        │   + extensions │ pgvector, schemas  │
        └───────┬────────┘                    │
                │                             │
                ▼                             │
        ┌────────────────┐                    │
        │ sinex-gateway  │◄───────────────────┘
        │   JSON-RPC     │ auth, rate limits
        └────────────────┘
```

### Core Invariants

- canonical persistence flows through `sinex-ingestd`
- `core.events` is append-only; corrections become new events with provenance
- derived events carry source, temporal, and replay metadata
- `UUIDv7` IDs provide ordering; `ts_orig` and `ts_coided` are distinct and load-bearing
- blobs are content-addressed and referenced stably
- long-running replay/lifecycle work is recorded in `operations_log`

### Operating Model

- services run as separate systemd units with NixOS-managed configuration
- observability is journald-first; service logs are part of the event universe
- nodes and derived automata recover through checkpoints and replay
- replay, archive, and restore are explicit control-plane operations
- direct DB access is diagnostic; the normal control/query boundary is the gateway

### Stack

- Rust
- PostgreSQL 18 + TimescaleDB + pgvector + pg_jsonschema
- NATS JetStream
- NixOS modules + systemd hardening

## Quick Start

```bash
git clone https://github.com/sinity/sinex.git
cd sinex
nix develop  # or: direnv allow

xtask infra start
xtask run core --logs
xtask run list
sinexctl recent -n 10
```

## Development

```bash
xtask check
xtask test
xtask check --full && xtask test
```

Useful entrypoints:

- local runtime loop: `xtask infra start` and `xtask run core --logs`
- xtask/tooling reference: [xtask/README.md](xtask/README.md)
- testing workflows: [xtask/docs/sandbox/README.md](xtask/docs/sandbox/README.md)

## Deployment & Operations

The canonical deployment surface is the NixOS module tree under `services.sinex`.
Stable operational guidance lives here and in [nixos/modules/README.md](nixos/modules/README.md).
There is no separate top-level operations runbook anymore.

Hardening defaults that are already part of the repo:

- gateway RPC is TLS-only and non-loopback binds require mTLS policy
- managed long-running units and helper/maintenance oneshots use systemd sandboxing
- managed local NATS now has typed server TLS under `services.sinex.nats.tls.*`
- managed local NATS now has typed subject-level authz for the current shared runtime identity under `services.sinex.nats.authorization.sharedClient.*`
- shared client transport still lives under `services.sinex.nodes.nats.{servers,tls,auth}` and is exported to all managed services automatically

Conventional secret names that the module now resolves automatically through agenix:

- gateway admin token: `sinex-gateway-admin-token`
- local NATS server TLS: `sinex-nats-server-cert`, `sinex-nats-server-key`, `sinex-nats-client-ca`
- shared NATS client TLS/auth: `sinex-nats-ca`, `sinex-nats-client-cert`, `sinex-nats-client-key`, `sinex-nats-client-creds`, `sinex-nats-client-nkey`, `sinex-nats-token`
- compatibility aliases are also accepted for the NATS client path: `nats-ca`, `nats-client-cert`, `nats-client-key`, `nats-client-creds`, `nats-client-nkey`, `nats-token`

Common operator entrypoints:

```bash
xtask doctor
xtask doctor --deployment-readiness
xtask status --summary
xtask infra status
journalctl -u sinex-gateway -u sinex-ingestd -f
```

## Documentation

| I want to... | Start here |
|--------------|------------|
| Understand the system shape | [README.md#architecture](README.md#architecture) |
| Deploy and harden the common NixOS path | [README.md#deployment--operations](README.md#deployment--operations) |
| Deploy on NixOS | [nixos/README.md](nixos/README.md) |
| Build a node or derived service | [crate/lib/sinex-node-sdk/docs/overview.md](crate/lib/sinex-node-sdk/docs/overview.md) |
| Understand event schemas | [crate/lib/sinex-schema/docs/event-taxonomy.md](crate/lib/sinex-schema/docs/event-taxonomy.md) |
| Work on the CLI/tooling loop | [xtask/README.md](xtask/README.md) |

## Security

Threat model shorthand:

- trusted single-user local host
- nodes submit over NATS; the gateway is the hardened external boundary
- canonical persistence stays single-writer through `sinex-ingestd`
- host full-disk encryption and capture-time privacy controls are the intended baseline

Current controls:

- typed payload validation with schema checks
- TLS-only gateway RPC; non-loopback binds require stronger transport policy
- bearer-token auth with constant-time comparison
- per-token rate limiting
- structured request access audit logs on RPC, SSE, and native-messaging dispatch paths
- systemd hardening from the NixOS deployment layer, including helper/maintenance units
- typed managed-NATS TLS and subject-level authorization surfaces in the NixOS module

## License

MIT. See [LICENSE](LICENSE).

---

<sub>Built for personal use. Not yet production-ready for general deployment.</sub>
