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
| Capture | source units over staged materials, input-shape adapters, and parsers under `crate/sinexd/src/sources/` |
| Query / control | `sinexd::api` + `sinexctl` |
| Persistence | `sinexd::event_engine` + PostgreSQL |
| Derived state | `sinexd::automata` and replay-aware stream runtime |
| Deployment | NixOS modules + systemd |
| Runtime extension | inline `sinexd` runtime support and source-unit/automaton traits |

## Architecture

The capture layer is being reframed from "one ingestor crate per source domain"
to a staged-source parser substrate: source material is registered, an
input-shape adapter enumerates records or bytes, and a parser emits
material-provenance events. See
[`crate/sinexd/docs/sources/staged_source_parser_substrate.md`](crate/sinexd/docs/sources/staged_source_parser_substrate.md).
The diagram below shows the deployed runtime shape; #1054 owns the remaining
decision about whether staged local parsers always cross NATS or can run closer
to persistence.

```text
sinexd::sources    sinexd::automata      Clients
  fs, terminal,      analytics,           CLI, browser
  desktop, system,   derived nodes        extension
  browser, exports
       │                 │                    │
       ▼                 ▼                    │
  ┌────────────────────────────────────────┐  │
  │         NATS JetStream                 │  │
  │       (event transport)                │  │
  └──────────────┬─────────────────────────┘  │
                 │                            │
                 ▼                            │
        ┌──────────────────┐                  │
        │ sinexd           │                  │
        │ ::event_engine   │ validate, persist │
        └───────┬──────────┘                  │
                │                             │
                ▼                             │
        ┌────────────────┐                    │
        │   PostgreSQL   │ TimescaleDB,       │
        │   + extensions │ pgvector, schemas  │
        └───────┬────────┘                    │
                │                             │
                ▼                             │
        ┌──────────────────┐                  │
        │ sinexd           │◄─────────────────┘
        │ ::api            │ auth, rate limits
        │   JSON-RPC       │
        └──────────────────┘
```

### Core Invariants

- canonical persistence flows through `sinexd::event_engine`
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
- direct DB access is diagnostic; the normal control/query boundary is `sinexd::api`

### Stack

- Rust
- PostgreSQL 18 + TimescaleDB + pgvector + pg_jsonschema
- NATS JetStream
- NixOS modules + systemd hardening

## Quick Start

```bash
git clone https://github.com/sinity/sinex.git
cd sinex
direnv allow  # loads the flake devShell and puts xtask on PATH

xtask infra start
xtask run core --logs
xtask run list
sinexctl recent -n 10
```

## Development

Start with the canonical repo workflow docs:

- contributing workflow: [CONTRIBUTING.md](CONTRIBUTING.md)
- testing workflow: [TESTING.md](TESTING.md)
- local runtime loop: `xtask infra start` and `xtask run core --logs`
- xtask/tooling reference: [xtask/docs/README.md](xtask/docs/README.md)
- sandbox harness details: [xtask/docs/sandbox/README.md](xtask/docs/sandbox/README.md)

## Deployment & Operations

The canonical deployment surface is the NixOS module tree under `services.sinex`.
Stable operational guidance lives here and in [nixos/modules/README.md](nixos/modules/README.md).
There is no separate top-level operations runbook anymore.

Hardening defaults that are already part of the repo:

- API RPC is TLS-only and non-loopback binds require mTLS policy
- managed long-running units and helper/maintenance oneshots use systemd sandboxing
- managed local NATS now has typed server TLS under `services.sinex.nats.tls.*`
- managed local NATS now has typed subject-level authz for the current shared runtime identity under `services.sinex.nats.authorization.sharedClient.*`
- shared client transport still lives under `services.sinex.nodes.nats.{servers,tls,auth}` and is exported to all managed services automatically

Conventional secret names that the module now resolves automatically through agenix:

- API admin token: `sinex-api-admin-token`
- local NATS server TLS: `sinex-nats-server-cert`, `sinex-nats-server-key`, `sinex-nats-client-ca`
- shared NATS client TLS/auth: `sinex-nats-ca`, `sinex-nats-client-cert`, `sinex-nats-client-key`, `sinex-nats-client-creds`, `sinex-nats-client-nkey`, `sinex-nats-token`
- compatibility aliases are also accepted for the NATS client path: `nats-ca`, `nats-client-cert`, `nats-client-key`, `nats-client-creds`, `nats-client-nkey`, `nats-token`

Common operator entrypoints:

```bash
xtask doctor
xtask doctor --deployment-readiness
xtask status --summary
xtask infra status
journalctl -u sinexd -f
```

## Documentation

| I want to... | Start here |
|--------------|------------|
| Understand the system shape | [README.md#architecture](README.md#architecture) |
| Deploy and harden the common NixOS path | [README.md#deployment--operations](README.md#deployment--operations) |
| Deploy on NixOS | [nixos/README.md](nixos/README.md) |
| Build a source unit or derived service | [crate/sinexd/docs/sources/README.md](crate/sinexd/docs/sources/README.md) |
| Understand event schemas | [crate/sinex-db/docs/schema/event-taxonomy.md](crate/sinex-db/docs/schema/event-taxonomy.md) |
| Separate notes, typed records, graph, and artifacts | [crate/sinex-primitives/docs/knowledge_boundaries.md](crate/sinex-primitives/docs/knowledge_boundaries.md) |
| Define current-state projections for event-native domains | [crate/sinex-primitives/docs/domain_reducers.md](crate/sinex-primitives/docs/domain_reducers.md) |
| Model tasks as event-native workflow objects | [issue #1107](https://github.com/Sinity/sinex/issues/1107) |
| Model sensitive health and self-observation logs | [issue #1108](https://github.com/Sinity/sinex/issues/1108) |
| Model declarations, omissions, and conceptual time | [issue #1113](https://github.com/Sinity/sinex/issues/1113) |
| Design interval-backed moment queries | [issue #1110](https://github.com/Sinity/sinex/issues/1110) |
| Define versioned SQL-shaped derivations | [issue #1117](https://github.com/Sinity/sinex/issues/1117) |
| Record replayable inference confidence and seeds | [issue #1118](https://github.com/Sinity/sinex/issues/1118) |
| Explain semantic composition beyond ancestry trace | [issue #1114](https://github.com/Sinity/sinex/issues/1114) |
| Run replay-safe semantic experiments | [issue #1109](https://github.com/Sinity/sinex/issues/1109) |
| Route model calls through prompts, policy, and budgets | [issue #1116](https://github.com/Sinity/sinex/issues/1116) |
| Promote generated suggestions through human or policy authority | [crate/sinex-primitives/docs/curation_authority.md](crate/sinex-primitives/docs/curation_authority.md) |
| Bound active instruction and actuator loops | [issue #1104](https://github.com/Sinity/sinex/issues/1104) |
| Expose read-only evidence to coding agents | [crate/sinexctl/docs/mcp_readonly_server.md](crate/sinexctl/docs/mcp_readonly_server.md) |
| Rename event taxonomy labels without parser replay | [issue #1101](https://github.com/Sinity/sinex/issues/1101) |
| Reason about replay evidence and source snapshots | [crate/sinexd/docs/sources/evidence_lanes.md](crate/sinexd/docs/sources/evidence_lanes.md) |
| Reason about large aggregate provenance | [crate/sinexd/docs/automata/high_fan_in_lineage.md](crate/sinexd/docs/automata/high_fan_in_lineage.md) |
| Coordinate late-arriving evidence in derived outputs | [issue #1111](https://github.com/Sinity/sinex/issues/1111) |
| Reason about runtime backpressure and loss policy | [crate/sinexd/docs/runtime_qos.md](crate/sinexd/docs/runtime_qos.md) |
| Suppress live capture through private mode | [crate/sinexctl/docs/private_mode.md](crate/sinexctl/docs/private_mode.md) |
| Add a staged personal-export parser | [crate/sinexd/docs/sources/adding_staged_export_parser.md](crate/sinexd/docs/sources/adding_staged_export_parser.md) |
| Drain and recover source-unit material cleanly | [crate/sinexd/docs/sources/source_unit_drain.md](crate/sinexd/docs/sources/source_unit_drain.md) |
| Snapshot or restore runtime state | [crate/sinexctl/docs/state_snapshot.md](crate/sinexctl/docs/state_snapshot.md) |
| Configure PostgreSQL backup/restore | [crate/sinex-db/docs/backup_restore.md](crate/sinex-db/docs/backup_restore.md) |
| Decide which surface owns a runtime or data concern | [.github/authority-surfaces.md](.github/authority-surfaces.md) |
| Integrate an external tool or sibling project | [crate/sinexd/docs/sources/integration_authority.md](crate/sinexd/docs/sources/integration_authority.md) |
| Work on repo workflow or verification | [CONTRIBUTING.md](CONTRIBUTING.md), [TESTING.md](TESTING.md) |
| Work on the CLI/tooling loop | [xtask/docs/README.md](xtask/docs/README.md) |

## Security

Threat model shorthand:

- trusted single-user local host
- nodes submit over NATS; the `sinexd` API is the hardened external boundary
- canonical persistence stays single-writer through `sinexd::event_engine`
- host full-disk encryption and capture-time privacy controls are the intended baseline

Current controls:

- typed payload validation with schema checks
- TLS-only API RPC; non-loopback binds require stronger transport policy
- bearer-token auth with constant-time comparison
- per-token rate limiting
- structured request access audit logs on RPC, SSE, and native-messaging dispatch paths
- systemd hardening from the NixOS deployment layer, including helper/maintenance units
- typed managed-NATS TLS and subject-level authorization surfaces in the NixOS module

## License

MIT.

---

<sub>Built for personal use. Not yet production-ready for general deployment.</sub>
