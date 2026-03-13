# Sinex

**Local-first event capture for your machine. Query your digital history like a database.**

<!-- Badges (CI not yet configured)
[![Build Status](https://img.shields.io/github/actions/workflow/status/...)](#)
[![License](https://img.shields.io/badge/license-MIT-blue)](#license)
-->

[Quick Start](#quick-start) · [Documentation](docs/README.md) · [Architecture](#architecture) · [Contributing](#contributing)

---

## What is Sinex?

Sinex captures events from your computer—filesystem changes, terminal commands, window focus, clipboard, system events—and stores them in a queryable PostgreSQL database with precise timestamps and provenance tracking.

It's infrastructure for building personal analytics, workflow automation, and context-aware tools on top of your own activity data.

## Why Sinex?

The value isn't individual events. It's the **relationships between them**.

### Temporal Joins

Ask questions impossible with disconnected data sources:

```sql
-- What was I researching when that build failed?
SELECT v.url
FROM commands c
JOIN visits v ON v.ts BETWEEN c.ts AND c.ts + interval '5 min'
WHERE c.command LIKE 'cargo test%'
  AND c.exit_code = 1
  AND v.domain = 'stackoverflow.com';
```

Shell history alone can't tell you this. Browser history alone can't either. Sinex captures both with shared timestamps, making the temporal join possible.

### Contextual Queries

Filter by activity context, not just content:

```bash
# Find Rust articles I read while my editor was focused
exo find --type "webpage" \
  --semantic-search "Rust procedural macros" \
  --since "1w" \
  --context '{"window_class": "Code - OSS"}'
```

### Pattern Detection

Automata can detect patterns across event streams:

```
# Repeated typo detection
(Command {text: "cargp ...", exit_code: 1}) ->
(Command {text: "cargo ...", exit_code: 0})
```

Five occurrences in an hour → suggest a shell alias.

---

## Architecture

```
Ingestors          Automata             Clients
  fs, terminal,      analytics,           CLI, browser
  desktop, system    derived nodes        extension
       │                 │                    │
       ▼                 ▼                    │
  ┌────────────────────────────────────────┐  │
  │         NATS JetStream                 │  │
  │       (Event Transport)                │  │
  └──────────────┬─────────────────────────┘  │
                 │                            │
                 ▼                            │
        ┌────────────────┐                    │
        │  sinex-ingestd │ Validate, persist  │
        └───────┬────────┘                    │
                │                             │
                ▼                             │
        ┌────────────────┐                    │
        │   PostgreSQL   │ TimescaleDB,       │
        │   + Extensions │ pgvector, pg_jsonschema     │
        └───────┬────────┘                    │
                │                             │
                ▼                             │
        ┌────────────────┐                    │
        │ sinex-gateway  │◄───────────────────┘
        │  (JSON-RPC)    │ Auth, rate limiting
        └────────────────┘
```

<details>
<summary><strong>Technical Stack</strong></summary>

- **Language:** Rust
- **Database:** PostgreSQL 18 + TimescaleDB + pgvector + pg_jsonschema
- **Messaging:** NATS JetStream for durable event transport
- **Deployment:** NixOS modules with systemd hardening
- **IDs:** UUIDv7-backed, time-ordered primary keys

</details>

<details>
<summary><strong>Component Details</strong></summary>

| Component | Purpose |
|-----------|---------|
| **Ingestors** | Capture events: filesystem, terminal, desktop, system |
| **NATS JetStream** | Durable message bus with replay capability |
| **sinex-ingestd** | Validate events, persist to Postgres, emit confirmations |
| **PostgreSQL** | Event storage with time-series optimization |
| **Automata** | Transform raw events into derived knowledge |
| **sinex-gateway** | JSON-RPC API with TLS, auth, rate limiting |

</details>

---

## Features

<details>
<summary><strong>For Users</strong></summary>

- **Event Sources:** Filesystem, terminal (Kitty/Atuin), clipboard, window focus, systemd, D-Bus
- **Dual-Mode Capture:** Real-time sensor mode + batch scanner mode
- **Query Interface:** `sinexctl`, SQL access, JSON-RPC API
- **Immutable History:** Events are append-only with full provenance

</details>

<details>
<summary><strong>For Developers</strong></summary>

- **Node SDK:** Build custom ingestors with `sinex-node-sdk`
- **Typed Events:** Derive macros for event payloads with validation
- **Test Infrastructure:** Isolated database pools, property testing, NATS fixtures
- **Schema Management:** JSON Schema generation from Rust types

</details>

<details>
<summary><strong>For Operators</strong></summary>

- **NixOS Module:** Declarative deployment with systemd integration
- **TLS/mTLS:** Required for non-loopback; optional client certs
- **Rate Limiting:** Per-token limits via governor (100 req/sec default)
- **Systemd Hardening:** NoNewPrivileges, ProtectSystem=strict, capability bounding

</details>

---

## Quick Start

```bash
# Clone and enter dev environment
git clone https://github.com/sinity/sinex.git && cd sinex
nix develop  # or: direnv allow

# Start infrastructure
xtask infra start

# Run core services
xtask run core --logs

# Inspect available nodes/bundles
xtask run list

# Query recent events
sinexctl recent -n 10
```

For database setup, TLS configuration, and RPC authentication, see the docs linked below.

---

## Principles

These guide architectural decisions:

- **Cognitive Sovereignty** — The human remains the ultimate authority. Every automation is explainable, inspectable, and reversible.

- **Local-First** — All core capabilities work without external services. Every byte of captured data stays under your direct control.

- **Single Event Stream** — Raw events and derived synthesis share one append-only log. Provenance (`source_event_ids`) distinguishes them, not separate storage.

- **Declarative Core** — Logic lives as data. Prefer SQL/flow definitions for deterministic synthesis; reserve imperative code for genuinely non-deterministic operations.

---

## Documentation

| I want to... | Start here |
|--------------|------------|
| Understand the architecture | [Core Architecture](docs/current/architecture/Core_Architecture.md) |
| Set up a development environment | [README.md](README.md#contributing) |
| Build a custom ingestor | [Node SDK Overview](crate/lib/sinex-node-sdk/docs/overview.md) |
| Write tests | [Testing Sandbox Guide](xtask/docs/sandbox/README.md) |
| Deploy on NixOS | [NixOS Module](nixos/README.md) |
| Understand event schemas | [Event Taxonomy](crate/lib/sinex-schema/docs/event-taxonomy.md) |
| Review security posture | [Security](docs/current/security.md) |

Full documentation index: [docs/README.md](docs/README.md)

---

## Contributing

### Codebase Orientation

```text
crate/
├── core/                      # Runtime binaries: ingestd + gateway
├── lib/                       # Shared libraries: primitives, schema, db, node-sdk, services, macros
├── nodes/                     # Ingestors + automatons
├── cli/                       # sinexctl
└── xtask/                     # Developer workflow automation
```

Rule of thumb:
- touch `crate/lib/` for shared types, schema, DB, and runtime patterns
- touch `crate/core/` for ingest/gateway runtime behavior
- touch `crate/nodes/` for event capture and derived-node logic
- touch `xtask/` for developer workflow automation

### Development Workflow

```bash
xtask check              # Fast iteration: compile check
xtask test               # Run affected tests
xtask check --full && xtask test  # Before commit
xtask ci workspace       # Full validation (before PR)
```

Use `xtask check --lint` when you want clippy in the fast loop, and
`xtask test --debug -E 'test(name)'` when you need a single test with full output.

If your change touches schema behavior, also run:

```bash
xtask ci schema-only
```

For local runtime work:

```bash
xtask infra start
xtask run core --logs
xtask run list
```

For payload/schema work:
- update payloads under `crate/lib/sinex-primitives/src/events/payloads/`
- update schema/taxonomy ownership in `sinex-schema` as needed
- run targeted `xtask test -p <package>`
- run `xtask ci schema-only` when schema behavior changes

For node work:
- implement against `sinex-node-sdk`
- use the standard node CLI shape (`service` / `scan` / `explore`)
- validate manually with `xtask run node <name>`

Deployment-facing details live with their owners:
- schema GitOps: [crate/core/sinex-ingestd/docs/schema_gitops.md](crate/core/sinex-ingestd/docs/schema_gitops.md)
- system deployment: [nixos/README.md](nixos/README.md)
- runtime invariants and operational architecture: [docs/current/architecture/SystemOperations_And_Integrity_Architecture.md](docs/current/architecture/SystemOperations_And_Integrity_Architecture.md)

See [CLAUDE.md](CLAUDE.md) for coding patterns and conventions.

### Good First Issues

- Add a new event source ingestor
- Improve query CLI ergonomics
- Write documentation for underdocumented components
- Add test coverage for edge cases

---

## Security

**Strengths:**
- Input validation with adversarial test coverage
- TLS-only gateway RPC; mTLS for non-loopback
- Bearer token authentication with constant-time comparison
- Per-token rate limiting (100 req/sec default)
- systemd hardening (NoNewPrivileges, ProtectSystem=strict)

**Gaps:**
- Core services still share one database role
- NATS transport is not enforced TLS-only by default
- Secret wiring is still only partially consolidated across services

Blanket at-rest encryption and automatic retention policies are not current system goals; the
intended model is capture-time privacy controls, host full-disk encryption, and explicit lifecycle
operations.

See [Security Posture](docs/current/security.md) for details.

---

## License

MIT. See [LICENSE](LICENSE).

---

<sub>Built for personal use. Not yet production-ready for general deployment.</sub>
