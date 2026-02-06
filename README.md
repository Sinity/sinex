# Sinex

**Local-first event capture for your machine. Query your digital history like a database.**

<!-- Badges (CI not yet configured)
[![Build Status](https://img.shields.io/github/actions/workflow/status/...)](#)
[![License](https://img.shields.io/badge/license-TBD-blue)](#license)
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
  desktop, system    search, pkm          extension
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
        │   + Extensions │ pgvector, ULID     │
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

- **Language:** Rust (core system), Python (CLI tools)
- **Database:** PostgreSQL 16 + TimescaleDB + pgvector + pgx_ulid + pg_jsonschema
- **Messaging:** NATS JetStream for durable event transport
- **Deployment:** NixOS modules with systemd hardening
- **IDs:** ULIDs for time-ordered, globally unique primary keys

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
- **Query Interface:** SQL access, Python CLI, JSON-RPC API
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
cargo xtask infra start

# Run the filesystem ingestor
cargo run --bin sinex-fs-ingestor -- sensor --watch ~/Documents

# Query recent events
cargo run --bin sinexctl -- events list --limit 10
```

For database setup, TLS configuration, and RPC authentication, see [Getting Started](docs/current/getting-started.md).

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
| Set up a development environment | [Getting Started](docs/current/getting-started.md) |
| Build a custom ingestor | [Node SDK Overview](crate/lib/sinex-node-sdk/docs/overview.md) |
| Write tests | [Testing Handbook](TESTING.md) |
| Deploy on NixOS | [NixOS Module](nixos/README.md) |
| Understand event schemas | [Event Taxonomy](crate/lib/sinex-schema/docs/event-taxonomy.md) |
| Review security posture | [Security](docs/current/security.md) |

Full documentation index: [docs/README.md](docs/README.md)

---

## Contributing

### Development Workflow

```bash
cargo xtask check              # Fast iteration: fmt + clippy (~10s)
cargo xtask test               # Run affected tests
cargo xtask check && cargo xtask test  # Before commit
cargo xtask ci workspace       # Full validation (before PR)
```

See [CLAUDE.md](CLAUDE.md) for patterns, conventions, and detailed workflows.

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
- No role-based authorization (all tokens have full access)
- No encryption at rest (relies on full-disk encryption)
- No automated data retention/cleanup tooling

See [Security Posture](docs/current/security.md) for details.

---

## License

License TBD. This is currently a personal project not yet released for public use.

---

<sub>Built for personal use. Not yet production-ready for general deployment.</sub>
