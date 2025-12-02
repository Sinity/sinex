# Sinex - The Sentient Archive

> *An ambitious endeavor to construct an empowering digital environment for thought: a persistent, universally capturing, and intelligently structured space that mirrors, supports, and augments the user's own mind.*

Sinex is a comprehensive event-driven data capture system that transforms the digital realm from a source of distraction and fragmentation into a coherent, queryable, and deeply personal extension of self. It records everything happening on a computer for later analysis through a distributed satellite-based architecture with immutable storage and real-time processing capabilities.

## Philosophy: Cognitive Sovereignty

We stand at a peculiar juncture in human history. Our digital tools grant us unprecedented access to information, yet this abundance often engenders a profound sense of fragmentation. We generate more data about ourselves than ever before, yet we *remember* less, *understand* less of our own cognitive trails, and feel increasingly alienated from the very digital environments designed to augment us.

Sinex confronts this crisis of digital amnesia by creating an "anti-forgetting machine" built on four inviolable pledges:

### The Exocortex Pledge

1. **To Capture Comprehensively and Losslessly** - Every potentially significant digital trace at the highest fidelity, preserving original detail with multi-modal, redundant strategies
2. **To Structure Meaningfully and Emergently** - Schemas evolve with user needs; order emerges gradually from raw data which remains inviolate for future reinterpretation
3. **To Empower User Agency Unconditionally** - You are the absolute sovereign of your data; all components are transparent, inspectable, and modifiable
4. **To Evolve Continuously and Transparently** - The system co-evolves with its user through iterative improvement, addressing personally-felt friction

### Core Design Ethos

- **Universal Capture as Default**: If a signal can be instrumented, it should be captured
- **Emergent Structure from Raw Data**: Meaning is discovered and refined, not preordained
- **Sovereign User Agency**: Radical transparency, universal hackability, user control over automation
- **Continuous and Rich Context**: Rigorous timestamping, global identifiers, explicit linking, meticulous provenance
- **Meta-Cognition as Valued Data**: Subjective experiences (intentions, friction, insights) are first-class eventified data

The promise is threefold: to restore **agency** by placing you in control of your data, to cultivate **insight** by making patterns visible, and to enable **intentional evolution** by providing a substrate for self-understanding and self-authorship.

## 🏗️ Architecture

Sinex is a "sentient archive" implemented as a satellite constellation architecture – independent services orchestrated by NixOS/systemd that comprehensively capture, intelligently process, and powerfully query personal digital experiences.

**Current Core Flow (implemented in code)**: Satellites → NATS JetStream → ingestd (consumer/archiver) → Postgres (`core.events`) + JetStream confirmations → Automata → Gateway (JSON‑RPC) → CLI

Satellites publish events directly to NATS JetStream (`events.raw.*` subjects); ingestd consumes from JetStream, validates, persists to Postgres, and publishes confirmations back to JetStream for consumption by automata and other processors.

- **Satellites**: Independent services that capture events (filesystem, terminals, window managers, system events)
- **NATS JetStream**: Durable message bus for event ingestion, distribution, and confirmation delivery
- **ingestd**: Consumes events from JetStream, validates, archives to Postgres, and publishes confirmations
- **Event Substrate**: PostgreSQL + TimescaleDB with ULID keys, stores immutable events
- **Automata**: Stream processors that transform raw events into canonical representations
- **Query Interface**: Python CLI (`exo.py`) for exploring captured events

### Technical Stack

**Foundation:**
- **OS**: NixOS for reproducible, declarative deployment
- **Database**: PostgreSQL 16 with extensions:
  - TimescaleDB for time-series event storage
  - pgx_ulid for time-ordered primary keys
  - pg_jsonschema for event validation
  - pgvector for semantic search (future)
- **Language**: Rust for core system, Python for CLI tools
- **Message Bus**: NATS JetStream for real-time event distribution

### Key Features
- **Satellite Architecture**: Each event source runs as an independent systemd service
- **Dual-Mode Operation**: Satellites support both sensor (real-time) and scanner (batch) modes
- **Immutable Event Storage**: All events preserved with full fidelity
- **ULID Primary Keys**: Time-ordered, globally unique identifiers
- **NATS JetStream Ingestion**: Satellites publish directly to JetStream; ingestd acts as consumer/archiver
- **Idempotency & Confirmation**: Message deduplication via NATS-Msg-Id headers; confirmation delivery for provisionals
- **Confirmation + DLQ Subjects**: Ingestd publishes canonical confirmations on `events.confirmations.*` and dead-letter entries on `events.dlq.*`, so automata can wait for durable IDs and handle failures deterministically
- **Schema Validation**: JSON Schema validation for event payloads
- **Git-Annex Integration**: Large file storage for blobs (source material slices)
- **NixOS Module**: First-class NixOS deployment support

### Satellite Architecture Details

The satellite architecture provides several key benefits:

1. **Isolation**: Each satellite runs as its own process with limited permissions
2. **Reliability**: If one satellite crashes, others continue operating
3. **Scalability**: Satellites can be distributed across machines
4. **Flexibility**: Easy to add new event sources without modifying core

#### Dual-Mode Operation

All satellites support two operational modes:

- **Sensor Mode**: Real-time event capture as they occur
  - Runs continuously as a systemd service
  - Publishes material slices and provisional events to NATS JetStream
  - Examples: monitoring filesystem changes, clipboard updates

- **Scanner Mode**: Batch processing of historical data
  - Runs on-demand via CLI
  - Processes existing data sources
  - Examples: importing shell history, scanning log files

## 📊 Implementation Status

### System Components Progress

- ✅ **Satellite Architecture**: Independent satellite services operational, unified SDK complete
- ✅ **Data Substrate**: PostgreSQL + TimescaleDB with ULID keys; `core.events` operational
- ✅ **Message Bus**: NATS JetStream ingestion and distribution fully implemented
- 🚧 **Event Sources**: Filesystem, Terminal, Desktop, System — expanding coverage
- 🚧 **Automaton Ecosystem**: Deterministic processors; more in progress
- 🚧 **Gateway & CLI**: Operational; iterative improvements
- 🔨 **AI/LLM Integration**: Early framework and schema; integration ongoing

### ✅ Implemented (Working Code)
- **Core Infrastructure**: Event storage, ULID keys, TimescaleDB integration
- **Satellite Architecture**: Independent satellites publish to NATS JetStream with idempotency headers
- **Event Source Satellites**: Filesystem, Terminal (Kitty/Atuin/recordings), Desktop (clipboard/Hyprland), System (D-Bus/systemd/udev)
- **Dual-Mode Support**: All satellites support sensor (real-time) and scanner (batch) modes
- **Processing Pipeline**: Satellites → JetStream (`events.raw.*`) → ingestd (consumer/archiver) → Postgres + JetStream confirmations → Automata
- **Confirmation Flow**: Provisional events receive confirmations with canonical IDs after persistence
- **Storage**: Git-annex blob storage (source material slices), JSON schema validation
- **Deployment**: NixOS module with systemd satellite services
- **Testing**: Comprehensive test suite including satellite integration tests

### 🚧 In Progress
- **Automaton Development**: Terminal command canonicalizer and other stream processors
- **Query Interface Enhancements**: Advanced query DSL
- **Performance Optimization**: Database tuning and indexing

### 📋 Planned
- **AI Integration**: LLM-based analysis and entity resolution
- **Advanced Sources**: Browser history, audio capture, email
- **Knowledge Graph**: Entity extraction and relationship mapping
- **Multi-device Sync**: Distributed event synchronization

## 📚 Documentation

### Quick Links
- **Development Guide**: See [CLAUDE.md](CLAUDE.md) for development workflows and patterns
- **Architecture Deep-Dive**: See [docs/current/architecture/](docs/current/architecture/) for domain-specific details
- **NixOS Module**: See [nixos/modules/](nixos/modules/) for deployment configuration
- **Roadmap**: See [docs/roadmap/](docs/roadmap/) for future features and architectural directions

### Key Architectural Decisions
- **ULID Primary Keys**: Time-ordered, globally unique identifiers for efficient indexing
- **Satellite Constellation**: Independent services with unified StatefulStreamProcessor interface
- **NATS JetStream Bus**: Real-time event distribution and durable buffering
- **Unified Events Table**: Single `core.events` with comprehensive provenance tracking
- **Checkpoint-based Recovery**: Unified state management for processors
- **Source Material Registry**: Immutable ground truth preservation with blob references

## 🧪 Test Coverage

All current guidance—suite layout, quick-start commands, Nextest profiles, and
property-testing conventions—lives in the [Testing Handbook](TESTING.md).
Keep that document handy when adding or reviewing tests; it links directly to
crate-level deep dives such as the `sinex-test-utils` API reference and the
NixOS VM harness.

## 🚀 Quick Start

### Prerequisites
- Nix package manager with flakes enabled
- [devenv](https://devenv.sh) CLI (installed alongside the dev environment; all helper commands assume it exists)
- PostgreSQL (automatically set up in dev shell)
- Git (for git-annex integration)

### Development Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/sinex.git
cd sinex

# Enter development shell (sets up PostgreSQL, migrations, etc.)
nix develop                # or: direnv allow && direnv reload

# Generate an RPC token (gateway refuses to start without one)
export SINEX_RPC_TOKEN=$(openssl rand -hex 32)   # or: SINEX_RPC_TOKEN_FILE=$HOME/.config/sinex/rpc-token

# Inspect available helper tasks
devenv tasks help                  # Shows the run/export subcommands
devenv tasks run --help            # Lists all task labels defined in devenv.nix
```

Direnv users will see a status banner (MOTD) sourced from `scripts/dev-env-banner.sh` whenever the
environment loads, matching the `nix develop` experience.

> **Note:** Nothing in this repository is expected to work outside `nix develop` / the devenv shell.
> Always enter the shell (or let `direnv` manage it) before invoking Cargo, scripts, or CLI helpers.

### Database defaults
The dev shell and Cargo configuration export the Postgres settings automatically:

- `PGHOST=/run/postgresql`
- `PGUSER=sinity`
- `PGDATABASE=sinex_dev`
- `DATABASE_URL=postgresql:///sinex_dev?host=/run/postgresql`

As long as you are inside `nix develop`/`direnv`, commands such as `cargo check`, `cargo expand`,
and `sqlx` no longer require per-command overrides. If you need to point at another database, copy
`.env.example` to `.env` (kept out of version control) and set those variables before launching the shell.

> **SQLx offline mode:** Only the Nix flake build exports `SQLX_OFFLINE=true` (so the sandbox can
> compile without a database). Leave it unset during normal development; that way every
> `sqlx::query!` is validated against the live schema. Regenerate the cache with
> `devenv tasks run sqlx:prepare` whenever SQL changes so the flake build stays in sync.

> **PostgreSQL extensions:** Migrations assume the database already has `timescaledb`,
> `ulid`, `pg_jsonschema`, and `vector` installed. Provision them once as the `postgres`
> superuser:
>
> ```sql
> CREATE EXTENSION timescaledb;
> CREATE EXTENSION ulid;
> CREATE EXTENSION pg_jsonschema;
> CREATE EXTENSION vector;
> ```
>
> The NixOS module (`services.postgresql-setup`) runs those statements automatically when
> `database.autoSetup = true`; for other deployments run them manually before the first
> `devenv tasks run db:migrate`.

### Running Sinex
```bash
# Start core services
devenv up nats ingestd gateway

# Start event source satellites
devenv up fs-watcher terminal desktop system
devenv up health document canonicalizer    # Optional processors

# Run satellites in scanner mode
cargo run --bin sinex-fs-watcher -- scan /path/to/scan
cargo run --bin sinex-terminal-satellite -- scan --source kitty

# Query recent events
devenv tasks run cli:query          # or: python3 cli/exo.py query --rpc-token "$SINEX_RPC_TOKEN"
LIMIT=50 devenv tasks run cli:query # Increase result window

# Monitor satellites via systemd
systemctl status sinex-ingestd
systemctl status sinex-fs-watcher
```

### Configuration
Configuration is managed through the NixOS module system. Each satellite can be enabled/disabled independently. See `nixos/example.nix` for example configuration.

### Combined Bundles (LLM/analysis helpers)
- `scripts/combine-files-batch.sh` snapshots sources/tests/docs into `combined-bundles/combined-{sources,tests,docs}.md`.
- The same helper emits `combined-bundles/tokei_plus_gitlog.md` plus eight commit-diff shards (`all_diffs_part_{1-8}.md`).
- The directory stays gitignored (`.gitignore` keeps `combined-bundles/` out of history); regenerate locally whenever you need a fresh view.
- Pass a root dir and optional output dir if you need custom slices: `./scripts/combine-files-batch.sh . combined-bundles`.
- Each bundle includes metadata (timestamp, token estimate) and raw file dumps so AI assistants have deterministic context without touching git history.

### RPC Authentication
- `sinex-gateway` **requires** a token via `SINEX_RPC_TOKEN` (or `SINEX_RPC_TOKEN_FILE`) before serving JSON-RPC. The CLI automatically reads the same environment variable or accepts `--rpc-token`.
- For automated tests or local experiments you can temporarily bypass auth via `SINEX_GATEWAY_ALLOW_INSECURE=1`, but never enable this in shared environments.
- Blob uploads are additionally capped by `SINEX_GATEWAY_MAX_BLOB_BYTES` (default 5 MiB). Oversized payloads are rejected before they reach git-annex, keeping RPC handling predictable.

## 🧪 Testing

The Sinex test suite is optimized for parallel execution, achieving 50%+ faster test runs:

- **Parallel Execution**: Automatically uses all available CPU cores
- **Database Isolation**: 64-database pool with PostgreSQL advisory locks
- **Fast Testing**: `devenv tasks run dev:test` for the common dev loop (Nextest-only; `cargo test` is unsupported)
- **Comprehensive Coverage**: Unit, integration, property, and adversarial tests

See [`TESTING.md`](TESTING.md) for the detailed testing guide.

Quick commands:
```bash
devenv tasks run dev:check      # cargo check --workspace
devenv tasks run dev:test       # Library + property suites via nextest (required workflow)
devenv tasks run test:all       # Full nextest matrix after db:migrate
devenv tasks run test:vm        # VM smoke tests
```

> Note: Every `devenv tasks …` invocation spins up a transient shell under the hood, so `direnv`
> will re-evaluate the environment each time. That’s expected; run the underlying `cargo …` or
> `scripts/…` command directly if you need maximum turnaround speed.

### CI expectations

GitHub Actions exercises the exact same scripts you run locally. Before pushing, make sure:

| When you touch… | Run locally | Why |
| --- | --- | --- |
| Any Rust code | `SQLX_OFFLINE=1 cargo check --workspace --all-features` | Mirrors the offline check inside `db-checks.yml` / `ci.yml`. |
| Event payloads or schema helpers | `./scripts/schema-dev.sh generate` | `ci.yml` and `schema-management.yml` refuse to run if `schemas/` drifts. |
| SQL queries, migrations, or SeaORM files | `./scripts/sqlx-prepare.sh` | Rebuilds `.sqlx/` metadata for every crate; `db-checks.yml` enforces a clean diff. |
| Nothing but want a fast sanity sweep | `devenv tasks run dev:test` | Matches the single Nextest run in CI. |

Additional notes:

- The dev shell automatically wires `scripts/rustc_wrapper.sh` through `sccache`. CI caches both `~/.cache/sccache` and Cargo registries, so you get the same benefit locally if you keep the cache warm.
- A single “Auto Update” workflow (cron + manual) now opens PRs for both schema bundles and `.sqlx` metadata by running the scripts above on `master`. You should rarely need to push straight to `master`; let the workflow generate refresh PRs whenever possible.

## 📚 Documentation

### Core
- **Docs Index**: [`docs/README.md`](docs/README.md) — Start here
- **Architecture**: [`docs/current/architecture/`](docs/current/architecture/)
- **Roadmap**: [`docs/roadmap/`](docs/roadmap/)
- **Integrity & Security**: [`docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`](docs/current/architecture/SystemOperations_And_Integrity_Architecture.md), [`docs/current/architecture/security-architecture.md`](docs/current/architecture/security-architecture.md)

### Key Components
- **Core Architecture**: [`docs/current/architecture/Core_Architecture.md`](docs/current/architecture/Core_Architecture.md)
- **Schema & Taxonomy**: [`crate/lib/sinex-schema/docs/overview.md`](crate/lib/sinex-schema/docs/overview.md), [`docs/current/architecture/event-taxonomy.md`](docs/current/architecture/event-taxonomy.md)
  - When any `EventPayload` changes, run `./scripts/schema-dev.sh generate` and commit the regenerated `schemas/` bundle (CI enforces this just like `cargo fmt`).
- **Satellites SDK & Patterns**: [`crate/lib/sinex-satellite-sdk/docs/overview.md`](crate/lib/sinex-satellite-sdk/docs/overview.md)

### For Contributors
- **Testing Guide**: [`TESTING.md`](TESTING.md)
- **CLAUDE Workflows**: [`CLAUDE.md`](CLAUDE.md)

## 🛠️ Project Structure

```
sinex/
├── crate/                         # Rust workspace crates
│   ├── core/                      # Runtime binaries
│   │   ├── sinex-ingestd/         # Central ingestion daemon
│   │   ├── sinex-gateway/         # API gateway service
│   │   └── sinex-rpc-dispatcher/  # RPC scan/explore worker
│   ├── lib/                       # Shared libraries
│   │   ├── sinex-core/            # Core types + database repositories
│   │   ├── sinex-schema/          # Database schema + migrations (SeaORM)
│   │   ├── sinex-processor-runtime/
│   │   ├── sinex-satellite-sdk/
│   │   ├── sinex-macros/
│   │   ├── sinex-services/
│   │   └── sinex-test-utils/
│   └── satellites/                # Event satellites & automata
│       ├── sinex-terminal-satellite/
│       ├── sinex-desktop-satellite/
│       ├── sinex-system-satellite/
│       ├── sinex-fs-watcher/
│       ├── sinex-terminal-command-canonicalizer/
│       ├── sinex-health-aggregator/
│       └── sinex-document-ingestor/
├── schemas/                       # Generated JSON schemas for GitOps
├── tests/e2e/                     # Workspace-level integration suites
├── cli/                           # Python query interface (exo.py)
├── docs/                          # Architecture, roadmap, and guide material
└── nixos/                         # NixOS module and deployment files
```

## 🤝 Contributing

See [`CLAUDE.md`](CLAUDE.md) for development workflows.

## 📄 License

This project is licensed under the terms specified in the LICENSE file.
