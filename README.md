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

**Current Core Flow (implemented in code)**: Satellites → ingestd (gRPC over Unix socket) → Postgres (`core.events`) → NATS JetStream fanout → Automata → Gateway (JSON‑RPC) → CLI

Note: A NATS‑native ingestion refactor (satellites publish directly to JetStream; ingestd acts as archiver/consumer) is planned. See `docs/plan_v3.txt`. Until that refactor lands, ingestion uses gRPC to ingestd as above.

- **Satellites**: Independent services that capture events (filesystem, terminals, window managers, system events)
- **ingestd**: Central ingestion daemon that receives events via gRPC and stores data atomically; also fans out persisted events via NATS JetStream
- **Event Substrate**: PostgreSQL + TimescaleDB with ULID keys, stores immutable events
- **NATS JetStream**: Real-time event distribution and durable buffering
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
- **Ingestion via gRPC (current)**: Satellites submit to ingestd over gRPC; ingestd validates/persists and fans out via NATS JetStream
- **NATS‑native ingest (planned)**: Per `docs/plan_v3.txt`, satellites will publish directly to JetStream; ingestd becomes an archiver/consumer
- **Schema Validation**: JSON Schema validation for event payloads
- **Git-Annex Integration**: Large file storage for blobs
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

- ✅ **Satellite Architecture**: Independent satellite services operational, unified SDK in progress
- ✅ **Data Substrate**: PostgreSQL + TimescaleDB with ULID keys; `core.events` operational
- 🚧 **Message Bus**: Converging on NATS JetStream for ingestion and distribution (see docs/plan_v3.txt)
- 🚧 **Event Sources**: Filesystem, Terminal, Desktop, System — expanding coverage
- 🚧 **Automaton Ecosystem**: Deterministic processors; more in progress
- 🚧 **Gateway & CLI**: Operational; iterative improvements
- 🔨 **AI/LLM Integration**: Early framework and schema; integration ongoing

### ✅ Implemented (Working Code)
- **Core Infrastructure**: Event storage, ULID keys, TimescaleDB integration
- **Satellite Architecture**: Independent satellites publish to NATS JetStream
- **Event Source Satellites**: Filesystem, Terminal (Kitty/Atuin/recordings), Desktop (clipboard/Hyprland), System (D-Bus/systemd/udev)
- **Dual-Mode Support**: All satellites support sensor (real-time) and scanner (batch) modes
- **Processing Pipeline**: Satellites → NATS JetStream → ingestd (archiver) → Automata
- **Storage**: Git-annex blob storage, JSON schema validation
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
- **Architecture Deep-Dive**: See [docs/architecture/](docs/architecture/) for domain-specific details
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

Sinex has comprehensive test coverage across multiple categories:

### Test Summary
- **Total test files**: 75+ (including new satellite tests)
- **Total tests**: 250+ tests
- **Test organization**: Hierarchical structure under `test/` directory

### Test Categories
- **Unit tests**: 52 tests - Component isolation and core logic
- **Integration tests**: 45+ tests - Component interaction, database, and satellite integration
- **System tests**: 22 tests - End-to-end pipeline validation
- **Adversarial tests**: 83 tests - Security, edge cases, and stress scenarios
- **Satellite tests**: Comprehensive tests for dual-mode operation, reconnection, coordination
- **VM tests**: NixOS integration tests (being updated for satellite architecture)

### Running Tests

```bash
# Run all tests
just test

# Run specific test categories  
just test-unit                # Unit tests only
just test-integration         # Integration tests only
just test-system             # System tests only
just test-adversarial        # Adversarial tests only

# Run satellite-specific tests
cargo test --test satellite_architecture_test
cargo test --test satellite_comprehensive_test

# Run with coverage
just coverage                # Generate coverage report
just coverage-html           # Generate HTML coverage report

# Run VM tests (when updated for satellites)
nix build .#checks.x86_64-linux.sinex-vm-satellite -L
```

## 🚀 Quick Start

### Prerequisites
- Nix package manager with flakes enabled
- PostgreSQL (automatically set up in dev shell)
- Git (for git-annex integration)

### Development Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/sinex.git
cd sinex

# Enter development shell (sets up PostgreSQL, migrations, etc.)
nix develop

# View available commands
just
```

### Running Sinex
```bash
# Start core services
just ingestd                  # Start the ingestion daemon
just gateway                  # Start the API gateway

# Start event source satellites
just fs-watcher              # Filesystem events
just terminal-satellite      # Terminal events (Kitty, Atuin, recordings)
just desktop-satellite       # Desktop events (clipboard, Hyprland)
just system-satellite        # System events (D-Bus, systemd, udev)

# Run satellites in scanner mode
cargo run --bin sinex-fs-watcher -- scan /path/to/scan
cargo run --bin sinex-terminal-satellite -- scan --source kitty

# Query recent events
just query                    # or: ./cli/exo.py query
just query 50                # Show 50 recent events

# Monitor satellites via systemd
systemctl status sinex-ingestd
systemctl status sinex-fs-watcher
```

### Configuration
Configuration is managed through the NixOS module system. Each satellite can be enabled/disabled independently. See `nixos/example.nix` for example configuration.

## 🧪 Testing

The Sinex test suite is optimized for parallel execution, achieving 50%+ faster test runs:

- **Parallel Execution**: Automatically uses all available CPU cores
- **Database Isolation**: 64-database pool with PostgreSQL advisory locks
- **Fast Testing**: `just test-parallel` for maximum speed
- **Comprehensive Coverage**: Unit, integration, property, and adversarial tests

See [`TESTING.md`](TESTING.md) for the detailed testing guide.

Quick commands:
```bash
just test-fast      # Fast tests only (~30s)
just test-parallel  # All tests with max parallelism
just test-dev       # Quick dev cycle (<2 min)
```

## 📚 Documentation

### Core
- **Docs Index**: [`docs/README.md`](docs/README.md) — Start here
- **Architecture**: [`docs/architecture/`](docs/architecture/)
- **Roadmap**: [`docs/roadmap/`](docs/roadmap/)
- **Integrity & Security**: [`docs/architecture/SystemOperations_And_Integrity_Architecture.md`](docs/architecture/SystemOperations_And_Integrity_Architecture.md), [`docs/architecture/security-architecture.md`](docs/architecture/security-architecture.md)

### Key Components
- **Core Architecture**: [`docs/architecture/Core_Architecture.md`](docs/architecture/Core_Architecture.md)
- **Schema & Taxonomy**: [`docs/architecture/SCHEMA.md`](docs/architecture/SCHEMA.md), [`docs/architecture/event-taxonomy.md`](docs/architecture/event-taxonomy.md)
- **Satellites SDK & Patterns**: [`docs/architecture/satellite-implementation.md`](docs/architecture/satellite-implementation.md)

### For Contributors
- **Testing Guide**: [`TESTING.md`](TESTING.md)
- **CLAUDE Workflows**: [`CLAUDE.md`](CLAUDE.md)

## 🛠️ Project Structure

```
sinex/
├── crate/                         # Rust workspace crates
│   ├── sinex-core/               # Core traits and types
│   ├── sinex-db/                 # Database layer
│   ├── sinex-satellite-sdk/      # SDK for building satellites
│   ├── sinex-ingestd/            # Central ingestion daemon
│   ├── sinex-gateway/            # API gateway service
│   ├── sinex-fs-watcher/         # Filesystem event satellite
│   ├── sinex-terminal-satellite/ # Terminal event satellite
│   ├── sinex-desktop-satellite/  # Desktop event satellite
│   ├── sinex-system-satellite/   # System event satellite
│   └── sinex-terminal-command-canonicalizer/  # Automaton example
├── crate/lib/sinex-schema/       # Database schema + migrations (sea-orm-migration)
├── nixos/                        # NixOS module and deployment
├── tests/                        # Comprehensive test suites
├── cli/                          # Python query interface (exo.py)
└── docs/                         # Documentation (architecture, roadmap, guides)
```

## 🤝 Contributing

See [`CLAUDE.md`](CLAUDE.md) for development workflows.

## 📄 License

This project is licensed under the terms specified in the LICENSE file.
