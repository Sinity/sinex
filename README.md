# Sinex - Event-Driven Data Capture System

Sinex is a comprehensive event-driven data capture system that records everything happening on a computer for later analysis. It provides a distributed satellite-based architecture for event capture with immutable storage and real-time processing capabilities.

## 🏗️ Architecture

**Core Flow**: Satellites → ingestd → Event Substrate → Redis Streams → Automata → Query Interface

- **Satellites**: Independent services that capture events (filesystem, terminals, window managers, system events)
- **ingestd**: Central ingestion daemon that receives events via gRPC and stores them atomically
- **Event Substrate**: PostgreSQL + TimescaleDB with ULID keys, stores immutable events
- **Redis Streams**: Real-time event distribution to automata for processing
- **Automata**: Stream processors that transform raw events into canonical representations
- **Query Interface**: Python CLI (`exo.py`) for exploring captured events

### Key Features
- **Satellite Architecture**: Each event source runs as an independent systemd service
- **Dual-Mode Operation**: Satellites support both sensor (real-time) and scanner (batch) modes
- **Immutable Event Storage**: All events preserved with full fidelity
- **ULID Primary Keys**: Time-ordered, globally unique identifiers
- **gRPC Communication**: Efficient binary protocol between satellites and ingestd
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
  - Streams events to ingestd via gRPC
  - Examples: monitoring filesystem changes, clipboard updates

- **Scanner Mode**: Batch processing of historical data
  - Runs on-demand via CLI
  - Processes existing data sources
  - Examples: importing shell history, scanning log files

## 📊 Implementation Status

### ✅ Implemented (Working Code)
- **Core Infrastructure**: Event storage, ULID keys, TimescaleDB integration
- **Satellite Architecture**: Independent satellites with gRPC communication
- **Event Source Satellites**: Filesystem, Terminal (Kitty/Atuin/recordings), Desktop (clipboard/Hyprland), System (D-Bus/systemd/udev)
- **Dual-Mode Support**: All satellites support sensor (real-time) and scanner (batch) modes
- **Processing Pipeline**: ingestd → Redis Streams → Automata
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

See [`docs/PARALLEL_TESTING.md`](docs/PARALLEL_TESTING.md) for detailed testing guide.

Quick commands:
```bash
just test-fast      # Fast tests only (~30s)
just test-parallel  # All tests with max parallelism
just test-dev       # Quick dev cycle (<2 min)
```

## 📚 Documentation

### Core Documentation
- **Project Vision**: [`spec/VISION.md`](spec/VISION.md) - Philosophy and long-term goals
- **Architecture Guide**: [`spec/STAD.md`](spec/STAD.md) - System technical architecture
- **Documentation Index**: [`spec/SADI.md`](spec/SADI.md) - Complete documentation map
- **Getting Started**: [`spec/PLAN.md`](spec/PLAN.md) - Development roadmap and status

### Technical Specifications
- **Implementation Specs**: `spec/implemented/` - Working features
- **Ready Specs**: `spec/ready/` - Designed and ready to implement
- **Future Plans**: `spec/planned/` - Long-term feature planning
- **Architecture Decisions**: `spec/docs/adr/` - Design rationale

### For Contributors
- **Development Guide**: [`CLAUDE.md`](CLAUDE.md) - Project patterns and workflows
- **Contribution Pathways**: [`spec/PATHWAYS.md`](spec/PATHWAYS.md) - Where to start contributing
- **Dependency Graph**: [`spec/DEPENDENCIES.md`](spec/DEPENDENCIES.md) - Feature dependencies

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
├── migrations/                   # SQL schema migrations
├── nixos/                        # NixOS module and deployment
├── test/                         # Comprehensive test suites
├── cli/                          # Python query interface (exo.py)
└── spec/                         # Documentation and specifications
```

## 🤝 Contributing

See [`spec/PATHWAYS.md`](spec/PATHWAYS.md) for contribution guidelines and [`CLAUDE.md`](CLAUDE.md) for development workflows.

## 📄 License

This project is licensed under the terms specified in the LICENSE file.