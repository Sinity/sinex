# Sinex - Event-Driven Data Capture System

Sinex is a comprehensive event-driven data capture system that records everything happening on a computer for later analysis. It provides a unified collection framework for various event sources with immutable storage and concurrent processing capabilities.

## 🏗️ Architecture

**Core Flow**: EventSources → UnifiedCollector → Event Substrate → Workers → Query Interface

- **EventSources**: Individual event capturing components (filesystem, terminals, window managers, clipboard)
- **UnifiedCollector**: Central coordinator that manages all event sources with hot-reload configuration
- **Event Substrate**: PostgreSQL + TimescaleDB with ULID keys, stores immutable events
- **Workers**: Process events concurrently using `SELECT FOR UPDATE SKIP LOCKED`
- **Query Interface**: Python CLI (`exo.py`) for exploring captured events

### Key Features
- **Unified Collection**: Single collector manages all event sources
- **Immutable Event Storage**: All events preserved with full fidelity
- **ULID Primary Keys**: Time-ordered, globally unique identifiers
- **Concurrent Processing**: Multiple workers with proper coordination
- **Schema Validation**: JSON Schema validation for event payloads
- **Git-Annex Integration**: Large file storage for blobs
- **NixOS Module**: First-class NixOS deployment support

## 📊 Implementation Status

### ✅ Implemented (Working Code)
- **Core Infrastructure**: Event storage, ULID keys, TimescaleDB integration
- **Event Sources**: Filesystem, Terminal (Kitty, Asciinema), Clipboard, Hyprland IPC
- **Processing Pipeline**: Unified collector, promotion queue, concurrent workers
- **Storage**: Git-annex blob storage, JSON schema validation
- **Deployment**: NixOS module with systemd services
- **Testing**: Comprehensive test suite (239 tests across all categories)

### 🚧 In Progress
- **Promotion Queue Worker**: Event processing and transformation
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
- **Total test files**: 73
- **Total tests**: 239
- **Test organization**: Hierarchical structure under `test/` directory

### Test Categories
- **Unit tests**: 52 tests - Component isolation and core logic
- **Integration tests**: 40 tests - Component interaction and database integration
- **System tests**: 22 tests - End-to-end pipeline validation
- **Adversarial tests**: 83 tests - Security, edge cases, and stress scenarios
- **VM tests**: 1 comprehensive NixOS integration test

### Running Tests

```bash
# Run all tests
just test

# Run specific test categories  
just test-unit                # Unit tests only
just test-integration         # Integration tests only
just test-system             # System tests only
just test-adversarial        # Adversarial tests only

# Run with coverage
just coverage                # Generate coverage report
just coverage-html           # Generate HTML coverage report

# Run VM tests
nix build .#checks.x86_64-linux.sinex-vm-basic -L
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
# Run the unified collector
just unified                  # or: cargo run --bin sinex-collector

# Run with custom config
cargo run --bin sinex-collector -- --config myconfig.toml

# Query recent events
just query                    # or: ./cli/exo.py query
just query 50                # Show 50 recent events

# Monitor in real-time
just unified                  # Collector shows live metrics
```

### Configuration
Configuration is now managed through the NixOS module system. See `config/nixos-example.nix` for example configuration.

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
├── crate/                    # Rust workspace crates
│   ├── sinex-core/          # Core traits and types
│   ├── sinex-collector/     # Unified collector binary
│   ├── sinex-events/        # Event source implementations
│   ├── sinex-db/            # Database layer
│   └── sinex-worker/        # Event processing workers
├── migrations/              # SQL schema migrations
├── nixos/                   # NixOS module and deployment
├── test/                    # Comprehensive test suites
├── cli/                     # Python query interface (exo.py)
└── spec/                    # Documentation and specifications
```

## 🤝 Contributing

See [`spec/PATHWAYS.md`](spec/PATHWAYS.md) for contribution guidelines and [`CLAUDE.md`](CLAUDE.md) for development workflows.

## 📄 License

This project is licensed under the terms specified in the LICENSE file.