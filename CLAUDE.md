# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 🏗️ Repository Overview

Sinex is a comprehensive event-driven data capture system that records everything happening on a computer for later analysis. The system uses a distributed satellite-based architecture where independent services capture events and feed them into a central PostgreSQL + TimescaleDB data substrate via NATS JetStream.

**Core Architecture**: Satellites → ingestd → Event Substrate → NATS JetStream → Automata → Query Interface

## 🚀 Essential Development Commands

### Environment Setup

```bash
# Enter development shell (always run first)
nix develop

# Apply database migrations
just migrate

# Verify workspace builds
just check
# or: cargo check --workspace --all-features

# Setup optimized build environment (recommended)
just setup-fast
```

### Testing (Hierarchical by Speed)

```bash
# Fast development feedback (~30s)
just test-fast               # Unit + property tests only

# Full test suite
just test-all               # Complete suite including VM tests (~10-15min)

# Test specific packages
just test-pkg sinex-db      # Test specific crate
just test-individual integration::database_test  # Specific test file
```

### Database Operations

```bash
# Database connection
just psql                   # Connect to dev database

# SQLX cache management (CRITICAL for Nix builds)
just sqlx-prepare          # Update .sqlx/ cache - MUST commit this!
just sqlx-check            # Verify cache is up to date

# Database reset/cleanup
just db-reset              # Reset test database
just db-setup              # Setup test database
```

### Code Quality

```bash
# Pre-commit workflow
just pre-commit            # fmt + lint + check + fast tests

# Individual operations
just fmt                   # Format code with rustfmt
just lint                  # Clippy with warnings as errors
just build                 # Build debug binaries
just check-all             # Check all targets including tests
```

### Running Services

```bash
# Core services
just ingestd               # Start central ingestion daemon
just gateway               # Start API gateway/RPC server

# Event source satellites
just fs-watcher           # Filesystem events
just terminal             # Terminal events
just desktop              # Desktop events (clipboard, window manager)
just system               # System events (D-Bus, systemd, udev)

# Processing satellites
just canonicalizer        # Terminal command canonicalizer
just health               # Health aggregator

# Query interface
just query                # Query recent events (default 10)
just query 50             # Query 50 recent events
```

## 🏛️ Architecture Overview

### Satellite Architecture

Sinex uses a satellite constellation pattern where independent services communicate via gRPC and Redis Streams:

- **Satellites**: Independent event capture services
- **ingestd**: Central ingestion hub and coordinator
- **Redis Streams**: Real-time event distribution message bus
- **PostgreSQL + TimescaleDB**: Event storage with time-series optimization
- **Gateway**: API layer for CLI and future web interfaces

### Key Technical Patterns

**Event-Driven Design**:

- All events stored immutably in `core.events` table
- ULID primary keys for time-ordered, distributed-safe IDs
- JSON Schema validation via pg_jsonschema
- Comprehensive provenance tracking via source_event_ids

**Satellite Services**:

- Each satellite runs as independent systemd service
- Dual-mode operation: sensor (real-time) and scanner (batch)
- StatefulStreamProcessor interface for consistency
- gRPC communication with automatic reconnection
- Services configured via NixOS modules in `nixos/`

**Testing Architecture**:

- Parallel test execution with database pool isolation
- Property-based testing with proptest
- VM tests for NixOS integration validation
- VM tests validate full system integration

## 📁 Project Structure

### Workspace Organization

```
crate/
  [TODO]
```

### Key Directories

- `test/` - Comprehensive test suite organized by category
- `cli/` - Python query interface (exo.py)
- `nixos/` - NixOS module for system deployment
- `docs/` - Documentation and specifications

## 🧪 Testing Strategy

### Test Categories & Runtime

- **Unit Tests** (~5s): Isolated component testing
- **Integration Tests** (~30s): Database and satellite integration
- **System Tests** (~2min): End-to-end pipeline validation
- **Property Tests** (~1min): Randomized edge case testing
- **Adversarial Tests** (~3min): Security and chaos scenarios
- **VM Tests** (~5-15min): Full NixOS deployment testing

### Database Testing

- Automated 64-database pool with PostgreSQL advisory locks
- Parallel execution optimized for fast feedback
- Comprehensive test data factories in sinex-test-utils

### Test Execution

```bash
# Development workflow
just test-dev              # Quick cycle: db-setup + test-fast

# Debugging
just test-reliable          # Limited parallelism for flaky tests
just test-verbose           # Full output for debugging
```

## 🔧 Development Workflows

### 🤖 IMPORTANT: AI Agent Guidelines

When checking compilation status:

```bash
# Get errors/warnings
just errors

# Get errors/warnings as JSON
just ai-errors-json
```

### Development Commands

```bash
# Compilation checks (optimized for speed)
just qc                    # Full workspace check (~2-3s)
just qcc sinex-db         # Check specific crate only (~0.67s) 
just qcs                   # Smart check - only changed crates (~0.2-0.7s)

# Continuous checking
just watch                 # Run bacon (uses smart check by default)
just watchc sinex-db      # Watch specific crate only
just watchs               # Smart watch - auto-detects active crate

# Error inspection
just errors               # Show compilation errors
just warnings             # Show compilation warnings
```

**Understanding Smart Commands**:

- `qcs` only checks crates with git changes - fast but won't show errors from unchanged crates
- `qcc <crate>` checks one crate - useful when focusing on specific area
- `qc` checks everything - use when you need to see all errors across workspace

**Development Strategy**:

1. Start with `just qc` to see all errors
2. Use `just qcs` for fast iteration while fixing
3. Run `just qc` again before committing to ensure nothing missed

### Cargo Timing Reports

```bash
# Build with detailed timing information
cargo build --timings

# View report at: target/cargo-timings/cargo-timing.html
```

### Development Workflow

```bash
# Quick check compilation
just qc                    # Full workspace check
just qcs                   # Smart check (only changed crates)
just qcc sinex-db         # Check specific crate

# Standard development cycle
just dev                   # fmt + check + test-fast
```

### Working with Database Changes

```bash
# After schema changes
just migrate               # Apply migrations
just sqlx-prepare         # Update SQLX cache for Nix builds
git add .sqlx/            # MUST commit SQLX cache
```

### Working with Satellites

```bash
# Test satellite in scanner mode
cargo run --bin sinex-fs-watcher -- scan /path/to/scan

# Test satellite in sensor mode (runs continuously)
cargo run --bin sinex-fs-watcher -- sensor

# Integration testing
cargo test --test satellite_architecture_test
```

### Performance Optimization

```bash
# Coverage analysis
just coverage-html         # Generate HTML coverage report

# Performance testing
just test-performance      # Load and stress tests

# Build optimization
cargo build --release      # Optimized builds
```

## 🔍 Debugging & Troubleshooting

### Common Issues

- **Database connection failed**: Ensure PostgreSQL is running via nix develop
- **SQLX offline errors**: Run `just sqlx-prepare` and commit `.sqlx/`
- **Compilation errors**: Check `compilation.log` via `just errors`

### Debugging Commands

```bash
just errors                # Show compilation errors
just warnings              # Show compilation warnings
just recent-changes        # Show recent git changes for context
```

# Da Plan: @docs/REFACTORING_PLAN_4.md

(followed by docs/REFACTORING_REDUX.md)
