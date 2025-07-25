# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 🏗️ Repository Overview

Sinex is a comprehensive event-driven data capture system that records everything happening on a computer for later analysis. The system uses a distributed satellite-based architecture where independent services capture events and feed them into a central PostgreSQL + TimescaleDB data substrate via Redis Streams.

**Core Architecture**: Satellites → ingestd → Event Substrate → Redis Streams → Automata → Query Interface

## 🚀 Essential Development Commands

### Environment Setup
```bash
# Enter development shell (always run first)
nix develop

# Apply database migrations
just migrate
# or manually: sqlx migrate run

# Verify workspace builds
just check
# or: cargo check --workspace --all-features
```

### Testing (Hierarchical by Speed)
```bash
# Fast development feedback (~30s)
just test-fast               # Unit + property tests only

# Individual test categories
just test-unit               # Unit tests (~5s)
just test-integration        # Integration tests (~30s)
just test-system            # System/E2E tests (~2min)
just test-property          # Property-based tests (~1min)
just test-adversarial       # Security/chaos tests (~3min)

# Full test suite
just test-all               # Complete suite including VM tests (~10-15min)

# Test specific packages
just test-pkg sinex-db      # Test specific crate
just test-individual integration::database_test  # Specific test file

# Watch tests during development
just watch-fast             # Re-run fast tests on file changes
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

**Testing Architecture**:
- Parallel test execution with database pool isolation
- Hierarchical test organization: unit → integration → system → adversarial
- Property-based testing with proptest
- VM tests for NixOS integration validation

## 📁 Project Structure

### Workspace Organization
```
crate/
├── sinex-core-*           # Core libraries (types, utils, runtime, fs)
├── sinex-db/              # Database layer with query builders
├── sinex-satellite-sdk/   # SDK for building satellites
├── sinex-ingestd/         # Central ingestion daemon
├── sinex-gateway/         # API gateway service
├── sinex-*-satellite/     # Event source satellites
├── sinex-*-automaton/     # Processing satellites
└── sinex-test-utils/      # Shared testing infrastructure
```

### Key Directories
- `migrations/` - Database schema migrations (numbered sequentially)
- `test/` - Comprehensive test suites organized by category
- `cli/` - Python query interface (exo.py)
- `nixos/` - NixOS module for system deployment
- `spec/` - Documentation and specifications

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

### Sinex DevTools Service
The `sinex-devtools` service provides a comprehensive development dashboard that runs automatically on system startup:

**What it does**:
- Runs `mprocs` in a persistent tmux session with multiple monitoring panes
- **Bacon**: Continuous compilation checking with output to `.claude-outputs/bacon.log`
- **Claude Activity**: Real-time monitoring of Claude's actions from session logs
- **Claude Code Monitor**: Live usage metrics and active blocks tracking
- **Database Activity**: PostgreSQL query monitoring for sinex_dev
- **Git Status**: Repository state and recent commits
- **System Resources**: btop for CPU/memory/process monitoring

**Usage**:
```bash
sinex-attach               # Attach to running development dashboard
# Press 'q' to detach from tmux (keeps running in background)
# Press Ctrl+q to quit mprocs entirely
```

**Automatic features**:
- Sets up the sinex_dev database if it doesn't exist
- Runs migrations on startup
- Maintains compilation state between sessions
- Provides instant feedback on code changes via bacon
- Compilation output available in `.claude-outputs/bacon.log` for scripts

**Important for Claude Code**:
- **ALWAYS use bacon-aware commands**: Never run `cargo check` or `just check` directly
- **Use `just qc`**: Quick compilation status check with smart waiting for bacon
- **Use `just errors`**: Get compilation errors with automatic wait for bacon to finish
- **Use `just warnings`**: Get compilation warnings with automatic wait
- **Use `just ce`**: Quick check and show errors in one command
- **No manual sleeps needed**: Commands automatically wait for bacon to finish (up to 10s)
- **Avoid triggering builds**: Bacon is already compiling continuously in background

Example workflow:
```bash
# After making code changes
just qc                    # Check compilation status (waits for bacon automatically)
just errors                # If there are errors, get details
just warnings              # Check for any warnings

# Or use the combined command:
just ce                    # Check and show errors immediately
```

These commands intelligently wait for bacon to finish compiling (checking if the log file was modified in the last 2 seconds) before showing results, eliminating the need for arbitrary sleep commands.

### Standard Development Cycle
```bash
just dev                   # fmt + check + test-fast

# Or monitor continuously with:
sinex-attach               # Attach to devtools dashboard
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

## ⚠️ Critical Requirements

### SQLX Cache Management
- **ALWAYS** run `just sqlx-prepare` after database schema changes
- **MUST** commit `.sqlx/` directory - Nix builds fail without it
- Verify with `just sqlx-check` before pushing

### NixOS Integration
- All development happens in `nix develop` shell
- Services configured via NixOS modules in `nixos/`
- VM tests validate full system integration

### Zero-Warning Policy
- Clippy configured to treat all warnings as errors
- Use `just lint` to enforce before committing
- Fix warnings with `just fix-warnings` when possible

### Test Quality Standards
- All features require comprehensive test coverage
- Property tests for complex logic (use proptest)
- Integration tests for database operations
- System tests for end-to-end workflows

## 🔍 Debugging & Troubleshooting

### Common Issues
- **Database connection failed**: Ensure PostgreSQL is running via nix develop
- **SQLX offline errors**: Run `just sqlx-prepare` and commit `.sqlx/`
- **Flaky tests**: Use `just test-reliable` with limited parallelism
- **Compilation errors**: Check `compilation.log` via `just errors`

### Debugging Commands
```bash
just errors                # Show compilation errors
just warnings              # Show compilation warnings
just daemon-status         # Check background compilation daemon
just recent-changes        # Show recent git changes for context
```

### Performance Analysis
```bash
just test-parallel-stats   # Test execution statistics
just coverage-fast         # Fast test coverage analysis
```

## 📊 Success Criteria

- All tests pass in under 2 minutes for fast feedback
- Zero clippy warnings/errors
- SQLX cache is up to date for Nix builds
- Database migrations apply cleanly
- Satellite services start and communicate properly
- Integration tests validate end-to-end workflows