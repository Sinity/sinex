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
just migrate               # or: just m

# Fast compilation check
just check

# Update SQLX cache (CRITICAL for Nix builds)
just sqlx-prepare         # MUST commit .sqlx/ after this!
```

### Development Workflow

```bash
# Quick check
just check                # Fast compilation check

# Run tests
just test                 # Unit + property tests (~30s)
just test-all            # Complete test suite
just test-integration    # Integration tests only

# Pre-commit workflow
just pre-commit          # Format + lint + check + test
```

### Database Operations

```bash
# Database connection
just psql                # Connect to dev database

# Migration management
just migrate             # Apply migrations (alias: m)
just migrate-create NAME # Create new migration
just migrate-status      # Check migration status

# Database utilities
just db-reset           # Drop, recreate, and migrate
just db-setup           # Create and migrate (for tests)
```

### Running Services

```bash
# Core services
just ingestd            # Central coordinator (gRPC)
just gateway            # API gateway

# Event satellites
just fs-watcher         # File system events
just terminal           # Terminal events
just desktop            # Desktop events
just system             # System events

# Processing services
just canonicalizer      # Terminal command canonicalizer
just health             # Health aggregator

# Development tools
just monitor            # All services in mprocs dashboard
just query              # Query recent events (alias: q)
just query 50           # Query 50 recent events
```

### Quick Utilities

```bash
# Code quality
just fmt                # Format code
just lint               # Clippy lints
just errors             # Show compilation errors
just warnings           # Show compilation warnings

# Development tools
just watch              # Watch for changes (bacon)
just docs               # Build and open documentation
just coverage           # Generate coverage report

# Maintenance
just clean              # Clean build artifacts
just update             # Update dependencies
just audit              # Security audit
just unused             # Check unused dependencies
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
  core/                    # Core service implementations
    sinex-gateway/         # API gateway service
    sinex-ingestd/         # Central ingestion coordinator
    sinex-rpc-dispatcher/  # RPC routing and dispatch
    sinex-sensd/           # Sensor management daemon
  lib/                     # Shared libraries
    sinex-core/            # Core types, db, utilities
    sinex-macros/          # Procedural macros
    sinex-migrations/      # Database migrations
    sinex-satellite-sdk/   # SDK for satellite development
    sinex-services/        # Service abstractions
    sinex-test-utils/      # Testing utilities
  satellites/              # Event capture satellites
    sinex-analytics-automaton/        # Analytics processing
    sinex-content-automaton/          # Content processing
    sinex-desktop-satellite/          # Desktop environment events
    sinex-document-ingestor/          # Document ingestion
    sinex-fs-watcher/                 # File system monitoring
    sinex-health-aggregator/          # Health metrics collection
    sinex-pkm-automaton/              # PKM processing
    sinex-search-automaton/           # Search processing
    sinex-system-satellite/           # System-level events
    sinex-terminal-command-canonicalizer/  # Terminal command processing
    sinex-terminal-satellite/         # Terminal session monitoring
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

## 🔧 Development Workflows

### 🤖 IMPORTANT: AI Agent Guidelines

When checking compilation status:

```bash
# Get errors/warnings
just errors
just warnings
```

### Development Workflow

```bash
# Standard development cycle
just check                # Fast compilation check
just test                 # Run unit + property tests
just pre-commit          # Full pre-commit checks

# Continuous development
just watch               # Watch for changes with bacon
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

# Rules for Claude
- Avoid reporting through arbitrary markdown files, prefer to output a direct report. If you create a markdown, do so in docs/, not in the project root.