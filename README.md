# Sinex - Event-Driven Data Capture System

Sinex is a comprehensive event-driven data capture system that records everything happening on a computer for later analysis. It provides a unified collection framework for various event sources with immutable storage and concurrent processing capabilities.

## Architecture

**Core Flow**: EventSources → UnifiedCollector → Event Substrate → Workers → Query Interface

- **EventSources**: Individual event capturing components (filesystem, terminals, window managers)
- **UnifiedCollector**: Central coordinator that manages all event sources
- **Event Substrate**: PostgreSQL + TimescaleDB with ULID keys, stores immutable events
- **Workers**: Process events concurrently using `SELECT FOR UPDATE SKIP LOCKED`
- **Query Interface**: Python CLI for exploring captured events

## Test Coverage

Sinex has comprehensive test coverage across multiple categories:

### Test Summary
- **Total test files**: 73
- **Total tests**: 239

### Test Categories
- **Unit tests**: 52 tests
  - Core functionality (event registry, raw event builder, context)
  - Database operations and validation
  - ULID edge cases and conversions
  
- **Integration tests**: 40 tests
  - Database integration (TimescaleDB, schema validation)
  - Collector configuration and lifecycle
  - Worker processing and backoff strategies
  - Event source integration (Atuin, terminal)

- **Adversarial tests**: 83 tests
  - Time-based attacks and ULID edge cases
  - Resource exhaustion scenarios
  - Security vulnerability testing
  - JSON payload attacks
  - Race conditions and state violations
  - Database boundary conditions

- **System tests**: 22 tests
  - End-to-end pipeline testing
  - Git Annex integration
  - Regression tests for known issues
  - Performance benchmarks

- **VM tests**: 1 comprehensive test
  - Full NixOS module integration
  - Database setup and permissions
  - Event capture verification
  - Service resilience testing

### Running Tests

```bash
# Run all tests
just test

# Run specific test categories
cargo test --test unit/
cargo test --test integration/
cargo test --test adversarial/

# Run VM tests
nix build .#checks.x86_64-linux.sinex-vm-basic -L

# Run with coverage
just test-coverage
```

## Quick Start

### Development Setup
```bash
nix develop                    # Enter development shell
just                          # See available commands
```

### Running the Collector
```bash
just unified                  # Run unified collector
just worker                   # Run promotion worker
just query                    # Query recent events
```

## Documentation

- **Architecture**: `spec/STAD.md`
- **Getting Started**: `spec/SADI.md` 
- **Implementation Details**: `spec/docs/tims/`
- **Design Decisions**: `spec/docs/adr/`

## License

See LICENSE file for details.