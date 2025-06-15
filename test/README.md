# Sinex Test Infrastructure

> **📋 Implementation Plan**: See [PLAN.md](PLAN.md) for the comprehensive reorganization and improvement plan.

## Quick Start

```bash
# All tests
cargo test

# Integration tests only  
cargo test --test integration

# Unit tests only
cargo test --lib

# Specific test categories
cargo test --test database/
cargo test --test adversarial/
cargo test --test worker/

# CLI tests
python -m pytest test/cli/

# Development environment with automatic database setup
nix develop
```

## What "Sinex Works" Means

For tests to validate that Sinex actually functions, they must verify the complete system flow:

1. **Event Capture**: Event sources successfully capture and stream events
2. **Event Storage**: Events are stored immutably in PostgreSQL with proper validation  
3. **Event Processing**: Workers process events from the promotion queue
4. **Event Query**: CLI can retrieve and display captured events
5. **End-to-End Flow**: Complete pipeline from capture → storage → processing → query

## Test Organization

### Current Structure
```
test/
├── README.md                    # This file - comprehensive test documentation
├── PLAN.md                      # Implementation plan for test improvements
├── common/                      # Shared test utilities and helpers
├── database/                    # Database layer tests (schema, migrations, ULID)
├── events/                      # Event source and capture tests
├── worker/                      # Worker processing and lifecycle tests
├── collector/                   # Collector coordination and management tests  
├── pipeline/                    # End-to-end pipeline integration tests
├── adversarial/                 # Security, edge cases, and robustness tests
├── bugs/                        # Regression prevention tests
├── agent/                       # Agent manifest and heartbeat tests
├── annex/                       # Git Annex large file management tests
├── ingestor/                    # Data ingestion tests
├── model/                       # Data model and serialization tests
├── validation/                  # Event validation tests
├── ulid/                        # ULID functionality tests
├── cli/                         # Python CLI tests
├── property_tests.rs            # Property-based tests
├── test_setup.rs               # Test infrastructure utilities
└── mod.rs                      # Root test module
```

## Test Categories

### Unit Tests
Fast, isolated tests with no external dependencies:
- **sinex-core**: Event builders, registry, context, validation
- **sinex-ulid**: ULID generation, conversion, monotonic properties
- **sinex-db**: Models, serialization, validation logic  
- **sinex-worker**: Backoff calculations, processing logic
- **sinex-events**: Event type definitions and builders

**Run with**: `cargo test --lib`

### Integration Tests  
Tests that verify component interaction with controlled environments:
- **Database Integration**: PostgreSQL operations with isolated test databases
- **Event Source Integration**: Event capture with real but controlled inputs
- **Worker Integration**: Queue processing with test promotion items
- **Collector Integration**: Event source coordination and output routing

**Run with**: `cargo test --test integration`

### System Tests
Full system validation with real components:
- **End-to-End Tests**: Complete pipeline from capture to query
- **External System Tests**: PostgreSQL extensions, Git Annex integration
- **Performance Tests**: Load testing and scalability validation

**Run with**: Custom commands (see justfile)

### Adversarial Tests
Security, edge cases, and robustness validation (16 test files, 100+ tests):
- **Time & ULID Attacks**: Clock manipulation, collision testing, timezone issues
- **Database Boundaries**: 1GB payloads, connection exhaustion, chunk boundaries
- **Security Vulnerabilities**: Injection attacks, unicode bypasses, DoS attempts
- **Race Conditions**: Worker conflicts, causality violations, thundering herds
- **Resource Exhaustion**: Memory, disk, connection limits
- **JSON Attacks**: Circular references, billion laughs, hash collisions
- **State Machine Violations**: Invalid transitions, corruption scenarios
- **Network Issues**: DNS timeouts, connection failures, distributed coordination
- **Query Exploits**: SQL injection, parameter manipulation

**Run with**: `cargo test --test adversarial/`

## Test Quality Standards

### Database Tests
Use `#[sqlx::test]` for automatic database isolation:
```rust
#[sqlx::test]
async fn test_event_storage(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Test with isolated database - automatic cleanup
    let event = create_test_event();
    let stored_id = insert_event(&pool, &event).await?;
    assert_eq!(retrieve_event(&pool, stored_id).await?, event);
    Ok(())
}
```

### Unit Tests  
Standard `#[test]` or `#[tokio::test]`:
```rust
#[test]
fn test_ulid_generation() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    assert_ne!(ulid1, ulid2);
    assert!(ulid1.timestamp() <= ulid2.timestamp());
}
```

### Test Naming
- Descriptive names explaining what's being tested
- Use `test_` prefix for unit tests
- Use domain-specific prefixes for integration tests
- Group related tests in modules

### Error Messages
- Clear assertions that explain what went wrong
- Include relevant context in failure messages
- Use `assert_eq!` with meaningful descriptions

## Shared Test Utilities

The `test/common/` module provides:

### Event Builders
```rust
use crate::common::events;

// Pre-built test events
let fs_event = events::filesystem_event("file.created", "/test/file.txt");
let terminal_event = events::kitty_event("ls -la");
let wm_event = events::hyprland_event("window.focus", data);
```

### Assertions
```rust
use crate::common::assertions;

// Database operation assertions
assertions::assert_event_inserted(&pool, &event).await?;
assertions::assert_events_equivalent(&actual, &expected);
```

### Test Data Generation
```rust
use crate::common::generators;

// Generate test data sets
let events = generators::test_events(100);
let ulids = generators::sequential_ulids(50);
```

## Coverage Analysis

### Well-Tested Components
- **sinex-ulid**: Excellent (unit + integration + adversarial)
- **sinex-db**: Good (models, database ops, validation)  
- **Adversarial Security**: Excellent (16 comprehensive test files)
- **Database Layer**: Excellent (schema, TimescaleDB, validation)

### Partially Tested Components  
- **sinex-collector**: Basic tests, missing core collection logic
- **sinex-events**: Minimal unit tests, missing event builders
- **sinex-core**: No dedicated unit tests, tested via integration

### Missing Coverage (High Priority)
1. **Core Functionality Tests**: Event registry, builders, context
2. **Database Operations**: Insert, query, promotion queue mechanics
3. **Worker Processing**: Queue processing, error handling, DLQ management
4. **Collector Coordination**: Event source lifecycle, output routing
5. **End-to-End Validation**: Complete pipeline functionality

## Test Execution

### Environment Setup
```bash
# Enter development environment (automatic database setup)
nix develop

# Verify database is ready
just migrate
```

### Running Test Suites
```bash
# Core functionality
just test-unit          # Unit tests only
just test-integration   # Integration tests  
just test-core          # Core crate tests

# System validation
just test-e2e           # End-to-end tests
just test-system        # System-level tests
just test-all           # Complete test suite

# Specific areas
just test-database      # Database layer
just test-adversarial   # Security and edge cases
just test-worker        # Worker processing
just test-cli           # CLI functionality

# Development
just watch              # Continuous testing
```

### Coverage Reporting
```bash
# Generate coverage report (when implemented)
just coverage           # Run tests with coverage
just coverage-html      # HTML coverage report
just coverage-lcov      # LCOV format for CI
```

## Test Infrastructure Rules

1. **Database Isolation**: Use `#[sqlx::test]` - never manual pools
2. **Test Independence**: No shared state between tests
3. **Self-Contained**: Tests clean up after themselves
4. **No External Dependencies**: Mock external services
5. **Deterministic**: Tests must be reproducible
6. **Fast Feedback**: Unit tests complete in milliseconds
7. **Clear Failures**: Meaningful error messages when tests fail

## Test File Index

### Core Functionality (To Be Improved)
- `database/database_integration_tests.rs` - Basic DB operations (needs work)
- `events/event_source_tests.rs` - Event source trait tests (incomplete)
- `worker/worker_lifecycle_tests.rs` - Worker management (partial)
- `collector/basic_collector_test.rs` - Collector tests (placeholder)

### Well-Implemented Tests  
- `adversarial/*` - 16 comprehensive security/edge case test files
- `database/ulid_integration_tests.rs` - ULID database integration
- `ulid/ulid_unit_tests.rs` - ULID functionality
- `pipeline/full_pipeline_tests.rs` - End-to-end pipeline validation
- `property_tests.rs` - Property-based ULID testing

### Python CLI Tests
- `cli/test_exo_cli.py` - CLI functionality
- `cli/test_exo_cli_integration.py` - CLI integration

### Crate-Specific Tests
- `crate/sinex-promo-worker/tests/promotion_tests.rs` - Promotion worker

## Current Test Statistics

- **Total Test Files**: 60 (44 Rust + 3 Python + 2 Shell + 11 docs)
- **Test Categories**: 13 major categories
- **Adversarial Tests**: 16 files with 100+ security/edge case tests
- **Database Tests**: 5 files covering data layer
- **Integration Tests**: Good coverage of cross-component interaction
- **Unit Tests**: Gaps in core crate functionality

## Success Criteria

Tests pass → Sinex works as a complete system:

1. **Core Functionality Validated**: All critical paths tested
2. **No False Positives**: Tests don't pass when functionality is broken
3. **Coverage Metrics**: ≥80% for core crates, ≥70% overall
4. **End-to-End Verification**: Complete pipeline validated
5. **Security Assurance**: Adversarial tests prevent vulnerabilities

## Implementation Roadmap

See [PLAN.md](PLAN.md) for detailed implementation plan including:
- Code coverage setup
- Test reorganization  
- Core functionality implementation
- System-level test development
- Performance and external system testing

The goal is comprehensive test coverage that ensures when tests pass, Sinex actually works as intended for capturing, storing, processing, and querying events.