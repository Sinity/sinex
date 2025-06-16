# Sinex Test Suite

Comprehensive testing infrastructure for the Sinex event-driven data capture system.

## Overview

This test suite validates that Sinex functions correctly as a complete system for capturing, storing, processing, and querying events. The tests are organized by scope and purpose to ensure thorough coverage of all functionality.

## Test Organization

```
test/
├── unit/                        # Unit tests by crate
│   ├── core/                    # sinex-core components
│   └── db/                      # sinex-db operations
├── integration/                 # Integration tests
│   ├── database/                # Database integration
│   ├── collector/               # Collector integration  
│   ├── worker/                  # Worker integration
│   └── event_sources/           # Event source integration
├── system/                      # System-level tests
│   ├── end_to_end/              # Full pipeline tests
│   ├── external/                # External dependency tests
│   ├── performance/             # Load and performance tests
│   └── regression/              # Bug regression tests
├── adversarial/                 # Security and edge cases
├── common/                      # Shared test utilities
├── cli/                         # Python CLI tests
└── property_tests.rs            # Property-based tests
```

## Running Tests

### Quick Commands

```bash
# Run all tests
just test-all

# Test categories
just test-unit          # Unit tests only
just test-integration   # Integration tests
just test-system        # System-level tests
just test-e2e           # End-to-end tests
just test-core          # Core library tests
just test-database      # Database tests
just test-worker        # Worker tests
just test-adversarial   # Security/edge case tests
just test-regression    # Regression tests

# CLI tests
just test-cli           # Python CLI unit tests
just test-cli-all       # All CLI tests

# Coverage
just coverage           # Run with coverage
just coverage-html      # Generate HTML report
just coverage-report    # Open coverage in browser
```

### Detailed Commands

```bash
# Run specific test files
cargo test --test integration unit::core::basic_functionality_test
cargo test --test integration integration::database::schema_validation_tests
cargo test --test integration system::end_to_end::full_pipeline_tests

# Run with output
cargo test --test integration system::end_to_end:: -- --nocapture

# Run ignored tests (long-running)
cargo test --test integration -- --ignored

# Run property tests
cargo test property_tests

# Run adversarial tests
cargo test --test integration adversarial::
```

## Test Categories

### Unit Tests (`unit/`)

Test individual components in isolation:

- **core/**: EventSource trait, RawEventBuilder, registry, context
- **db/**: Database models, queries, validation, connections

**Coverage Goal**: ≥90% line coverage

### Integration Tests (`integration/`)

Test component interactions:

- **database/**: Database operations, TimescaleDB, schema validation
- **collector/**: Event source lifecycle, configuration, coordination
- **worker/**: Queue processing, concurrency, error handling
- **event_sources/**: Individual event sources with real events

**Coverage Goal**: ≥80% line coverage

### System Tests (`system/`)

Test complete system behavior:

- **end_to_end/**: Full pipeline from capture → storage → processing → query
- **external/**: Git Annex, PostgreSQL extensions, external dependencies
- **performance/**: High-volume ingestion, concurrent processing, latency
- **regression/**: Previously fixed bugs, edge cases

**Coverage Goal**: ≥70% functional coverage

### Adversarial Tests (`adversarial/`)

Test security, edge cases, and failure scenarios:

- Time-based attacks (clock skew, timezone confusion)
- Filesystem edge cases (symlinks, permissions, special files)
- Network issues (DNS timeouts, connection failures)
- Resource exhaustion (memory, disk, connections)
- Configuration attacks (file replacement, hot reload races)

### Property Tests (`property_tests.rs`)

Property-based testing using `proptest`:

- ULID generation properties
- Event validation properties
- Database consistency properties

## Test Guidelines

### Writing Tests

1. **Naming**: Use descriptive test names that explain what's being tested
   ```rust
   #[test]
   fn test_filesystem_event_capture_creates_valid_event() { ... }
   ```

2. **Structure**: Follow Arrange-Act-Assert pattern
   ```rust
   // Arrange
   let pool = create_test_pool().await?;
   let event = events::filesystem_event("file.created", "/test/file.txt");
   
   // Act
   let inserted_id = queries::insert_event(&pool, &event).await?;
   
   // Assert
   assert!(!inserted_id.to_string().is_empty());
   ```

3. **Cleanup**: Tests must clean up after themselves
   ```rust
   #[sqlx::test]  // Automatic transaction rollback
   async fn test_event_insertion(pool: PgPool) -> Result<(), BoxError> {
       // Test automatically rolls back
   }
   ```

4. **Error Messages**: Provide clear failure messages
   ```rust
   assert!(result.is_ok(), "Expected event insertion to succeed, got: {:?}", result);
   ```

### Test Data

Use the `common/` module utilities:

```rust
use crate::common::{events, assertions, generators};

// Create test events
let event = events::filesystem_event("file.created", "/test/file.txt");
let events = generators::test_events(10);

// Assert outcomes
assertions::assert_event_inserted(&pool, &event).await?;
assertions::assert_events_equivalent(&actual, &expected);
```

### Database Tests

Use `#[sqlx::test]` for automatic isolation:

```rust
#[sqlx::test]
async fn test_event_validation(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Each test gets a fresh transaction that rolls back
    let event = events::filesystem_event("file.created", "/test");
    let result = queries::insert_event(&pool, &event).await;
    assert!(result.is_ok());
    Ok(())
}
```

## Coverage Requirements

| Component | Line Coverage | Branch Coverage |
|-----------|---------------|-----------------|
| sinex-core | ≥90% | ≥85% |
| sinex-db | ≥90% | ≥85% |
| sinex-worker | ≥85% | ≥80% |
| sinex-collector | ≥80% | ≥75% |
| sinex-events | ≥75% | ≥70% |
| Overall | ≥80% | ≥75% |

Generate coverage reports:

```bash
just coverage-html    # HTML report in target/llvm-cov/html/
just coverage-lcov    # LCOV for CI in target/llvm-cov/coverage.lcov
```

## Performance Benchmarks

Performance tests verify system can handle expected loads:

- **Event Ingestion**: ≥1,000 events/second sustained
- **Query Response**: ≤100ms for recent events (p95)
- **Worker Processing**: ≥500 events/second/worker
- **Database Storage**: ≤10ms insert latency (p95)

Run performance tests:

```bash
cargo test --test integration system::performance:: -- --ignored
```

## Debugging Tests

### Failed Tests

1. Run with output to see details:
   ```bash
   cargo test failing_test_name -- --nocapture
   ```

2. Check database state:
   ```bash
   just psql
   SELECT * FROM raw.events ORDER BY ts_ingest DESC LIMIT 10;
   ```

3. Enable debug logging:
   ```bash
   RUST_LOG=debug cargo test failing_test_name
   ```

### Test Environment

The test suite uses isolated test databases and temporary files:

- **Database**: Each `#[sqlx::test]` gets a transaction that rolls back
- **Files**: Tests use temporary directories that auto-cleanup
- **Network**: Mock external services where possible

## CI/CD Integration

The test suite is designed for CI/CD environments:

```bash
# Fast feedback (< 30 seconds)
just test-unit

# Integration verification (< 2 minutes)  
just test-integration

# Full validation (< 10 minutes)
just test-all

# Coverage reporting
just coverage-lcov
```

## Test Index

### Unit Tests

| File | Purpose | Coverage |
|------|---------|----------|
| `unit/core/basic_functionality_test.rs` | Core constants and builders | Event creation, validation |
| `unit/core/event_registry_tests.rs` | Event type registry | Source registration, lookup |
| `unit/core/event_source_context_tests.rs` | Configuration context | Config loading, sharing |
| `unit/core/raw_event_builder_tests.rs` | Event builder patterns | Field validation, JSON payloads |
| `unit/db/basic_db_test.rs` | Database connections | Pool management, migrations |
| `unit/db/database_operations_tests.rs` | CRUD operations | Insert, query, update events |
| `unit/db/event_validator_tests.rs` | Event validation | Schema validation, constraints |

### Integration Tests

| File | Purpose | Coverage |
|------|---------|----------|
| `integration/database/database_integration_tests.rs` | Database integration | Full database workflow |
| `integration/database/timescaledb_tests.rs` | TimescaleDB features | Hypertables, compression |
| `integration/database/ulid_integration_tests.rs` | ULID database integration | ULID storage, queries |
| `integration/database/jsonschema_validation_tests.rs` | JSON schema validation | PostgreSQL validation |
| `integration/database/schema_validation_tests.rs` | Schema enforcement | Constraint validation |
| `integration/collector/basic_collector_test.rs` | Collector functionality | Event source coordination |
| `integration/collector/config_tests.rs` | Configuration management | Config loading, validation |
| `integration/worker/backoff_tests.rs` | Retry logic | Exponential backoff, limits |
| `integration/worker/concurrent_processing_tests.rs` | Worker concurrency | Multiple workers, locking |
| `integration/worker/worker_lifecycle_tests.rs` | Worker management | Start, stop, error handling |
| `integration/event_sources/atuin_tests.rs` | Atuin history integration | Command history parsing |
| `integration/event_sources/event_source_tests.rs` | Event source lifecycle | Initialize, stream, shutdown |
| `integration/event_sources/terminal_tests.rs` | Terminal event capture | Command execution events |

### System Tests

| File | Purpose | Coverage |
|------|---------|----------|
| `system/end_to_end/complete_system_test.rs` | Complete system validation | Full pipeline testing |
| `system/end_to_end/comprehensive_flow_test.rs` | Complex flow scenarios | Multi-source event flows |
| `system/end_to_end/full_pipeline_tests.rs` | Pipeline integrity | Capture → process → query |
| `system/external/git_annex_integration_tests.rs` | Git Annex integration | Large file handling |
| `system/regression/concurrent_database_test.rs` | Concurrency regression | Database race conditions |
| `system/regression/config_reload_test.rs` | Config reload bugs | Hot reload issues |
| `system/regression/json_payload_test.rs` | JSON handling bugs | Payload validation edge cases |
| `system/regression/ulid_overflow_test.rs` | ULID edge cases | Overflow, collision handling |
| `system/regression/validation_edge_cases_test.rs` | Validation bugs | Edge case validation |

### Adversarial Tests

| File | Purpose | Coverage |
|------|---------|----------|
| `adversarial/advanced_time_attacks_test.rs` | Time-based attacks | Clock manipulation, skew |
| `adversarial/config_reload_attacks_test.rs` | Configuration attacks | File replacement, races |
| `adversarial/database_boundary_test.rs` | Database boundaries | Connection limits, timeouts |
| `adversarial/event_type_specific_test.rs` | Event-specific attacks | Malformed events, injection |
| `adversarial/filesystem_edge_cases_test.rs` | Filesystem edge cases | Permissions, special files |
| `adversarial/network_distributed_issues_test.rs` | Network failures | DNS, connection issues |
| `adversarial/race_conditions_test.rs` | Race conditions | Concurrent access patterns |
| `adversarial/resource_exhaustion_test.rs` | Resource exhaustion | Memory, disk, connections |
| `adversarial/security_attacks_test.rs` | Security attacks | Injection, privilege escalation |
| `adversarial/state_machine_violations_test.rs` | State violations | Invalid state transitions |
| `adversarial/worker_coordination_test.rs` | Worker coordination | Coordination failures |

## Maintenance

### Adding New Tests

1. Determine appropriate category (unit/integration/system)
2. Place in correct directory following naming conventions
3. Update relevant `mod.rs` file
4. Add entry to this README index
5. Ensure tests follow guidelines above

### Updating Tests

1. Maintain backward compatibility where possible
2. Update test data generation if schemas change
3. Keep coverage metrics above thresholds
4. Update documentation when behavior changes

### Performance

Keep tests fast:
- Unit tests: <100ms each
- Integration tests: <5s each  
- System tests: <30s each
- Use `#[ignore]` for slow tests, run with `--ignored`

## Success Criteria

The test suite succeeds when:

✅ **Functional**: All critical paths validated end-to-end  
✅ **Coverage**: Meets minimum coverage thresholds  
✅ **Performance**: System handles expected loads  
✅ **Reliability**: Tests are deterministic and stable  
✅ **Security**: Adversarial scenarios are covered  
✅ **Maintainable**: Tests are clear and easy to update  

When tests pass, Sinex works correctly as a complete event capture and processing system.