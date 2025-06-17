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
- **failure_modes/**: System behavior under failure conditions

**Coverage Goal**: ≥80% line coverage

#### Failure Mode Tests

The `integration/failure_modes/` directory contains tests for various failure scenarios:

1. **Channel Backpressure** (`channel_backpressure_test.rs`):
   - Tests event channel overflow when producers outpace consumers
   - Verifies graceful degradation and event dropping behavior
   - Simulates memory pressure with large event payloads
   - Tests event source crash and recovery mechanisms

2. **Configuration Reload** (`config_reload_test.rs`):
   - Tests configuration changes during active processing
   - Validates config reload timing (during batch, between batches)
   - Tests invalid configuration rejection
   - Ensures graceful handling of reload during shutdown

3. **Network Timeouts** (`network_timeout_test.rs`):
   - Tests database connection timeout scenarios
   - Simulates various network conditions (fast, slow, intermittent)
   - Tests retry logic with exponential backoff
   - Verifies connection pool behavior under timeout conditions

4. **Worker Orphans** (`worker_orphan_test.rs`):
   - Tests detection of orphaned workers (crashed but holding work)
   - Verifies work item recovery from dead workers
   - Tests zombie worker prevention mechanisms
   - Ensures proper cleanup of worker state

5. **Connection Pool Exhaustion** (`connection_pool_test.rs`):
   - Tests behavior when connection pool is exhausted
   - Simulates various workload patterns (steady, burst, long-running)
   - Tests connection leak detection and reporting
   - Verifies deadlock prevention in connection acquisition

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

## NixOS VM Testing

Sinex includes comprehensive NixOS VM-based end-to-end testing for complete system validation:

### VM Test Structure
- **Basic Flow Test**: System setup, individual event sources, service resilience
- **Multi-Source Stress Test**: Concurrent operation under configurable load intensities
- **Failure Recovery Test**: Database disconnection, crash recovery, resource pressure
- **Performance Validation**: High-frequency events, query performance, resource monitoring

### Wayland/GUI Environment Fixes
The VM tests include proper Wayland compositor setup for GUI event testing:
- Fixed greetd configuration by replacing with direct Weston service
- Proper XDG_RUNTIME_DIR and permissions setup
- Service dependency ordering to ensure Wayland readiness
- Headless mode suitable for automated testing
- Support for wl-clipboard, Kitty terminal, and Hyprland integration

### Running VM Tests
```bash
just test-vm-basic           # Basic flow test
just test-vm-multi-source    # Multi-source stress test  
just test-vm-failure-recovery # Failure recovery test
just test-vm-performance     # Performance validation test
just test-vm-all            # All VM tests
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
| `integration/failure_modes/channel_backpressure_test.rs` | Channel overflow handling | Backpressure, event drops |
| `integration/failure_modes/config_reload_test.rs` | Config reload scenarios | Hot reload, validation |
| `integration/failure_modes/network_timeout_test.rs` | Network timeout handling | Connection timeouts, retries |
| `integration/failure_modes/worker_orphan_test.rs` | Orphaned worker detection | Worker crashes, cleanup |
| `integration/failure_modes/connection_pool_test.rs` | Connection pool exhaustion | Pool limits, deadlocks |

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

## What "Sinex Works" Means

Based on the architecture, a working Sinex system must demonstrate:

1. **Event Capture**: Event sources successfully capture and stream events
2. **Event Storage**: Events are stored immutably in PostgreSQL with proper validation
3. **Event Processing**: Workers process events from the promotion queue
4. **Event Query**: CLI can retrieve and display captured events
5. **End-to-End Flow**: Complete pipeline from capture → storage → processing → query

## Coverage Requirements

### Target Coverage Metrics

- **Core crates** (sinex-core, sinex-db, sinex-worker): ≥80%
- **Overall project coverage**: ≥70%
- **Critical paths**: 100% coverage for event capture → storage → query

### Coverage Commands

```bash
just coverage           # Run tests with coverage
just coverage-html      # Generate HTML coverage report
just coverage-lcov      # Generate LCOV format for CI
just coverage-report    # Open coverage report in browser
```

Coverage reports are generated in `.coverage/` directory (gitignored).

## Adversarial Testing Philosophy

Beyond functional correctness, the test suite includes comprehensive adversarial tests to ensure resilience:

### Resource Exhaustion
- Channel overflow and backpressure (10K channel capacity)
- Connection pool exhaustion (test with pool_size + 10 concurrent ops)
- Memory leak detection over 24-hour runs
- File descriptor leaks from filesystem watchers

### Race Conditions
- ULID generation with clock regression handling
- Worker queue double-claim prevention using `FOR UPDATE SKIP LOCKED`
- Concurrent event updates with lost update detection
- Multi-process ULID collision testing (100 processes × 1000 ULIDs)

### Security Vulnerabilities
- Path traversal prevention (including symlinks and null bytes)
- Command injection protection in shell metacharacter detection
- JSON schema DoS protection (ReDoS patterns, deep nesting)
- ULID injection attempts and timestamp corruption

### Data Corruption Scenarios
- Partial batch failure with complete rollback verification
- Transaction consistency under concurrent modifications
- Lost update prevention in read-modify-write patterns
- Orphaned state detection in promotion queue

## Known Issues and Workarounds

### Common Test Failures

1. **ts_ingest insertion error**: 
   - **Issue**: `ts_ingest` is a GENERATED column from ULID timestamp
   - **Fix**: Use `insert_event()` function instead of raw SQL

2. **Connection pool timeout**:
   - **Issue**: Tests exhaust connection pool
   - **Fix**: Ensure test cleanup in correct FK dependency order

3. **ULID monotonicity violations**:
   - **Issue**: Counter overflow creates non-monotonic ULIDs
   - **Fix**: Handle overflow by incrementing timestamp when counter wraps

4. **Worker claim races**:
   - **Issue**: Multiple workers can claim same queue item
   - **Fix**: Use `SELECT FOR UPDATE SKIP LOCKED` pattern consistently

### Wayland Compositor Issues

When testing window manager event sources in VMs:

1. **greetd configuration conflicts**: Replace with direct Weston service
2. **XDG_RUNTIME_DIR permissions**: Ensure proper ownership and 0700 mode
3. **Service dependency ordering**: Wayland must be ready before dependent services
4. **Headless mode**: Use Weston with virtual output for CI environments

## Red-Team Test Implementation

The test suite includes "red-team" adversarial tests targeting specific known vulnerabilities:

### Critical Bug Categories

1. **ULID Implementation Bugs**:
   - Monotonic counter overflow → all-zero random part
   - Invalid timestamp silently returns current time
   - Multi-process collision without proper entropy

2. **Worker Queue Race Conditions**:
   - TOCTOU race in promotion queue claims
   - Orphaned claims on partial failure
   - Delete without state validation

3. **Resource Leaks**:
   - Filesystem watcher thread accumulation
   - Unbounded clipboard history growth
   - Connection pool with disabled health checks

4. **Security Vulnerabilities**:
   - Command injection via shell splitting
   - Path traversal in git-annex operations
   - Predictable temp file patterns

### Test Implementation Strategy

Each vulnerability test follows this pattern:

```rust
#[tokio::test]
async fn test_specific_vulnerability() {
    // 1. Set up conditions that trigger the bug
    // 2. Execute the vulnerable code path
    // 3. Assert the bug manifests (test FAILS before fix)
    // 4. After fix: test PASSES, confirming resolution
}
```

## Performance Baselines

Tests establish and monitor performance baselines:

- **Event ingestion**: 50K events/second sustained
- **Query latency**: p99 < 100ms for recent events
- **Memory usage**: < 500MB for collector under load
- **Connection pool**: No exhaustion with 2× expected load

Performance regression alerts trigger on >10% degradation.

## Security Considerations

- Tests should not log sensitive information (passwords, keys)
- Use test-specific databases to avoid data corruption
- Clean up all resources after each test run
- Don't commit real credential files or production configs
- Sanitize error messages that might leak system details