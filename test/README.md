# Sinex Test Suite Organization

This directory contains the main integration and system tests for the Sinex project.

## Test Categories

### Database Tests (`database/`)
- `database_integration_tests.rs` - Database connection and basic operations
- `migration_tests.rs` - Database migration verification
- `timescaledb_tests.rs` - TimescaleDB-specific functionality
- `ulid_integration_tests.rs` - ULID primary key implementation
- `jsonschema_validation_tests.rs` - Event payload validation
- `schema_validation_tests.rs` - Schema registry and validation

### Pipeline Tests (`pipeline/`)
- `event_pipeline_integration_tests.rs` - Event ingestion and routing
- `promotion_worker_integration.rs` - Event promotion queue processing
- `worker_concurrency_tests.rs` - Concurrent worker behavior
- `end_to_end_pipeline_test.rs` - Full pipeline integration test
- `real_pipeline_test.rs` - Realistic pipeline scenarios

### Agent Tests (`agents/`)
- `agent_manifest_tests.rs` - Agent registration and manifest management
- `heartbeat_tests.rs` - Agent heartbeat mechanism

### Reliability Tests (`reliability/`)
- `error_handling_tests.rs` - Error recovery and DLQ functionality
- `realistic_failure_tests.rs` - Failure scenario simulation
- `assumption_mismatch_tests.rs` - Handling invalid assumptions

### Ingestor Tests (`ingestor/`)
- `filesystem_tests.rs` - Filesystem ingestor tests
- `integration_tests.rs` - Ingestor integration tests

### Runtime Tests (`runtime/`)
- `runtime_test.rs` - IngestorRuntime tests
- `runtime_tests.rs` - Additional runtime tests
- `event_sink_test.rs` - EventSink implementation tests
- `validation_unit_tests.rs` - Event validation tests

### Property-Based Tests
- `property_tests.rs` - Property-based testing for edge cases

## Running Tests

```bash
# Run all tests
cargo test

# Run specific test file
cargo test --test database_integration_tests

# Run with database (requires TEST_DATABASE_URL)
TEST_DATABASE_URL=postgresql://... cargo test

# Run integration tests only
cargo test --test '*integration*'
```

## Test Utilities

Common test utilities are in `common/mod.rs`.