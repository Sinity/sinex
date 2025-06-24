# Test Streamlining Baseline

## Overview
This document establishes the baseline metrics for the Sinex test suite before streamlining efforts begin.

**Generated**: 2025-06-22

## Overall Metrics

### Test Suite Size
- **Total test files**: 135
- **Total lines of code**: 40,371
- **Total test functions**: 463

### Test Distribution by Category
- **Unit tests**: 77 test functions
- **Integration tests**: 150 test functions  
- **System tests**: 26 test functions
- **Other categories**: 210 test functions (property, adversarial, validation, etc.)

## Coverage Dimensions

### Event Types Tested
Based on pattern analysis, the following event type prefixes are tested:

| Event Type Prefix | Test Occurrences |
|-------------------|------------------|
| file.*            | 167              |
| test.*            | 49               |
| command.*         | 31               |
| terminal.*        | 25               |
| window.*          | 21               |
| system.*          | 10               |
| user.*            | 6                |
| hyprland.*        | 5                |
| clipboard.*       | 4                |
| error.*           | 1                |

**Total unique event type patterns**: ~10 major categories

### Validation Coverage
- **Files with validation tests**: 120
- **Validation scenarios include**:
  - Schema validation
  - Invalid payloads
  - Malformed data
  - Type mismatches
  - Required field validation

### Error Condition Coverage  
- **Files testing error conditions**: 143
- **Error scenarios include**:
  - Database connection failures
  - Transaction rollbacks
  - Concurrent access conflicts
  - Resource exhaustion
  - Invalid operations

### Concurrency & Timing Coverage
- **Files with timing/concurrency tests**: 118
- **Scenarios covered**:
  - Race conditions
  - Deadlock prevention
  - Timeout handling
  - Parallel event processing
  - Backpressure handling

## Integration Test Analysis

### Current State
- **Total integration test files**: 48
- **Total integration test functions**: 150
- **Files using direct SQL INSERT**: 13
- **Files using sleep patterns**: 20

### Integration Test Categories
1. **Database** (test/integration/database/)
   - Connection pooling
   - Schema validation
   - Work queue operations
   - TimescaleDB features
   - ULID integration
   
2. **Collector** (test/integration/collector/)
   - Basic collection
   - Multi-source coordination
   - Backpressure handling
   - Hot reload
   - Configuration

3. **Event Sources** (test/integration/event_sources/)
   - Terminal events
   - Filesystem events
   - Window manager events
   - Atuin integration

4. **Worker** (test/integration/worker/)
   - Concurrent processing
   - Failure handling
   - TTL management

## Streamlining Priorities

### Phase 1: High-Impact Integration Tests
Priority files for immediate streamlining based on complexity and redundancy:

1. **test/integration/database/** (Highest Priority)
   - database_integration_tests.rs - 52 tests, many SQL INSERTs
   - work_queue_tests.rs - Complex timing logic
   - ulid_integration_tests.rs - Repetitive patterns
   - timescaledb_tests.rs - Database-heavy operations

2. **test/integration/collector/** (High Priority)
   - basic_collector_test.rs - Event generation patterns
   - multi_source_coordination_test.rs - Complex coordination
   - backpressure_test.rs - Timing-sensitive

3. **test/integration/worker/** (Medium Priority)
   - concurrent_processing_test.rs - Concurrency patterns
   - failure_handling_test.rs - Error scenarios

### Streamlining Opportunities

#### Pattern Replacements
1. **SQL INSERT statements** → Event builder helpers
   - Current: 13 files with direct SQL
   - Target: 0 direct SQL statements

2. **Sleep patterns** → Deterministic waits
   - Current: 20 files with sleep
   - Target: <5 files with sleep (only where absolutely necessary)

3. **Boilerplate setup** → Shared test utilities
   - Database setup
   - Event generation
   - Assertion helpers

#### Expected Impact
- **Code reduction**: ~30-40% fewer lines in integration tests
- **Clarity improvement**: Tests focus on behavior, not setup
- **Maintenance reduction**: Single place to update patterns
- **Speed improvement**: Deterministic waits faster than sleeps

## Tracking Metrics

### Before Streamlining
- Integration test files: 48
- Integration test lines: ~12,000
- Direct SQL INSERTs: 13 files
- Sleep patterns: 20 files
- Average lines per test: ~80

### After Streamlining (Target)
- Integration test files: 48 (same)
- Integration test lines: ~7,000 (-40%)
- Direct SQL INSERTs: 0 files
- Sleep patterns: <5 files
- Average lines per test: ~45

## Next Steps

1. Begin with test/integration/database/ directory
2. Apply event builder patterns from test/common/
3. Replace SQL INSERT with helper functions
4. Convert sleep patterns to deterministic waits
5. Track metrics after each file conversion

## Phase 1 Progress

### Files Streamlined (2025-06-22)

#### test/integration/database/database_integration_tests.rs
- **Changes Made**:
  - Replaced direct SQL query with `common::get_events_by_source()` helper
  - Simplified import statements
  - Maintained all test behavior
- **Lines reduced**: ~5 lines
- **SQL INSERTs removed**: 1 (replaced with helper)

#### test/integration/database/ulid_integration_tests.rs
- **Changes Made**:
  - Replaced `insert_test_event_raw` with `assertions::assert_event_inserted`
  - Replaced direct SQL INSERT with event builder pattern
  - Used `get_events_by_source` helper instead of raw SQL query
  - Simplified ULID timestamp test using event builders
- **Lines reduced**: ~20 lines
- **SQL INSERTs removed**: 2

#### test/integration/collector/backpressure_test.rs
- **Changes Made**:
  - Added deterministic wait utilities import
  - Replaced `sleep(Duration::from_secs(3))` with `wait_for_condition_or_timeout`
  - Replaced `sleep(Duration::from_millis(100))` with `wait_for_condition`
  - Made test behavior more predictable by waiting for specific conditions
- **Sleep patterns removed**: 2
- **Test reliability improved**: Now waits for actual conditions rather than fixed time

#### test/integration/query_interface_test.rs
- **Changes Made**:
  - Replaced manual event construction with event builder helpers
  - Replaced SQL INSERT for schema with `upsert_event_schema` helper
  - Replaced SQL INSERT for agent manifest with test utilities
  - Simplified imports
- **Lines reduced**: ~25 lines
- **SQL INSERTs removed**: 2

### Phase 1 Summary
- **Files streamlined**: 4
- **Total lines reduced**: ~50 lines
- **SQL INSERTs removed**: 5
- **Sleep patterns removed**: 2
- **Test clarity**: Significantly improved - tests now focus on behavior
- **Test reliability**: Improved with deterministic waits

### Patterns Established
1. **Event creation**: Use `events::` helpers instead of manual construction
2. **Event insertion**: Use `assertions::assert_event_inserted` instead of direct SQL
3. **Event queries**: Use `common::get_events_by_*` helpers instead of raw SQL
4. **Timing**: Use `wait_for_condition` instead of `sleep`
5. **Schema/manifest**: Use query module helpers instead of SQL INSERT

### Remaining Work
- Continue streamlining remaining integration test files
- Focus on test/integration/database/ directory first
- Apply established patterns consistently
- Track cumulative impact on test suite maintainability