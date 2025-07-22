# Test Suite Restoration Plan

## Current Status
- **Restored**: 65/146 files (45%)
- **Missing**: 81 files (55%)
- **Most Critical Gap**: System tests (5/6 missing)

## Immediate Actions Required

### 1. Restore System Tests (CRITICAL)
These tests validate end-to-end behavior and production readiness.

```bash
# Check if these files exist in git history
git log --all --full-history -- test/system/external_test.rs
git log --all --full-history -- test/system/reliability_test.rs
git log --all --full-history -- test/system/stress_test.rs
git log --all --full-history -- test/system/performance_test.rs
git log --all --full-history -- test/system/regression_test.rs
git log --all --full-history -- test/system/temporal_chaos_test.rs

# Restore from commit before deletion
git checkout bc7e647~1 -- test/system/external_test.rs
git checkout bc7e647~1 -- test/system/reliability_test.rs
git checkout bc7e647~1 -- test/system/stress_test.rs
```

### 2. Restore Core Integration Tests
These test critical subsystems.

```bash
# Database and event processing
git checkout bc7e647~1 -- test/integration/database_test.rs
git checkout bc7e647~1 -- test/integration/process_event_test.rs
git checkout bc7e647~1 -- test/integration/system_integration_test.rs
git checkout bc7e647~1 -- test/integration/event_sources_test.rs

# Satellite and collection
git checkout bc7e647~1 -- test/integration/satellite_architecture_test.rs
git checkout bc7e647~1 -- test/integration/collector_test.rs

# Schema and validation
git checkout bc7e647~1 -- test/integration/schema_validation_test.rs
git checkout bc7e647~1 -- test/integration/import_deduplication_test.rs
```

### 3. Restore Test Utilities
These support the test infrastructure.

```bash
# Core utilities
git checkout bc7e647~1 -- test/common/event_builders.rs
git checkout bc7e647~1 -- test/common/schema_test_utils.rs
git checkout bc7e647~1 -- test/common/worker_test_utils.rs
git checkout bc7e647~1 -- test/common/validation_test_utils.rs
```

## Test Categories Still Missing

### System Tests (5 missing)
- `external_test.rs` - Git Annex, PostgreSQL integration
- `performance_test.rs` - System-wide performance
- `regression_test.rs` - Regression prevention
- `reliability_test.rs` - Fault tolerance
- `stress_test.rs` - Load testing

### Integration Tests (30 missing)
**Core functionality:**
- `database_test.rs`
- `process_event_test.rs`
- `system_integration_test.rs`
- `event_sources_test.rs`
- `satellite_architecture_test.rs`

**Services:**
- `blob_manager_test.rs`
- `collector_test.rs`
- `rpc_handlers_test.rs`
- `scanner_test.rs`

**Preflight system:**
- `preflight_integration_test.rs`
- `preflight_failure_scenarios_test.rs`
- `preflight_rollback_recovery_test.rs`
- `preflight_timeout_performance_test.rs`

**Validation:**
- `schema_validation_test.rs`
- `import_deduplication_test.rs`
- `typed_clipboard_integration_test.rs`
- `version_tracking_integration_test.rs`

**Reliability:**
- `critical_failure_modes_test.rs`
- `edge_case_coverage_test.rs`
- `end_to_end_workflows_test.rs`
- `failure_modes_test.rs`
- `redis_consumer_group_fault_tolerance_test.rs`

### Test Utilities (15 missing)
- Event builders and factories
- Schema and validation utilities
- Performance and snapshot testing
- Scenario DSL framework

## Verification After Restoration

```bash
# Count restored files
find test/ -name "*.rs" -type f | wc -l

# Verify compilation
cargo check --workspace --tests

# Run specific test categories
cargo test --test system
cargo test --test integration
```

## Notes
- Some tests may have been intentionally removed due to obsolescence
- Others may have been consolidated into the new abstraction framework
- Priority should be on tests that provide unique coverage not available elsewhere