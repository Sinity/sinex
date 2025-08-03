# Test Streamlining Plan for sinex-test-utils

## Current State
- 195 tests total across 21 files
- Significant duplication and overlap
- Tests hanging - likely due to database pool exhaustion

## Consolidation Strategy

### 1. test_context.rs (20 tests → 8 tests)
Remove duplicates:
- `test_contexts_are_isolated` + `test_context_provides_isolation` → Keep one
- `test_context_tracks_event_count` → Remove (covered by other tests)
- `test_context_timing_measurement` → Remove (covered by test_timing_utilities)
- `test_assertion_helpers` → Merge with `test_assertion_api`
- `test_query_builder_chaining` + `test_query_builder_flexibility` → Merge into `test_query_builder_chains`

### 2. lib.rs (24 tests → 6 tests)
Core integration tests only:
- Keep: `test_complete_workflow`, `test_error_propagation`, `test_timeout_handling`
- Remove: Most parameterized tests (covered by property testing)
- Remove: Duplicate builder tests (covered in test_context.rs)
- Remove: Database pool tests (covered in database_pool.rs)

### 3. fixtures.rs (22 tests → 8 tests)
Consolidate fixture categories:
- Merge all "standard fixture" tests into one comprehensive test
- Merge transaction fixture tests
- Keep unique: caching, dependency, lazy loading tests

### 4. coverage_assurance.rs (17 tests → 5 tests)
- Merge similar edge case tests
- Combine error injection tests
- Keep: comprehensive scenario tests

### 5. timing_utils.rs (16 tests → 6 tests)
- Merge barrier tests
- Combine phase tracking tests
- Keep: unique coordination patterns

### 6. deployment_scenario_utils.rs (15 tests → 5 tests)
- Merge migration tests
- Combine compatibility tests
- Keep: unique deployment scenarios

### 7. satellite_management_utils.rs (14 tests → 5 tests)
- Merge lifecycle tests
- Combine health check tests
- Keep: unique satellite patterns

### 8. property_testing.rs (12 tests → 4 tests)
- Keep: core property testing infrastructure
- Remove: redundant edge case tests (covered by actual property tests)

### 9. error_testing.rs (11 tests → 4 tests)
- Merge error category tests
- Keep: unique error patterns

### 10. channel_behavior_utils.rs (11 tests → 4 tests)
- Merge backpressure tests
- Combine ordering tests

## Implementation Approach

1. **Merge Pattern**: Combine related tests using parameterized test patterns
2. **Coverage Focus**: Ensure each major feature has ONE comprehensive test
3. **Remove Redundancy**: Delete tests that are subsets of other tests
4. **Fast Feedback**: Prioritize tests that run quickly

## Expected Outcome
- ~60-70 total tests (65% reduction)
- Faster test execution
- Better maintainability
- No loss of coverage