# Test Macro Application Report

**Total potential conversions**: 8


### Macro Usage Summary

- `test_batch_events`: 1 uses
- `test_event_insertion`: 1 uses
- `test_concurrent_operations`: 6 uses

## integration/failure_modes_test.rs

- **test_concurrent_operations**: 5 potential conversions
  - Line 120: `initialize`
  - Line 217: `initialize`
  - Line 411: `acquire_connection`
  - ... and 2 more

## integration/preflight_timeout_performance_test.rs

- **test_concurrent_operations**: 1 potential conversions
  - Line 16: `test_database_connectivity_timeout`

## integration/system_integration_test.rs

- **test_event_insertion**: 1 potential conversions
  - Line 679: `initialize`
- **test_batch_events**: 1 potential conversions
  - Line 679: `initialize`