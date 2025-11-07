# SINEX TEST FILE INVENTORY & PATTERNS

## COMPLETE TEST FILE LISTING

### sinex-core Tests (93 files, 38,026 LOC)

#### Unit Tests (5 files)
- `unit/database_test.rs` - Database operations, transaction handling
- `unit/event_type_system_test.rs` - Event type definitions and validation
- `unit/schema_validator_test.rs` - JSON schema validation
- `unit/version_system_test.rs` - Version tracking and management
- `unit/preflight_test.rs` - Pre-execution validation checks

#### Integration Tests (17 files, ~6,000 LOC)
- `integration/checkpoint_consistency_test.rs` - Checkpoint recovery, state management
- `integration/distributed_locking_test.rs` - Concurrent write coordination (SELECT FOR UPDATE)
- `integration/event_ordering_test.rs` - ULID temporal ordering guarantees
- `integration/ingest_service_test.rs` - Event ingestion pipeline
- `integration/pipeline_integration_test.rs` - Full event flow validation
- `integration/provenance_test.rs` - Event lineage tracking (source_event_ids)
- `integration/resource_management_test.rs` - Resource allocation and cleanup
- `integration/schema_integration_test.rs` - Schema contract validation
- `integration/single_writer_enforcement_test.rs` - Phase 1.2 pattern (ingestd as single writer)
- `integration/state_management_test.rs` - State persistence and recovery
- `integration/subscription_service_test.rs` - Event subscription patterns
- `integration/test_automation_integration_test.rs` - Automaton coordination
- `integration/timestamp_test.rs` - Timestamp handling across DST, timezones
- `integration/type_safety_test.rs` - Type system correctness
- `integration/validation_cache_test.rs` - Schema validation caching
- `integration/work_queue_test.rs` - Batch event processing
- `integration/mod.rs` - Module organization

#### Property Tests (8 files, ~3,500 LOC)
- `property/event_model_fuzzing_test.rs` - 1000+ fuzzing cases: problematic strings, numbers
- `property/event_validation_property_test.rs` - Validation invariant checking
- `property/schema_property_test.rs` - Schema compliance fuzzing
- `property/ulid_property_test.rs` - ULID generation, ordering, uniqueness
- `property/time_range_property_test.rs` - Time range edge cases
- `property/path_sanitization_property_test.rs` - Path validation across edge cases
- `property/validation_roundtrip_property_test.rs` - Serialization roundtrip invariants
- Event property and property_tests.rs - Additional property test organization

#### Adversarial Tests (8 files, ~2,000 LOC)
- `adversarial/attack_simulation_test.rs` - DST transitions, clock regression, ULID timing attacks
- `adversarial/boundary_test.rs` - Edge value validation
- `adversarial/chaos_engineering_test.rs` - System disruption scenarios
- `adversarial/concurrency_test.rs` - Race condition simulation (100+ concurrent writes)
- `adversarial/enhanced_boundary_test.rs` - Extended boundary testing
- `adversarial/security_test.rs` - SQL injection, config attacks
- `adversarial/ulid_edge_cases_test.rs` - ULID collision attempts, extreme dates
- `adversarial/mod.rs` - Module organization

#### Performance Tests (10 files, ~2,500 LOC)
- `performance/concurrent_load_test.rs` - 1000+ concurrent event insertions
- `performance/database_performance_test.rs` - Query performance benchmarks
- `performance/jetstream_performance_test.rs` - NATS JetStream throughput
- `performance/checkpoint_performance_test.rs` - Checkpoint save/restore speed
- `performance/large_payload_test.rs` - 10MB+ event payloads
- `performance/memory_usage_test.rs` - Memory footprint analysis
- `performance/resource_exhaustion_test.rs` - Out-of-memory, disk full scenarios
- `performance/regression_detection_test.rs` - Performance regression tracking
- `performance/bottleneck_identification_test.rs` - Latency hotspot detection
- `performance/mod.rs` - Module organization

#### Security Tests (3 files, ~800 LOC)
- `security/unicode_attack_test.rs` - Unicode normalization, homoglyphs
- `security/secure_path_validation_test.rs` - Path traversal, symlink attacks
- `security/mod.rs` - Organization

#### System Tests (4 files)
- `system/performance_test.rs` - System-level performance
- `system/reliability_test.rs` - Error recovery patterns
- `system/stress_test.rs` - High-load scenarios
- `system/temporal_chaos_test.rs` - Time-based chaos

#### Test Helpers (4 files)
- `events_test_helpers.rs` - Event creation utilities
- `unit_tests.rs` - Unit test organization
- `integration_tests.rs` - Integration test organization  
- `simple_tests.rs` - Basic sanity tests

#### Supporting Files (~15 files)
- `database.rs`, `coordination.rs`, `directory_manager.rs`
- `distributed_locking.rs`, `domain.rs`, `dry_run.rs`
- `environment.rs`, `error.rs`, `event_model.rs`
- `event_property.rs`, `events_blanket_impls.rs`, `file_watcher.rs`
- `json_helpers.rs`, `non_empty.rs`, `payloads_clipboard.rs`
- `payloads_process.rs`, `query_helpers.rs`, `repositories_*.rs`
- `resource_guard.rs`, `sanitization.rs`, `seaquery_helpers.rs`
- `sqlite_helpers.rs`, `timestamp_helpers.rs`, `validation_*.rs`

---

### sinex-satellite-sdk Tests (33 files, 7,785 LOC)

#### Unit Tests (11 files)
- `annex_blob_manager.rs` - Blob storage operations
- `annex_mod.rs` - Module interface validation
- `annex_path_validator.rs` - Path sanitization
- `error_helpers.rs` - Error construction and matching
- `config_loading_tests.rs` - Configuration parsing
- `heartbeat.rs` - Satellite heartbeat signals
- `ingestion_helpers.rs` - Event ingestion utilities
- `processor_runner.rs` - Stream processor execution
- `replay_control.rs` - Replay mode operations
- `sensor_guard.rs` - Sensor lifecycle management
- `version.rs` - Version management

#### Integration Tests (7 files, ~3,000 LOC)
- `integration/satellite_architecture_test.rs` - Phase 1 unified trait
- `integration/satellite_lifecycle_test.rs` - Start/stop/pause operations
- `integration/satellite_coordination_test.rs` - Multi-satellite coordination
- `integration/checkpoint_persistence_test.rs` - State recovery from DB
- `integration/critical_failure_modes_test.rs` - Error scenarios
- `integration/blob_manager_test.rs` - Blob persistence
- `integration/version_migration_test.rs` - Version compatibility
- `integration/stage_as_you_go_integration_test.rs` - Incremental staging
- `integration/config_environment_validation_test.rs` - Config from env
- `integration/event_generation_test.rs` - Event creation patterns

#### Property Tests (5 files, ~1,500 LOC)
- `property/automation_property_test.rs` - Automaton invariants
- `property/checkpoint_property_test.rs` - Checkpoint state fuzzing
- `property/satellite_property_test.rs` - Satellite lifecycle fuzzing
- `property/error_handling_property_test.rs` - Error path coverage
- `property/queue_property_test.rs` - Queue behavior under load
- `property/validation_invariants_property_test.rs` - Validation rules
- `property_checkpoint_state.rs` - Checkpoint state properties

#### Security Tests (2 files)
- `security/path_validation_test.rs` - Path traversal attacks
- `security/mod.rs`

#### System Tests (1 file)
- `system/external_test.rs` - External process integration

#### Supporting Files
- `property_checkpoint_state.proptest-regressions` - Proptest regression data

---

### sinex-test-utils Tests (16 files, ~1,500 LOC)

#### Core Infrastructure Tests
- `basic_enhanced_test.rs` - TestContext enhancement validation
- `channel_enhancements_tests.rs` - Async channel utilities
- `database_pool_tests.rs` - 64-slot pool isolation verification
- `deployment_scenario_utils_tests.rs` - Multi-service deployment
- `fully_integrated_test.rs` - End-to-end test infrastructure
- `test_context_tests.rs` - TestContext functionality
- `modern_test_infrastructure_example.rs` - Usage examples
- `modern_test_example.rs` - Simple usage patterns
- `rstest_integration_example.rs` - Parametrized test patterns
- `streamlined_api_demo.rs` - API demonstration
- `macro_conversion_examples.rs` - Macro usage examples

#### Integration Tests (4 files)
- `integration/multi_source_integration_test.rs` - Multiple event sources
- `integration/collector_test.rs` - Event collection
- `integration/configuration_test.rs` - Configuration handling
- `integration/event_processing_integration_test.rs` - Event pipeline
- `integration/stream_processing_test.rs` - Stream processing patterns

---

### sinex-schema Tests (7 files, 2,829 LOC)

- `serde_tests.rs` - Serialization/deserialization
- `ulid_conversions_tests.rs` - ULID to/from conversions
- `ulid_property_from_main_tests.rs` - Property-based ULID tests
- `ulid_property_tests.rs` - Additional ULID properties
- `ulid_tests.rs` - Core ULID functionality
- `validation_tests.rs` - Schema validation
- `schema_tests.rs` - Schema structure tests

---

### sinex-services Tests (3 files, ~300 LOC)

- `analytics_service_test.rs` - Analytics service API
- `query_service_test.rs` - Query interface
- `service_integration_test.rs` - Service coordination

---

### Core Services Tests

#### sinex-ingestd (5 files, 902 LOC)
- `config_loading_tests.rs` - Configuration loading
- `schema_sync_tests.rs` - Schema synchronization
- `service_outbox_tests.rs` - Event outbox pattern
- `config_security_tests.rs` - Secure configuration
- `ingestd_grpc_test.rs` - gRPC service (Phase 1.2)

#### sinex-gateway (3 files, 531 LOC)
- `cascade_analyzer_tests.rs` - Event cascade analysis
- `replay_state_machine_tests.rs` - State replay logic
- `service_container_test.rs` - DI container initialization

#### sinex-sensd (3 files, ~150 LOC)
- `integration_test.rs` - Basic integration (273 LOC in src/)
- `tree_watch_tests.rs` - File tree watching
- `config_security_tests.rs` - Config validation

#### sinex-macros (1 file)
- `basic_satellite_test.rs` - Procedural macro validation

---

### Satellite Tests (7 files, 892 LOC)

#### fs-watcher (3 files, 510 LOC)
- `config_validation_tests.rs` - Configuration validation
- `unified_processor_tests.rs` - Stream processor impl
- `security/fs_watcher_security_test.rs` - Path security

#### terminal-satellite (3 files, 349 LOC)
- `config_validation_tests.rs` - Configuration validation
- `shell_detection_tests.rs` - Shell type detection
- `security/history_watcher_security_test.rs` - History file security

#### system-satellite (1 file, 33 LOC)
- `systemd_integration_tests.rs` - Systemd integration

#### UNTESTED SATELLITES (9 services)
- sinex-analytics-automaton - .gitkeep only
- sinex-content-automaton - .gitkeep only
- sinex-desktop-satellite - .gitkeep only
- sinex-document-ingestor - .gitkeep only
- sinex-health-aggregator - .gitkeep only
- sinex-pkm-automaton - .gitkeep only
- sinex-search-automaton - .gitkeep only
- sinex-terminal-command-canonicalizer - .gitkeep only
- sinex-rpc-dispatcher - No test directory

---

### E2E Tests (5 files, 1,290 LOC)

- `nix_module_integration_test.rs` (1,159 LOC) - NixOS module deployment
- `pipeline_end_to_end.rs` (82 LOC) - Event pipeline validation
- `cli_smoke_test.rs` (31 LOC) - CLI tool functionality
- `schema_compatibility_test.rs` (15 LOC) - Schema compatibility
- `e2e/src/lib.rs` (3 LOC) - Module organization

---

## TEST PATTERNS AND ANNOTATIONS

### Test Decorators

**#[sinex_test]** - Primary decorator, 0 other test annotations in codebase
- Provides TestContext injection
- Handles async/await
- Sets up tracing automatically
- Manages database isolation
- Cleans up after test completes

**#[rstest]** - Parametrized tests (12 uses)
- Combined with #[sinex_test]
- Used for matrix testing
- Example: multiple event types × payload variants

**#[traced_test]** - Log capture (8+ uses)
- Captures tracing output
- Enables log assertions
- Works with ctx.assert_logged()

**#[ignore]** - Conditional tests
- `#[ignore = "requires DATABASE_URL and SINEX_INGEST_SOCKET"]`
- Used for gateway, sensd live tests

---

## ASSERTION PATTERNS

### Custom Context Assertions
```rust
ctx.assert("operation")
    .eq(&actual, &expected)?
    .that(condition, "message")?
    .has_size(&collection, 5)?
    .some(&option)?
    .none(&option)?
    .not_empty(&vec)?
```

### Snapshot Assertions
```rust
insta::assert_json_snapshot!(value);
insta::assert_yaml_snapshot!(value);
insta::assert_debug_snapshot!(value);
```

### Error Matching
```rust
assert!(result.is_err());
assert_eq!(result.unwrap_err().kind(), ExpectedErrorKind);
```

### Standard Assertions (Still Used)
```rust
assert!(condition);
assert_eq!(actual, expected);
assert_ne!(left, right);
```

---

## TEST DATA PATTERNS

### Event Creation
```rust
// Production API directly used in tests
Event::<JsonValue>::test_event(source, event_type, json!(payload))

// Via TestContext helper
ctx.create_test_event(source, event_type, json!(payload)).await?
```

### Fixture Generation
- `TestContext::new()` - Creates isolated database
- `TestContext::with_name(name)` - Named for debugging
- Database automatically cleaned up on drop

### Proptest Strategies
- `problematic_strings()` - Unicode, control chars, SQL injection
- `edge_case_numbers()` - i64::MIN/MAX, overflow
- `event_sources()`, `event_types()` - Domain value generation
- `problematic_json()` - Malformed but parseable JSON

---

## ORGANIZATION & METRICS

### File Count by Test Type
- Integration: 33 files (largest category)
- Unit: ~30 files
- Property: 8 files
- Adversarial: 8 files  
- Performance: 10 files
- Security: 3 files
- E2E: 5 files

### Lines of Code Distribution
- sinex-core: 38,026 (63% of total)
- sinex-satellite-sdk: 7,785 (13% of total)
- sinex-schema: 2,829 (5% of total)
- All others: ~11,000 (19% of total)

### Test-to-Source Ratio
- sinex-core: ~2.5:1
- sinex-satellite-sdk: ~1.8:1
- sinex-test-utils: ~2:1 (testing is test infrastructure)

### Database Pool Architecture
- **Slots**: 64 isolated PostgreSQL databases
- **Allocation**: On-demand per test
- **Cleanup**: Automatic on TestContext drop
- **Concurrency**: Supports 64 parallel tests
- **Performance**: <10ms overhead per test

### Proptest Coverage
- **119 proptest! invocations** across codebase
- **1000+ cases per property test**
- Strategies cover: strings, numbers, paths, events, checkpoints
- Regression tracking via proptest-regressions files

---

## CRITICAL GAPS BY SERVICE

### No Tests (9 Services)
1. sinex-analytics-automaton (0 LOC tests, ~400 LOC source)
2. sinex-content-automaton (0 LOC tests, ~300 LOC source)
3. sinex-desktop-satellite (0 LOC tests, ~200 LOC source)
4. sinex-document-ingestor (0 LOC tests, ~250 LOC source)
5. sinex-health-aggregator (0 LOC tests, ~300 LOC source)
6. sinex-pkm-automaton (0 LOC tests, ~350 LOC source)
7. sinex-search-automaton (0 LOC tests, ~400 LOC source)
8. sinex-terminal-command-canonicalizer (0 LOC tests, ~200 LOC source)
9. sinex-rpc-dispatcher (0 LOC tests, 229 LOC source) - No test directory

### Inadequate Tests
- sinex-ingestd: 902 LOC tests for 2,400 LOC source (37% coverage)
- sinex-gateway: 531 LOC tests for 400+ LOC source (API untested)
- sinex-sensd: ~150 LOC tests for 2,800+ LOC source (5% coverage)
