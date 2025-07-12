# Sinex Test Suite Satellite Architecture Transformation Analysis

## Executive Summary

The Sinex test suite contains **70 test files** that require varying degrees of updates for the satellite architecture transformation. My analysis identified **21 files** referencing old unified_collector/work_queue concepts and **28 files** with satellite-related patterns, indicating approximately **60% of the test suite** needs updates.

## Key Architectural Changes Required

### From Unified Collector → Satellite Services
```rust
// OLD: UnifiedCollector coordination
let collector = UnifiedCollector::new(config, output_config, pool, validator);

// NEW: Satellite services coordination  
let ingest_client = IngestClient::new("/run/sinex/ingest.sock").await?;
let satellite = EventSourceRunner::new(event_source, ingest_client);
```

### From Work Queue → Redis + Automaton Checkpoints
```rust
// OLD: work_queue table operations
let items = claim_work_queue_items(pool, "agent", "worker", 1).await?;

// NEW: Redis message queue with automaton checkpoints
let checkpoint = checkpoint_manager.load_checkpoint().await?;
let messages = redis_client.consume_messages("automaton-group").await?;
```

### From EventSource Trait → Satellite SDK
```rust
// OLD: stream_events() method
async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()>

// NEW: Satellite SDK EventSource trait
async fn start_streaming(&mut self) -> SatelliteResult<()>
```

## Test Files by Transformation Complexity

### Category 1: Major Rewrites Required (5-8 days each)

**1. `/realm/project/sinex/test/integration/collector_test.rs` (782 lines)**
- **Current:** Extensively tests `UnifiedCollector`, `EventSource` trait with `stream_events()`
- **Changes:** Complete rewrite to satellite architecture with ingestd/gRPC patterns
- **Key Patterns:** Backpressure tests, hot reload tests, multi-source coordination
- **Impact:** Core collector functionality testing

**2. `/realm/project/sinex/test/integration/system_integration_test.rs`**
- **Current:** Full system startup tests using unified collector patterns  
- **Changes:** Transform to test satellite service coordination
- **Impact:** End-to-end system validation

**3. `/realm/project/sinex/test/integration/worker_test.rs` (1163 lines)**
- **Current:** Tests `work_queue` table and `SELECT FOR UPDATE SKIP LOCKED` patterns
- **Changes:** Replace with Redis message queue and automaton checkpoint patterns
- **Key Patterns:** Work distribution fairness, contention handling, retry mechanisms
- **Impact:** Core work distribution algorithm changes

### Category 2: Significant Changes Required (2-3 days each)

**4. `/realm/project/sinex/test/integration/event_sources_test.rs`**
- **Current:** Partially updated for satellites but has TODOs starting at line 100
- **Changes:** Complete satellite event streaming implementation
- **Status:** Already has new `EventSource` trait usage but incomplete

**5. `/realm/project/sinex/test/unit/database_test.rs`**
- **Current:** Contains `work_queue` operations and `claim_work_queue_items`
- **Changes:** Remove work_queue, add automaton checkpoint testing
- **Impact:** Database operation patterns

**6. `/realm/project/sinex/test/integration/database_test.rs`** 
- **Current:** Similar work_queue testing patterns
- **Changes:** Transform to automaton checkpoints and Redis patterns

**7. `/realm/project/sinex/test/integration/failure_modes_test.rs`**
- **Current:** Tests failure scenarios with unified collector
- **Changes:** Update for satellite failure scenarios and reconnection

**8. NixOS VM Tests (8 files in `/realm/project/sinex/test/nixos-vm/`)**
- **Current:** Reference work_queue and unified collector
- **Changes:** Update for satellite service testing and deployment

### Category 3: Medium Changes Required (1-2 days each)

**9. Property Tests (5 files in `/realm/project/sinex/test/property/`)**
- **Files:** `queue_property_test.rs`, `ulid_property_test.rs`, `event_property_test.rs`
- **Current:** May reference old `EventSource` patterns and work_queue
- **Changes:** Update for new data structures and Redis patterns

**10. System Tests (6 files in `/realm/project/sinex/test/system/`)**
- **Files:** `end_to_end_test.rs`, `stress_test.rs`, `temporal_chaos_test.rs`, etc.
- **Current:** Reference unified collector and work_queue
- **Changes:** Update for satellite architecture performance testing

**11. Unit Tests (6 files in `/realm/project/sinex/test/unit/`)**
- **Files:** `api_test.rs`, `core_test.rs`, etc.
- **Current:** Various old architecture references
- **Changes:** Update imports and patterns as needed

### Category 4: Minor Updates Required (0.5-1 day each)

**12. `/realm/project/sinex/test/integration/satellite_architecture_test.rs`**
- **Current:** Basic satellite tests implemented (273 lines)
- **Changes:** Complete TODO items and add comprehensive coverage
- **Status:** Foundation exists, needs expansion

**13. `/realm/project/sinex/test/integration/satellite_comprehensive_test.rs`**
- **Current:** Has placeholder TODOs for multi-satellite scenarios (670 lines)
- **Changes:** Implement full satellite coordination tests
- **Status:** Structure exists, implementation needed

**14. Test Infrastructure (4 files in `/realm/project/sinex/test/common/`)**
- **Files:** `prelude.rs`, `test_context.rs`, `worker_test_utils.rs`, `database_pool.rs`
- **Current:** Imports both old and new patterns
- **Changes:** Clean up imports, update helper functions

### Category 5: Minimal Changes Required (<0.5 day each)

**15. Remaining Files (35+ files)**
- CLI tests, adversarial tests, external integration tests
- Files not directly tied to core architecture changes
- Simple import updates and minor pattern adjustments

## Parallel Execution Strategy

### Phase 1: Foundation (Parallel - Week 1)
- **Task A:** Update test infrastructure (`prelude.rs`, `test_context.rs`, common utilities)
- **Task B:** Complete satellite test implementations (`satellite_*.rs` files)  
- **Task C:** Update database tests (remove work_queue, add automaton patterns)

### Phase 2: Core Architecture (Parallel - Week 2)
- **Task D:** Rewrite `collector_test.rs` for satellite architecture
- **Task E:** Transform `system_integration_test.rs`
- **Task F:** Update `worker_test.rs` for Redis/automaton patterns

### Phase 3: Event Sources & Integration (Parallel - Week 3)
- **Task G:** Complete `event_sources_test.rs` satellite implementation
- **Task H:** Update property tests for new data structures
- **Task I:** Update system tests for satellite performance patterns

### Phase 4: Validation & Cleanup (Week 4)
- **Task J:** Update NixOS VM tests for satellite services
- **Task K:** Update remaining unit tests and minor files
- **Task L:** Final integration testing and documentation

## Key Transformation Patterns

### Database Schema Changes
- Remove `sinex_schemas.work_queue` table references
- Add `core.automaton_checkpoints` table testing
- Update event routing tests for new schema

### Service Coordination
- Replace unified collector startup with satellite service management
- Add ingestd gRPC server testing
- Implement satellite reconnection and failure handling

### Event Flow Testing
- Transform from direct database writes to gRPC → ingestd → database
- Add Redis message queue testing
- Update event validation to use satellite SDK

## Estimated Total Effort

**Total Engineering Time:** 18-28 days across parallel tracks
- **Major rewrites:** 15-24 days (3 files × 5-8 days)
- **Significant changes:** 16-24 days (8 categories × 2-3 days)
- **Medium changes:** 11-22 days (11 categories × 1-2 days)
- **Minor updates:** 4-8 days (various categories)
- **Integration & validation:** 4-6 days

## Critical Dependencies

1. **Database migration completion** - Automaton checkpoints table must exist
2. **Satellite SDK stability** - Core traits and patterns finalized
3. **ingestd service** - gRPC server implementation complete
4. **Redis integration** - Message queue patterns established
5. **NixOS module updates** - Service configuration completed

## Risk Mitigation

### High Risk Areas
- **Work queue removal** - Extensive references throughout test suite
- **EventSource trait changes** - Fundamental interface transformation
- **Service coordination complexity** - Multi-satellite testing scenarios

### Mitigation Strategies
- Phase implementation to maintain working tests at each step
- Implement new patterns alongside old initially
- Comprehensive integration testing before old pattern removal
- Automated test coverage verification

## Success Criteria

- [ ] All 70 test files updated for satellite architecture
- [ ] No references to `UnifiedCollector` or deprecated `work_queue` patterns
- [ ] Satellite coordination properly tested with realistic scenarios
- [ ] Performance regression tests updated for new architecture
- [ ] NixOS deployment tests validate satellite services
- [ ] Property tests cover new Redis and checkpoint data structures
- [ ] Test execution time remains reasonable (<30 minutes for full suite)

## Current Status (Deferred)

**Decision:** Test suite transformation has been **deferred** pending completion of other refactoring work that would break tests again. The analysis above provides a comprehensive roadmap for when test updates become appropriate.

**Reasoning:** Additional refactoring work in progress would require re-updating tests multiple times. More efficient to complete core architecture changes first, then perform single comprehensive test suite transformation.

**Next Steps:** 
1. Complete remaining satellite architecture work
2. Finalize database schema changes  
3. Stabilize service coordination patterns
4. Return to test suite transformation with stable target architecture

This analysis provides a comprehensive roadmap for transforming the test suite while enabling parallel development work and maintaining system reliability throughout the transition.