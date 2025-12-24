# Testing Priorities and Implementation Roadmap

**Document Date:** 2025-10-25  
**Status:** Action Plan for JetStream Migration (phases 1–5)  
**Total Estimated Work:** 8-12 weeks, ~8,000-11,000 LOC

---

## Critical Path: P0 Gaps (Blocks All Work)

These gaps MUST be addressed before Phase 1 can be considered production-ready. They block the event consumer loop and confirmations—the core of the migration.

### Week 1-2: JetStream Consumer Infrastructure

**Goal:** Establish ephemeral NATS test harness and verify events consumer loop.

#### Deliverable 1.1.1: EphemeralNats Test Fixture
- **File:** `crate/lib/sinex-test-utils/src/nats.rs` (enhance existing)
- **Scope:**
  - Extend existing `EphemeralNats` with stream/consumer factories
  - Add helper to await stream message counts
  - Add chaos injection: latency, failures, drops
- **Tests to Enable:**
  - `jetstream_consumer_processes_batches` (400 LOC)
  - `jetstream_consumer_handles_validation_failure` (300 LOC)
  - `jetstream_consumer_survives_database_failure` (350 LOC)
- **Acceptance Criteria:**
  - EphemeralNats starts/stops cleanly
  - JetStream streams created on demand
  - Consumers can pull messages with explicit ACK
  - Timeouts/assertions work reliably

**Effort:** 2 engineer-days

#### Deliverable 1.1.2: Events Consumer Loop Tests
- **File:** `crate/core/sinex-ingestd/tests/events_consumer_integration_test.rs` (new)
- **Tests:**
  1. `jetstream_consumer_processes_batches` - Verify batch insertion via UNNEST
  2. `jetstream_consumer_handles_validation_failure` - Schema validation → DLQ
  3. `jetstream_consumer_survives_database_failure` - Crash recovery
  4. `jetstream_consumer_respects_ack_wait` - AckWait timeout behavior
- **Property Tests:**
  - `prop_consumer_idempotency`: Same message N times → 1 DB insert
  - `prop_batch_ordering`: Published order preserved in DB
  - `prop_offset_monotonic`: Consumer offset never goes backward
- **Acceptance Criteria:**
  - All tests pass with embedded NATS
  - Idempotency verified with property tests
  - No silent data loss detected

**Effort:** 3 engineer-days

---

### Week 2-3: Confirmation and Acknowledgment Flow

**Goal:** Verify end-to-end confirmation from database insert to automaton visibility.

#### Deliverable 1.3.1: Confirmation Publishing Tests
- **File:** `crate/core/sinex-ingestd/tests/confirmation_flow_test.rs` (new)
- **Tests:**
  1. `confirmation_published_after_database_commit` - Outbox → NATS flow
  2. `idempotency_prevents_duplicate_confirmations` - Nats-Msg-Id dedup
  3. `confirmation_message_format_correct` - JSON structure validation
- **Acceptance Criteria:**
  - Event persisted → confirmation published within 100ms
  - Nats-Msg-Id header set and used for dedup
  - Confirmation message contains required fields

**Effort:** 2 engineer-days

#### Deliverable 1.3.2: Automaton Confirmation Consumer Tests
- **File:** `crate/lib/sinex-satellite-sdk/tests/integration/confirmation_consumer_test.rs` (new)
- **Tests:**
  1. `automaton_consumes_confirmation_stream` - Read confirmations
  2. `automaton_crash_recovery_resumes_offset` - Durable consumer
  3. `multiple_automata_no_message_duplication` - Load sharing
- **Acceptance Criteria:**
  - Automaton consumes all confirmations
  - Offset persists across restart
  - Multiple automata don't process same message

**Effort:** 2 engineer-days

---

### Week 3: DLQ Routing Establishment

**Goal:** Ensure all errors route to DLQ, not silent failure.

#### Deliverable 1.4.1: DLQ Consumer Integration Tests
- **File:** `crate/core/sinex-ingestd/tests/dlq_integration_test.rs` (new)
- **Tests:**
  1. `schema_validation_failure_routes_to_dlq` - Invalid JSON/schema
  2. `database_constraint_violation_routes_to_dlq` - Constraint errors
  3. `dlq_consumer_retrieves_messages` - Read and replay
  4. `dlq_respects_retention_policy` - Cleanup old messages
- **Property Tests:**
  - `prop_dlq_preserves_payload`: Event → failure → DLQ → payload intact
  - `prop_unrecoverable_errors_isolated`: 1000 events, 5 bad → correct isolation
- **Acceptance Criteria:**
  - All validation errors route to DLQ
  - Original payload preserved
  - DLQ can be consumed for replay
  - Retention limits respected

**Effort:** 3 engineer-days

---

### Week 4: Idempotency and Restart Resilience

**Goal:** Verify no duplicates on network retry, no data loss on restart.

#### Deliverable 1.5.1: Idempotency and Dedup Tests
- **File:** `crate/core/sinex-ingestd/tests/idempotency_test.rs` (new)
- **Tests:**
  1. `duplicate_message_id_deduplicated` - Same Msg-Id → one insert
  2. `consumer_offset_recovery_after_restart` - Durable consumer
  3. `partial_batch_committed_before_crash` - NACK prevents duplication
- **Acceptance Criteria:**
  - Duplicate publishes → single database entry
  - Consumer resumes from last ACK on restart
  - Partial batch rollback on failure

**Effort:** 2 engineer-days

#### Deliverable 1.6.1: Ingestd Restart Resilience Tests
- **File:** `crate/core/sinex-ingestd/tests/restart_resilience_test.rs` (new)
- **Tests:**
  1. `ingestd_restart_recovers_offset` - Resume consumption
  2. `confirmed_events_survive_restart` - Confirmation stream persists
  3. `outbox_events_reprocessed_on_restart` - Outbox resilience
- **Acceptance Criteria:**
  - Offset recovered from JetStream consumer metadata
  - No duplicate events on restart
  - Confirmed events not lost

**Effort:** 2 engineer-days

---

## Phase 1 Baseline: Single-Source Integration (Week 5-6)

**Goal:** Verify entire path: satellite → NATS → ingestd → DB → confirmation.

#### Deliverable 2.1.1: End-to-End Integration Test
- **File:** `crate/lib/sinex-test-utils/tests/integration/e2e_satellite_to_db_test.rs` (new)
- **Tests:**
  1. `end_to_end_single_satellite_full_flow` - 100 events published → DB
  2. `end_to_end_confirmation_received` - Satellite receives confirmation
  3. `end_to_end_latency_measured` - Publish to DB < 1s
  4. `provenance_chain_valid` - Source tracking correct
- **Property Tests:**
  - `prop_event_ordering_preserved`: Published order = DB order
  - `prop_no_event_loss`: N published → N in DB
- **Acceptance Criteria:**
  - All 100 events persisted
  - Confirmations delivered
  - Latency < 1s P95
  - No duplicates or missing events

**Effort:** 3 engineer-days

---

## Phase 2 Foundation: Material Assembler (Week 6-7)

**Goal:** Verify material slicing, hashing, git-annex, and ledger.

#### Deliverable 1.2.1: Material Assembler Tests
- **File:** `crate/core/sinex-ingestd/tests/material_assembler_test.rs` (new)
- **Tests:**
  1. `material_assembler_slices_into_file` - Slice assembly
  2. `material_assembler_verifies_hash` - Hash validation
  3. `material_assembler_handles_missing_slice` - Timeout/DLQ
  4. `material_assembler_concurrent_materials` - Isolation
  5. `material_assembler_rotation_trigger` - Size-based rotation
  6. `material_ledger_entry_creation` - `raw.temporal_ledger` insertion
- **Property Tests:**
  - `prop_material_hash_invariant`: Random slice order → same hash
  - `prop_ledger_monotonic_offsets`: offset_start < offset_end
  - `prop_concurrent_isolation`: N materials → N independent files
- **Acceptance Criteria:**
  - All slices assembled correctly
  - Hash matches expected
  - Ledger entries created
  - No file corruption on concurrent materials
  - Rotation happens at size boundary

**Effort:** 4 engineer-days

---

## Phase 2 Validation: Automaton Integration (Week 7-8)

**Goal:** Verify automata can consume and process confirmed streams.

#### Deliverable 2.2.1: Automaton Consumption and Processing Tests
- **File:** `crate/lib/sinex-satellite-sdk/tests/integration/automaton_integration_test.rs` (new)
- **Tests:**
  1. `automaton_full_pipeline` - Consume → validate → transform → store
  2. `automaton_crash_recovery` - Durable consumer restart
  3. `multiple_automata_load_sharing` - Concurrent automata
  4. `automaton_backpressure_handling` - Fall-behind scenarios
- **Acceptance Criteria:**
  - Automata consume all confirmed events
  - Processing completes in-order
  - Crash recovery works without reprocessing
  - Multiple automata distribute load

**Effort:** 3 engineer-days

---

## Stability and Performance (Week 8-10)

### Week 8: Error Path Hardening

**Goal:** Ensure all error scenarios route to DLQ gracefully.

#### Deliverable 3.1: Error Path Coverage (2000+ LOC)
- **Files:**
  - `crate/core/sinex-ingestd/tests/error_scenarios_test.rs` (new)
  - `crate/lib/sinex-test-utils/src/chaos.rs` (new module)
- **Tests by Category:**

  **Database Errors (400 LOC):**
  - Constraint violations (duplicate event_id, FK violations)
  - Transaction rollback scenarios
  - Connection pool exhaustion
  - OOM simulation

  **Schema Validation Errors (300 LOC):**
  - Invalid JSON payload
  - Type mismatches
  - Missing required fields
  - Payload size limits

  **Provenance Errors (200 LOC):**
  - XOR constraint violations
  - Invalid ULID references
  - Missing provenance fields

  **NATS/Network Errors (300 LOC):**
  - NATS unavailability at startup
  - Connection drops mid-batch
  - Publish failures (outbox resilience)
  - Network timeouts

  **Corruption/Abuse (200 LOC):**
  - Truncated payloads
  - Corrupted messages
  - Oversized payloads
  - Malicious inputs (XSS, injection patterns)

- **Acceptance Criteria:**
  - All error types reach DLQ with context
  - Consumer doesn't crash or hang
  - Human-readable error messages in DLQ
  - Metrics track error counts by type

**Effort:** 4 engineer-days

### Week 9-10: Performance Optimization and Load Testing

#### Deliverable 4.1: High-Throughput Load Tests (1200 LOC)
- **File:** `crate/lib/sinex-core/tests/performance/load_test.rs` (enhanced)
- **Benchmarks:**
  1. Sustained throughput: 5K evt/sec for 60s (measure latency percentiles, memory, CPU)
  2. Batch size optimization: 10-1000 (find optimal for target latency)
  3. Large batch handling: 1000 events per batch
  4. Material streaming: 1000 slices/60s (memory stability)
  5. Concurrent satellites: 5 sources × 1K events (ordering within source)
- **Acceptance Criteria:**
  - P50 latency < 100ms, P99 < 500ms
  - Memory growth sub-linear (< 500MB peak for 5K evt)
  - CPU stays < 80% during normal load
  - No GC pauses > 100ms

**Effort:** 3 engineer-days

---

## Security and Chaos Testing (Week 10-11)

#### Deliverable 5: Chaos and Security Tests (1500 LOC)
- **File:** `crate/lib/sinex-core/tests/adversarial/jetstream_chaos_test.rs` (new)
- **Scenarios:**
  1. Network partition (ingestd ↔ NATS split)
  2. Service crash during transaction
  3. Corrupted NATS messages
  4. Malicious payloads (XSS, oversized, unicode attacks)
  5. ULID collision detection
  6. Time-based attacks (future/past timestamps)
  7. Cascading failures (DB down, then NATS down)
- **Acceptance Criteria:**
  - System survives all chaos scenarios
  - No data loss or duplication
  - Graceful degradation (not panic)
  - Operator can diagnose root cause from logs/metrics

**Effort:** 3 engineer-days

---

## Migration and Upgrade Testing (Week 11-12)

#### Deliverable 6: Migration and Compatibility Tests (1000 LOC)
- **File:** `crate/lib/sinex-schema/tests/migration_test.rs` (new)
- **Tests:**
  1. Schema forward migration with existing data
  2. Schema migration rollback safety
  3. Backwards compatibility (old satellite + new ingestd)
  4. Forwards compatibility (new satellite + old ingestd)
  5. Dual-path operation (gRPC + JetStream simultaneous)
  6. sensd removal without data loss
  7. SQLX cache regeneration after schema changes
- **Acceptance Criteria:**
  - All data preserved during migration
  - Rollback returns to previous state
  - Version mismatches handled gracefully
  - Dual-path tested before removing gRPC

**Effort:** 3 engineer-days

---

## Summary: Implementation Schedule

```
Week 1-2: Consumer + Idempotency Infrastructure
  ├─ EphemeralNats enhancements (test fixture)
  ├─ Events consumer tests (400-500 LOC)
  ├─ Property tests for idempotency
  └─ Acceptance: Consumer loop works, idempotency verified

Week 2-3: Confirmations and Acknowledgments
  ├─ Confirmation publishing tests (300-400 LOC)
  ├─ Automaton confirmation consumer tests (300-400 LOC)
  └─ Acceptance: Confirmations reliable, automata ready

Week 3: DLQ Establishment
  ├─ DLQ consumer tests (400-500 LOC)
  ├─ Error-to-DLQ routing tests
  └─ Acceptance: All errors reach DLQ

Week 4: Restart Resilience
  ├─ Idempotency verification (300-400 LOC)
  ├─ Ingestd restart tests (300-400 LOC)
  └─ Acceptance: No duplicates, no data loss on restart

Week 5-6: Phase 1 Baseline (E2E)
  ├─ Test satellite publisher fixture
  ├─ End-to-end satellite→DB tests (500-700 LOC)
  └─ Acceptance: Full pipeline working, latency measured

Week 6-7: Material Assembler (Phase 3 Foundation)
  ├─ Material assembler tests (600-900 LOC)
  ├─ Ledger and git-annex tests
  ├─ Property tests for slice ordering, hash invariance
  └─ Acceptance: Materials assembled correctly, no corruption

Week 7-8: Automaton Processing
  ├─ Automaton integration tests (400-500 LOC)
  ├─ Load sharing and crash recovery
  └─ Acceptance: Automata reliably process streams

Week 8: Error Path Hardening
  ├─ Database error scenarios (400 LOC)
  ├─ Schema validation errors (300 LOC)
  ├─ Provenance errors (200 LOC)
  ├─ Network error scenarios (300 LOC)
  ├─ Chaos injection utilities
  └─ Acceptance: All error paths safe, DLQ functioning

Week 9-10: Performance and Load
  ├─ High-throughput benchmarks (800 LOC)
  ├─ Batch size optimization
  ├─ Memory profiling
  └─ Acceptance: 5K evt/sec sustained, latency < 500ms P99

Week 10-11: Chaos and Security
  ├─ Network partition chaos (300 LOC)
  ├─ Service crash scenarios (300 LOC)
  ├─ Corrupted message handling (200 LOC)
  ├─ Malicious payload tests (300 LOC)
  ├─ Time-based attack tests (150 LOC)
  └─ Acceptance: System survives chaos, no silent failures

Week 11-12: Migration and Compatibility
  ├─ Schema migration tests (400 LOC)
  ├─ Backwards/forwards compatibility (300 LOC)
  ├─ Dual-path operation (300 LOC)
  ├─ sensd removal validation (200 LOC)
  └─ Acceptance: Safe upgrade path established

Total: ~8,000-11,000 LOC, 12 weeks, 1 FTE
```

---

## Acceptance Criteria Per Phase

### Phase 1: Ready (week 6)
- [ ] Consumer loop processes events (100/100 persisted)
- [ ] Idempotency verified (duplicates deduplicated)
- [ ] Confirmations published reliably
- [ ] DLQ functional for all error types
- [ ] Restart recovery verified (no duplicates)
- [ ] E2E latency < 1s (P95)
- [ ] Load test: 1K evt/sec sustained

### Phase 2: Ready (week 8)
- [ ] Automata consume confirmation stream
- [ ] Crash recovery works without reprocessing
- [ ] Multiple automata load-share correctly
- [ ] Connection pool exhaustion handled

### Phase 3: Ready (week 7)
- [ ] Materials assemble correctly from slices
- [ ] Hashes verified (no corruption)
- [ ] Ledger entries created correctly
- [ ] Concurrent materials isolated
- [ ] Rotation triggers at size boundary

### Phase 4: Ready (week 12)
- [ ] Lease coordination tested
- [ ] Leader election under load verified
- [ ] Control plane (replay/preflight) working

### Phase 5: Ready (week 12)
- [ ] gRPC path removed safely
- [ ] sensd removed safely
- [ ] No data loss during removal
- [ ] All satellites using JetStream
- [ ] Backwards compatibility ensured during cutover

---

## Success Metrics

| Metric | Target | Success Criteria |
|--------|--------|-----------------|
| Test Coverage | 8,000+ LOC | >50 integration tests, 10+ property tests |
| P0 Gap Closure | 100% | All 6 P0 gaps have passing tests |
| Event Idempotency | 100% | 1M duplicates → 1 DB entry |
| Confirmation Reliability | 100% | Event persisted → confirmation guaranteed |
| Error Coverage | 20+ paths | All error scenarios route to DLQ |
| E2E Latency | P99 < 500ms | From publish to DB < 1s |
| Throughput | 5K evt/sec | Sustained for 60s without degradation |
| Memory Stability | Sub-linear | Peak < 500MB growth for 5K events |
| Crash Recovery | 100% | No duplicates or loss after restart |
| Production Ready | Green | All phase criteria met |

---

## Implementation Notes

### Setup Required
1. Enhance `EphemeralNats` in `sinex-test-utils` with stream/consumer factories
2. Create `TestSatellitePublisher` for E2E tests
3. Create `ChaosInjestor` in test utils for failure injection
4. Add `TestSnapshot` observation utility
5. Extend existing `error_testing.rs` utilities

### Test Infrastructure Patterns
- Use `#[sinex_test]` macro (database pool auto-managed)
- Embed NATS via `EphemeralNats::start()` for all JetStream tests
- Use `TestContext` for database access
- Leverage existing `sinex_test_utils` builders and factories
- Property tests via `proptest` crate (already in workspace)

### CI/CD Integration
- Add GitHub Actions job to run full test suite with embedded NATS
- Segregate long-running tests (performance, chaos) in separate workflow
- Cache compiled test binaries
- Generate HTML coverage reports

### Documentation
- Update README with new test organization
- Add testing best practices guide
- Document each gap and its test solution
- Add performance benchmark baseline

---

## Risk Mitigation

### Risk: Tests Take Too Long
**Mitigation:**
- Use `#[sinex_bench]` for long-running tests (separated from unit/integration)
- Parallelization via `nextest`
- Use smaller datasets where possible (100 events instead of 10K)

### Risk: Flaky Tests (Network/Timing)
**Mitigation:**
- Embed NATS (no external dependency)
- Use deterministic timeouts (tokio::time for control)
- Retry transient failures (3x on network errors)

### Risk: Test Maintenance Burden
**Mitigation:**
- Reuse test fixtures/builders extensively
- Document patterns in comments
- One test per logical scenario (not mega-tests)
- Regular review of test organization

---

## References

- **crate/lib/sinex-test-utils**: Existing test infrastructure
- **Acceptance Criteria**: See “Acceptance Criteria Per Phase” above
