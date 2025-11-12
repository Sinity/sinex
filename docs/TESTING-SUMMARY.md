# Testing Gap Analysis: Executive Summary

**Date:** October 25, 2025  
**Analysis Scope:** JetStream Migration Testing (way.md Phases 1-5)  
**Status:** Complete - 2 comprehensive reports delivered

---

## Quick Facts

- **Current Test Code:** ~6,344 lines
- **Identified Gaps:** 26 gaps across 6 categories
- **Critical (P0) Gaps:** 6 (blocks all production work)
- **High Priority (P1) Gaps:** 12 (required for stability)
- **Medium Priority (P2) Gaps:** 8 (needed for completeness)
- **Estimated New Test Code:** 8,000-11,000 lines
- **Estimated Timeline:** 12 weeks at 1 FTE
- **Total Integration Tests Needed:** 50+
- **Total Property Tests Needed:** 10+

---

## Critical Findings

### Most Urgent (Must Fix Before Phase 1 Ship)

1. **Events Consumer Loop (P0)** - NO TESTS
   - JetStream consumer pulls from `events.raw.*`
   - Batch inserts via UNNEST
   - Explicit ACK after commit
   - Current: Only outbox → NATS tested (marked `#[ignore]`)
   - Impact: Silent data loss risk, no idempotency verification
   - Fix Effort: 2-3 weeks

2. **Confirmation/ACK Flow (P0)** - PARTIAL TESTS
   - Database commit → confirmation publishing
   - Automaton consumption of confirmations
   - Idempotency via Nats-Msg-Id headers
   - Current: Outbox processor exists but no consumer test
   - Impact: Automata won't know when to process events
   - Fix Effort: 2 weeks

3. **DLQ Routing (P0)** - NO TESTS
   - All errors must reach DLQ, not disappear
   - Current: temp file write in event_processor, not NATS DLQ
   - Impact: Operations blind to failures, no recovery path
   - Fix Effort: 2-3 weeks

4. **Material Assembler (P0)** - NO TESTS
   - Slice assembly, hashing, git-annex placement
   - Ledger entry creation
   - Current: E2E SDK test only, not ingestd assembler
   - Impact: Phase 3 blocked, silent corruption risk
   - Fix Effort: 3-4 weeks

5. **Restart Resilience (P0)** - PARTIAL TESTS
   - Consumer offset recovery (durable consumer)
   - No duplicate processing on restart
   - Current: VM tests only, no JetStream-specific tests
   - Impact: Data loss or duplication on ingestd restart
   - Fix Effort: 2 weeks

6. **Satellite → DB E2E (P0)** - PARTIAL TESTS
   - True end-to-end from satellite publish to DB insert
   - Multiple concurrent satellites
   - Confirmation flow back to satellite
   - Current: stage_as_you_go_integration test, but not raw event publishing
   - Impact: Production bugs undiscovered until live
   - Fix Effort: 2-3 weeks

---

## Category Breakdown

### 1. JetStream-Specific Gaps (Critical for Migration)

| Gap | Severity | Status | Fix Time | Impact |
|-----|----------|--------|----------|--------|
| Events Consumer | P0 | NO TESTS | 2-3w | Core ingestion path |
| Material Assembler | P0 | NO TESTS | 3-4w | Phase 3 blocker |
| Confirmations | P0 | PARTIAL | 2w | Automata readiness |
| DLQ Routing | P0 | NO TESTS | 2-3w | Error isolation |
| Idempotency (Msg-Id) | P0 | NO TESTS | 2w | Exactly-once guarantee |
| Stream Replay | P0 | PARTIAL | 2w | Crash resilience |

**Subtotal:** 6 weeks of focused work on critical path

### 2. Component Integration Gaps

| Gap | Severity | Status | Fix Time | Impact |
|-----|----------|--------|----------|--------|
| E2E Satellite→DB | P0 | PARTIAL | 2-3w | Production validation |
| Automaton Consumption | P1 | SKETCHED | 2w | Phase 2 |
| Connection Pool Exhaustion | P1 | INCOMPLETE | 1w | Stability |

**Subtotal:** 1-2 weeks for integration completeness

### 3. Error Path Testing Gaps

| Gap | Severity | Status | Fix Time | Impact |
|-----|----------|--------|----------|--------|
| NATS Unavailability | P1 | NO TESTS | 1w | Graceful degradation |
| DB Transaction Failures | P1 | NO TESTS | 1w | Data safety |
| Schema Validation | P1 | PARTIAL | 1w | Correctness |
| Provenance XOR | P1 | NO TESTS | 0.5w | Invariants |
| ULID Collision | P2 | NO TESTS | 0.5w | Edge case |

**Subtotal:** 2-3 weeks for error handling

### 4. Performance/Load Testing Gaps

| Gap | Severity | Status | Fix Time | Impact |
|-----|----------|--------|----------|--------|
| High-Throughput (5K evt/s) | P1 | PARTIAL BENCH | 2w | Production capacity |
| Large Batches (1000+) | P2 | NO TESTS | 1w | Scalability |
| Long Material Streams | P2 | NO TESTS | 1w | Stability |

**Subtotal:** 2-3 weeks for performance validation

### 5. Security/Chaos Testing Gaps

| Gap | Severity | Status | Fix Time | Impact |
|-----|----------|--------|----------|--------|
| Malicious Payloads | P1 | PARTIAL | 1w | Security |
| Network Partition | P1 | NO TESTS | 1w | Reliability |
| Service Crash | P1 | NO TESTS | 1w | Crash recovery |
| Corrupted Messages | P2 | NO TESTS | 0.5w | Error handling |
| Time-Based Attacks | P2 | NO TESTS | 0.5w | Security |

**Subtotal:** 2-3 weeks for chaos/resilience

### 6. Migration/Upgrade Testing Gaps

| Gap | Severity | Status | Fix Time | Impact |
|-----|----------|--------|----------|--------|
| Schema Migrations | P1 | NO TESTS | 1w | Upgrade path |
| Backwards Compatibility | P1 | NO TESTS | 1w | Deployment |
| Dual-Path (gRPC+JS) | P1 | NO TESTS | 1w | Migration strategy |
| sensd Removal | P2 | NO TESTS | 0.5w | Cleanup |

**Subtotal:** 2-3 weeks for upgrade safety

---

## Recommended Implementation Order

### Phase A: Unblock Phase 1 (Weeks 1-4, Critical Path)
**Must complete before shipping events consumer to production**

1. Week 1-2: Events consumer loop + idempotency (600-700 LOC)
2. Week 2-3: Confirmations + automaton consumer (600-700 LOC)
3. Week 3: DLQ routing establishment (400-500 LOC)
4. Week 4: Restart resilience verification (600-700 LOC)

**Result:** Events consumer production-ready with exactly-once semantics

### Phase B: Complete Phase 1 (Weeks 5-6)
**Integration validation before removing gRPC**

5. Week 5-6: E2E satellite→DB full pipeline (500-700 LOC)

**Result:** Full event flow validated end-to-end

### Phase C: Prepare Phase 2-3 (Weeks 6-8)
**Materials and automaton support**

6. Week 6-7: Material assembler tests (600-900 LOC)
7. Week 7-8: Automaton integration (400-500 LOC)

**Result:** Materials and automata ready for production

### Phase D: Stability & Performance (Weeks 8-10)
**Production hardening**

8. Week 8: Error path hardening (2000+ LOC)
9. Week 9-10: Load testing & optimization (1200 LOC)

**Result:** System survives all error scenarios, meets throughput targets

### Phase E: Chaos & Security (Weeks 10-11)
**Resilience validation**

10. Week 10-11: Chaos and security tests (1500 LOC)

**Result:** System survives partition, crashes, attacks

### Phase F: Upgrade Path (Weeks 11-12)
**Safe migration from gRPC to JetStream**

11. Week 11-12: Migration & compatibility tests (1000 LOC)

**Result:** Safe upgrade procedure documented and tested

---

## Success Metrics

### Before Phase 1 Production Ship
- [ ] All 6 P0 gaps have passing tests
- [ ] 100% idempotency verified (property tests)
- [ ] DLQ functional for 20+ error paths
- [ ] E2E latency < 1s (P95)
- [ ] Crash recovery verified (no duplicates)
- [ ] Load test: 1K evt/sec sustained

### Before Phase 2 Production Ship
- [ ] Automata consume confirmations reliably
- [ ] Material assembler hashes verified
- [ ] Concurrent material isolation proven
- [ ] Connection pool exhaustion handled

### Before Phase 3 Production Ship
- [ ] 5K evt/sec sustained throughput
- [ ] Memory growth sub-linear (< 500MB for 5K)
- [ ] All error paths safe (no crashes)
- [ ] Network partition recovery verified

### Before Phase 5 (sensd Removal)
- [ ] Dual-path operation tested
- [ ] Backwards compatibility verified
- [ ] Zero data loss on cutover
- [ ] Safe rollback procedure

### Newly Landed Suites (2025-02-23)
- `jetstream_consumer_test::duplicate_events_are_idempotent` (ingestd) – covers idempotency replay guarantees.
- `jetstream_consumer_test::dlq_captures_multiple_validation_failures` – validates DLQ fan-out under burst rejection.
- `pipeline_resilience_test::{ingestion_handles_burst_under_latency_budget,replaying_events_after_restart_does_not_duplicate}` – throughput + restart-no-duplicate assertions.
- `material_acquisition::material_acquisition_concurrent_sessions_isolated` – concurrent material pipeline safeguards.
- `replay_control::tests::telemetry_reports_state_counts` and `replay_control::tests::replay_client_errors_when_broker_disappears` – replay telemetry + control-plane HA coverage.

---

## Key Testing Infrastructure Needed

### 1. Enhance EphemeralNats (2 days)
```rust
pub struct EphemeralNats {
    // Existing: start NATS server
    // New: stream factory, consumer factory, chaos injection
    pub async fn create_stream(&self, name: &str, subjects: &[&str]) -> Result<()>
    pub async fn create_consumer(&self, stream: &str, ...) -> Result<Consumer>
    pub fn with_chaos(&mut self, latency: Duration, failure_rate: f64)
}
```

### 2. Test Satellite Publisher (2 days)
```rust
pub struct TestSatellitePublisher {
    js: jetstream::Context,
    source: String,
}

impl TestSatellitePublisher {
    pub async fn publish_event(&self, type_: &str, payload: JsonValue) -> Result<String>
    pub async fn publish_material_stream(&self, slices: Vec<&[u8]>) -> Result<String>
    pub async fn wait_confirmation(&self, event_id: &str, timeout: Duration) -> Result<()>
}
```

### 3. Chaos Injection Utils (3 days)
```rust
pub struct ChaosIngestor {
    failure_rate: f64,
    latency: Duration,
}

impl ChaosIngestor {
    pub async fn with_simulated_failures<F, T>(&self, op: F) -> Result<T>
    pub async fn simulate_network_partition(&self) -> Result<()>
    pub async fn simulate_database_crash(&self) -> Result<()>
}
```

### 4. Test Observation Snapshot (2 days)
```rust
pub struct TestSnapshot {
    db_events: u64,
    jetstream_msgs: u64,
    outbox_pending: u64,
    dlq_entries: u64,
    metrics: HashMap<String, u64>,
}

impl TestSnapshot {
    pub fn assert_events_persisted(&self, expected: u64) -> Result<()>
    pub fn assert_confirmations_received(&self, expected: u64) -> Result<()>
    pub fn assert_no_dlq_entries(&self) -> Result<()>
}
```

---

## Risk Mitigation

### Risk: Test Suite Takes Too Long to Run
**Mitigation:**
- Use `#[sinex_bench]` for performance tests (separate workflow)
- Parallelize via `nextest` (already in use)
- Embed NATS (no external service latency)
- Reasonable dataset sizes (100-1K events, not 1M)

### Risk: Flaky Tests from Timing Issues
**Mitigation:**
- Use deterministic time via `tokio::time` (not wall clock)
- Embed NATS (controlled environment)
- Generous timeouts with explicit waits (not sleep loops)
- Property tests for timing invariants (idempotency, ordering)

### Risk: High Maintenance Burden
**Mitigation:**
- Reuse test fixtures extensively
- Document test patterns in comments
- One test per logical scenario (not mega-tests)
- Regular test organization reviews

---

## Documentation Delivered

1. **testing-gap-analysis.md** (1,219 lines)
   - Detailed gap descriptions (26 gaps)
   - Root cause analysis
   - Recommended test scenarios for each gap
   - Property test suggestions
   - Acceptance criteria per gap
   - Summary table by priority
   - Success metrics

2. **testing-priorities-and-roadmap.md** (517 lines)
   - 12-week implementation schedule
   - Weekly deliverables (Weeks 1-12)
   - Lines of code estimates per deliverable
   - Acceptance criteria per phase
   - Risk mitigation strategies
   - Success metrics
   - Test infrastructure requirements

3. **TESTING-SUMMARY.md** (this document)
   - Executive overview
   - Critical findings
   - Category breakdown
   - Quick implementation order
   - Key metrics

---

## Recommendations

### Immediate Actions (This Week)

1. Review gap analysis with team
2. Prioritize P0 gaps in backlog
3. Assign owner for each week's deliverables
4. Enhance EphemeralNats test fixture
5. Begin Week 1 work: Events consumer tests

### Short-Term (Next 4 Weeks)

- Land all P0 gap tests (critical path)
- Establish CI/CD for JetStream tests
- Document test patterns for team

### Medium-Term (Next 12 Weeks)

- Follow implementation roadmap
- Weekly deliverables with passing tests
- Performance benchmarking
- Chaos/security validation

### Long-Term (Post-Migration)

- Maintain test coverage (no regressions)
- Adapt tests as way.md phases land
- Monitor production metrics against benchmarks

---

## Contact & Questions

For questions about specific gaps:
- **JetStream Consumer**: See section 1.1 of testing-gap-analysis.md
- **DLQ Design**: See section 1.4
- **Performance Targets**: See section 4 of roadmap
- **Chaos Scenarios**: See section 5 of gap analysis

For implementation details:
- See testing-priorities-and-roadmap.md for weekly breakdown
- Estimated effort per deliverable included
- Test infrastructure requirements listed

---

## Appendix: Files Analyzed

### Test Coverage Assessment
- **Property Tests:** 6,344 LOC (260 proptest uses)
- **Integration Tests:** Multiple categories (adversarial, concurrency, performance, security)
- **Existing JetStream Tests:** 1 (service_outbox_tests.rs, marked `#[ignore]`)
- **Test Utilities:** sinex-test-utils (comprehensive)

### Key Source Files
- **ingestd:** service.rs (outbox processor, NATS publishing)
- **Satellite SDK:** stage_as_you_go, replay, coordination
- **way.md:** JetStream migration blueprint
- **Error Testing:** error_testing.rs (reusable patterns)

---

**End of Summary**

For full details, see:
- `docs/testing-gap-analysis.md` - Complete gap analysis (1,219 lines)
- `docs/testing-priorities-and-roadmap.md` - Implementation roadmap (517 lines)
