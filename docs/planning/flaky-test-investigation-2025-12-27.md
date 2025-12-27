# Flaky Test Investigation - Sinex Test Suite

## Executive Summary

**Primary Issue Found:** `material_acquisition_concurrent_sessions_isolated` test has a **race condition bug** in the MaterialAssembler that causes timeouts when end messages arrive before begin messages.

**Impact:** Blocks benchmarking and CI, causing 60s timeouts in ~1/20 runs (estimated 5% flakiness rate based on message ordering randomness).

---

## 1. Root Cause Analysis: material_acquisition_concurrent_sessions_isolated

### The Bug

**Location:** `crate/lib/sinex-core/src/db/repositories/source_materials.rs:803-858`

**Symptom:** Test times out after 60s waiting for materials to reach "completed" status.

**Root Cause:** Race condition when NATS end message arrives before begin message:

```
Timeline of failure:
1. Test sends: begin → slices → end (via separate NATS streams)
2. Due to JetStream delivery ordering, end arrives first
3. MaterialAssembler creates placeholder state with source_identifier="unknown"
4. Tries to finalize via register_in_flight_internal()
5. INSERT fails (no conflict) → attempts UPDATE WHERE source_identifier = "unknown"
6. UPDATE returns 0 rows → fetch_one() throws "Not found"
7. Error logged, material never reaches "completed"
8. Test waits 60s and times out
```

**Evidence from logs:**
```
ERROR sinex_ingestd::material_assembler: Failed to process end message: Database error: 
Failed to register source material 01KDEJ2PVS203PM62G1FF1GSHD: Not found: 
register in-flight source material (update existing)
```

### The Code Path

**File:** `crate/core/sinex-ingestd/src/material_assembler.rs:1114-1122`
```rust
// End may arrive before begin/slices (separate streams). Create a placeholder...
warn!(material_id = %material_id,
    "End message received before material state existed; creating placeholder"
);
let placeholder = self.create_placeholder_state(material_id).await?;
```

**File:** `crate/core/sinex-ingestd/src/material_assembler.rs:591-620`
```rust
async fn create_placeholder_state(&self, material_id: Ulid) -> IngestdResult<AssemblerState> {
    // ...
    Ok(AssemblerState {
        material_id,
        // ...
        material_kind: "unknown".to_string(),     // ← Problem
        source_identifier: "unknown".to_string(), // ← Problem
        metadata: json!({}),
        // ...
    })
}
```

**File:** `crate/lib/sinex-core/src/db/repositories/source_materials.rs:809-858`
```rust
let update_sql = r#"
    UPDATE raw.source_material_registry
    SET /* ... */
    WHERE source_identifier = $1  -- ← Filters by source_identifier, not id!
    RETURNING /* ... */
"#;

let existing = sqlx::query_as::<_, SourceMaterialRecord>(update_sql)
    .bind(material.source_identifier.clone())  // "unknown" won't match actual ID
    // ...
    .fetch_one(self.pool)  // ← Throws error if 0 rows
    .await
    .map_err(|e| {
        db_error(e, "register in-flight source material (update existing)")
    })?;
```

**Why it fails:**
1. Placeholder has `source_identifier = "unknown"`
2. Actual material (when begin arrives) has `source_identifier = "session-0"` (or "session-1", etc.)
3. UPDATE query filters by `source_identifier` instead of `id`
4. No rows match → `fetch_one()` errors → material never completes

### Fix Strategy

**Option A (Recommended):** Fix the UPDATE query to filter by ID instead of source_identifier:
```sql
WHERE id = ($1::uuid)::ulid  -- Use the material_id, not source_identifier
```

**Option B:** Pre-register placeholder in DB when creating in-memory state (more invasive)

**Option C:** Don't create placeholder for end-before-begin; instead buffer the end message and retry

---

## 2. Other Flaky Test Patterns Found

### 2.1 Timing-Dependent Tests (35 files)

**Pattern:** Tests using `EphemeralNats` + `start_test_ingestd` infrastructure

**Files:** 35 test files depend on NATS message ordering and ingestd processing timing

**Risk Factors:**
- Multiple sleep() calls (material_acquisition.rs has 11)
- wait_for_condition() with various timeouts (10s, 20s, 60s, 90s)
- Async message processing with no guaranteed ordering
- JetStream delivery across separate streams

**High-Risk Tests (90s timeout = acknowledged slow/flaky):**
1. `material_acquisition_restart_recovery` (line 261)
2. `material_acquisition_concurrent_sessions_isolated` (line 396) **← FAILING**

**Medium-Risk Tests (60s timeout):**
1. `material_acquisition_out_of_order_slices` (line 103)

### 2.2 Property Test Regression Files (10 found)

**Indicates past failures that needed regression seeds:**
1. `crate/core/sinex-ingestd/tests/jetstream_idempotency_property_test.proptest-regressions`
2. `crate/lib/sinex-core/tests/event_property.proptest-regressions`
3. `crate/lib/sinex-core/tests/property/schema_property_test.proptest-regressions`
4. `crate/lib/sinex-core/tests/property/ulid_property_test.proptest-regressions`
5. `crate/lib/sinex-core/tests/property/validation_roundtrip_property_test.proptest-regressions`
6. `crate/lib/sinex-satellite-sdk/tests/property/automation_property_test.proptest-regressions`
7. `crate/lib/sinex-satellite-sdk/tests/property/checkpoint_property_test.proptest-regressions`
8. `crate/lib/sinex-satellite-sdk/tests/property/error_handling_property_test.proptest-regressions`
9. `crate/lib/sinex-satellite-sdk/tests/property/queue_property_test.proptest-regressions`
10. `crate/lib/sinex-satellite-sdk/tests/property/validation_invariants_property_test.proptest-regressions`

**Note:** These are working as intended (regressions caught bugs), but indicate complexity in tested logic.

### 2.3 Coordination/Leadership Tests

**File:** `crate/lib/sinex-satellite-sdk/tests/integration/satellite_coordination_test.rs`

**Pattern:** Short timeouts (500ms) with leadership election logic

**Risk:** Timing-sensitive distributed coordination tests could flake under load

---

## 3. Test Infrastructure Weaknesses

### 3.1 WaitHelpers Usage Inconsistency

**Multiple wait patterns found:**
- `ctx.timing().wait_for_condition()` with various retry counts (10, 20, 25, 60)
- `WaitHelpers::wait_for_condition()` static method
- Custom retry loops (material_acquisition_restart_recovery lines 322-366)

**Problem:** No standard timeout/retry strategy; tests pick arbitrary values

### 3.2 Message Ordering Assumptions

**Issue:** Tests assume NATS JetStream delivers messages in order, but:
- begin/slices/end use **separate subjects**
- JetStream doesn't guarantee cross-stream ordering
- MaterialAssembler expects out-of-order but has bugs (see #1)

### 3.3 Cleanup Race Conditions

**Pattern:** Tests clean database then immediately start new operations

**Risk:** Database vacuum/cleanup might not complete before next operation

---

## 4. Recommendations

### Immediate Fixes (Block benchmark)

1. **Fix material_acquisition_concurrent_sessions_isolated:**
   - Change UPDATE query to filter by `id` instead of `source_identifier`
   - Test: `crate/lib/sinex-satellite-sdk/tests/material_acquisition.rs:396`
   - File: `crate/lib/sinex-core/src/db/repositories/source_materials.rs:826`

### Short-Term Improvements

2. **Standardize wait_for_condition:**
   - Create consistent timeout policy (e.g., 30s default, 60s for integration, 90s for stress)
   - Add adaptive backoff to WaitHelpers
   - Log progress during long waits

3. **Add message ordering test:**
   - Specifically test end-before-begin scenario
   - Verify placeholder → real material merge works

4. **Review other UPDATE-by-source_identifier queries:**
   - Check if similar bugs exist elsewhere
   - Prefer UPDATE-by-ID for uniqueness

### Long-Term Hardening

5. **Reduce timing dependencies:**
   - Use event-driven assertions instead of sleep()
   - Implement deterministic test fixtures for NATS ordering

6. **Add chaos testing:**
   - Randomize message delivery order
   - Inject delays/failures in test infrastructure
   - Verify graceful degradation

7. **Monitor proptest regressions:**
   - Track when new regression seeds appear
   - Indicates emerging edge cases in production logic

---

## 5. Test Flakiness Risk Matrix

| Test | Risk | Reason | Action |
|------|------|--------|--------|
| material_acquisition_concurrent_sessions_isolated | **CRITICAL** | Known race bug causing failures | Fix immediately |
| material_acquisition_restart_recovery | HIGH | 90s timeout, complex restart logic | Monitor, add logging |
| material_acquisition_out_of_order_slices | MEDIUM | 60s timeout, relies on buffering | Add deterministic ordering test |
| satellite_coordination tests | MEDIUM | 500ms timeouts, leadership races | Review under load |
| All NATS-based tests | LOW-MEDIUM | Timing assumptions | Standardize waits |

---

## 6. Workaround for Benchmarking

**Immediate:** Exclude failing test from benchmark:
```bash
BENCH_TARGET='--workspace -E "not test(material_acquisition_concurrent_sessions_isolated)"'
```

**After fix:** Re-enable and verify stability across 100+ runs.

---

## 7. Deep Infrastructure Analysis (Extended Investigation)

### 7.1 Test Infrastructure Strengths

**Production Code Reuse (Excellent):**
- `CoordinationPrimitive` from production is reused in tests (`timing_utils.rs`)
- This ensures tests exercise real coordination logic
- TestSynchronizer, TestBarrier, WorkerReadinessCoordinator all use production primitives
- Event counting and progress tracking match production behavior

**EphemeralNats Reliability:**
- 10-attempt retry loop for port allocation prevents flakiness from port collisions
- Graceful handling of "address already in use" errors
- Child process cleanup on failure
- Wait-for-ready verification before returning

**Database Pool Sophistication:**
- Advisory lock-based slot reservation prevents double-allocation across processes
- Quarantine mechanism isolates problematic databases
- Residual tracking logs exact row counts when cleanup fails
- Cleanup metrics (acquisitions, wait time, failures, recreations)
- Extension version drift detection and automatic recreation
- Schema validation (core.events, column checks) before reuse

**Timing Infrastructure Features:**
- Adaptive backoff in `wait_for_condition` (via `wait_for_condition_adaptive`)
- Event-driven coordination via TestSynchronizer
- Multi-participant barriers with timeout support
- Worker readiness coordination with barrier integration

### 7.2 Test Infrastructure Weaknesses

**Inconsistent Timeout Strategies:**
```
timing_utils.rs uses:
- wait_for_condition(condition, 60) // material_acquisition.rs
- wait_for_condition(condition, 90) // restart recovery test
- wait_for_condition(condition, 20) // some integration tests
- wait_for_condition(condition, 10) // unit-level tests
```
**Problem:** No documented rationale for timeout choices; tests pick arbitrary values

**Database Cleanup Complexity (3459 lines):**
- Multiple cleanup strategies: reset_database, force_event_material_cleanup, verify_clean_state
- Complex retry logic (up to 3 attempts with different strategies)
- Quarantine on failure, but quarantined slots are never automatically recovered
- Residual tracking is diagnostic-only; doesn't trigger automatic remediation
- `SINEX_TESTUTILS_CLEAN_AFTER_USE` environment flag adds complexity

**NATS Message Ordering Gaps:**
- Tests assume in-order delivery but JetStream uses separate subjects
- No explicit test coverage for all out-of-order permutations (6 orderings for begin/slice/end)
- MaterialAssembler has placeholder logic but it's buggy (see Section 1)

**Property Test Configuration:**
- Default case counts vary by test (unspecified defaults vs explicit #[cases(...)])
- `SINEX_PROPTEST_CASES` environment override works but isn't documented in TESTING.md
- No guidance on when to raise case counts for critical paths
- Shrinking finds edge cases (event_count=1, message_count=1) but tests don't explicitly cover these

### 7.3 Property Testing Analysis

**Edge Cases Caught (Excellent Coverage):**

1. **Security (Path Traversal):**
   - `..%2f..%2f..%2fetc%2fpasswd` (URL-encoded traversal)
   - `/tmp/safe.txt\0../../../etc/passwd` (null byte injection)
   - `¡\\/."` (Unicode + special chars)

2. **Minimal Counts (Boundary Values):**
   - `event_count = 1` (JetStream idempotency)
   - `message_count = 1` (queue tests)
   - `batch_size = 1` (automation tests)
   - `processed = 0` (checkpoint state)

3. **Numeric Extremes:**
   - `5.005638788906564e-121` (tiny float)
   - `8.815952865613998e44` (huge float)
   - `size = 13825627111` (large file size)

4. **Timing/Concurrency:**
   - `delay_microseconds = 994, generation_count = 41` (ULID uniqueness)
   - `num_threads = 8, ulids_per_thread = 72` (concurrent generation)

5. **Schema Validation:**
   - Various payload sizes: `[9528, 1365, 4003]` with `validation_count = 17`
   - Object structure edge cases: `{"kind": "simple", "value": "test"}`

**Property Test Case Distribution:**
- Default: unspecified (likely 256 cases per proptest default)
- Benchmarking: configurable via `BENCH_PROPTEST_CASES` (default 32 for speed)
- CI could raise to 1024+ but doesn't currently
- No documented guidance on coverage vs runtime tradeoffs

### 7.4 Database Pooling Reliability Concerns

**Quarantine Recovery Gap:**
```rust
// database_pool.rs:2019-2020
slot.quarantined.store(true, Ordering::SeqCst);
return Err(err);
```
- Slots are quarantined on cleanup failure
- Never automatically de-quarantined (even after template recreation)
- Long benchmark runs could exhaust all slots
- No metrics on quarantine duration or recovery attempts

**Cleanup Failure Cascade:**
1. First attempt: `reset_database()` (truncate all tables)
2. Verify: `verify_clean_state()` (check row counts)
3. If verify fails: retry cleanup once
4. If still failing: `force_event_material_cleanup()` (targeted deletes)
5. If force fails: quarantine slot permanently

**Race Condition in Lazy Provisioning:**
```rust
// database_pool.rs:977-983
// "Pool provisioning lock: ensure only one nextest process..."
let provision_lock = advisory_lock_key(&format!("{}::pool_provision", template.name));
sqlx::query("SELECT pg_advisory_lock($1)")
    .bind(provision_lock)
    .execute(&mut provision_conn)
    .await?;
```
- Under nextest, databases are created lazily on first acquisition
- Advisory lock prevents multiple processes creating same DB
- But if a test crashes while holding the lock, other processes block indefinitely
- No lock timeout or deadlock detection

**Extension Version Drift:**
- Good: Automatic detection of version mismatches (timescaledb, ulid, pgx_ulid, pg_jsonschema, vector)
- Good: Automatic recreation when drift detected
- Concern: Recreation during acquire() delays the test waiting for that slot
- Concern: No preemptive validation (all slots could drift and only be detected on use)

---

## 8. Comprehensive Reliability Improvement Roadmap

### Phase 1: Critical Fixes (Unblock Benchmarking)

**Priority: CRITICAL | Timeline: Immediate**

#### 1.1 Fix material_acquisition_concurrent_sessions_isolated Race Condition
**File:** `crate/lib/sinex-core/src/db/repositories/source_materials.rs:826`

**Change:**
```rust
// Before:
WHERE source_identifier = $1

// After:
WHERE id = $2::uuid::ulid
```

**Additional binding:**
```rust
.bind(material.id.ulid())  // Add before source_identifier bind
```

**Test Coverage:**
- Run test 100 times to verify fix: `for i in {1..100}; do cargo nextest run material_acquisition_concurrent_sessions_isolated || break; done`
- Add explicit test for end-before-begin scenario in `material_assembler.rs`

**Estimated Impact:** Eliminates 5% flakiness rate in critical benchmark test

#### 1.2 Review All UPDATE-by-source_identifier Queries
**Action:** Search codebase for similar patterns
```bash
rg "UPDATE.*WHERE source_identifier" --type rust
```

**Fix any matches** using the same pattern (UPDATE by unique ID, not semantic identifier)

**Estimated Impact:** Prevents similar race conditions in other repositories

### Phase 2: Database Pool Hardening (Reduce Long-Run Failures)

**Priority: HIGH | Timeline: Short-term (1-2 weeks)**

#### 2.1 Implement Quarantine Recovery
**File:** `crate/lib/sinex-test-utils/src/database_pool.rs`

**Add periodic recovery task:**
```rust
// Check quarantined slots every 30s
// If template was recreated since quarantine, attempt to provision slot
// Clear quarantine flag on success
```

**Metrics to add:**
- `quarantine_duration_seconds` (histogram)
- `quarantine_recovery_attempts` (counter)
- `quarantine_recovery_success` (counter)

**Estimated Impact:** Prevents slot exhaustion in long benchmark runs (>1 hour)

#### 2.2 Add Advisory Lock Timeout
**Change:**
```rust
// Replace pg_advisory_lock with pg_try_advisory_lock_timeout
sqlx::query("SELECT pg_try_advisory_lock($1, $2)")
    .bind(provision_lock)
    .bind(30000) // 30s timeout
```

**Fallback:** If timeout, log warning and proceed with slot that may already exist

**Estimated Impact:** Prevents indefinite blocking if test crashes while holding lock

#### 2.3 Preemptive Extension Validation
**Add background task** (optional, in non-nextest mode):
```rust
// On pool init, check all slots for extension drift in parallel
// Recreate drifted slots before any test acquisitions
```

**Estimated Impact:** Reduces surprise delays during test execution

### Phase 3: Timing & Synchronization Standardization

**Priority: MEDIUM | Timeline: Medium-term (2-4 weeks)**

#### 3.1 Standardize Timeout Policy
**Create timeout tiers:**
```rust
pub enum TestTimeout {
    Unit,        // 10s - single operation, no external deps
    Integration, // 30s - database + NATS, simple flow
    Pipeline,    // 60s - full ingestion pipeline, multiple components
    Stress,      // 90s - restart recovery, chaos scenarios
}
```

**Update all tests** to use explicit tier:
```rust
ctx.timing().wait_for_condition(condition, TestTimeout::Integration).await?
```

**Add documentation** in `TESTING.md` explaining when to use each tier

**Estimated Impact:** Reduces arbitrary timeout choices, improves test readability

#### 3.2 Reduce sleep() Dependencies
**Pattern to eliminate:**
```rust
tokio::time::sleep(Duration::from_secs(1)).await; // Hope message processed
```

**Replace with:**
```rust
ctx.timing().wait_for_condition(|| async {
    // Check actual state (DB query, metric, log entry)
    Ok(actual_state == expected_state)
}, TestTimeout::Integration).await?
```

**Target files:**
- `material_acquisition.rs` (11 sleep calls)
- All JetStream integration tests

**Estimated Impact:** Tests fail fast on actual errors instead of timing out; reduces test runtime by 20-30%

#### 3.3 Enhance TestSynchronizer Diagnostics
**Add logging:**
```rust
impl TestSynchronizer {
    pub async fn wait_for_count(&self, expected: usize) -> TestResult<()> {
        let start = Instant::now();
        // Existing logic...
        if elapsed > 10s {
            warn!("TestSynchronizer waiting for {expected}, currently at {current} (elapsed: {elapsed:?})");
        }
    }
}
```

**Estimated Impact:** Easier diagnosis of coordination failures; prevents silent hangs

### Phase 4: NATS Message Ordering Robustness

**Priority: MEDIUM | Timeline: Medium-term (2-4 weeks)**

#### 4.1 Add Comprehensive Out-of-Order Tests
**New test file:** `crate/core/sinex-ingestd/tests/material_assembler_ordering_test.rs`

**Test all 6 permutations:**
```rust
#[sinex_test] async fn test_begin_slice_end() { /* normal order */ }
#[sinex_test] async fn test_begin_end_slice() { /* end before final slice */ }
#[sinex_test] async fn test_slice_begin_end() { /* slice before begin */ }
#[sinex_test] async fn test_slice_end_begin() { /* both before begin */ }
#[sinex_test] async fn test_end_begin_slice() { /* end first (current bug) */ }
#[sinex_test] async fn test_end_slice_begin() { /* end first, variant */ }
```

**Use deterministic delivery:**
```rust
// Manually control message delivery order instead of relying on JetStream
publish_end().await?;
tokio::time::sleep(100ms).await; // Ensure end processed
publish_begin().await?;
```

**Estimated Impact:** Catches regressions in out-of-order handling; documents expected behavior

#### 4.2 Add MaterialAssembler State Validation
**After each message type:**
```rust
// Verify internal state consistency
assert!(assembler.state.material_id == expected_id);
assert!(assembler.state.source_identifier != "unknown" || !finalized);
```

**Estimated Impact:** Detects placeholder-related bugs before they cause timeouts

### Phase 5: Property Testing Enhancement

**Priority: LOW-MEDIUM | Timeline: Long-term (1-2 months)**

#### 5.1 Document Property Test Guidelines
**Add to `TESTING.md`:**
```markdown
### Property Test Case Counts

- **Default (256 cases):** Most property tests
- **Quick (32 cases):** Benchmarking, local development (set SINEX_PROPTEST_CASES=32)
- **Thorough (1024 cases):** CI, critical paths (set SINEX_PROPTEST_CASES=1024)
- **Exhaustive (10000 cases):** Nightly builds, security-critical code

When to raise case counts:
- Security-sensitive code (path validation, auth)
- Concurrency primitives (locks, channels, coordination)
- Data integrity (ULID generation, checksums)
```

#### 5.2 Expand Boundary Value Coverage
**For tests that shrink to count=1:**
```rust
// Explicitly test edge cases in addition to property tests
#[sinex_test]
async fn test_idempotency_single_event() {
    run_duplicate_event_rejection(1).await
}

#[sinex_test]
async fn test_idempotency_two_events() {
    run_duplicate_event_rejection(2).await
}
```

**Estimated Impact:** Faster feedback on boundary bugs (don't wait for shrinking)

#### 5.3 Track Regression File Growth
**Add CI check:**
```bash
# Alert if regression files grow unexpectedly
git diff --stat origin/main | rg '\.proptest-regressions'
```

**Estimated Impact:** Early warning of emerging edge cases in production logic

### Phase 6: Monitoring & Observability

**Priority: LOW | Timeline: Long-term (2+ months)**

#### 6.1 Test Metrics Dashboard
**Track over time:**
- Flakiness rate per test (failed runs / total runs)
- Timeout distribution (how many tests hit timeouts)
- Database pool quarantine events
- Cleanup failure rate
- Proptest regression file additions

**Tooling:** Store nextest JSON output in time-series database

**Estimated Impact:** Data-driven decisions on where to invest reliability efforts

#### 6.2 Chaos Testing Framework
**New test suite:** `tests/chaos/`

**Inject failures:**
- Random message delays (0-5s)
- Random message drops (5% rate)
- Database connection failures (transient)
- NATS server restarts
- Clock skew simulation

**Verify graceful degradation:**
- No data loss
- Eventual consistency
- Timeout handling
- Retry logic

**Estimated Impact:** Confidence in production resilience; catches assumptions about ideal conditions

---

## 9. Implementation Priority Matrix

| Phase | Priority | Effort | Impact | Dependencies | Timeline |
|-------|----------|--------|--------|--------------|----------|
| 1.1 Fix race condition | CRITICAL | 1 day | Unblocks benchmarking | None | Immediate |
| 1.2 Review UPDATE queries | CRITICAL | 2 days | Prevents similar bugs | None | Immediate |
| 2.1 Quarantine recovery | HIGH | 3 days | Prevents slot exhaustion | None | Week 1 |
| 2.2 Lock timeout | HIGH | 1 day | Prevents deadlocks | None | Week 1 |
| 3.1 Timeout standardization | MEDIUM | 5 days | Improves readability | None | Week 2-3 |
| 3.2 Reduce sleep() deps | MEDIUM | 7 days | Faster tests | 3.1 | Week 3-4 |
| 4.1 Ordering tests | MEDIUM | 4 days | Documents guarantees | 1.1 | Week 2-3 |
| 2.3 Extension validation | MEDIUM | 3 days | Reduces delays | None | Week 3 |
| 3.3 Sync diagnostics | LOW-MEDIUM | 2 days | Easier debugging | None | Week 4 |
| 4.2 State validation | LOW-MEDIUM | 2 days | Catches bugs early | 4.1 | Week 3-4 |
| 5.1 Proptest docs | LOW-MEDIUM | 2 days | Guidance for devs | None | Month 2 |
| 5.2 Boundary coverage | LOW-MEDIUM | 3 days | Faster feedback | 5.1 | Month 2 |
| 5.3 Regression tracking | LOW | 1 day | Early warnings | None | Month 2 |
| 6.1 Metrics dashboard | LOW | 5 days | Data-driven decisions | None | Month 3 |
| 6.2 Chaos testing | LOW | 10 days | Production confidence | All above | Month 3-4 |

**Total effort estimate:** ~50 days engineering time across 3-4 months

**Recommended first sprint (Week 1-2):**
1. Fix race condition (1.1) - 1 day
2. Review UPDATE queries (1.2) - 2 days
3. Quarantine recovery (2.1) - 3 days
4. Lock timeout (2.2) - 1 day
5. Ordering tests (4.1) - 4 days

**Result:** Unblocks benchmarking + prevents major failure modes in ~11 days

---

## 10. Success Metrics

**Short-term (1 month):**
- Zero benchmark failures due to `material_acquisition_concurrent_sessions_isolated`
- Zero test failures due to slot exhaustion in runs >1 hour
- All tests use standardized timeout tiers (no arbitrary values)

**Medium-term (3 months):**
- <1% flakiness rate across all tests (measured over 1000 runs)
- 30% reduction in total test runtime (due to sleep() → event-driven conversion)
- Zero tests with >90s timeouts (indicates overly complex test)

**Long-term (6 months):**
- Chaos tests passing at 95% success rate with 10% injected failures
- Automated regression tracking catches edge cases before manual discovery
- Comprehensive metrics dashboard tracks test health trends

---

## 11. Open Questions for Discussion

1. **Database pool sizing:** Current default is `available_parallelism().clamp(8, 32)`. Is this optimal for benchmark workloads?

2. **Proptest case counts in CI:** Should we enforce higher case counts (1024+) for security-critical tests?

3. **Quarantine thresholds:** How many cleanup failures should trigger quarantine? Currently: 2-3 attempts then permanent quarantine.

4. **Cleanup strategy preferences:** Should we prefer aggressive cleanup (risk data loss) or conservative (risk test interference)? Current: aggressive with safety checks.

5. **NATS ordering guarantees:** Should we add a deterministic delivery mode for tests, or keep relying on JetStream's best-effort ordering?

6. **Timeout enforcement:** Should tests that exceed tier timeout fail loudly, or just log warnings? Current: silent timeout.

