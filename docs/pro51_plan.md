Here’s a roadmap that:

* Uses the Claude testing-gap analysis structure (P0/P1/P2, phases A–F)
* Integrates the Gemini3 review (env-gated tests, DB/test infra realities, etc.)
* Drops the “12 weeks” fantasy entirely and treats this as an ordered backlog, not a calendar.

I’ll keep it practical and biased toward “what to build where” rather than PM-speak.

---

## 0. Ground Rules & Reality Check

**Scope:** Close the JetStream / migration testing gaps (26 total across 6 categories) using new infra: enhanced `EphemeralNats`, `TestSatellitePublisher`, `ChaosInjestor`, `TestSnapshot`, plus VM/E2E wiring.

**Key constraints / assumptions:**

1. **Env-gated “native” tests stay** (system + desktop satellites). Gating with `SINEX_NATIVE_SYSTEM_TESTS` etc. is correct; the fix is to **ensure VM/E2E suites flip them on**, not to un-gate them in CI.
2. **Runtime `sqlx::query` is allowed in test infra only** (`sinex-test-utils`, DB reset, etc.). Production code stays on `query!`/`query_as!`/`QueryBuilder`. Enforced via `scripts/check_forbidden_patterns.sh`.
3. **DB pool global lock is a tactical band-aid.** Long-term fix is per-test ephemeral DBs; put that into the roadmap instead of pretending the mutex is “fine forever”.

---

## Phase 0 – Test Infrastructure Foundation

Goal: one coherent “bus+satellite” harness so later tests don’t reinvent plumbing.

### 0.1 Enhance `EphemeralNats`

**File:** `crate/lib/sinex-test-utils/src/nats.rs`

**Status:** Stream/consumer factory is in place (`ensure_stream_with_consumer`, prefix helpers, overlap-tolerant creation) plus chaos hooks and subject wait helpers. Major JetStream suites now use `EphemeralNats` (consumer integration, pipeline resilience, DLQ/idempotency property, stream-name, material assembler tests, e2e satellite, stream-processing helpers). Remaining refactors are limited to a few performance/legacy suites that still hand-roll JetStream contexts.

Next actions:
- Provide a concise stream+consumer factory and subject wait helper; refactor tests to use it.
- Thread chaos hooks into selected error-path scenarios.

**Acceptance:**

* Any test can stand up a JetStream stream + consumer in <10 LOC.
* No test reaches directly for `async_nats::jetstream::Context` without going through `EphemeralNats`.

---

### 0.2 `TestSatellitePublisher`

**File:** `crate/lib/sinex-test-utils/src/satellite_publisher.rs` (new)

**Status:** Implemented with event/material publishing, confirmation waiting, and a convenience constructor from `EphemeralNats`. Needs broader adoption across E2E/ingestion tests.

```rust
pub struct TestSatellitePublisher {
    js: jetstream::Context,
    source: String,
}

impl TestSatellitePublisher {
    pub async fn publish_event(
        &self,
        event_type: &str,
        payload: JsonValue,
    ) -> Result<String> { ... }

    pub async fn publish_material_stream(
        &self,
        slices: Vec<&[u8]>,
    ) -> Result<String> { ... }

    pub async fn wait_confirmation(
        &self,
        event_id: &str,
        timeout: Duration,
    ) -> Result<()> { ... }
}
```

* Uses the same subjects/headers production satellites use (Msg-Id, provenance, etc.).
* Encapsulates “publish then wait for confirmation” handshake so all E2E tests share one path.

**Acceptance:**

* E2E tests no longer reimplement ad-hoc publishers; they all use `TestSatellitePublisher`.
* Confirmations are observed via the same JetStream subject(s) automata use.

---

### 0.3 `ChaosInjestor` & `TestSnapshot`

**File:** `crate/lib/sinex-test-utils/src/chaos.rs`, `…/src/snapshot.rs`

**Status:** ChaosConfig/Chaos helper now exists; TestSnapshot exists. Need to wire these into chaos/error-path suites and replace bespoke assertions.

`ChaosInjestor` (used by error/chaos tests):

```rust
pub struct ChaosIngestor {
    failure_rate: f64,
    latency: Duration,
}

impl ChaosIngestor {
    pub async fn with_simulated_failures<F, T>(&self, op: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    { ... }

    pub async fn simulate_network_partition(&self) -> Result<()> { ... }

    pub async fn simulate_database_crash(&self) -> Result<()> { ... }
}
```

`TestSnapshot` (state observer):

```rust
pub struct TestSnapshot {
    pub db_events: u64,
    pub jetstream_msgs: u64,
    pub outbox_pending: u64,
    pub dlq_entries: u64,
    pub metrics: HashMap<String, u64>,
}

impl TestSnapshot {
    pub fn assert_events_persisted(&self, expected: u64) -> Result<()>;
    pub fn assert_confirmations_received(&self, expected: u64) -> Result<()>;
    pub fn assert_no_dlq_entries(&self) -> Result<()>;
}
```

**Acceptance:**

* Any chaos/chaos+performance test uses `ChaosInjestor` + `TestSnapshot` instead of bespoke asserts.
* Snapshots are the one place where we encode “what does healthy look like” (no duplicates, no DLQ, etc.).

---

### 0.4 Wire env-gated native tests into VM runs

**Status:** Env gates are present. Need to confirm VM jobs set them so native/system/desktop tests actually run in VM scenarios; container jobs remain skipped as intended.

**Status:** ✅ `tests/e2e/nixos-vm/common/test-base.nix` now exports both `SINEX_NATIVE_SYSTEM_TESTS` and `SINEX_NATIVE_DESKTOP_TESTS` via `environment.variables`/`sessionVariables`, so every VM scenario runs with those gates enabled.

---

## Phase 1 – Event Backbone / P0 JetStream Correctness

Goal: core ingest path is actually provable: JetStream → ingestd → DB with correct idempotency, confirmations, DLQ, restart guarantees. These are the P0 gaps.

Some of this is already partially covered by fresh tests like `duplicate_events_are_idempotent`, `dlq_captures_multiple_validation_failures`, and restart resilience tests. The roadmap treats those as “check off if already green”.

### 1.1 Events consumer loop integration tests

**Status:** ✅ Added `crate/core/sinex-ingestd/tests/events_consumer_integration_test.rs` covering the happy-path batch ingest (100 events, DLQ stays empty), transient failure retry (forced persist failure, NAK/redelivery, confirmation delivered, single row persisted), confirmations-after-persist, ack-wait redelivery (slow ack triggers redelivery; idempotency holds, DLQ empty), and validation-to-DLQ routing (invalid payload rejected, valid payload persisted). Consumer config sets explicit `max_deliver` and supports test-only hooks (fail-once, processing delay, delivery counters) with a validator toggle.

**File:** `crate/core/sinex-ingestd/tests/events_consumer_integration_test.rs` (new or expanded)

Scenarios:

1. **Happy path batching:** `jetstream_consumer_processes_batches`

   * 100 events → 100 rows via `UNNEST` insert, no DLQ.
2. **Schema validation → DLQ:** `jetstream_consumer_handles_validation_failure`

   * Malformed events routed to DLQ; good events still processed.
3. **DB failure with retry:** `jetstream_consumer_survives_database_failure`

   * Simulate transaction failure; consumer recovers without loss or duplication.
4. **AckWait behavior:** `jetstream_consumer_respects_ack_wait`

   * If we exceed AckWait, message is re-delivered but overall still idempotent.

Property tests (can live next to these or in a `proptest` module):

* `prop_consumer_idempotency`: publish same message N times; DB ends with 1 row.
* `prop_batch_ordering`: inserted order matches JetStream sequence (or defined invariant).
* `prop_offset_monotonic`: consumer checkpoints advance monotonically.

---

### 1.2 Confirmations & ACK flow

**File:** `crate/core/sinex-ingestd/tests/confirmation_flow_test.rs`

Scenarios:

1. `confirmation_published_after_database_commit` – outbox triggers confirmation only after successful commit.
2. `idempotency_prevents_duplicate_confirmations` – duplicates on confirmation subject don’t cause double-processing.
3. `confirmation_message_format_correct` – JSON shape and headers match contract (Msg-Id, event ids, timestamps).

Plus automaton side tests (may live in SDK or ingestd):

* Automaton receives confirmation and processes exactly once.

---

### 1.3 DLQ routing & replay/restart

* **DLQ tests:** Cover schema failure, DB violation, unexpected internal error paths, ensuring they all end up in DLQ and are visible to operators.
* **Restart tests:** Expand/align with `pipeline_resilience_test::{ingestion_handles_burst_under_latency_budget, replaying_events_after_restart_does_not_duplicate}` to explicitly assert durable consumer offset semantics.

**Acceptance for Phase 1:**

Matches the existing phase-1 criteria, but used as a checklist rather than a dated milestone:

* [ ] Consumer loop processes events (N/N persisted)
* [ ] Idempotency verified (duplicates deduplicated)
* [ ] Confirmations published reliably
* [ ] DLQ functional for all error types under test
* [ ] Restart recovery verified (no duplicates, no loss)

---

## Phase 2 – End-to-End Satellite → DB, Materials, Automata

Goal: prove that **real** satellites, using the SDK, can publish data all the way to DB and automata in a realistic environment. This closes the “Satellite→DB E2E” and materials/automata gaps.

### 2.1 E2E satellite → DB tests

**File:** `crate/lib/sinex-test-utils/tests/e2e_satellite_to_db_test.rs`

**Status:** ✅ Implemented `end_to_end_single_satellite_full_flow`: spins up an ingestd consumer, publishes 25 events via `TestSatellitePublisher`, waits for DB persistence, and verifies confirmations via a wildcard subscription with DLQ remaining empty.

Scenarios:

1. `end_to_end_single_satellite_full_flow` – 100 events from a single satellite end up in DB.
2. `end_to_end_confirmation_received` – satellite sees confirmations via `StreamProcessorRunner`.
3. `end_to_end_latency_measured` – publish→persist within budget (no need to hardcode numbers in the tests; assert “reasonable” bounds).
4. `provenance_chain_valid` – event source and material provenance match expected values.

---

### 2.2 Material assembler verification

**File:** `crate/core/sinex-ingestd/tests/material_assembler_test.rs`

Scenarios:

* Slice assembly and hashing (happy path).
* Missing slice → timeout and DLQ.
* Concurrent materials from different sources → isolation.
* Rotation at configured size boundaries.
* Correct ledger entries in `raw.temporal_ledger`.

Property tests:

* Hash invariance under slice reordering (or precisely defined invariant).
* Monotonic offsets in ledger.

---

### 2.3 Automaton integration

**File:** `crate/lib/sinex-satellite-sdk/tests/automaton_integration_test.rs`

Scenarios:

* Automaton processing full stream end-to-end (confirmations → automaton → DB).
* Crash recovery – durable consumer restarts without reprocessing.
* Multiple automata load sharing.
* Backpressure / slow automaton scenarios.

**Acceptance for Phase 2:**

* [ ] Satellite→DB E2E tests green with real JetStream.
* [ ] Material assembler tests cover concurrency + corruption risk.
* [ ] Automata consume confirmations & survive restarts.

---

## Phase 3 – Error-Path Hardening & Performance

Goal: exercise all “it blew up” paths and scale scenarios: NATS down, DB down, schema broken, plus target throughput.

### 3.1 Error-path suite

**File:** `crate/lib/sinex-core/tests/adversarial/jetstream_error_paths_test.rs` (or similar)

Cover the P1 error gaps:

* NATS unavailability (publish + consume both sides).
* DB transaction failures (including deadlocks / unique violations).
* Schema validation edge cases (including malformed events generated via `malformed_detection`).
* Provenance XOR invariants.
* ULID collision behavior (even if astronomically unlikely, one adversarial test).

Use `ChaosInjestor` for injecting failures in a controlled way.

---

### 3.2 Performance & regression tests

You already have a performance suite (`jetstream_performance_test`, `resource_exhaustion_test`, etc.). The roadmap here is about **aligning them with the migration goals**:

* Explicit tests for:

  * Target throughput (e.g., 1k+ events/s; 5k events/s scenarios).
  * Large batches (1000+ events per batch).
  * Long material streams.
* Codify pass/fail thresholds based on the success metrics table (but keep them configurable if you don’t want hard-coded magic numbers in CI).

**Acceptance for Phase 3:**

* [ ] All error gaps in the analysis have at least one targeted test.
* [ ] Regression/performance suite runs in CI or at least in a scheduled job.
* [ ] Throughput / latency metrics are captured and compared over time.

---

## Phase 4 – Chaos & Security

Goal: resilience under “evil” conditions: partitions, crashes, corrupted messages, adversarial payloads, time weirdness.

### 4.1 Chaos suite

**File:** `crate/lib/sinex-core/tests/adversarial/jetstream_chaos_test.rs`

Scenarios:

1. Network partition between ingestd and NATS.
2. Service crash in the middle of processing a batch (crash → restart).
3. Corrupted NATS messages (garbage payloads, truncated JSON, etc.).
4. Malicious payloads (XSS-like strings, overlong Unicode, oversized).
5. Time-warp attacks (events with timestamps far in the past/future).
6. Cascading failures (DB down then NATS down, etc.).

Use `ChaosInjestor` + `TestSnapshot` to assert “no data loss, no duplicates, graceful degradation (errors not panics), and diagnosable logs/metrics”.

---

### 4.2 Security-focused tests

Alongside the chaos suite:

* Tests for the new `sanitization.rs` behavior (double-encoding, null byte attacks) to guarantee the regression that prompted the fix is permanently covered.
* Tests for path validation macros and any other “guardrail” macros that protect from path traversal, etc.

**Acceptance for Phase 4:**

* [ ] All security/chaos gaps from the analysis have targeted tests.
* [ ] Chaos tests assert both functional correctness and operator observability (logs/metrics).

---

## Phase 5 – Migration / Upgrade Safety

Goal: you can move from gRPC + sensd → JetStream-only safely, and revert if needed.

### 5.1 Schema + migration tests

**File:** `crate/lib/sinex-schema/tests/migration_test.rs`

Scenarios:

* Forward migration with live-like data.
* Rollback safety.
* SQLx cache regeneration still works after schema changes.
* Dual-path “both gRPC + JetStream live” before you cut the old path.

### 5.2 Upgrade scenarios at the VM level

In `tests/e2e/nixos-vm`:

* Scenario that bootstraps a “pre-JetStream” configuration, upgrades to JetStream world, and verifies no data loss / acceptable downtime.

**Acceptance for Phase 5:**

* [ ] Migrations have tests proving no data loss and clean rollback.
* [ ] A documented and tested dual-path period exists.
* [ ] VM tests cover at least one “upgrade + rollback” path.

---

## Cross-Cutting: Test Infra Hygiene & Future Refactors

These are not strictly migration-critical but should live in the same roadmap so they don’t get forgotten.

1. **Document the `sinex_test` modes** (sync vs async vs `TestContext`) so contributors know when to use it vs bare `#[test]`. This codifies the Gemini3 insight that `sinex_test` is smart & low-overhead for sync tests.
2. **Track `DATABASE_POOL_TEST_LOCK` as technical debt**, with a follow-up epic: “Per-test ephemeral DBs instead of shared pool”.
3. **Keep the `sqlx::query` allowlist tight and reviewed** (only test infra / bootstrap).
4. **Align docs with this roadmap:** `TESTING-SUMMARY.md` and `testing-priorities-and-roadmap.md` already have structure; replace “Week N” with “Phase N” and point to actual tests by name as they land.

---

## How to Use This Practically

If you want something you can drop into your tracker:

* Create Epics: `Phase 0 – Infra`, `Phase 1 – Event Backbone`, … `Phase 5 – Migration`.
* Under each, create issues that map to the file+test names above.
* When a test suite exists (e.g. some P0 JetStream tests already landed), mark that issue as “validate + extend” instead of “greenfield”.

If you’d like, next step I can do is:
*take your current `cargo test`/`nextest` output and this roadmap and produce a literal checkbox doc with “already done vs missing” by test name.*
