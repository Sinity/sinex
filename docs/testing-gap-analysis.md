# Sinex Testing Gap Analysis and Recommendations

**Analysis Date:** 2025-10-25  
**Scope:** JetStream migration (way.md), component integration, error handling, performance, security, and upgrade scenarios  
**Severity Levels:** P0 (Critical), P1 (High), P2 (Medium)

---

## Executive Summary

The sinex codebase demonstrates strong testing infrastructure with:
- 6,344 lines of property tests across multiple categories
- Comprehensive adversarial and chaos engineering suites
- Error testing utilities integrated with TestContext
- Integration tests covering core pipelines

However, critical gaps exist in **JetStream-specific scenarios**, particularly for the Phase 1-3 migration pathway outlined in `way.md`. The most urgent gaps are:

1. **JetStream Consumer Loop** (P0): No tests for the events consumer or material assembler
2. **Confirmation/Acknowledgment Flow** (P0): Missing end-to-end confirmation verification
3. **DLQ Routing and Error Handling** (P0): No tests for error-to-DLQ pipeline
4. **Stream Replay After Restart** (P0): No resilience tests for consumer recovery
5. **Satellite → NATS → ingestd → DB** (P0): No true end-to-end integration test

---

## Part 1: JetStream Migration Testing Gaps (Phase 1-3)

### Gap 1.1: Events Consumer Loop (P0 - CRITICAL)

**What's Not Tested:**
- Events consumer pull from `events.raw.*` stream
- Batch event processing and `UNNEST` inserts
- Explicit acknowledgment after database commit
- NACK/requeue on validation failure
- Consumer restart and offset recovery

**Current State:**
- One integration test exists: `service_outbox_tests.rs::process_outbox_publishes_and_cleans_up`
- This test is **marked `#[ignore]`** and requires local NATS
- Only tests outbox → NATS publishing, NOT events consumption

**Why This Matters:**
- Events consumer is the **core ingestion path** for Phase 1
- Without testing, we cannot verify idempotency or ordering guarantees
- Silent data loss risk if messages are ACKed before database commit

**Recommended Test Scenarios:**

```
Test: jetstream_consumer_processes_batches
  Given: events.raw stream with 100 provisional events
  When: events consumer runs for 5 seconds
  Then:
    - All 100 events inserted into core.events
    - All messages explicitly ACKed
    - Confirmation entries written to outbox
    - Metrics show batch count and latency
  
Test: jetstream_consumer_handles_validation_failure
  Given: events.raw with 1 valid + 1 invalid JSON event
  When: consumer processes both
  Then:
    - Valid event persists in core.events
    - Invalid event NACKed and eventually reaches events.dlq
    - Database remains consistent
    - Consumer can continue processing

Test: jetstream_consumer_survives_database_failure
  Given: events consumer running, database connection drops mid-batch
  When: database connection restored
  Then:
    - Events are NOT duplicated (NACK/requeue ensures exactly-once)
    - Consumer resumes from last ACK
    - No events lost

Test: jetstream_consumer_respects_ack_wait
  Given: Consumer with AckWait = 30s
  When: Event fetched but ACK delayed > 30s
  Then: Message requeued to another consumer or DLQ
```

**Property Test Suggestions:**
- `prop_jetstream_consumer_idempotency`: Publish same event N times → exactly 1 database insert
- `prop_batch_ordering_preserved`: Events published in order → events table maintains order
- `prop_offset_tracking_consistency`: Consumer offset never moves backward

---

### Gap 1.2: Material Consumer and Assembler (P0 - CRITICAL)

**What's Not Tested:**
- Source material begin/slice/end message flow
- Temp file creation and slice appending
- Hash verification and content hashing
- git-annex file placement and `raw.source_material_registry` updates
- `raw.temporal_ledger` entry creation
- Material rotation logic and file size thresholds

**Current State:**
- Stage-as-You-Go integration test exists: `stage_as_you_go_integration_test.rs`
- Tests end-to-end from SDK to database but NOT the ingestd material assembler
- No tests for slice reassembly or hash mismatch detection

**Why This Matters:**
- Phase 3 depends entirely on material consumer correctness
- Hash mismatches cause silent data corruption
- Slice loss during assembly breaks stream replay

**Recommended Test Scenarios:**

```
Test: material_assembler_slices_into_file
  Given: source_material.begin + 10 slices + source_material.end
  When: ingestd processes all messages
  Then:
    - Temp file created and written to disk
    - All 10 slices appended in order
    - Final file size matches total_size_bytes from .end message
    - File moved to git-annex

Test: material_assembler_verifies_hash
  Given: source_material.end with content_hashes: ["abc123"]
  When: ingestd finalizes material
  Then:
    - Calculated hash matches expected hash
    - raw.source_material_registry.content_hash_verified = true
    - Metrics show hash verification time

Test: material_assembler_handles_missing_slice
  Given: source_material.begin with total_slices=10, but only 8 slices received
  When: Timeout occurs (e.g., 5 min)
  Then:
    - Material marked as "incomplete" in database
    - DLQ entry created with reason
    - Temp file cleaned up
    - No orphaned blobs

Test: material_assembler_concurrent_materials
  Given: 5 concurrent materials being assembled simultaneously
  When: All complete normally
  Then:
    - Each material isolated (no file mixing)
    - All 5 entries in raw.source_material_registry
    - All offsets recorded in raw.temporal_ledger
    - No race conditions in git-annex placement

Test: material_assembler_rotation_trigger
  Given: max_material_size_bytes = 1MB
  When: 1.5MB of data received in slices
  Then:
    - First material rotated at 1MB boundary
    - Ledger entry created for rotation
    - Second material continues collecting
```

**Property Test Suggestions:**
- `prop_material_hash_invariant`: Random slice order → same final hash
- `prop_ledger_monotonic_offsets`: ledger.offset_start < ledger.offset_end always
- `prop_concurrent_material_isolation`: N materials → N independent files, no data mixing

---

### Gap 1.3: Confirmation Stream and Acknowledgments (P0 - CRITICAL)

**What's Not Tested:**
- Confirmation message publishing to `events.confirmations` after commit
- Confirmation consumer (automata) receiving confirmations
- Idempotency headers: `Nats-Msg-Id` dedupe logic
- Max-msgs-per-subject compaction for confirmations
- Confirmation timeout/missing confirmation scenarios

**Current State:**
- Transactional outbox processor exists (can see it in `service.rs`)
- Outbox publishes to NATS but there's no test for confirmation receipt
- No automata consumer tests receiving confirmations

**Why This Matters:**
- Phase 2 explicitly requires confirmation-aware consumption
- Without confirmation flow testing, automata won't know when to process
- Duplicates can occur if idempotency headers aren't verified

**Recommended Test Scenarios:**

```
Test: confirmation_published_after_database_commit
  Given: Event ingested and persisted to core.events
  When: Outbox processor runs
  Then:
    - Confirmation message published to events.confirmations.{event_id}
    - Message contains { event_id, persisted: true, ts_ingest }
    - Nats-Msg-Id header equals event_id (for idempotency)

Test: idempotency_prevents_duplicate_confirmations
  Given: Outbox entry published twice (e.g., network retry)
  When: Nats-Msg-Id header is identical
  Then:
    - JetStream deduplicates the message
    - Only ONE confirmation entry exists in stream
    - No duplicate automata triggers

Test: confirmation_missing_triggers_timeout
  Given: Automaton awaiting confirmation for event_id=X
  When: No confirmation arrives within timeout (e.g., 5s)
  Then:
    - Automaton marks as "unconfirmed"
    - Publishes to control subject for replay
    - Operator alerted via metrics/logs

Test: automaton_consumes_confirmation_stream
  Given: 10 confirmed events in events.confirmations
  When: Automaton runs with confirmation consumer
  Then:
    - Automaton processes all 10 confirmed events
    - Cursor advances through stream
    - ACKs consumable message only after processing
```

**Property Test Suggestions:**
- `prop_confirmation_idempotency`: Publish confirmation N times → automaton processes exactly once
- `prop_confirmation_ordering_preserved`: Events confirmed in order → automata see them in order

---

### Gap 1.4: DLQ Routing and Error Handling (P0 - CRITICAL)

**What's Not Tested:**
- Schema validation failures → DLQ
- Database constraint violations → DLQ
- Unrecoverable errors (e.g., OOM) → DLQ
- DLQ consumer (replay tool) retrieving failed messages
- DLQ metrics (depth, age of oldest message)
- DLQ message format and traceability

**Current State:**
- Error testing utilities exist (error_testing.rs)
- Dead letter queue mentioned in event_processor.rs but writes to temp file, not NATS DLQ
- No integration tests verifying DLQ flow

**Why This Matters:**
- DLQ is the **failure isolation mechanism**
- Without testing, errors silently disappear or crash ingestd
- Operations teams cannot diagnose why events were lost

**Recommended Test Scenarios:**

```
Test: schema_validation_failure_routes_to_dlq
  Given: Event with invalid schema (e.g., missing required field)
  When: Ingestion attempted
  Then:
    - Event rejected by schema validator
    - Entry written to events.dlq.ingestd with reason
    - Original event payload preserved in DLQ
    - Metrics incremented: validation_error_count

Test: database_constraint_violation_routes_to_dlq
  Given: Event with duplicate event_id (ULID collision, unlikely but possible)
  When: Inserted into core.events
  Then:
    - Database constraint violation caught
    - DLQ entry created with error details
    - Transaction rolled back (no partial inserts)
    - Consumer continues processing next message

Test: dlq_consumer_retrieves_and_replays
  Given: 5 messages in events.dlq.ingestd
  When: DLQ consumer runs
  Then:
    - All 5 messages retrieved
    - Original payloads intact
    - Operator can filter by error type
    - Replay tool can re-ingest after fix

Test: dlq_respects_retention_policy
  Given: DLQ retention = 7 days
  When: 14 days pass
  Then:
    - Old DLQ messages automatically purged
    - Recent messages retained
    - Metrics show DLQ age distribution
```

**Property Test Suggestions:**
- `prop_dlq_preserves_payload_integrity`: Event → validation failure → DLQ → payload recovered = original
- `prop_unrecoverable_errors_isolation`: 1000 events, 5 unrecoverable → 995 processed, 5 in DLQ

---

### Gap 1.5: Idempotency with Nats-Msg-Id Headers (P0 - CRITICAL)

**What's Not Tested:**
- Duplicate event_id (same Nats-Msg-Id) handling
- Network retry scenarios (same message published twice)
- Consumer offset recovery without duplicate processing
- Idempotent key format validation (ULID → string conversion)

**Current State:**
- Code shows Nats-Msg-Id header usage in `service.rs::process_outbox`
- No test verifies actual deduplication behavior

**Why This Matters:**
- Exactly-once delivery guarantee depends on idempotency
- Without testing, network retries cause duplicate events in database
- Schema constraints (unique event_id) mask the real issue

**Recommended Test Scenarios:**

```
Test: duplicate_message_id_deduplicated_by_jetstream
  Given: Same message published twice with identical Nats-Msg-Id
  When: JetStream receives both
  Then:
    - Second message deduplicated by JetStream
    - Consumer sees message exactly once
    - Database contains exactly one event

Test: consumer_restart_recovers_offset
  Given: Consumer processes 100 messages, commits 50, crashes
  When: Consumer restarts
  Then:
    - Resumes from offset 50 (not offset 0)
    - Next 50 messages processed
    - No reprocessing of first 50
    - Ledger offset remains monotonic

Test: idempotency_key_format_validated
  Given: Event with various payload_id formats
  When: Published with Nats-Msg-Id header
  Then:
    - ULID format validated
    - Non-ULID rejected or converted consistently
    - No format mismatches between event_id and header
```

---

### Gap 1.6: Stream Replay After ingestd Restart (P0 - CRITICAL)

**What's Not Tested:**
- ingestd shutdown during message processing
- Consumer durable name persistence
- Offset tracking and resumption
- Partial batch recovery (50 of 100 inserted when crash occurs)
- Confirmed events surviving restart

**Current State:**
- VM tests may partially cover restart, but not JetStream-specific scenarios
- No explicit test for offset recovery after crash

**Why This Matters:**
- ingestd restarts will happen (deployments, failures)
- Without proper testing, we risk losing events or creating duplicates
- Durable consumer name MUST be recoverable

**Recommended Test Scenarios:**

```
Test: ingestd_restart_recovers_consumer_offset
  Given: ingestd consuming events.raw, processed 100/200 messages
  When: ingestd killed unexpectedly
  Then:
    - On restart, consumer resumes at message 101
    - Messages 1-100 not reprocessed
    - Offset persisted in JetStream consumer metadata

Test: partial_batch_committed_before_crash
  Given: Batch of 50 events, first 30 committed to database
  When: ingestd crashes during 31st insert
  Then:
    - Database has 30 events (transaction rolled back on #31)
    - Consumer offset at message 31 (NACK on failure)
    - No duplicate inserts on restart
    - Message 31 reprocessed successfully

Test: confirmed_events_survive_restart
  Given: 100 events confirmed before crash
  When: ingestd restarts and queries confirmations
  Then:
    - Confirmation stream still contains all 100
    - Confirmations not lost due to restart
    - Automata can rely on persistent confirmation log
```

---

## Part 2: Component Integration Testing Gaps

### Gap 2.1: Satellite → NATS → ingestd → DB End-to-End (P0 - CRITICAL)

**What's Not Tested:**
- True end-to-end flow from satellite publishing to database persistence
- Multiple satellites publishing concurrently
- Event ordering preservation across the pipeline
- Provenance chain validation (source → material → synthesis)
- Latency measurements from publish to database insert

**Current State:**
- `stage_as_you_go_integration_test.rs` tests SDK + ingestd
- But no test with actual satellite simulator publishing raw events
- No multi-satellite concurrency test

**Why This Matters:**
- This is the **critical path for production**
- Real satellites will publish concurrently
- Without testing, we'll discover ordering bugs in production

**Recommended Test Scenarios:**

```
Test: end_to_end_event_flow_single_satellite
  Given: Test satellite publishing 100 events via NATS
  When: Events published and ingestd running
  Then:
    - All 100 events appear in core.events
    - Event IDs match publication order
    - Timestamps in chronological order
    - Provenance records point to source correctly
    - Latency < 1s from publish to database

Test: end_to_end_multiple_satellites_concurrent
  Given: 5 satellites, each publishing 100 events simultaneously
  When: All satellites run in parallel
  Then:
    - All 500 events persisted
    - No duplicates
    - Source field correctly identifies originating satellite
    - No ordering guarantees needed across satellites

Test: end_to_end_with_network_jitter
  Given: Simulated network delays (100-500ms) between satellite and NATS
  When: Satellites publish while network jittering
  Then:
    - All events eventually persisted
    - No events lost
    - No corrupted payloads
    - Jitter does not affect ordering within satellite stream

Test: end_to_end_confirmation_flow
  Given: Satellite publishes event, waits for confirmation
  When: Event reaches ingestd, persisted, confirmation sent
  Then:
    - Satellite receives confirmation < 1s
    - Event_id in confirmation matches published event
    - Confirmation delivery is reliable (no loss)
```

**Property Test Suggestions:**
- `prop_provenance_chain_valid`: Every event → provenance references valid sources
- `prop_no_event_loss`: N events published → N events in database (with retries)

---

### Gap 2.2: Automaton Consuming from NATS Streams (P1 - HIGH)

**What's Not Tested:**
- Automaton subscribing to `events.confirmations` or provisional stream
- Processing pipeline: fetch → validate → transform → store
- Automaton crash recovery with durable consumer
- Coordination between multiple automaton instances
- Backpressure when automaton falls behind

**Current State:**
- StreamProcessorRunner exists but no real automaton consumption test
- automation_property_test.rs exists but may be mock-based

**Why This Matters:**
- Automata are the next layer after ingestion
- Without testing, they won't reliably process confirmed events

**Recommended Test Scenarios:**

```
Test: automaton_consumes_confirmed_stream
  Given: 50 confirmed events in events.confirmations
  When: Automaton runs with durable consumer
  Then:
    - All 50 consumed by automaton
    - Processed in order
    - ACKs move consumer offset forward
    - Metrics show throughput

Test: automaton_crash_recovery
  Given: Automaton consuming 100 messages, crashes at #75
  When: Automaton restarts
  Then:
    - Resumes at message 75 (from durable consumer)
    - Processes messages 75-100
    - No reprocessing of 1-74
    - State recovered from checkpoint

Test: multiple_automata_load_distribution
  Given: 2 automata subscribed to same stream
  When: 200 messages in stream
  Then:
    - Messages distributed between automata
    - No two automata process same message
    - Both make progress independently
```

---

### Gap 2.3: Database Connection Pool Exhaustion (P1 - HIGH)

**What's Not Tested:**
- Connection pool limits under sustained load
- Graceful degradation when all connections exhausted
- Recovery after pool exhaustion lifts
- Timeout behavior for waiting clients

**Current State:**
- critical_failure_modes_test.rs has a sketch but incomplete

**Recommended Test Scenarios:**

```
Test: connection_pool_exhaustion_and_recovery
  Given: Pool size = 10, 50 concurrent requests
  When: All 10 connections consumed
  Then:
    - New requests queued (not crashed)
    - Timeout occurs after 5s wait
    - Requests rejected gracefully
    - After some complete, new requests proceed
```

---

## Part 3: Error Path Testing Gaps

### Gap 3.1: NATS Unavailability (P1 - HIGH)

**What's Not Tested:**
- ingestd startup when NATS unreachable
- Publishing failures when NATS down
- Graceful shutdown during NATS outage
- Recovery when NATS comes back online

**Recommended Test Scenarios:**

```
Test: ingestd_handles_nats_unavailable_at_startup
  Given: NATS server not running, ingestd starting
  When: ingestd tries to connect
  Then:
    - Connection error logged
    - ingestd exits with clear error message (not hang)
    - Systemd can restart properly

Test: ingestd_publishes_to_outbox_when_nats_down
  Given: NATS goes down mid-operation
  When: ingestd tries to publish confirmation
  Then:
    - Publish fails immediately (not hang)
    - Event remains in outbox for retry
    - Metrics show NATS unavailable error
    - When NATS recovers, outbox processor retries

Test: graceful_shutdown_during_nats_outage
  Given: NATS down, ingestd running with buffered events
  When: SIGTERM received
  Then:
    - ingestd flushes database
    - Closes connections cleanly
    - Exits without hanging
```

---

### Gap 3.2: Database Transaction Failures During Ingestion (P1 - HIGH)

**What's Not Tested:**
- Constraint violation (duplicate event_id)
- Foreign key violation (invalid material_id reference)
- Transaction rollback and requeue behavior
- Partial batch failures (10 of 20 events in batch fail)

**Recommended Test Scenarios:**

```
Test: batch_insert_with_duplicate_event_id
  Given: Batch of 20 events, event #10 has duplicate ID
  When: Batch inserted with UNNEST
  Then:
    - Transaction rolled back completely
    - No events from batch persisted
    - Message NACKed and can be retried
    - DLQ entry created with reason

Test: batch_insert_with_foreign_key_violation
  Given: Event referencing non-existent source_material_id
  When: Batch insert attempted
  Then:
    - Foreign key constraint violation caught
    - Transaction rolled back
    - DLQ entry created
    - Consumer continues (no crash)
```

---

### Gap 3.3: Schema Validation Failures (P1 - HIGH)

**What's Not Tested:**
- Invalid JSON in event payload
- JSON schema validation with malformed schema
- Missing required fields in payload
- Type mismatches (string where number expected)
- Large payload exceeding limits

**Recommended Test Scenarios:**

```
Test: invalid_json_payload_rejected
  Given: Event with payload = "not json {]"
  When: Validation attempted
  Then:
    - JSON parse error caught
    - Event rejected
    - DLQ entry created
    - Human-readable error message in DLQ

Test: schema_validation_type_mismatch
  Given: Schema requires { "count": number }, payload has { "count": "abc" }
  When: Validation performed
  Then:
    - Type mismatch detected
    - Validation fails with error pointing to "count" field
    - DLQ entry includes validation error details
```

---

### Gap 3.4: Provenance XOR Constraint Violations (P1 - HIGH)

**What's Not Tested:**
- Event with both Material AND Synthesis provenance (violates XOR)
- Event with neither provenance
- Missing required provenance fields
- Invalid ULID references in provenance

**Recommended Test Scenarios:**

```
Test: both_material_and_synthesis_provenance_rejected
  Given: Event with provenance containing both source_material_id AND source_event_ids
  When: Validation checks provenance
  Then:
    - XOR constraint violation detected
    - Event rejected
    - DLQ entry explains XOR violation

Test: missing_provenance_entirely_rejected
  Given: Event with provenance = null or empty
  When: Validation performs
  Then:
    - Required field error
    - DLQ entry created
```

---

### Gap 3.5: Duplicate Event IDs (ULID Collision) (P2 - MEDIUM)

**What's Not Tested:**
- Two events with same ULID (astronomically unlikely but possible)
- Handling of such collision in database
- Metrics for collision detection

**Recommended Test Scenarios:**

```
Test: ulid_collision_detected_and_logged
  Given: Two events with same event_id ULID
  When: Database insert attempted
  Then:
    - Unique constraint violation caught
    - Metrics incremented: ulid_collision_detected
    - DLQ entry created
    - Operator alerted (log warning)
```

---

## Part 4: Performance and Load Testing Gaps

### Gap 4.1: High-Throughput Event Ingestion (P1 - HIGH)

**What's Not Tested:**
- 10,000+ events/second sustained throughput
- Memory usage under sustained load
- GC pause times and impact on latency
- CPU utilization profiling

**Current State:**
- jetstream_performance_test.rs exists (100+ msg throughput benchmark)
- But no sustained load test for several minutes

**Recommended Test Scenarios:**

```
Benchmark: sustained_ingestion_throughput
  Target: 5,000 events/sec for 60 seconds
  Measure:
    - Events processed per second
    - P50, P95, P99 latency
    - Memory growth (should be sub-linear)
    - CPU usage
    - GC pause count and duration
  
Benchmark: batch_size_optimization
  Vary: batch_size from 10 to 1000
  Measure: Throughput and latency per batch size
  Goal: Find optimal batch size for target latency
```

---

### Gap 4.2: Large Batch Processing (1000+ Events) (P2 - MEDIUM)

**What's Not Tested:**
- Batch size = 1000 events
- Memory overhead of batching
- Transaction overhead for large batches
- Query plan performance for UNNEST with 1000 rows

**Recommended Test Scenarios:**

```
Test: large_batch_insert_performance
  Given: Batch of 1000 events
  When: Inserted via UNNEST
  Then:
    - All 1000 persisted
    - Transaction commits in < 500ms
    - No OOM errors
    - Memory peak reasonable (< 500MB for batch)
```

---

### Gap 4.3: Long-Running Material Streams (P2 - MEDIUM)

**What's Not Tested:**
- Material assembly lasting hours (e.g., log file rotation)
- Hundreds of slices accumulated
- Memory usage in MaterialAssembler state tracking

**Recommended Test Scenarios:**

```
Test: material_assembly_sustained_streaming
  Given: 1000 slices arriving over 60 seconds
  When: Material assembled incrementally
  Then:
    - Memory usage remains constant (not growing per slice)
    - Temp file size correct
    - Final hash verification successful
```

---

## Part 5: Security and Chaos Testing Gaps

### Gap 5.1: Malicious Event Payloads (P1 - HIGH)

**What's Not Tested:**
- XSS payload in JSON string field
- SQL injection-like patterns in payload
- Extremely large payloads (> 1GB)
- Unicode normalization attacks
- Special characters and control characters

**Recommended Test Scenarios:**

```
Test: xss_payload_in_json_string
  Given: Event payload = { "user_input": "<script>alert('xss')</script>" }
  When: Validation and storage occur
  Then:
    - Payload stored as-is (JSON escape handling)
    - No interpretation as code
    - Retrieved payload matches original

Test: oversized_payload_rejected
  Given: Payload > JetStream max message size (e.g., 512MB)
  When: Publish attempted
  Then:
    - JetStream rejects message
    - Clear error message
    - DLQ entry created
```

---

### Gap 5.2: Network Partition During Coordination (P1 - HIGH)

**What's Not Tested:**
- Network split between ingestd and JetStream
- Network split between satellite and NATS
- Asymmetric network (one direction works, other doesn't)
- Recovery from partition

**Recommended Test Scenarios:**

```
Test: network_partition_prevents_confirmation_publishing
  Given: ingestd → NATS connection severed
  When: Event ready to publish confirmation
  Then:
    - Publish fails
    - Event remains in outbox
    - No duplicate confirmation sent
    - When partition heals, outbox retries

Test: asymmetric_network_failure_handling
  Given: ingestd can receive from NATS but can't publish
  When: ingestd processes events
  Then:
    - Events inserted to database
    - Confirmation publish fails
    - Outbox marks failed
    - Metrics show publish error
```

---

### Gap 5.3: Service Crash During Transaction (P1 - HIGH)

**What's Not Tested:**
- ingestd crash in middle of database transaction
- NATS acknowledgment vs database commit race
- Recovery state consistency

**Recommended Test Scenarios:**

```
Test: crash_during_batch_insert
  Given: ingestd inserting batch of 100 events
  When: Process killed at event 50
  Then:
    - Database transaction rolled back
    - All 100 events NOT in database
    - NATS message not ACKed
    - On restart, message reprocessed
    - Final state: all 100 inserted (not 50)
```

---

### Gap 5.4: Corrupted NATS Messages (P2 - MEDIUM)

**What's Not Tested:**
- Truncated payload
- Invalid protobuf/JSON encoding
- Corrupted slice data
- Missing required message headers

**Recommended Test Scenarios:**

```
Test: corrupted_event_payload_handling
  Given: NATS message with truncated JSON payload
  When: Consumed by ingestd
  Then:
    - Parse error caught
    - Message NACKed
    - DLQ entry created
    - Consumer continues
```

---

### Gap 5.5: Time-Based Attacks (ULID Manipulation) (P2 - MEDIUM)

**What's Not Tested:**
- Event with future timestamp (years ahead)
- Event with past timestamp (before system start)
- Rapid-fire ULIDs from same microsecond
- ULID monotonicity violation

**Recommended Test Scenarios:**

```
Test: future_timestamp_in_event_accepted_or_normalized
  Given: Event with ts_orig = 2099-01-01
  When: Validation occurs
  Then:
    - Decision made: accept as-is or normalize to now()
    - Behavior documented and consistent
    - Stored timestamp is what was intended

Test: ulid_uniqueness_under_rapid_fire
  Given: Generate 10,000 ULIDs in tight loop
  When: All converted to string and deduplicated
  Then:
    - All ULIDs unique (no collisions)
    - Monotonicity preserved where applicable
```

---

## Part 6: Migration and Upgrade Testing Gaps

### Gap 6.1: Schema Migrations and Rollback (P1 - HIGH)

**What's Not Tested:**
- Forward migration with existing data
- Rollback of migration
- Data integrity after rollback
- Concurrent operations during migration

**Recommended Test Scenarios:**

```
Test: schema_migration_with_existing_data
  Given: 1000 events in core.events table
  When: Migration adding new column (e.g., optional_blob_id) runs
  Then:
    - Migration completes
    - All 1000 events still present
    - New column populated with defaults
    - Queries still work

Test: schema_migration_rollback
  Given: Migration applied and tested
  When: Rollback executed
  Then:
    - New column removed
    - Data intact
    - Database consistent
```

---

### Gap 6.2: Backwards Compatibility During Deployment (P1 - HIGH)

**What's Not Tested:**
- Old satellite publishing while new ingestd running
- New automaton reading events created by old ingestd
- Provenance field format compatibility

**Recommended Test Scenarios:**

```
Test: old_satellite_new_ingestd_compat
  Given: Satellite built with v0.4.0, ingestd v0.5.0
  When: Satellite publishes event
  Then:
    - ingestd accepts payload
    - Schema validation succeeds
    - No version mismatch errors
```

---

### Gap 6.3: Dual-Path Operation (gRPC + JetStream) (P1 - HIGH)

**What's Not Tested:**
- gRPC ingestion and JetStream ingestion simultaneously
- Events from both paths ending up in database
- Confirmation flow from both paths

**Recommended Test Scenarios:**

```
Test: dual_ingest_paths_both_operational
  Given: ingestd supporting both gRPC and JetStream
  When: 50 events via gRPC, 50 via JetStream simultaneously
  Then:
    - All 100 in database
    - Confirmations published for both paths
    - No conflicts or duplication
```

---

### Gap 6.4: sensd Removal Without Data Loss (P2 - MEDIUM)

**What's Not Tested:**
- Migration of sensd jobs to JetStream equivalents
- No events lost during sensd removal
- sensd tables safely dropped

**Recommended Test Scenarios:**

```
Test: sensd_job_table_dropped_safely
  Given: All sensd jobs migrated to JetStream
  When: sensd tables dropped
  Then:
    - No foreign key violations
    - No orphaned data references
    - All events migrated successfully
```

---

## Part 7: SQLX Cache Regeneration (P2 - MEDIUM)

**What's Not Tested:**
- `.sqlx/` cache regeneration after schema changes
- Nix build with regenerated cache
- Cache invalidation scenarios

**Recommended Test Scenarios:**

```
Test: sqlx_cache_regeneration_on_schema_change
  Given: Schema migration applied
  When: `devenv tasks run sqlx:prepare` runs
  Then:
    - `.sqlx/` files updated
    - New queries cached
    - Nix build succeeds with new cache
```

---

## Summary Table: Testing Gaps by Priority

| Gap ID | Component | Severity | Current State | Estimated Lines | Blockers |
|--------|-----------|----------|---------------|-----------------|----------|
| 1.1 | Events Consumer Loop | P0 | Ignore test only | 500-800 | Phase 1 |
| 1.2 | Material Assembler | P0 | No real tests | 600-900 | Phase 3 |
| 1.3 | Confirmations/ACKs | P0 | Partial only | 400-600 | Phase 2 |
| 1.4 | DLQ Routing | P0 | Stub only | 500-700 | Core safety |
| 1.5 | Idempotency (Msg-Id) | P0 | No tests | 300-400 | Exactly-once |
| 1.6 | Stream Replay/Restart | P0 | VM test sketch | 400-600 | Resilience |
| 2.1 | E2E Satellite→DB | P0 | Partial | 500-700 | Integration |
| 2.2 | Automaton Consumption | P1 | Sketched | 400-500 | Phase 2 |
| 2.3 | Conn Pool Exhaustion | P1 | Incomplete | 300-400 | Stability |
| 3.1 | NATS Unavailable | P1 | None | 300-400 | Resilience |
| 3.2 | DB Transaction Failures | P1 | None | 400-500 | Data safety |
| 3.3 | Schema Validation | P1 | Partial | 300-400 | Correctness |
| 3.4 | Provenance XOR | P1 | None | 200-300 | Invariants |
| 3.5 | ULID Collision | P2 | None | 100-150 | Edge case |
| 4.1 | High-Throughput Load | P1 | Partial bench | 400-500 | Performance |
| 4.2 | Large Batches (1000+) | P2 | None | 200-300 | Scalability |
| 4.3 | Long Material Streams | P2 | None | 200-300 | Stability |
| 5.1 | Malicious Payloads | P1 | Partial | 300-400 | Security |
| 5.2 | Network Partition | P1 | None | 400-500 | Chaos |
| 5.3 | Crash During TX | P1 | None | 300-400 | Resilience |
| 5.4 | Corrupted Messages | P2 | None | 200-300 | Error handling |
| 5.5 | ULID Time Attacks | P2 | None | 150-250 | Security |
| 6.1 | Schema Migrations | P1 | None | 300-400 | Upgrade path |
| 6.2 | Backwards Compat | P1 | None | 300-400 | Deployment |
| 6.3 | Dual-Path (gRPC+JS) | P1 | None | 300-400 | Migration |
| 6.4 | sensd Removal | P2 | None | 200-300 | Cleanup |

**Total Estimated New Test Code: 8,000-11,000 lines**

---

## Recommended Implementation Order

### Phase A: Critical Path (Blocks Production Readiness)
1. **Week 1-2**: Implement gaps 1.1, 1.2, 1.3, 1.4, 1.5, 1.6
2. **Week 2-3**: Implement gaps 2.1 (E2E satellite test)
3. **Week 3**: Implement gaps 3.1-3.5 (error paths)

### Phase B: Stability (Required Before Deleting gRPC)
4. **Week 4**: Implement gaps 2.2, 2.3 (automaton, pool)
5. **Week 4-5**: Implement gaps 4.1 (high-throughput load)
6. **Week 5**: Implement gaps 5.1-5.4 (security/chaos)

### Phase C: Upgrade Path (Required Before Production Cutover)
7. **Week 6**: Implement gaps 6.1-6.4 (migrations, compat)
8. **Week 6-7**: Property tests and remaining P2 gaps

### Phase D: Polish
9. **Week 7-8**: Documentation, CI/CD integration, performance benchmarking

---

## Recommended Test Infrastructure Improvements

### 1. EphemeralNats Test Fixture
```rust
pub struct EphemeralNats {
    server: NatsServer,
    client: async_nats::Client,
}

impl EphemeralNats {
    pub async fn start() -> Result<Self> { ... }
    pub async fn connect(&self) -> Result<Client> { ... }
    pub async fn get_stream(&self, name: &str) -> Result<Stream> { ... }
}
```

### 2. Test Satellite Publisher
```rust
pub struct TestSatellitePublisher {
    js: jetstream::Context,
    source: String,
}

impl TestSatellitePublisher {
    pub async fn publish_event(&self, event_type: &str, payload: JsonValue) -> Result<String> { ... }
    pub async fn publish_material_stream(&self, slices: Vec<&[u8]>) -> Result<String> { ... }
}
```

### 3. Chaos Injection Utilities
```rust
pub struct ChaosIngestor {
    failure_rate: f64,
    latency: Duration,
}

impl ChaosIngestor {
    pub async fn inject_failures<F, T>(&self, operation: F) -> Result<T> { ... }
}
```

### 4. Observability Snapshot
```rust
pub struct TestSnapshot {
    db_event_count: u64,
    jetstream_message_count: u64,
    outbox_pending_count: u64,
    dlq_entry_count: u64,
    errors: Vec<String>,
}
```

---

## Property Test Recommendations

### Invariants to Verify with proptest
1. **Idempotency**: N duplicates of message → 1 database entry
2. **Ordering**: Events published in order → seen in order (within source)
3. **Provenance Validity**: Every event → provenance references valid sources
4. **No Corruption**: Payload in → payload out, byte-for-byte
5. **XOR Provenance**: Never both Material AND Synthesis
6. **Ledger Monotonicity**: offset_start < offset_end always
7. **ULID Uniqueness**: 1M random ULIDs → no collisions
8. **Hash Invariance**: Random slice order → same final hash
9. **Confirmation Delivery**: Event persisted → confirmation guaranteed
10. **DLQ Preservation**: Error event → DLQ entry preserves original payload

### Generators Needed
- `arb_valid_event()`: Generate valid Event<JsonValue>
- `arb_event_batches(1..1000)`: Generate variable-size batches
- `arb_network_delay(0..500ms)`: Simulate latency
- `arb_jetstream_failure()`: Simulate NATS failures
- `arb_database_failure()`: Simulate DB failures
- `arb_malicious_payload()`: XSS, injection, oversized
- `arb_valid_provenance()`: Either Material OR Synthesis, not both
- `arb_ulid_collision()`: Generate colliding ULIDs for edge case

---

## Checklist for Test Coverage

### Before Phase 1 Landing
- [ ] Events consumer loop test (happy path + restart)
- [ ] Idempotency test (duplicate Msg-Id)
- [ ] Confirmation publishing test
- [ ] Outbox → NATS path test (already have, but enable it)
- [ ] DLQ entry for validation failure
- [ ] E2E single satellite test
- [ ] Performance bench (5K evt/sec for 60s)

### Before Phase 2 Landing
- [ ] Confirmation-aware automaton consumer
- [ ] Dual confirmation consumer (confirmed + provisional)
- [ ] Automaton crash recovery
- [ ] Connection pool exhaustion test
- [ ] NATS unavailability handling

### Before Phase 3 Landing
- [ ] Material assembler slicing/hashing
- [ ] Material rotation on size boundary
- [ ] Ledger entry creation
- [ ] Hash mismatch detection
- [ ] Concurrent material isolation
- [ ] Large material stream (1000 slices)

### Before sensd Removal
- [ ] Dual-path (gRPC + JetStream) simultaneous
- [ ] sensd table drop safety
- [ ] Backwards compatibility (old sat + new ingestd)

### Before Production Deployment
- [ ] All P0 gaps closed
- [ ] Chaos scenarios passing (network partition, service crash, corrupted messages)
- [ ] Load test: 10K evt/sec sustained
- [ ] Upgrade path tested
- [ ] Observability metrics validated

---

## Success Metrics

| Metric | Target | Current | Gap |
|--------|--------|---------|-----|
| Test coverage (LOC) | 8000+ | ~6344 | 1656+ |
| JetStream-specific tests | 50+ | ~3 | 47+ |
| E2E integration tests | 10+ | ~1 | 9+ |
| Chaos/adversarial scenarios | 30+ | ~10 | 20+ |
| Property test invariants | 10+ | ~5 | 5+ |
| Error path coverage | 20+ | ~5 | 15+ |
| Load test suites | 5+ | ~1 | 4+ |
| P0 gaps closed | 100% | ~0% | 100% |

---

## References

- **way.md**: JetStream migration phases and acceptance criteria
- **service.rs**: Current outbox processor implementation
- **jetstream_performance_test.rs**: Existing performance baseline
- **stage_as_you_go_integration_test.rs**: Current E2E pattern
- **error_testing.rs**: Error assertion utilities (reuse for new tests)
- **critical_failure_modes_test.rs**: Existing chaos testing patterns
